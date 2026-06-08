/**
 * IronClawAdapter — maps IronClaw Web Gateway events to T3 Code's
 * `ProviderRuntimeEvent` types.
 *
 * IronClaw uses a dual-request model:
 *   1. `POST /api/chat/send` — fire-and-forget message submission (202)
 *   2. `GET /api/chat/events` — long-lived SSE stream for all responses
 *
 * The SSE stream carries typed `AppEvent` payloads (tagged JSON).
 * This adapter subscribes to the SSE stream and maps events to T3 Code's
 * canonical runtime event types.
 *
 * @module provider/Layers/IronClawAdapter
 */

import {
  EventId,
  type IronClawSettings,
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  RuntimeRequestId,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
} from "../Errors.ts";

// ─── Constants ─────────────────────────────────────────────────────────

const DRIVER = ProviderDriverKind.make("ironclaw");

// ─── IronClaw AppEvent types (mirrors Rust enum) ──────────────────────

interface IronClawResponseEvent {
  type: "response";
  content: string;
  thread_id: string;
}

interface IronClawStreamChunkEvent {
  type: "stream_chunk";
  content: string;
  thread_id?: string;
}

interface IronClawThinkingEvent {
  type: "thinking";
  message: string;
  thread_id?: string;
}

interface IronClawToolStartedEvent {
  type: "tool_started";
  name: string;
  thread_id?: string;
}

interface IronClawToolCompletedEvent {
  type: "tool_completed";
  name: string;
  success: boolean;
  error?: string;
  parameters?: string;
  thread_id?: string;
}

interface IronClawToolResultEvent {
  type: "tool_result";
  name: string;
  preview: string;
  thread_id?: string;
}

interface IronClawStatusEvent {
  type: "status";
  message: string;
  thread_id?: string;
}

interface IronClawErrorEvent {
  type: "error";
  message: string;
  thread_id?: string;
}

interface IronClawHeartbeatEvent {
  type: "heartbeat";
}

interface IronClawApprovalNeededEvent {
  type: "approval_needed";
  request_id: string;
  tool_name: string;
  description: string;
  parameters: string;
  thread_id?: string;
  allow_always: boolean;
}

interface IronClawJobStartedEvent {
  type: "job_started";
  job_id: string;
  title: string;
  browse_url: string;
}

interface IronClawReasoningUpdateEvent {
  type: "reasoning_update";
  narrative: string;
  decisions: Array<{ tool_name: string; rationale: string }>;
  thread_id?: string;
}

interface IronClawSuggestionsEvent {
  type: "suggestions";
  suggestions: string[];
  thread_id?: string;
}

interface IronClawTurnCostEvent {
  type: "turn_cost";
  input_tokens: number;
  output_tokens: number;
  cost_usd: string;
  thread_id?: string;
}

interface IronClawImageGeneratedEvent {
  type: "image_generated";
  data_url: string;
  path?: string;
  thread_id?: string;
}

type IronClawAppEvent =
  | IronClawResponseEvent
  | IronClawStreamChunkEvent
  | IronClawThinkingEvent
  | IronClawToolStartedEvent
  | IronClawToolCompletedEvent
  | IronClawToolResultEvent
  | IronClawStatusEvent
  | IronClawErrorEvent
  | IronClawHeartbeatEvent
  | IronClawApprovalNeededEvent
  | IronClawJobStartedEvent
  | IronClawReasoningUpdateEvent
  | IronClawSuggestionsEvent
  | IronClawTurnCostEvent
  | IronClawImageGeneratedEvent;

// ─── SSE line parser ──────────────────────────────────────────────────

function parseSseLines(chunk: string): IronClawAppEvent[] {
  const events: IronClawAppEvent[] = [];
  for (const line of chunk.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("data:")) continue;
    const jsonStr = trimmed.slice(5).trim();
    if (!jsonStr || jsonStr === "[DONE]") continue;
    try {
      const parsed = JSON.parse(jsonStr) as IronClawAppEvent;
      if (parsed && typeof parsed === "object" && "type" in parsed) {
        events.push(parsed);
      }
    } catch {
      // Skip unparseable lines
    }
  }
  return events;
}

// ─── Session context ──────────────────────────────────────────────────

interface IronClawSessionContext {
  session: ProviderSession;
  readonly serverUrl: string;
  readonly apiToken: string;
  readonly eventQueue: Queue.Queue<ProviderRuntimeEvent>;
  readonly sseAbortController: AbortController;
  readonly activeTurnId: Ref.Ref<TurnId | undefined>;
  readonly activeThreadId: Ref.Ref<string | undefined>;
  readonly stopped: Ref.Ref<boolean>;
  readonly sessionScope: Scope.Closeable;
}

// ─── Adapter implementation ───────────────────────────────────────────

export interface IronClawAdapterShape {
  readonly startSession: (
    session: ProviderSession,
  ) => Effect.Effect<void, never, Scope.Scope | HttpClient.HttpClient>;

  readonly stopSession: (
    sessionId: string,
  ) => Effect.Effect<void, never, never>;

  readonly startTurn: (
    sessionId: string,
    turnId: TurnId,
  ) => Effect.Effect<void, never, HttpClient.HttpClient>;

  readonly stopTurn: (
    sessionId: string,
  ) => Effect.Effect<void, never, never>;

  readonly sendMessage: (
    sessionId: string,
    content: string,
  ) => Effect.Effect<string, never, HttpClient.HttpClient>;

  readonly events: (
    sessionId: string,
  ) => Stream.Stream<ProviderRuntimeEvent, never, never>;

  readonly stop: () => Effect.Effect<void, never, never>;
}

function makeIronClawAdapterLive(
  settings: IronClawSettings,
): Effect.Effect<
  IronClawAdapterShape,
  never,
  HttpClient.HttpClient | Scope.Scope | ServerConfig
> {
  return Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const sessions = new Map<string, IronClawSessionContext>();

    const serverUrl =
      settings.serverUrl?.replace(/\/+$/, "") || "http://127.0.0.1:3000";
    const apiToken = settings.apiToken || "";

    function authHeaders(): Record<string, string> {
      const headers: Record<string, string> = { "Content-Type": "application/json" };
      if (apiToken) {
        headers["Authorization"] = `Bearer ${apiToken}`;
      }
      return headers;
    }

    function subscribeToSSE(
      ctx: IronClawSessionContext,
    ): Effect.Effect<void, never, never> {
      return Effect.async<void, never>(() => {
        const url = `${ctx.serverUrl}/api/chat/events${apiToken ? `?token=${encodeURIComponent(apiToken)}` : ""}`;

        fetch(url, {
          headers: { Accept: "text/event-stream" },
          signal: ctx.sseAbortController.signal,
        })
          .then(async (response) => {
            if (!response.body) return;
            const reader = response.body.getReader();
            const decoder = new TextDecoder();

            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                const text = decoder.decode(value, { stream: true });
                const events = parseSseLines(text);

                for (const event of events) {
                  const runtimeEvent = mapAppEvent(event, ctx);
                  if (runtimeEvent) {
                    await Effect.runPromise(
                      Queue.offer(ctx.eventQueue, runtimeEvent),
                    );
                  }
                }
              }
            } catch {
              // SSE stream ended or aborted — this is normal
            }
          })
          .catch(() => {
            // Connection failed — will be retried on next turn
          });

        return Effect.sync(() => {
          ctx.sseAbortController.abort();
        });
      });
    }

    // ─── Map IronClaw AppEvent → T3 Code ProviderRuntimeEvent ────

    function mapAppEvent(
      event: IronClawAppEvent,
      _ctx: IronClawSessionContext,
    ): ProviderRuntimeEvent | null {
      switch (event.type) {
        case "stream_chunk":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: event.content,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "response":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: event.content,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "thinking":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `*${event.message}*`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "tool_started":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `🔧 ${event.name}...\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "tool_completed":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: event.success
              ? `✅ ${event.name} done\n`
              : `❌ ${event.name} failed${event.error ? `: ${event.error}` : ""}\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "tool_result":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `📋 ${event.name}: ${event.preview.slice(0, 200)}\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "error":
          return {
            _tag: "runtime.error" as const,
            error: new Error(event.message),
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "status":
          // Status events are informational — we could show as system messages
          return null;

        case "heartbeat":
          // Ignore heartbeats
          return null;

        case "job_started":
          return {
            _tag: "task.started" as const,
            title: event.title,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "approval_needed":
          // Surface approval requests as tool calls needing user action
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `⚠️ Approval needed for **${event.tool_name}**: ${event.description}\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "reasoning_update":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `💭 ${event.narrative}\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        case "suggestions":
          // Could map to UI suggestions — skip for now
          return null;

        case "turn_cost":
          // Could map to token usage display — skip for now
          return null;

        case "image_generated":
          return {
            _tag: "content.delta" as const,
            streamKind: "assistant_text" as const,
            content: `🖼️ Image generated${event.path ? `: ${event.path}` : ""}\n`,
            turnId: RuntimeItemId.make(EventId.make("")),
          } as ProviderRuntimeEvent;

        default:
          return null;
      }
    }

    // ─── Public API ────────────────────────────────────────────────

    return {
      startSession(session: ProviderSession) {
        return Effect.gen(function* () {
          const eventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();
          const abortController = new AbortController();

          const ctx: IronClawSessionContext = {
            session,
            serverUrl,
            apiToken,
            eventQueue,
            sseAbortController: abortController,
            activeTurnId: yield* Ref.make<TurnId | undefined>(undefined),
            activeThreadId: yield* Ref.make<string | undefined>(undefined),
            stopped: yield* Ref.make(false),
            sessionScope: yield* Scope.make(),
          };

          sessions.set(session.id, ctx);

          // Start SSE subscription in background
          yield* Effect.fork(subscribeToSSE(ctx));

          // Emit session started
          yield* Queue.offer(ctx.eventQueue, {
            _tag: "session.state.changed",
            state: "ready",
          } as ProviderRuntimeEvent);
        });
      },

      stopSession(sessionId: string) {
        return Effect.sync(() => {
          const ctx = sessions.get(sessionId);
          if (ctx) {
            ctx.sseAbortController.abort();
            Ref.set(ctx.stopped, true);
            sessions.delete(sessionId);
          }
        });
      },

      startTurn(sessionId: string, turnId: TurnId) {
        return Effect.gen(function* () {
          const ctx = sessions.get(sessionId);
          if (!ctx) return;
          yield* Ref.set(ctx.activeTurnId, turnId);
        });
      },

      stopTurn(sessionId: string) {
        return Effect.gen(function* () {
          const ctx = sessions.get(sessionId);
          if (!ctx) return;
          yield* Ref.set(ctx.activeTurnId, undefined);

          // Emit turn completed
          yield* Queue.offer(ctx.eventQueue, {
            _tag: "turn.completed",
          } as ProviderRuntimeEvent);
        });
      },

      sendMessage(sessionId: string, content: string) {
        return Effect.gen(function* () {
          const ctx = sessions.get(sessionId);
          if (!ctx) {
            return "" as string;
          }

          const threadId = yield* Ref.get(ctx.activeThreadId);

          const body: Record<string, unknown> = {
            content,
            ...(threadId ? { thread_id: threadId } : {}),
          };

          const response = yield* httpClient
            .execute(
              httpClient.request.post(`${serverUrl}/api/chat/send`).pipe(
                HttpClient.setHeaders(authHeaders()),
                HttpClient.body.json(body),
              ),
            )
            .pipe(
              Effect.mapError(
                (e) =>
                  new ProviderAdapterRequestError({
                    driver: DRIVER,
                    sessionId,
                    detail: `Failed to send message: ${String(e)}`,
                    cause: e,
                  }),
              ),
            );

          const result = yield* Effect.tryPromise(() =>
            response.json as Promise<{ message_id: string; status: string }>,
          );

          return result.message_id ?? "";
        });
      },

      events(sessionId: string) {
        return Stream.fromQueue(
          sessions.get(sessionId)?.eventQueue ??
            (() => {
              const q = Effect.runSync(Queue.unbounded<ProviderRuntimeEvent>());
              sessions.set(sessionId, {
                session: { id: sessionId } as ProviderSession,
                serverUrl,
                apiToken,
                eventQueue: q,
                sseAbortController: new AbortController(),
                activeTurnId: Effect.runSync(Ref.make(undefined)),
                activeThreadId: Effect.runSync(Ref.make(undefined)),
                stopped: Effect.runSync(Ref.make(false)),
                sessionScope: Effect.runSync(Scope.make()),
              });
              return q;
            })(),
        );
      },

      stop() {
        return Effect.sync(() => {
          for (const [id, ctx] of sessions) {
            ctx.sseAbortController.abort();
            Ref.set(ctx.stopped, true);
          }
          sessions.clear();
        });
      },
    } satisfies IronClawAdapterShape;
  });
}

export { makeIronClawAdapterLive };
export type { IronClawAdapterShape };
