/**
 * IronClawAdapter — minimal adapter for the IronClaw Web Gateway.
 *
 * Implements `ProviderAdapterShape` from the T3 Code driver SPI.
 * Currently supports basic chat via `POST /api/chat/send` with
 * SSE responses via `GET /api/chat/events`.
 *
 * @module provider/Layers/IronClawAdapter
 */

import type { IronClawSettings } from "@t3tools/contracts";
import {
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeItemId,
  EventId,
  TurnId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { ProviderDriverError } from "../Errors.ts";

const DRIVER = ProviderDriverKind.make("ironclaw");

function parseSseLines(chunk: string): Array<{ type: string; content?: string; message?: string }> {
  const events: Array<{ type: string; content?: string; message?: string }> = [];
  for (const line of chunk.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("data:")) continue;
    const jsonStr = trimmed.slice(5).trim();
    if (!jsonStr || jsonStr === "[DONE]") continue;
    try {
      const parsed = JSON.parse(jsonStr);
      if (parsed && typeof parsed === "object" && "type" in parsed) {
        events.push(parsed);
      }
    } catch {
      // skip
    }
  }
  return events;
}

export function makeIronClawAdapter(
  settings: IronClawSettings,
  _input: { readonly context: "test" | "live" },
): Effect.Effect<
  ProviderAdapterShape<ProviderDriverError>,
  never,
  Scope.Scope | ServerConfig
> {
  return Effect.gen(function* () {
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<string, { abortController: AbortController }>();

    const serverUrl =
      settings.serverUrl?.replace(/\/+$/, "") || "http://127.0.0.1:3000";
    const apiToken = settings.apiToken || "";

    function authHeaders(): Record<string, string> {
      const h: Record<string, string> = { "Content-Type": "application/json" };
      if (apiToken) h["Authorization"] = `Bearer ${apiToken}`;
      return h;
    }

    // ── SSE subscription per session ──────────────────────────────
    function subscribeSSE(sessionId: string) {
      const ctx = sessions.get(sessionId);
      if (!ctx) return;

      const url = `${serverUrl}/api/chat/events${apiToken ? `?token=${encodeURIComponent(apiToken)}` : ""}`;
      fetch(url, {
        headers: { Accept: "text/event-stream" },
        signal: ctx.abortController.signal,
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
              for (const event of parseSseLines(text)) {
                const runtimeEvent = mapEvent(event);
                if (runtimeEvent) {
                  await Effect.runPromise(Queue.offer(runtimeEvents, runtimeEvent));
                }
              }
            }
          } catch {
            // stream ended
          }
        })
        .catch(() => {});
    }

    function mapEvent(
      event: { type: string; content?: string; message?: string },
    ): ProviderRuntimeEvent | null {
      switch (event.type) {
        case "stream_chunk":
        case "response":
          return {
            _tag: "content.delta",
            streamKind: "assistant_text",
            content: event.content ?? "",
            turnId: RuntimeItemId.make(EventId.make("")),
          } as unknown as ProviderRuntimeEvent;
        case "error":
          return {
            _tag: "runtime.error",
            error: new Error(event.message ?? "IronClaw error"),
            turnId: RuntimeItemId.make(EventId.make("")),
          } as unknown as ProviderRuntimeEvent;
        default:
          return null;
      }
    }

    // ── Adapter shape ─────────────────────────────────────────────

    const startSession = (_input: ProviderSessionStartInput) =>
      Effect.gen(function* () {
        const sessionId = `ic-${Date.now()}`;
        const abortController = new AbortController();
        sessions.set(sessionId, { abortController });
        subscribeSSE(sessionId);
        return { id: sessionId } as ProviderSession;
      }).pipe(
        Effect.mapError(
          (e) =>
            new ProviderDriverError({
              provider: DRIVER,
              detail: `Failed to start session: ${String(e)}`,
            }),
        ),
      );

    const sendTurn = (input: ProviderSendTurnInput) =>
      Effect.gen(function* () {
        const sessionId = input.sessionId;
        const ctx = sessions.get(sessionId);
        if (!ctx) {
          return yield* new ProviderDriverError({
            provider: DRIVER,
            detail: `Session not found: ${sessionId}`,
          });
        }

        const body = { content: input.content, thread_id: sessionId };
        const response = yield* Effect.tryPromise(() =>
          fetch(`${serverUrl}/api/chat/send`, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(body),
          }),
        ).pipe(
          Effect.mapError(
            (e) =>
              new ProviderDriverError({
                provider: DRIVER,
                detail: `Send failed: ${String(e)}`,
              }),
          ),
        );

        const result = yield* Effect.tryPromise(() =>
          response.json() as Promise<{ message_id: string }>,
        );

        return {
          turnId: result.message_id ?? "unknown",
        } as ProviderTurnStartResult;
      });

    const interruptTurn = (_sessionId: string) =>
      Effect.succeed(undefined);

    const respondToRequest = () =>
      Effect.succeed(undefined);

    const respondToUserInput = () =>
      Effect.succeed(undefined);

    const stopSession = (sessionId: string) =>
      Effect.sync(() => {
        const ctx = sessions.get(sessionId);
        if (ctx) {
          ctx.abortController.abort();
          sessions.delete(sessionId);
        }
      });

    const listSessions = () =>
      Effect.succeed([...sessions.keys()]);

    const hasSession = (sessionId: string) =>
      Effect.succeed(sessions.has(sessionId));

    const readThread = () =>
      Effect.succeed(undefined);

    const rollbackThread = () =>
      Effect.succeed(undefined);

    const stopAll = () =>
      Effect.sync(() => {
        for (const [, ctx] of sessions) {
          ctx.abortController.abort();
        }
        sessions.clear();
      });

    return {
      provider: DRIVER,
      capabilities: {},
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies ProviderAdapterShape<ProviderDriverError>;
  });
}
