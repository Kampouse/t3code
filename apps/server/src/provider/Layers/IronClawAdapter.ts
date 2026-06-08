import {
  EventId,
  ProviderDriverKind,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderSendTurnInput,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Hub from "effect/Hub";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import type { IronClawSettings } from "@t3tools/contracts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";

const PROVIDER = ProviderDriverKind.make("ironclaw");

interface IronClawSession {
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly active: boolean;
}

function makeEventId(): EventId {
  return EventId.make(crypto.randomUUID());
}

function makeRuntimeEvent(
  partial: Omit<ProviderRuntimeEvent, "provider" | "providerInstanceId">,
  providerInstanceId?: string,
): ProviderRuntimeEvent {
  return {
    ...partial,
    provider: PROVIDER,
    ...(providerInstanceId ? { providerInstanceId } : {}),
  } as ProviderRuntimeEvent;
}

export const makeIronClawAdapter = (
  ironClawSettings: IronClawSettings,
  instanceId: string,
): Effect.Effect<ProviderAdapterShape<ProviderAdapterError>, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const sessions = new Map<string, IronClawSession>();
    const eventHub = yield* Hub.unbounded<ProviderRuntimeEvent>();

    const publishEvent = (event: ProviderRuntimeEvent) =>
      Hub.publish(eventHub, event);

    const baseUrl = ironClawSettings.serverUrl.replace(/\/+$/, "");

    const authHeaders: Record<string, string> = {};
    if (ironClawSettings.apiToken) {
      authHeaders["Authorization"] = `Bearer ${ironClawSettings.apiToken}`;
    }

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] =
      (input: ProviderSessionStartInput) =>
        Effect.gen(function* () {
          const threadId = input.threadId;
          const session: IronClawSession = {
            threadId,
            turnId: undefined,
            active: true,
          };
          sessions.set(threadId, session);

          const now = yield* DateTime.now;
          yield* publishEvent(
            makeRuntimeEvent({
              type: "session.started",
              eventId: makeEventId(),
              threadId,
              createdAt: DateTime.formatIso(now),
              payload: {},
            }),
          );
          yield* publishEvent(
            makeRuntimeEvent({
              type: "session.state.changed",
              eventId: makeEventId(),
              threadId,
              createdAt: DateTime.formatIso(now),
              payload: { state: "ready" },
            }),
          );

          const providerSession: ProviderSession = {
            threadId,
            state: "ready",
          };
          return providerSession;
        }).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: "ironclaw",
                method: "startSession",
                detail: `Failed to start IronClaw session: ${cause}`,
                cause,
              }),
          ),
        );

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] =
      (input: ProviderSendTurnInput) =>
        Effect.gen(function* () {
          const threadId = input.threadId;
          const session = sessions.get(threadId);
          if (!session || !session.active) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: "ironclaw",
              threadId,
            });
          }

          const turnId = TurnId.make(crypto.randomUUID());
          session.turnId = turnId;

          const now = yield* DateTime.now;
          const createdAt = DateTime.formatIso(now);

          // Emit turn started
          yield* publishEvent(
            makeRuntimeEvent({
              type: "turn.started",
              eventId: makeEventId(),
              threadId,
              createdAt,
              turnId,
              payload: {},
            }),
          );

          // Send message to IronClaw and stream response
          const messageContent =
            typeof input.content === "string"
              ? input.content
              : input.content
                ? JSON.stringify(input.content)
                : "";

          const url = new URL("/prompt", baseUrl).toString();
          const body = JSON.stringify({ message: messageContent });

          const response = yield* httpClient
            .post(url, {
              body,
              headers: {
                "Content-Type": "application/json",
                ...authHeaders,
              },
            })
            .pipe(
              Effect.mapError(
                (cause) =>
                  new ProviderAdapterRequestError({
                    provider: "ironclaw",
                    method: "sendTurn",
                    detail: `Failed to connect to IronClaw: ${cause}`,
                    cause,
                  }),
              ),
            );

          // Stream the SSE response
          const stream = response.stream;
          yield* stream.pipe(
            Stream.decodeText(),
            Stream.splitLines,
            Stream.map((line) => line.trim()),
            Stream.filter((line) => line.startsWith("data: ")),
            Stream.map((line) => line.slice(6)),
            Stream.runForEach((data) =>
              Effect.gen(function* () {
                if (data === "[DONE]") {
                  const completedAt = DateTime.formatIso(yield* DateTime.now);
                  yield* publishEvent(
                    makeRuntimeEvent({
                      type: "turn.completed",
                      eventId: makeEventId(),
                      threadId,
                      createdAt: completedAt,
                      turnId,
                      payload: {},
                    }),
                  );
                  return;
                }

                let parsed: Record<string, unknown>;
                try {
                  parsed = JSON.parse(data);
                } catch {
                  return;
                }

                const choices = parsed.choices;
                if (!Array.isArray(choices) || choices.length === 0) return;

                const delta = (choices[0] as Record<string, unknown>)?.delta;
                if (!delta || typeof delta !== "object") return;

                const content = (delta as Record<string, unknown>).content;
                if (typeof content !== "string" || content.length === 0) return;

                const deltaAt = DateTime.formatIso(yield* DateTime.now);
                yield* publishEvent(
                  makeRuntimeEvent({
                    type: "content.delta",
                    eventId: makeEventId(),
                    threadId,
                    createdAt: deltaAt,
                    turnId,
                    payload: {
                      streamKind: "assistant_text",
                      delta: content,
                    },
                  }),
                );
              }),
            ),
          ).pipe(
            Effect.mapError(
              (cause) =>
                new ProviderAdapterRequestError({
                  provider: "ironclaw",
                  method: "sendTurn",
                  detail: `IronClaw stream error: ${cause}`,
                  cause,
                }),
            ),
          );

          const result: ProviderTurnStartResult = { turnId };
          return result;
        });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] =
      (_threadId: ThreadId, _turnId?: TurnId) =>
        Effect.gen(function* () {
          // IronClaw doesn't have a dedicated interrupt endpoint;
          // aborting the HTTP connection achieves this via scope cleanup.
        });

    const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] =
      (_threadId, _requestId, _decision) =>
        Effect.succeed(undefined);

    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] =
      (_threadId, _requestId, _answers) =>
        Effect.succeed(undefined);

    const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] =
      (threadId: ThreadId) =>
        Effect.gen(function* () {
          const session = sessions.get(threadId);
          if (session) {
            session.active = false;
            sessions.delete(threadId);
            const now = DateTime.formatIso(yield* DateTime.now);
            yield* publishEvent(
              makeRuntimeEvent({
                type: "session.state.changed",
                eventId: makeEventId(),
                threadId,
                createdAt: now,
                payload: { state: "stopped" },
              }),
            );
            yield* publishEvent(
              makeRuntimeEvent({
                type: "session.exited",
                eventId: makeEventId(),
                threadId,
                createdAt: now,
                payload: {},
              }),
            );
          }
        });

    const listSessions: ProviderAdapterShape<ProviderAdapterError>["listSessions"] = () =>
      Effect.succeed(
        Array.from(sessions.values())
          .filter((s) => s.active)
          .map((s) => ({ threadId: s.threadId, state: "ready" as const })),
      );

    const hasSession: ProviderAdapterShape<ProviderAdapterError>["hasSession"] =
      (threadId: ThreadId) =>
        Effect.succeed(sessions.has(threadId) && (sessions.get(threadId)?.active ?? false));

    const readThread = (_threadId: ThreadId) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: "ironclaw",
          method: "readThread",
          detail: "IronClaw does not support thread snapshots.",
        }),
      );

    const rollbackThread = (_threadId: ThreadId, _numTurns: number) =>
      Effect.fail(
        new ProviderAdapterRequestError({
          provider: "ironclaw",
          method: "rollbackThread",
          detail: "IronClaw does not support thread rollback.",
        }),
      );

    const stopAll: ProviderAdapterShape<ProviderAdapterError>["stopAll"] = () =>
      Effect.gen(function* () {
        for (const [threadId, session] of sessions) {
          if (session.active) {
            session.active = false;
            const now = DateTime.formatIso(yield* DateTime.now);
            yield* publishEvent(
              makeRuntimeEvent({
                type: "session.exited",
                eventId: makeEventId(),
                threadId,
                createdAt: now,
                payload: {},
              }),
            );
          }
        }
        sessions.clear();
      });

    const streamEvents: ProviderAdapterShape<ProviderAdapterError>["streamEvents"] =
      Stream.fromHub(eventHub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
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
      streamEvents,
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
