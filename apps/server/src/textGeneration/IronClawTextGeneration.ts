/**
 * IronClawTextGeneration — text generation via IronClaw Web Gateway.
 *
 * Sends messages via `POST /api/chat/send` and streams responses
 * through the SSE event bus (`GET /api/chat/events`).
 *
 * @module textGeneration/IronClawTextGeneration
 */

import type { IronClawSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

export interface IronClawTextGenerationResult {
  readonly content: string;
  readonly inputTokens?: number;
  readonly outputTokens?: number;
}

export interface IronClawTextGeneration {
  readonly generate: (
    content: string,
    threadId?: string,
  ) => Effect.Effect<IronClawTextGenerationResult, Error, HttpClient.HttpClient>;

  readonly generateStream: (
    content: string,
    threadId?: string,
  ) => Stream.Stream<string, Error, HttpClient.HttpClient>;
}

function makeIronClawTextGeneration(
  settings: IronClawSettings,
): Effect.Effect<IronClawTextGeneration, never, never> {
  const serverUrl =
    settings.serverUrl?.replace(/\/+$/, "") || "http://127.0.0.1:3000";
  const apiToken = settings.apiToken || "";

  function authHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (apiToken) {
      headers["Authorization"] = `Bearer ${apiToken}`;
    }
    return headers;
  }

  return Effect.succeed({
    generate(content: string, threadId?: string) {
      return Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;

        const body: Record<string, unknown> = {
          content,
          ...(threadId ? { thread_id: threadId } : {}),
        };

        // Send message (fire-and-forget, returns 202)
        const sendResponse = yield* client
          .execute(
            client.request.post(`${serverUrl}/api/chat/send`).pipe(
              HttpClient.setHeaders(authHeaders()),
              HttpClient.body.json(body),
            ),
          )
          .pipe(Effect.mapError((e) => new Error(`Send failed: ${String(e)}`)));

        const sendResult = yield* Effect.tryPromise(() =>
          sendResponse.json as Promise<{ message_id: string; status: string }>,
        ).pipe(Effect.mapError((e) => new Error(`Parse send response failed: ${String(e)}`)));

        // Poll for completion via the history endpoint
        const messageId = sendResult.message_id;
        let attempts = 0;
        const maxAttempts = 120; // 2 min at 1s intervals
        let lastContent = "";

        while (attempts < maxAttempts) {
          yield* Effect.sleep("1 second");

          const historyResponse = yield* client
            .execute(
              client.request
                .get(
                  `${serverUrl}/api/chat/history${threadId ? `?thread_id=${threadId}` : ""}`,
                )
                .pipe(HttpClient.setHeaders(authHeaders())),
            )
            .pipe(
              Effect.mapError((e) => new Error(`History poll failed: ${String(e)}`)),
            );

          const history = yield* Effect.tryPromise(() =>
            historyResponse.json as Promise<{
              turns: Array<{
                response?: string;
                state: string;
              }>;
            }>,
          ).pipe(
            Effect.mapError((e) => new Error(`Parse history failed: ${String(e)}`)),
          );

          // Find the latest turn with a response
          const latestTurn = history.turns?.[history.turns.length - 1];
          if (latestTurn?.response) {
            lastContent = latestTurn.response;
          }

          if (latestTurn?.state === "Completed" || latestTurn?.state === "Idle") {
            break;
          }

          attempts++;
        }

        return {
          content: lastContent,
        } satisfies IronClawTextGenerationResult;
      });
    },

    generateStream(content: string, threadId?: string) {
      const body: Record<string, unknown> = {
        content,
        ...(threadId ? { thread_id: threadId } : {}),
      };

      // Stream via SSE — connect to /api/chat/events and send the message
      return Stream.fromEffect(
        Effect.gen(function* () {
          const client = yield* HttpClient.HttpClient;

          // Send the message
          yield* client
            .execute(
              client.request.post(`${serverUrl}/api/chat/send`).pipe(
                HttpClient.setHeaders(authHeaders()),
                HttpClient.body.json(body),
              ),
            )
            .pipe(
              Effect.mapError((e) => new Error(`Send failed: ${String(e)}`)),
            );

          return { client };
        }),
      ).pipe(
        Stream.flatMap(({ client }) =>
          Stream.async<string, Error>((emit) => {
            const url = `${serverUrl}/api/chat/events${apiToken ? `?token=${encodeURIComponent(apiToken)}` : ""}`;
            const abortController = new AbortController();

            fetch(url, {
              headers: { Accept: "text/event-stream" },
              signal: abortController.signal,
            })
              .then(async (response) => {
                if (!response.body) {
                  emit(Effect.fail(new Error("No response body")));
                  return;
                }
                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = "";

                try {
                  while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });

                    // Parse SSE lines
                    const lines = buffer.split("\n");
                    buffer = lines.pop() ?? "";

                    for (const line of lines) {
                      const trimmed = line.trim();
                      if (!trimmed.startsWith("data:")) continue;
                      const jsonStr = trimmed.slice(5).trim();
                      if (!jsonStr || jsonStr === "[DONE]") continue;

                      try {
                        const event = JSON.parse(jsonStr) as { type: string; content?: string; message?: string };

                        switch (event.type) {
                          case "stream_chunk":
                          case "response":
                            if (event.content) {
                              emit(Effect.succeed(event.content));
                            }
                            break;
                          case "thinking":
                            if (event.message) {
                              emit(Effect.succeed(`*${event.message}*\n`));
                            }
                            break;
                          case "tool_started":
                            // Could emit tool name
                            break;
                          case "error":
                            emit(Effect.fail(new Error(event.message ?? "IronClaw error")));
                            return;
                          case "status":
                            if (event.message === "idle" || event.message === "Turn completed") {
                              // Signal end of response
                              emit(Effect.succeed(""));
                              return;
                            }
                            break;
                        }
                      } catch {
                        // Skip unparseable SSE lines
                      }
                    }
                  }
                } catch {
                  // Stream ended
                }

                emit(Effect.fail(new Error("Stream ended")));
              })
              .catch((e) => {
                emit(Effect.fail(new Error(`SSE connection failed: ${String(e)}`)));
              });

            return Effect.sync(() => {
              abortController.abort();
            });
          }),
        ),
      );
    },
  } satisfies IronClawTextGeneration);
}

export { makeIronClawTextGeneration };
