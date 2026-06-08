/**
 * IronClawProvider — health check and snapshot via `GET /api/health`.
 *
 * @module provider/Layers/IronClawProvider
 */

import {
  type IronClawSettings,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import { ServerConfig } from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";

const DRIVER = ProviderDriverKind.make("ironclaw");

function checkIronClawHealth(
  settings: IronClawSettings,
): Effect.Effect<ServerProviderDraft, Error, HttpClient.HttpClient> {
  const serverUrl =
    settings.serverUrl?.replace(/\/+$/, "") || "http://127.0.0.1:3000";
  const apiToken = settings.apiToken || "";

  return Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;

    const headers: Record<string, string> = {
      Accept: "application/json",
    };
    if (apiToken) {
      headers["Authorization"] = `Bearer ${apiToken}`;
    }

    const response = yield* client
      .execute(
        client.request.get(`${serverUrl}/api/health`).pipe(
          HttpClient.setHeaders(headers),
        ),
      )
      .pipe(
        Effect.tap((resp) =>
          Effect.logDebug(
            `[IronClaw] Health check: ${resp.status} from ${serverUrl}/api/health`,
          ),
        ),
        Effect.mapError(
          (e) =>
            new Error(`IronClaw health check failed (${serverUrl}): ${String(e)}`),
        ),
      );

    if (response.status >= 400) {
      yield* Effect.logWarning(
        `[IronClaw] Health check returned ${response.status}`,
      );
      return {
        state: "error" as const,
        error: Option.some(
          new ProviderDriverError({
            driver: DRIVER,
            instanceId: "ironclaw" as any,
            detail: `Health check returned HTTP ${response.status}`,
          }),
        ),
      } satisfies ServerProviderDraft;
    }

    const body = yield* Effect.tryPromise(() =>
      response.json as Promise<{
        status?: string;
        channel?: string;
      }>,
    ).pipe(
      Effect.mapError(
        (e) => new Error(`Failed to parse health response: ${String(e)}`),
      ),
    );

    yield* Effect.logDebug(
      `[IronClaw] Health OK: status=${body.status}, channel=${body.channel}`,
    );

    return {
      state: "ready" as const,
      capabilities: {
        streaming: true,
        tools: true,
        threads: true,
        memory: true,
        jobs: true,
      },
    } satisfies ServerProviderDraft;
  }).pipe(
    Effect.catchAll((e) =>
      Effect.succeed({
        state: "unavailable" as const,
        error: Option.some(
          new ProviderDriverError({
            driver: DRIVER,
            instanceId: "ironclaw" as any,
            detail: `IronClaw unavailable: ${e instanceof Error ? e.message : String(e)}`,
            cause: e,
          }),
        ),
      } satisfies ServerProviderDraft),
    ),
  );
}

function makePendingIronClawProvider(
  _settings: IronClawSettings,
): Effect.Effect<ServerProviderDraft, never, never> {
  return Effect.succeed({
    state: "pending",
  } satisfies ServerProviderDraft);
}

export { checkIronClawHealth, makePendingIronClawProvider };
