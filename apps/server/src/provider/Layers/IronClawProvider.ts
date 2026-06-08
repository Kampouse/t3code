/**
 * IronClawProvider — health check and snapshot via `GET /api/health`.
 *
 * @module provider/Layers/IronClawProvider
 */

import type { IronClawSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import { ProviderDriverError } from "../Errors.ts";
import { buildServerProvider, type ServerProviderDraft } from "../providerSnapshot.ts";

function checkIronClawHealth(
  settings: IronClawSettings,
): Effect.Effect<ServerProviderDraft, never, never> {
  const serverUrl =
    settings.serverUrl?.replace(/\/+$/, "") || "http://127.0.0.1:3000";
  const apiToken = settings.apiToken || "";

  return Effect.tryPromise({
    try: async () => {
      const headers: Record<string, string> = {
        Accept: "application/json",
      };
      if (apiToken) {
        headers["Authorization"] = `Bearer ${apiToken}`;
      }

      const response = await fetch(`${serverUrl}/api/health`, {
        headers,
        signal: AbortSignal.timeout(4000),
      });

      if (!response.ok) {
        return buildServerProvider({
          presentation: { displayName: "IronClaw" },
          enabled: settings.enabled ?? true,
          checkedAt: new Date().toISOString(),
          models: [],
          probe: {
            installed: false,
            version: null,
            status: "error",
            auth: { authenticated: false },
            message: `Health check returned HTTP ${response.status}`,
          },
        });
      }

      return buildServerProvider({
        presentation: { displayName: "IronClaw" },
        enabled: settings.enabled ?? true,
        checkedAt: new Date().toISOString(),
        models: [],
        probe: {
          installed: true,
          version: null,
          status: "ready",
          auth: { authenticated: true },
        },
      });
    },
    catch: (e) =>
      buildServerProvider({
        presentation: { displayName: "IronClaw" },
        enabled: settings.enabled ?? true,
        checkedAt: new Date().toISOString(),
        models: [],
        probe: {
          installed: false,
          version: null,
          status: "unavailable",
          auth: { authenticated: false },
          message: `IronClaw unavailable: ${e instanceof Error ? e.message : String(e)}`,
        },
      }),
  });
}

function makePendingIronClawProvider(
  _settings: IronClawSettings,
): Effect.Effect<ServerProviderDraft, never, never> {
  return Effect.succeed(
    buildServerProvider({
      presentation: { displayName: "IronClaw" },
      enabled: true,
      checkedAt: new Date().toISOString(),
      models: [],
      probe: {
        installed: false,
        version: null,
        status: "pending",
        auth: { authenticated: false },
      },
    }),
  );
}

export { checkIronClawHealth, makePendingIronClawProvider };
