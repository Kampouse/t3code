import {
  ProviderDriverKind,
  type IronClawSettings,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import { HttpClient } from "effect/unstable/http";

import { createModelCapabilities } from "@t3tools/shared/model";
import {
  buildServerProvider,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PROVIDER = ProviderDriverKind.make("ironclaw");
const IRONCLAW_PRESENTATION = {
  displayName: "IronClaw",
  showInteractionModeToggle: false,
} as const;

const DEFAULT_IRONCLAW_MODEL_CAPABILITIES = createModelCapabilities({
  optionDescriptors: [],
});

export const makePendingIronClawProvider = (
  ironClawSettings: IronClawSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models: ReadonlyArray<ServerProviderModel> = providerModelsFromSettings(
      [],
      PROVIDER,
      [],
      DEFAULT_IRONCLAW_MODEL_CAPABILITIES,
    );

    if (!ironClawSettings.enabled) {
      return buildServerProvider({
        presentation: IRONCLAW_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "IronClaw is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: IRONCLAW_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "IronClaw provider status has not been checked in this session yet.",
      },
    });
  });

export const checkIronClawProviderStatus = (
  ironClawSettings: IronClawSettings,
): Effect.Effect<ServerProviderDraft, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const checkedAt = DateTime.formatIso(yield* DateTime.now);
    const models = providerModelsFromSettings(
      [],
      PROVIDER,
      [],
      DEFAULT_IRONCLAW_MODEL_CAPABILITIES,
    );

    if (!ironClawSettings.enabled) {
      return buildServerProvider({
        presentation: IRONCLAW_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "IronClaw is disabled in T3 Code settings.",
        },
      });
    }

    const statusExit = yield* Effect.exit(
      Effect.gen(function* () {
        const response = yield* httpClient.get(
          new URL("/status", ironClawSettings.serverUrl).toString(),
        );
        if (response.status !== 200) {
          yield* Effect.fail(
            new Error(`IronClaw /status returned HTTP ${response.status}`),
          );
        }
        return yield* response.json;
      }),
    );

    if (statusExit._tag === "Failure") {
      const cause = statusExit.cause;
      const message =
        cause != null && cause instanceof Error
          ? cause.message
          : "Failed to connect to IronClaw server.";

      const lower = message.toLowerCase();
      let probeMessage: string;
      if (
        lower.includes("econnrefused") ||
        lower.includes("enotfound") ||
        lower.includes("fetch failed")
      ) {
        probeMessage = `Couldn't reach IronClaw server at ${ironClawSettings.serverUrl}. Check that the server is running.`;
      } else {
        probeMessage = message;
      }

      return buildServerProvider({
        presentation: IRONCLAW_PRESENTATION,
        enabled: true,
        checkedAt,
        models,
        probe: {
          installed: true,
          version: null,
          status: "error",
          auth: { status: "unknown" },
          message: probeMessage,
        },
      });
    }

    const statusData = statusExit.value as Record<string, unknown>;
    const version =
      typeof statusData.version === "string" ? statusData.version : null;

    return buildServerProvider({
      presentation: IRONCLAW_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "authenticated" },
        message: `IronClaw agent is running${version ? ` (v${version})` : ""}.`,
      },
    });
  });
