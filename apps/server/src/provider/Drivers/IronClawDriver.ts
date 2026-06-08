import { IronClawSettings, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import { makeIronClawTextGeneration } from "../../textGeneration/IronClawTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeIronClawAdapter } from "../Layers/IronClawAdapter.ts";
import {
  checkIronClawProviderStatus,
  makePendingIronClawProvider,
} from "../Layers/IronClawProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";

const decodeIronClawSettings = Schema.decodeSync(IronClawSettings);

const DRIVER_KIND = ProviderDriverKind.make("ironclaw");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

export type IronClawDriverEnv = HttpClient.HttpClient;

const withInstanceIdentity =
  (input: {
    readonly instanceId: ProviderInstance["instanceId"];
    readonly displayName: string | undefined;
    readonly accentColor: string | undefined;
    readonly continuationGroupKey: string;
  }) =>
  (snapshot: ServerProviderDraft): ServerProvider => ({
    ...snapshot,
    instanceId: input.instanceId,
    driver: DRIVER_KIND,
    ...(input.displayName ? { displayName: input.displayName } : {}),
    ...(input.accentColor ? { accentColor: input.accentColor } : {}),
    continuation: { groupKey: input.continuationGroupKey },
  });

export const IronClawDriver: ProviderDriver<IronClawSettings, IronClawDriverEnv> = {
  driverKind: DRIVER_KIND,
  metadata: {
    displayName: "IronClaw",
    supportsMultipleInstances: true,
  },
  configSchema: IronClawSettings,
  defaultConfig: (): IronClawSettings => decodeIronClawSettings({}),
  create: ({ instanceId, displayName, accentColor, environment, enabled, config }) =>
    Effect.gen(function* () {
      const httpClient = yield* HttpClient.HttpClient;
      const processEnv = mergeProviderInstanceEnvironment(environment);
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER_KIND,
        instanceId,
      });
      const stampIdentity = withInstanceIdentity({
        instanceId,
        displayName,
        accentColor,
        continuationGroupKey: continuationIdentity.continuationKey,
      });
      const effectiveConfig = { ...config, enabled } satisfies IronClawSettings;

      const adapter = yield* makeIronClawAdapter(effectiveConfig, instanceId);
      const textGeneration = yield* makeIronClawTextGeneration(effectiveConfig);

      const checkProvider = checkIronClawProviderStatus(effectiveConfig).pipe(
        Effect.map(stampIdentity),
      );

      const snapshot = yield* makeManagedServerProvider<IronClawSettings>({
        maintenanceCapabilities: {
          provider: DRIVER_KIND,
          packageName: null,
          update: null,
        },
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: (settings) =>
          makePendingIronClawProvider(settings).pipe(Effect.map(stampIdentity)),
        checkProvider,
        enrichSnapshot: undefined,
        refreshInterval: SNAPSHOT_REFRESH_INTERVAL,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER_KIND,
              instanceId,
              detail: `Failed to build IronClaw snapshot: ${cause.message ?? String(cause)}`,
              cause,
            }),
        ),
      );

      return {
        instanceId,
        driverKind: DRIVER_KIND,
        continuationIdentity,
        displayName,
        accentColor,
        enabled,
        snapshot,
        adapter,
        textGeneration,
      } satisfies ProviderInstance;
    }),
};
