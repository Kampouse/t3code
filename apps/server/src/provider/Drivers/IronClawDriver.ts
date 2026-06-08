/**
 * IronClawDriver — `ProviderDriver` for the IronClaw Web Gateway.
 *
 * Connects to a running IronClaw instance (local or hosted) via its
 * HTTP Web Gateway API (default `http://127.0.0.1:3000`).
 *
 * No child process management — IronClaw always runs externally.
 *
 * @module provider/Drivers/IronClawDriver
 */

import {
  IronClawSettings,
  ProviderDriverKind,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as HttpClient from "effect/HttpClient";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeIronClawAdapterLive } from "../Layers/IronClawAdapter.ts";
import {
  checkIronClawHealth,
  makePendingIronClawProvider,
} from "../Layers/IronClawProvider.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import {
  defaultProviderContinuationIdentity,
  type ProviderDriver,
  type ProviderInstance,
} from "../ProviderDriver.ts";
import type { ServerProviderDraft } from "../providerSnapshot.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";

const decodeIronClawSettings = Schema.decodeSync(IronClawSettings);

const DRIVER_KIND = ProviderDriverKind.make("ironclaw");
const SNAPSHOT_REFRESH_INTERVAL = Duration.minutes(5);

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

export type IronClawDriverEnv =
  | HttpClient.HttpClient
  | ServerConfig;

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
      const serverConfig = yield* ServerConfig;
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

      // Build adapter
      const adapter = yield* makeIronClawAdapterLive(effectiveConfig).pipe(
        Effect.provideService(HttpClient.HttpClient, httpClient),
        Effect.provideService(ServerConfig, serverConfig),
      );

      // Build snapshot with health check
      const snapshot = yield* makeManagedServerProvider<IronClawSettings>({
        maintenanceCapabilities: {
          canUpdate: false,
          canConfigure: true,
        },
        getSettings: Effect.succeed(effectiveConfig),
        streamSettings: Stream.never,
        haveSettingsChanged: () => false,
        initialSnapshot: () =>
          makePendingIronClawProvider(effectiveConfig).pipe(
            Effect.map(stampIdentity),
          ),
        checkProvider: checkIronClawHealth(effectiveConfig).pipe(
          Effect.map(stampIdentity),
          Effect.provideService(HttpClient.HttpClient, httpClient),
        ),
        enrichSnapshot: ({ snapshot }) => Effect.succeed(snapshot),
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
        textGeneration: {
          generate: () => Effect.die("not implemented — use adapter events"),
          generateStream: () => Stream.die("not implemented — use adapter events"),
        },
      } satisfies ProviderInstance;
    }),
};
