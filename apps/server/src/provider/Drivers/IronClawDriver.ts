/**
 * IronClawDriver — `ProviderDriver` for the IronClaw Web Gateway.
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
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { ProviderDriverError } from "../Errors.ts";
import { makeIronClawAdapter } from "../Layers/IronClawAdapter.ts";
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

export const IronClawDriver: ProviderDriver<IronClawSettings, ServerConfig> = {
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

      const adapter = yield* makeIronClawAdapter(effectiveConfig, {
        context: "live",
      }).pipe(
        Effect.provideService(ServerConfig, serverConfig),
      );

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
