import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { TrimmedString } from "./baseSchemas.ts";
import { makeProviderSettingsSchema } from "./settings.ts";

export const IronClawSettings = makeProviderSettingsSchema(
  {
    enabled: Schema.Boolean.pipe(
      Schema.withDecodingDefault(Effect.succeed(true)),
      Schema.annotateKey({ providerSettingsForm: { hidden: true } }),
    ),
    serverUrl: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("http://127.0.0.1:3000")),
      Schema.annotateKey({
        title: "Server URL",
        description: "URL of the IronClaw agent server.",
        providerSettingsForm: {
          placeholder: "http://127.0.0.1:3000",
          clearWhenEmpty: "omit",
        },
      }),
    ),
    apiToken: TrimmedString.pipe(
      Schema.withDecodingDefault(Effect.succeed("")),
      Schema.annotateKey({
        title: "API Token",
        description: "Optional API token for authenticating with the IronClaw server.",
        providerSettingsForm: {
          control: "password",
          placeholder: "Optional",
          clearWhenEmpty: "omit",
        },
      }),
    ),
  },
  {
    order: ["serverUrl", "apiToken"],
  },
);
export type IronClawSettings = typeof IronClawSettings.Type;

export const decodeIronClawSettings = Schema.decodeSync(IronClawSettings);
