import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";

import {
  TextGenerationError,
  type IronClawSettings,
} from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import type { TextGenerationShape } from "./TextGeneration.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

function parseContentFromSSE(rawText: string): string {
  let result = "";
  for (const line of rawText.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed.startsWith("data: ")) continue;
    const data = trimmed.slice(6);
    if (data === "[DONE]") break;
    try {
      const parsed = JSON.parse(data);
      const choices = parsed.choices;
      if (!Array.isArray(choices) || choices.length === 0) continue;
      const delta = choices[0]?.delta;
      if (delta?.content && typeof delta.content === "string") {
        result += delta.content;
      }
    } catch {
      // skip unparseable lines
    }
  }
  return result.trim();
}

export const makeIronClawTextGeneration = (
  ironClawSettings: IronClawSettings,
): Effect.Effect<TextGenerationShape, never, HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const httpClient = yield* HttpClient.HttpClient;
    const baseUrl = ironClawSettings.serverUrl.replace(/\/+$/, "");

    const authHeaders: Record<string, string> = {};
    if (ironClawSettings.apiToken) {
      authHeaders["Authorization"] = `Bearer ${ironClawSettings.apiToken}`;
    }

    const promptIronClaw = (
      operation: string,
      prompt: string,
    ): Effect.Effect<string, TextGenerationError> =>
      Effect.gen(function* () {
        const url = new URL("/prompt", baseUrl).toString();
        const response = yield* httpClient
          .post(url, {
            body: JSON.stringify({ message: prompt }),
            headers: {
              "Content-Type": "application/json",
              ...authHeaders,
            },
          })
          .pipe(
            Effect.mapError(
              (cause) =>
                new TextGenerationError({
                  operation,
                  detail: `Failed to connect to IronClaw: ${cause}`,
                  cause,
                }),
            ),
          );

        if (response.status !== 200) {
          return yield* new TextGenerationError({
            operation,
            detail: `IronClaw returned HTTP ${response.status}`,
          });
        }

        const rawText = yield* response.text.pipe(
          Effect.mapError(
            (cause) =>
              new TextGenerationError({
                operation,
                detail: `Failed to read IronClaw response: ${cause}`,
                cause,
              }),
          ),
        );

        // The response may be SSE or plain JSON — handle both
        const content = rawText.includes("data: ")
          ? parseContentFromSSE(rawText)
          : (() => {
              try {
                const parsed = JSON.parse(rawText);
                if (typeof parsed === "string") return parsed;
                if (parsed.content && typeof parsed.content === "string") return parsed.content;
                return rawText;
              } catch {
                return rawText;
              }
            })();

        if (content.length === 0) {
          return yield* new TextGenerationError({
            operation,
            detail: "IronClaw returned empty output.",
          });
        }

        return content;
      });

    const generateCommitMessage: TextGenerationShape["generateCommitMessage"] = Effect.fn(
      "IronClawTextGeneration.generateCommitMessage",
    )(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
      });
      const rawOutput = yield* promptIronClaw("generateCommitMessage", prompt);
      const generated = yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
        extractJsonObject(rawOutput),
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateCommitMessage",
              detail: "IronClaw returned invalid structured output.",
              cause,
            }),
          ),
        ),
      );
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

    const generatePrContent: TextGenerationShape["generatePrContent"] = Effect.fn(
      "IronClawTextGeneration.generatePrContent",
    )(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
      });
      const rawOutput = yield* promptIronClaw("generatePrContent", prompt);
      const generated = yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
        extractJsonObject(rawOutput),
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: "generatePrContent",
              detail: "IronClaw returned invalid structured output.",
              cause,
            }),
          ),
        ),
      );
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

    const generateBranchName: TextGenerationShape["generateBranchName"] = Effect.fn(
      "IronClawTextGeneration.generateBranchName",
    )(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const rawOutput = yield* promptIronClaw("generateBranchName", prompt);
      const generated = yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
        extractJsonObject(rawOutput),
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateBranchName",
              detail: "IronClaw returned invalid structured output.",
              cause,
            }),
          ),
        ),
      );
      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

    const generateThreadTitle: TextGenerationShape["generateThreadTitle"] = Effect.fn(
      "IronClawTextGeneration.generateThreadTitle",
    )(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const rawOutput = yield* promptIronClaw("generateThreadTitle", prompt);
      const generated = yield* Schema.decodeEffect(Schema.fromJsonString(outputSchema))(
        extractJsonObject(rawOutput),
      ).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateThreadTitle",
              detail: "IronClaw returned invalid structured output.",
              cause,
            }),
          ),
        ),
      );
      return {
        title: sanitizeThreadTitle(generated.title),
      };
    });

    return {
      generateCommitMessage,
      generatePrContent,
      generateBranchName,
      generateThreadTitle,
    } satisfies TextGenerationShape;
  });
