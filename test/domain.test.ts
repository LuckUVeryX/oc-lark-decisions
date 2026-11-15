import { expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { Options } from "../src/domain.ts";

import { settings } from "./settings.ts";

it.effect("decodes the complete native request and defaults without normalizing the threshold", () =>
  Effect.gen(function* () {
    const options = yield* Schema.decodeUnknownEffect(Options)(settings);
    expect(options.debounceMs).toBe(15_000);
    expect(options.maxWorkers).toBe(4);
    expect(options.decisions).toEqual(settings.decisions);
    expect(options.forwarding.threshold).toBe(1.4);
  }),
);

it.effect("rejects missing rules, invalid limits, and a non-score forwarding question", () =>
  Effect.gen(function* () {
    for (const invalid of [
      {},
      { decisions: settings.decisions },
      { ...settings, maxWorkers: 0 },
      { ...settings, maxWorkers: 1.5 },
      { ...settings, debounceMs: -1 },
      { ...settings, forwarding: { question: "absent", threshold: 1 } },
      { ...settings, forwarding: { question: "attention", threshold: Infinity } },
    ]) {
      expect(yield* Schema.decodeUnknownEffect(Options)(invalid).pipe(Effect.flip)).toBeDefined();
    }
  }),
);
