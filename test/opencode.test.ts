import { expect, it } from "@effect/vitest";
import { SessionInbox } from "@opencode/schema/session-inbox";
import { Context, Effect, Layer, Queue, Schema, Stream } from "effect";
import { Options, type Queued } from "../src/domain.ts";
import { type Native, OpenCode } from "../src/opencode.ts";
import { settings } from "./settings.ts";
import { incoming, result, sessionA } from "./support.ts";

it.effect(
  "calls native decisions directly, preserves the result, and queues the entire payload without creating sessions",
  () =>
    Effect.gen(function* () {
      const prompts = yield* Queue.unbounded<Parameters<Native["session"]["prompt"]>[0]>();
      const decisions =
        yield* Queue.unbounded<Parameters<Native["experimental"]["generate"]["decisions"]>[0]>();
      const lookups = yield* Queue.unbounded<Parameters<Native["session"]["get"]>[0]>();
      const nativeResult = result();
      let creates = 0;
      const session = {
        get: (input: Parameters<Native["session"]["get"]>[0]) =>
          Queue.offer(lookups, input).pipe(Effect.andThen(Effect.fail("fixture missing session"))),
        prompt: (input: Parameters<Native["session"]["prompt"]>[0]) =>
          Queue.offer(prompts, input).pipe(
            Effect.as(
              Schema.decodeUnknownSync(SessionInbox.User)({
                id: "msg_test",
                sessionID: sessionA,
                type: "user",
                payload: { text: input.text },
                delivery: "queue",
                time: { created: 0 },
              }),
            ),
          ),
        create: () =>
          Effect.sync(() => {
            creates++;
          }),
      };
      const ctx: Native = {
        session,
        experimental: {
          generate: { decisions: (request) => Queue.offer(decisions, request).pipe(Effect.as(nativeResult)) },
        },
        event: { subscribe: () => Stream.empty },
      };
      const context = yield* Layer.build(OpenCode.layer(ctx));
      const adapter = Context.get(context, OpenCode);
      expect(yield* adapter.requireSession(sessionA).pipe(Effect.flip)).toBe("fixture missing session");
      expect(yield* Queue.take(lookups)).toEqual({ sessionID: sessionA });
      const options = yield* Schema.decodeUnknownEffect(Options)(settings);
      expect(yield* adapter.decide(options.decisions)).toBe(nativeResult);
      expect(yield* Queue.take(decisions)).toBe(options.decisions);
      const message = incoming();
      const payload: Queued = {
        context: {
          root: message.root,
          incoming: [message.message],
          history: { messages: [], hasMore: true },
        },
        result: nativeResult,
      };
      yield* adapter.queue(sessionA, payload);
      expect(yield* Queue.take(prompts)).toEqual({
        sessionID: sessionA,
        text: JSON.stringify(payload),
        delivery: "queue",
      });
      expect(creates).toBe(0);
    }),
);
