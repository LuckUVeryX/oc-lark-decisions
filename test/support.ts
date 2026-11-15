import { Generate } from "@opencode/schema/generate";
import { Session } from "@opencode/schema/session";
import { Context, Deferred, Effect, Exit, Layer, Queue, Ref, Schema, Scope, Stream } from "effect";
import { Incoming, Options, type Queued, type Root } from "../src/domain.ts";
import { Lark, type LarkError } from "../src/lark.ts";
import { OpenCode } from "../src/opencode.ts";
import { Watcher } from "../src/watcher.ts";
import { settings } from "./settings.ts";

export const sessionA = Session.ID.make("ses_a");
export const sessionB = Session.ID.make("ses_b");
export const incoming = (rootId = "root", id = "message", chatId = "chat", content = id) =>
  Schema.decodeUnknownSync(Incoming)({ root: { chatId, rootId }, message: { id, content } });
export const result = (score = 1.4, confidence = 0.01) =>
  Schema.decodeUnknownSync(Generate.DecisionResult)({
    answers: [
      { type: "score", name: "attention", score, probabilities: [], confidence },
      { type: "error", name: "other", reason: "unsupported", message: "preserve me" },
    ],
    cost: 0.42,
    tokens: { input: 22, output: 11 },
    native: { provider: { opaque: ["keep", 7] } },
  });

export const harness = Effect.fn("Test.harness")(function* (
  overrides: {
    readonly debounceMs?: number;
    readonly maxWorkers?: number;
    readonly history?: Lark["Service"]["history"];
    readonly decide?: OpenCode["Service"]["decide"];
    readonly queue?: OpenCode["Service"]["queue"];
    readonly requireSession?: OpenCode["Service"]["requireSession"];
    readonly startup?: Effect.Effect<void, LarkError>;
  } = {},
) {
  const events = yield* Queue.unbounded<
    {
      readonly incoming: Incoming;
      readonly ack: Deferred.Deferred<void>;
    },
    LarkError
  >();
  const deleted = yield* Queue.unbounded<Session.ID>();
  const histories = yield* Queue.unbounded<Root>();
  const requests = yield* Queue.unbounded<Generate.DecisionRequest>();
  const deliveries = yield* Queue.unbounded<{ readonly id: Session.ID; readonly payload: Queued }>();
  const opened = yield* Ref.make(0);
  const closed = yield* Ref.make(0);
  const ready = yield* Deferred.make<void>();
  const opening = yield* Deferred.make<void>();
  const scope = yield* Scope.fork(yield* Scope.Scope);
  const lark = Layer.succeed(
    Lark,
    Lark.of({
      open: Effect.fn("Test.Lark.open")(function* () {
        yield* Ref.update(opened, (n) => n + 1);
        yield* Effect.addFinalizer(() => Ref.update(closed, (n) => n + 1));
        yield* Deferred.succeed(opening, undefined);
        yield* overrides.startup ?? Effect.void;
        return Stream.fromEffect(Deferred.succeed(ready, undefined)).pipe(
          Stream.drain,
          Stream.concat(
            Stream.fromQueue(events).pipe(
              Stream.flatMap((item) =>
                Stream.make(item.incoming).pipe(
                  Stream.concat(Stream.fromEffect(Deferred.succeed(item.ack, undefined)).pipe(Stream.drain)),
                ),
              ),
            ),
          ),
        );
      }),
      history: Effect.fn("Test.Lark.history")(function* (root) {
        yield* Queue.offer(histories, root);
        return yield* overrides.history?.(root) ?? Effect.succeed({ messages: [], hasMore: false });
      }),
    }),
  );
  const native = Layer.succeed(
    OpenCode,
    OpenCode.of({
      requireSession: overrides.requireSession ?? (() => Effect.void),
      decide: Effect.fn("Test.OpenCode.decide")(function* (request) {
        yield* Queue.offer(requests, request);
        return yield* overrides.decide?.(request) ?? Effect.succeed(result());
      }),
      queue: Effect.fn("Test.OpenCode.queue")(function* (id, payload) {
        yield* overrides.queue?.(id, payload) ?? Effect.void;
        yield* Queue.offer(deliveries, { id, payload });
      }),
      deletedSessions: Stream.fromQueue(deleted),
    }),
  );
  const options = yield* Schema.decodeUnknownEffect(Options)({ ...settings, ...overrides });
  const services = yield* Layer.buildWithScope(
    Watcher.layer(options).pipe(Layer.provide(Layer.merge(lark, native))),
    scope,
  );
  const watcher = Context.get(services, Watcher);
  const send = Effect.fn("Test.send")(function* (message: Incoming) {
    const ack = yield* Deferred.make<void>();
    yield* Queue.offer(events, { incoming: message, ack });
    yield* Deferred.await(ack);
  });
  const start = Effect.fn("Test.start")(function* () {
    yield* watcher.toggle(sessionA);
    yield* Deferred.await(ready);
  });
  return {
    watcher,
    events,
    deleted,
    histories,
    requests,
    deliveries,
    opened,
    closed,
    opening,
    options,
    send,
    start,
    dispose: Scope.close(scope, Exit.void),
  };
});
