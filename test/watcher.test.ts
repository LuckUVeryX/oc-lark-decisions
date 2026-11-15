import { expect, it } from "@effect/vitest";
import { Generate } from "@opencode/schema/generate";
import { Deferred, Effect, Fiber, Queue, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { LarkError } from "../src/lark.ts";
import { harness, incoming, result, sessionA, sessionB } from "./support.ts";

it.effect("isolates roots, replaces duplicates without postponing, and preserves native fidelity", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    yield* h.start();
    yield* h.send(incoming("a", "same"));
    yield* h.send(incoming("b", "same"));
    yield* h.send(incoming("a", "same", "other-chat"));
    yield* TestClock.adjust(10_000);
    yield* h.send(incoming("a", "same", "chat", "replacement"));
    yield* TestClock.adjust(5_000);
    const deliveries = yield* Effect.all([
      Queue.take(h.deliveries),
      Queue.take(h.deliveries),
      Queue.take(h.deliveries),
    ]);
    expect(deliveries.map((item) => item.payload.context.root)).toEqual(
      expect.arrayContaining([
        { chatId: "chat", rootId: "a" },
        { chatId: "chat", rootId: "b" },
        { chatId: "other-chat", rootId: "a" },
      ]),
    );
    expect(
      deliveries.find(
        (item) => item.payload.context.root.chatId === "chat" && item.payload.context.root.rootId === "a",
      )?.payload.context.incoming,
    ).toEqual([{ id: "same", content: "replacement" }]);
    for (const delivery of deliveries) {
      expect(delivery.payload.result).toEqual(result());
      expect(delivery.id).toBe(sessionA);
    }
    const request = yield* Queue.take(h.requests);
    expect(request).not.toBe(h.options.decisions);
    expect(request.input).not.toBe(h.options.decisions.input);
    expect(request.model).toBe(h.options.decisions.model);
    expect(request.questions).toBe(h.options.decisions.questions);
    expect(h.options.decisions.input).toHaveLength(3);
    expect(request.input).toHaveLength(4);
    expect(request.input.slice(0, 3)).toEqual([
      { type: "text", text: "Classify this conversation." },
      { type: "json", value: { preserved: true } },
      { type: "file", data: "YWJj", mime: "text/plain", name: "rubric.txt" },
    ]);
    expect(request.input[3]?.type).toBe("json");
    expect(request.model.variant).toBe("test");
  }),
);

it.effect("includes required history in the appended context and preserves its truncation flag", () =>
  Effect.gen(function* () {
    const history = { messages: [incoming("root", "older").message], hasMore: true };
    const h = yield* harness({ history: () => Effect.succeed(history) });
    yield* h.start();
    yield* h.send(incoming());
    yield* TestClock.adjust(15_000);
    const delivery = yield* Queue.take(h.deliveries);
    const request = yield* Queue.take(h.requests);
    expect(delivery.payload.context.history).toBe(history);
    expect(request.input.at(-1)).toEqual({ type: "json", value: delivery.payload.context });
  }),
);

it.effect("source failure during a switch closes both scopes without waiting on the lifecycle lock", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const cleaning = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const h = yield* harness({
      queue: () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(cleaning, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        ),
    });
    yield* h.start();
    yield* h.send(incoming());
    yield* TestClock.adjust(15_000);
    yield* Deferred.await(entered);
    const switching = yield* h.watcher.toggle(sessionB).pipe(Effect.forkScoped);
    yield* Deferred.await(cleaning);
    yield* Queue.fail(h.events, new LarkError({ operation: "source", cause: "fixture" }));
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(switching);
    yield* Effect.repeat(h.watcher.status(), { until: (status) => status.state === "stopped" });
    expect(yield* Ref.get(h.closed)).toBe(1);
    expect(yield* Ref.get(h.opened)).toBe(1);
  }),
);

it.effect("only new IDs reset the deadline and arrivals during work form detached batches", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    const h = yield* harness({ decide: () => Deferred.await(gate).pipe(Effect.as(result())) });
    yield* h.start();
    yield* h.send(incoming("a", "first"));
    yield* TestClock.adjust(10_000);
    yield* h.send(incoming("a", "second"));
    yield* TestClock.adjust(5_000);
    expect(yield* Queue.size(h.requests)).toBe(0);
    yield* TestClock.adjust(10_000);
    const first = yield* Queue.take(h.requests);
    yield* h.send(incoming("a", "third"));
    yield* TestClock.adjust(15_000);
    const second = yield* Queue.take(h.requests);
    const firstPart = first.input.at(-1);
    const secondPart = second.input.at(-1);
    expect(firstPart).toMatchObject({ value: { incoming: [{ id: "first" }, { id: "second" }] } });
    expect(secondPart).toMatchObject({ value: { incoming: [{ id: "third" }] } });
    yield* Deferred.succeed(gate, undefined);
    yield* Queue.take(h.deliveries);
    yield* Queue.take(h.deliveries);
  }),
);

it.effect("admits four workers, drops full batches, and frees capacity after completion", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    const h = yield* harness({ decide: () => Deferred.await(gate).pipe(Effect.as(result())) });
    yield* h.start();
    for (const root of ["1", "2", "3", "4", "dropped"]) yield* h.send(incoming(root));
    yield* TestClock.adjust(15_000);
    expect(yield* Queue.size(h.requests)).toBe(4);
    yield* Deferred.succeed(gate, undefined);
    for (let n = 0; n < 4; n++) yield* Queue.take(h.deliveries);
    yield* h.send(incoming("after"));
    yield* TestClock.adjust(15_000);
    expect((yield* Queue.take(h.deliveries)).payload.context.root.rootId).toBe("after");
    expect(yield* Queue.size(h.deliveries)).toBe(0);
    expect(yield* Queue.size(h.requests)).toBe(5);
  }),
);

it.effect("uses score rather than confidence, forwards equality, and skips unusable answers", () =>
  Effect.gen(function* () {
    const answers = [
      result(),
      result(1.39, 1),
      Schema.decodeUnknownSync(Generate.DecisionResult)({ answers: [] }),
      Schema.decodeUnknownSync(Generate.DecisionResult)({
        answers: [{ type: "predicate", name: "attention", probability: 1 }],
      }),
      Schema.decodeUnknownSync(Generate.DecisionResult)({
        answers: [{ type: "error", name: "attention", reason: "failed" }],
      }),
    ];
    const results = yield* Queue.unbounded<Generate.DecisionResult>();
    for (const answer of answers) yield* Queue.offer(results, answer);
    const h = yield* harness({ decide: () => Queue.take(results) });
    yield* h.start();
    for (let n = 0; n < answers.length; n++) {
      yield* h.send(incoming(`${n}`));
      yield* TestClock.adjust(15_000);
    }
    expect(yield* Queue.size(h.deliveries)).toBe(1);
    expect((yield* Queue.take(h.deliveries)).payload.result).toEqual(result());
  }),
);

it.effect("fails history early and isolates history, decision, and queue failures", () =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const h = yield* harness({
      maxWorkers: 1,
      history: (root) =>
        root.rootId === "history"
          ? Effect.fail(new LarkError({ operation: "history", cause: "fixture" }))
          : Effect.succeed({ messages: [], hasMore: false }),
      decide: () =>
        Ref.updateAndGet(calls, (n) => n + 1).pipe(
          Effect.flatMap((n) => (n === 1 ? Effect.fail("decision failed") : Effect.succeed(result()))),
        ),
      queue: (_id, payload) =>
        payload.context.root.rootId === "queue" ? Effect.fail("queue failed") : Effect.void,
    });
    yield* h.start();
    yield* h.send(incoming("history"));
    yield* TestClock.adjust(15_000);
    expect(yield* Queue.size(h.requests)).toBe(0);
    for (const root of ["decision", "queue", "success"]) {
      yield* h.send(incoming(root));
      yield* TestClock.adjust(15_000);
    }
    expect((yield* Queue.take(h.deliveries)).payload.context.root.rootId).toBe("success");
    expect(yield* Queue.size(h.deliveries)).toBe(0);
  }),
);

it.effect("switch cancels and awaits blocked queue admission, clears timers, and keeps the source", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const cancelled = yield* Deferred.make<void>();
    const releaseCleanup = yield* Deferred.make<void>();
    const h = yield* harness({
      queue: (id) =>
        id === sessionA
          ? Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(cancelled, undefined).pipe(Effect.andThen(Deferred.await(releaseCleanup))),
              ),
            )
          : Effect.void,
    });
    yield* h.start();
    yield* h.send(incoming("active"));
    yield* TestClock.adjust(15_000);
    yield* Deferred.await(entered);
    yield* h.send(incoming("pending"));
    const switching = yield* h.watcher.toggle(sessionB).pipe(Effect.forkScoped);
    yield* Deferred.await(cancelled);
    expect(switching.pollUnsafe()).toEqual(undefined);
    yield* Deferred.succeed(releaseCleanup, undefined);
    expect(yield* Fiber.join(switching)).toEqual({ state: "watching", sessionID: sessionB });
    expect(yield* Ref.get(h.opened)).toBe(1);
    expect(yield* Ref.get(h.closed)).toBe(0);
    yield* TestClock.adjust(15_000);
    expect(yield* Queue.size(h.deliveries)).toBe(0);
    yield* h.send(incoming("new"));
    yield* TestClock.adjust(15_000);
    expect((yield* Queue.take(h.deliveries)).id).toBe(sessionB);
    expect(yield* h.watcher.toggle(sessionB)).toEqual({ state: "stopped" });
    expect(yield* Ref.get(h.closed)).toBe(1);
  }),
);

it.effect("disposal during switch awaits blocked queue cleanup", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const cleaning = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const cleaned = yield* Deferred.make<void>();
    const h = yield* harness({
      queue: () =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(cleaning, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.andThen(Deferred.succeed(cleaned, undefined)),
            ),
          ),
        ),
    });
    yield* Effect.gen(function* () {
      yield* h.start();
      yield* h.send(incoming());
      yield* TestClock.adjust(15_000);
      yield* Deferred.await(entered);
      const switching = yield* h.watcher.toggle(sessionB).pipe(Effect.forkScoped);
      yield* Deferred.await(cleaning);
      const disposing = yield* h.dispose.pipe(Effect.forkScoped);
      yield* TestClock.adjust(0);
      expect(yield* Deferred.isDone(cleaned)).toBe(false);
      expect(disposing.pollUnsafe()).toBeUndefined();
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(disposing);
      yield* Fiber.join(switching);
      expect(yield* Deferred.isDone(cleaned)).toBe(true);
      expect(yield* h.watcher.status()).toEqual({ state: "stopped" });
      expect(yield* Ref.get(h.closed)).toBe(1);
    }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
  }),
);

it.effect("stops interruptible startup, source failure, and plugin disposal", () =>
  Effect.gen(function* () {
    const h = yield* harness({ startup: Effect.never });
    expect(yield* h.watcher.toggle(sessionA)).toEqual({ state: "starting", sessionID: sessionA });
    yield* Deferred.await(h.opening);
    expect(yield* h.watcher.toggle(sessionA)).toEqual({ state: "stopped" });
    expect(yield* Ref.get(h.closed)).toBe(1);
    const running = yield* harness();
    yield* running.start();
    yield* running.dispose;
    expect(yield* running.watcher.status()).toEqual({ state: "stopped" });
    expect(yield* Ref.get(running.closed)).toBe(1);
  }),
);

it.effect("source failure and destination deletion stop the watch, but unrelated deletion does not", () =>
  Effect.gen(function* () {
    const h = yield* harness();
    yield* h.start();
    yield* Queue.offer(h.deleted, sessionB);
    yield* h.send(incoming("still-watching"));
    expect(yield* h.watcher.status()).toEqual({ state: "watching", sessionID: sessionA });
    yield* Queue.offer(h.deleted, sessionA);
    yield* Effect.repeat(h.watcher.status(), { until: (status) => status.state === "stopped" });
    expect(yield* Ref.get(h.closed)).toBe(1);
    const failed = yield* harness();
    yield* failed.start();
    yield* Queue.fail(failed.events, new LarkError({ operation: "source", cause: "fixture" }));
    yield* Effect.repeat(failed.watcher.status(), { until: (status) => status.state === "stopped" });
    expect(yield* Ref.get(failed.closed)).toBe(1);
  }),
);

it.effect("validates switches before cancelling the old destination", () =>
  Effect.gen(function* () {
    const h = yield* harness({
      requireSession: (id) => (id === sessionB ? Effect.fail("missing") : Effect.void),
    });
    yield* h.start();
    expect(yield* h.watcher.toggle(sessionB).pipe(Effect.flip)).toBe("missing");
    yield* h.send(incoming());
    yield* TestClock.adjust(15_000);
    expect((yield* Queue.take(h.deliveries)).id).toBe(sessionA);
    expect(yield* Ref.get(h.opened)).toBe(1);
  }),
);

it.effect("switches during startup without restarting the source and stops after startup failure", () =>
  Effect.gen(function* () {
    const ready = yield* Deferred.make<void>();
    const h = yield* harness({ startup: Deferred.await(ready) });
    yield* h.watcher.toggle(sessionA);
    yield* Deferred.await(h.opening);
    expect(yield* h.watcher.toggle(sessionB)).toEqual({ state: "starting", sessionID: sessionB });
    yield* Deferred.succeed(ready, undefined);
    yield* h.send(incoming());
    expect(yield* h.watcher.status()).toEqual({ state: "watching", sessionID: sessionB });
    expect(yield* Ref.get(h.opened)).toBe(1);
    yield* TestClock.adjust(15_000);
    expect((yield* Queue.take(h.deliveries)).id).toBe(sessionB);
    const failed = yield* harness({
      startup: Effect.fail(new LarkError({ operation: "ready", cause: "fixture" })),
    });
    yield* failed.watcher.toggle(sessionA);
    yield* Deferred.await(failed.opening);
    yield* Effect.repeat(failed.watcher.status(), { until: (status) => status.state === "stopped" });
    expect(yield* Ref.get(failed.closed)).toBe(1);
  }),
);

for (const operation of ["history", "decide", "queue"]) {
  for (const action of ["switch", "stop", "dispose"]) {
    it.effect(`${action} cancels and awaits active ${operation}`, () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const cleaning = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const blocked = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(cleaning, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        const h = yield* harness({
          ...(operation === "history" ? { history: () => blocked } : {}),
          ...(operation === "decide" ? { decide: () => blocked } : {}),
          ...(operation === "queue" ? { queue: () => blocked } : {}),
        });
        yield* h.start();
        yield* h.send(incoming());
        yield* TestClock.adjust(15_000);
        yield* Deferred.await(entered);
        const end =
          action === "dispose" ? h.dispose : h.watcher.toggle(action === "switch" ? sessionB : sessionA);
        const fiber = yield* end.pipe(Effect.forkScoped);
        yield* Deferred.await(cleaning);
        expect(fiber.pollUnsafe()).toBeUndefined();
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(fiber);
        expect(yield* Queue.size(h.deliveries)).toBe(0);
        expect((yield* h.watcher.status()).state).toBe(action === "switch" ? "watching" : "stopped");
      }),
    );
  }
}
