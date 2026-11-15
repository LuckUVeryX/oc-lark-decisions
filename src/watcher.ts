import type { Session } from "@opencode/schema/session";
import { Clock, Context, Effect, Exit, Fiber, Layer, Scope, Semaphore, Stream } from "effect";
import type {
  Batch,
  ConversationContext,
  Incoming,
  Message,
  MessageId,
  Options,
  WatchStatus,
} from "./domain.ts";
import { Lark } from "./lark.ts";
import { OpenCode } from "./opencode.ts";

interface Pending {
  readonly messages: Map<MessageId, Message>;
  deadline: number;
}
interface Work {
  readonly scope: Scope.Closeable;
  readonly sessionID: Session.ID;
  readonly pending: Map<string, Pending>;
  activeWorkers: number;
}
interface Watch {
  readonly scope: Scope.Closeable;
  work: Work;
  phase: "starting" | "watching";
  closed: boolean;
}

export class Watcher extends Context.Service<
  Watcher,
  {
    readonly toggle: (id: Session.ID) => Effect.Effect<WatchStatus, unknown>;
    readonly status: () => Effect.Effect<WatchStatus>;
  }
>()("oc-lark-relay/Watcher") {
  static readonly layer = (options: Options) =>
    Layer.effect(
      Watcher,
      Effect.gen(function* () {
        const lark = yield* Lark;
        const native = yield* OpenCode;
        const pluginScope = yield* Scope.Scope;
        const lifecycle = yield* Semaphore.make(1);
        let current: { readonly watch: Watch; readonly controller: Fiber.Fiber<void> } | undefined;

        const status = Effect.fn("Watcher.status")(function* (): Effect.fn.Return<WatchStatus> {
          return yield* Effect.sync(() =>
            current === undefined
              ? ({ state: "stopped" } satisfies WatchStatus)
              : ({
                  state: current.watch.phase,
                  sessionID: current.watch.work.sessionID,
                } satisfies WatchStatus),
          );
        });

        const triage = Effect.fn("Watcher.triage")(
          function* (batch: Batch, sessionID: Session.ID) {
            const history = yield* lark.history(batch.root);
            const context: ConversationContext = { root: batch.root, incoming: batch.messages, history };
            const result = yield* native.decide({
              ...options.decisions,
              input: [...options.decisions.input, { type: "json", value: context }],
            });
            const answer = result.answers.find((answer) => answer.name === options.forwarding.question);
            if (answer === undefined || answer.type !== "score") {
              yield* Effect.logWarning("Lark batch has no successful forwarding score", {
                root: batch.root,
                question: options.forwarding.question,
                answer,
              });
              return;
            }
            if (answer.score < options.forwarding.threshold) return;
            yield* native.queue(sessionID, { context, result });
          },
          Effect.catch((error) => Effect.logError("Lark batch discarded", error)),
        );

        const debounce = Effect.fn("Watcher.debounce")(function* (
          watch: Watch,
          work: Work,
          key: string,
          pending: Pending,
          root: Incoming["root"],
        ) {
          while (true) {
            const now = yield* Clock.currentTimeMillis;
            yield* Effect.sleep(Math.max(0, pending.deadline - now));
            const done = yield* lifecycle.withPermit(
              Effect.gen(function* () {
                if (watch.closed || watch.work !== work || work.pending.get(key) !== pending) return true;
                const now = yield* Clock.currentTimeMillis;
                if (now < pending.deadline) return false;
                work.pending.delete(key);
                if (work.activeWorkers >= options.maxWorkers) {
                  yield* Effect.logWarning("Lark batch discarded at worker capacity", { root });
                  return true;
                }
                work.activeWorkers++;
                yield* triage({ root, messages: [...pending.messages.values()] }, work.sessionID).pipe(
                  Effect.scoped,
                  Effect.ensuring(
                    Effect.sync(() => {
                      work.activeWorkers--;
                    }),
                  ),
                  Effect.forkIn(work.scope),
                );
                return true;
              }),
            );
            if (done) return;
          }
        });

        const ingest = Effect.fn("Watcher.ingest")(function* (watch: Watch, incoming: Incoming) {
          if (watch.closed || current?.watch !== watch) return;
          const work = watch.work;
          const key = JSON.stringify([incoming.root.chatId, incoming.root.rootId]);
          const existing = work.pending.get(key);
          if (existing !== undefined) {
            if (!existing.messages.has(incoming.message.id)) {
              existing.deadline = (yield* Clock.currentTimeMillis) + options.debounceMs;
            }
            existing.messages.set(incoming.message.id, incoming.message);
            return;
          }
          const pending: Pending = {
            messages: new Map([[incoming.message.id, incoming.message]]),
            deadline: (yield* Clock.currentTimeMillis) + options.debounceMs,
          };
          work.pending.set(key, pending);
          yield* debounce(watch, work, key, pending, incoming.root).pipe(Effect.forkIn(work.scope));
        }, lifecycle.withPermit);

        const makeWork = Effect.fn("Watcher.makeWork")(function* (scope: Scope.Scope, sessionID: Session.ID) {
          const work: Work = {
            scope: yield* Scope.fork(scope),
            sessionID,
            pending: new Map(),
            activeWorkers: 0,
          };
          yield* Scope.addFinalizer(
            work.scope,
            Effect.sync(() => work.pending.clear()),
          );
          return work;
        });

        const control = Effect.fn("Watcher.control")(
          function* (watch: Watch) {
            const source = yield* lark.open().pipe(Effect.provideService(Scope.Scope, watch.scope));
            yield* lifecycle.withPermit(
              Effect.sync(() => {
                watch.phase = "watching";
              }),
            );
            yield* source.pipe(Stream.runForEach((incoming) => ingest(watch, incoming)));
            yield* Effect.logWarning("Lark source ended");
          },
          Effect.catch((error) => Effect.logError("Lark watch stopped after source failure", error)),
          (effect, watch) =>
            effect.pipe(
              Effect.ensuring(
                Effect.gen(function* () {
                  watch.closed = true;
                  yield* Scope.close(watch.scope, Exit.void);
                  if (current?.watch === watch) current = undefined;
                }),
              ),
            ),
        );

        const toggle = Effect.fn("Watcher.toggle")(function* (sessionID: Session.ID) {
          if (current !== undefined) {
            const running = current;
            if (running.watch.work.sessionID === sessionID) {
              yield* Fiber.interrupt(running.controller);
              return yield* status();
            }
            yield* native.requireSession(sessionID);
            if (running.watch.closed) {
              yield* Fiber.await(running.controller);
              return yield* status();
            }
            yield* Effect.uninterruptible(
              Effect.gen(function* () {
                yield* Scope.close(running.watch.work.scope, Exit.void);
                if (running.watch.closed) {
                  yield* Fiber.await(running.controller);
                  return;
                }
                running.watch.work = yield* makeWork(running.watch.scope, sessionID);
              }),
            );
            return yield* status();
          }
          yield* native.requireSession(sessionID);
          yield* Effect.uninterruptible(
            Effect.gen(function* () {
              const scope = yield* Scope.make();
              const watch: Watch = {
                scope,
                work: yield* makeWork(scope, sessionID),
                phase: "starting",
                closed: false,
              };
              const controller = yield* control(watch).pipe(Effect.interruptible, Effect.forkIn(pluginScope));
              current = { watch, controller };
            }),
          );
          return yield* status();
        }, lifecycle.withPermit);

        yield* native.deletedSessions.pipe(
          Stream.runForEach(
            Effect.fn("Watcher.deleted")(function* (sessionID) {
              yield* lifecycle.withPermit(
                Effect.gen(function* () {
                  if (current?.watch.work.sessionID === sessionID) {
                    yield* Fiber.interrupt(current.controller);
                  }
                }),
              );
            }),
          ),
          Effect.catch((error) => Effect.logError("Lark session deletion listener stopped", error)),
          Effect.forkScoped,
        );
        return Watcher.of({ toggle, status });
      }),
    );
}
