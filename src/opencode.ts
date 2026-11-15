import type { Plugin } from "@opencode/plugin/effect";
import type { Session } from "@opencode/schema/session";
import { Context, Effect, Layer, Stream } from "effect";
import type { DecisionRequest, DecisionResult, Queued } from "./domain.ts";

export type Native = {
  readonly session: Pick<Plugin.Context["session"], "get" | "prompt">;
  readonly experimental: {
    readonly generate: Pick<Plugin.Context["experimental"]["generate"], "decisions">;
  };
  readonly event: Plugin.Context["event"];
};

export class OpenCode extends Context.Service<
  OpenCode,
  {
    readonly requireSession: (id: Session.ID) => Effect.Effect<void, unknown>;
    readonly decide: (request: DecisionRequest) => Effect.Effect<DecisionResult, unknown>;
    readonly queue: (id: Session.ID, payload: Queued) => Effect.Effect<void, unknown>;
    readonly deletedSessions: Stream.Stream<Session.ID, unknown>;
  }
>()("oc-lark-relay/OpenCode") {
  static readonly layer = (ctx: Native) =>
    Layer.effect(
      OpenCode,
      Effect.gen(function* () {
        return OpenCode.of({
          requireSession: Effect.fn("OpenCode.requireSession")(function* (sessionID) {
            yield* ctx.session.get({ sessionID });
          }),
          decide: Effect.fn("OpenCode.decide")(function* (request) {
            return yield* ctx.experimental.generate.decisions(request);
          }),
          queue: Effect.fn("OpenCode.queue")(function* (sessionID, payload) {
            yield* ctx.session.prompt({ sessionID, text: JSON.stringify(payload), delivery: "queue" });
          }),
          deletedSessions: ctx.event
            .subscribe({ types: ["session.deleted"] })
            .pipe(Stream.map((event) => event.data.sessionID)),
        });
      }),
    );
}
