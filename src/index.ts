import { NodeChildProcessSpawner, NodeFileSystem, NodePath } from "@effect/platform-node";
import { Plugin } from "@opencode/plugin/effect";
import { Context, Effect, Layer, Schema } from "effect";
import { Options } from "./domain.ts";
import { Lark } from "./lark.ts";
import { OpenCode } from "./opencode.ts";
import { Watcher } from "./watcher.ts";

export default Plugin.define({
  id: "oc-lark-relay",
  effect: Effect.fn("LarkPlugin.setup")(function* (ctx) {
    const options = yield* Schema.decodeUnknownEffect(Options)(ctx.options);
    const adapters = Layer.merge(
      Lark.layer(ctx.location).pipe(
        Layer.provide(NodeChildProcessSpawner.layer),
        Layer.provide(Layer.merge(NodeFileSystem.layer, NodePath.layer)),
      ),
      OpenCode.layer(ctx),
    );
    const services = yield* Layer.build(Watcher.layer(options).pipe(Layer.provide(adapters)));
    const instance = Context.get(services, Watcher);
    yield* ctx.command.transform((editor) =>
      editor.add({
        name: "lark",
        description: "Toggle Lark watching in this session",
        execute: Effect.fn("LarkPlugin.command")(function* (input) {
          yield* instance.toggle(input.sessionID);
        }),
      }),
    );
  }, Effect.orDie),
} satisfies Plugin.Plugin);
