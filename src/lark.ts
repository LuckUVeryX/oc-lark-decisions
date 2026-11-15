import { Context, Deferred, Effect, Layer, Schema, type Scope, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { ChatId, type History, type Incoming, MessageId, type Root } from "./domain.ts";

export class LarkError extends Schema.TaggedError<LarkError>()("LarkError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

const Event = Schema.Struct({
  message_id: MessageId,
  chat_id: ChatId,
  root_id: Schema.optionalKey(MessageId),
  content: Schema.optionalKey(Schema.String),
  sender_id: Schema.optionalKey(Schema.String),
  create_time: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
});
const RawMessage = Schema.Struct({
  message_id: MessageId,
  root_id: Schema.optionalKey(Schema.String),
  thread_id: Schema.optionalKey(Schema.String),
  create_time: Schema.optionalKey(Schema.String),
  sender: Schema.optionalKey(Schema.Struct({ id: Schema.optionalKey(Schema.String) })),
  body: Schema.optionalKey(Schema.Struct({ content: Schema.optionalKey(Schema.String) })),
});
const Page = Schema.Struct({
  ok: Schema.Literal(true),
  data: Schema.Struct({
    items: Schema.Array(RawMessage),
    has_more: Schema.optionalKey(Schema.Boolean),
  }),
});

const normalize = (message: typeof RawMessage.Type) => ({
  id: message.message_id,
  content: message.body?.content ?? "",
  ...(message.sender?.id === undefined ? {} : { senderId: message.sender.id }),
  ...(message.create_time === undefined ? {} : { timestamp: message.create_time }),
});

export class Lark extends Context.Service<
  Lark,
  {
    readonly open: () => Effect.Effect<Stream.Stream<Incoming, LarkError>, LarkError, Scope.Scope>;
    readonly history: (root: Root) => Effect.Effect<History, LarkError>;
  }
>()("oc-lark-relay/Lark") {
  static readonly layer = (location: { readonly directory: string }) =>
    Layer.effect(
      Lark,
      Effect.gen(function* () {
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const command = (args: ReadonlyArray<string>) =>
          ChildProcess.make("lark-cli", args, { cwd: location.directory });

        const open = Effect.fn("Lark.open")(
          function* () {
            const child = yield* spawner.spawn(
              command(["event", "consume", "im.message.receive_v1", "--as", "bot"]),
            );
            const ready = yield* Deferred.make<void, LarkError>();
            yield* child.stderr.pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.runForEach((line) =>
                line === "[event] ready event_key=im.message.receive_v1"
                  ? Deferred.succeed(ready, undefined)
                  : Effect.void,
              ),
              Effect.catch((cause) => Deferred.fail(ready, new LarkError({ operation: "stderr", cause }))),
              Effect.forkScoped,
            );
            yield* Effect.raceFirst(
              Deferred.await(ready),
              child.exitCode.pipe(
                Effect.flatMap((code) =>
                  Effect.fail(new LarkError({ operation: "ready", cause: `Exited before ready: ${code}` })),
                ),
              ),
            );
            return child.stdout.pipe(
              Stream.decodeText(),
              Stream.splitLines,
              Stream.filter((line) => line.trim().length > 0),
              Stream.mapEffect(
                Effect.fn("Lark.decodeEvent")(function* (line: string) {
                  const event = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Event))(line);
                  const timestamp = event.create_time ?? event.timestamp;
                  return {
                    root: { chatId: event.chat_id, rootId: event.root_id ?? event.message_id },
                    message: {
                      id: event.message_id,
                      content: event.content ?? "",
                      ...(event.sender_id === undefined ? {} : { senderId: event.sender_id }),
                      ...(timestamp === undefined ? {} : { timestamp }),
                    },
                  };
                }),
              ),
              Stream.concat(
                Stream.fromEffect(
                  child.exitCode.pipe(
                    Effect.flatMap((code) =>
                      code === 0
                        ? Effect.void
                        : Effect.fail(new LarkError({ operation: "consume", cause: `Exit code: ${code}` })),
                    ),
                  ),
                ).pipe(Stream.drain),
              ),
              Stream.mapError((cause) => new LarkError({ operation: "consume", cause })),
            );
          },
          Effect.mapError((cause) => new LarkError({ operation: "open", cause })),
        );

        const page = Effect.fn("Lark.page")(function* (args: ReadonlyArray<string>) {
          return yield* Effect.scoped(
            Effect.gen(function* () {
              const child = yield* spawner.spawn(command(["api", "GET", ...args, "--as", "bot"]));
              const [stdout, stderr, code] = yield* Effect.all(
                [
                  child.stdout.pipe(Stream.decodeText(), Stream.mkString),
                  child.stderr.pipe(Stream.decodeText(), Stream.mkString),
                  child.exitCode,
                ],
                { concurrency: "unbounded" },
              );
              if (code !== 0) {
                return yield* Effect.fail(new LarkError({ operation: "history", cause: { code, stderr } }));
              }
              return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Page))(stdout);
            }),
          );
        });

        const history = Effect.fn("Lark.history")(
          function* (root: Root): Effect.fn.Return<History, unknown> {
            const fetched = yield* page([`/open-apis/im/v1/messages/${encodeURIComponent(root.rootId)}`]);
            const original = fetched.data.items.find((message) => message.message_id === root.rootId);
            if (original === undefined) {
              return yield* Effect.fail(new LarkError({ operation: "history", cause: "Root not returned" }));
            }
            const threadId = original.thread_id;
            const threaded = threadId !== undefined && threadId.length > 0;
            const recent = yield* page([
              "/open-apis/im/v1/messages",
              "--params",
              JSON.stringify({
                container_id_type: threaded ? "thread" : "chat",
                container_id: threaded ? threadId : root.chatId,
                sort_type: "ByCreateTimeDesc",
                page_size: 50,
                ...(threaded ? {} : { only_thread_root_messages: false }),
              }),
            ]);
            const messages = new Map<MessageId, typeof RawMessage.Type>();
            messages.set(original.message_id, original);
            for (const message of [...recent.data.items].reverse()) {
              if (threaded || message.message_id === root.rootId || message.root_id === root.rootId) {
                messages.set(message.message_id, message);
              }
            }
            return {
              messages: [...messages.values()]
                .sort((a, b) => Number(a.create_time ?? 0) - Number(b.create_time ?? 0))
                .map(normalize),
              hasMore: recent.data.has_more ?? false,
            };
          },
          Effect.mapError((cause) => new LarkError({ operation: "history", cause })),
        );
        return Lark.of({ open, history });
      }),
    );
}
