import { existsSync, watch } from "node:fs";
import { chmod, copyFile, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { NodeChildProcessSpawner, NodeFileSystem, NodePath } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Effect, Exit, Fiber, Layer, Logger, Queue, Schema, Scope, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { Root } from "../src/domain.ts";
import { Lark } from "../src/lark.ts";

const root = Schema.decodeUnknownSync(Root)({ chatId: "chat", rootId: "root" });
const rawRoot = { message_id: "root", create_time: "10", body: { content: '{"text":"root"}' } };
const waitForFile = (directory: string, name: string) =>
  Effect.callback<void>((resume) => {
    const check = () => {
      if (existsSync(join(directory, name))) resume(Effect.void);
    };
    const watcher = watch(directory, check);
    check();
    return Effect.sync(() => watcher.close());
  });
const fixture = Effect.fn("Test.fixture")(function* (config: unknown) {
  const directory = yield* Effect.acquireRelease(
    Effect.promise(() => mkdtemp("/tmp/opencode/lark-fixture-")),
    (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
  );
  yield* Effect.promise(async () => {
    await copyFile(new URL("./fixtures/lark-cli.cjs", import.meta.url), join(directory, "lark-cli"));
    await chmod(join(directory, "lark-cli"), 0o755);
    await writeFile(join(directory, "fixture.json"), JSON.stringify(config));
  });
  const spawner = Layer.effect(
    ChildProcessSpawner.ChildProcessSpawner,
    Effect.gen(function* () {
      const real = yield* ChildProcessSpawner.ChildProcessSpawner;
      return ChildProcessSpawner.ChildProcessSpawner.of({
        ...real,
        spawn: (command) =>
          real.spawn(
            command.pipe(
              ChildProcess.setEnv({ PATH: `${directory}:${dirname(process.execPath)}:/usr/bin:/bin` }),
            ),
          ),
      });
    }),
  ).pipe(
    Layer.provide(NodeChildProcessSpawner.layer),
    Layer.provide(Layer.merge(NodeFileSystem.layer, NodePath.layer)),
  );
  const services = yield* Layer.build(Lark.layer({ directory }).pipe(Layer.provide(spawner)));
  return { directory, lark: Context.get(services, Lark) };
});

it.live(
  "waits for split readiness, decodes split NDJSON, keeps stdin open, and terminates the process group",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({ mode: "consume" });
      expect(existsSync(join(f.directory, "argv.ndjson"))).toBe(false);
      const scope = yield* Scope.make();
      const source = yield* f.lark.open().pipe(Effect.provideService(Scope.Scope, scope));
      const messages = yield* source.pipe(Stream.take(2), Stream.runCollect);
      expect(messages).toEqual([
        {
          root: { chatId: "chat", rootId: "root" },
          message: { id: "one", content: '{"interactive":true}', senderId: "sender", timestamp: "10" },
        },
        { root: { chatId: "chat", rootId: "two" }, message: { id: "two", content: "" } },
      ]);
      yield* Scope.close(scope, Exit.void);
      const leader = yield* Effect.promise(() => readFile(join(f.directory, "leader-terminated"), "utf8"));
      const descendant = yield* Effect.promise(() =>
        readFile(join(f.directory, "descendant-terminated"), "utf8"),
      );
      expect(Number(leader)).toBeGreaterThan(0);
      expect(Number(descendant)).toBeGreaterThan(0);
      const pids = yield* Effect.promise(() => readFile(join(f.directory, "pids.json"), "utf8"));
      expect(JSON.parse(pids)).toEqual([Number(leader), Number(descendant)]);
      expect(() => process.kill(Number(leader), 0)).toThrow();
      expect(() => process.kill(Number(descendant), 0)).toThrow();
    }),
);

it.live("logs CLI diagnostics after readiness while continuing to consume events", () =>
  Effect.gen(function* () {
    const logs = yield* Queue.unbounded<Logger.Options<unknown>>();
    const logger = Logger.make((options) => Queue.offerUnsafe(logs, options));
    yield* Effect.gen(function* () {
      const f = yield* fixture({ mode: "consume" });
      const source = yield* f.lark.open();
      const messages = yield* source.pipe(Stream.take(2), Stream.runCollect);
      const diagnostic = yield* Queue.take(logs);
      expect(diagnostic.message).toEqual(["after readiness"]);
      expect(diagnostic.logLevel).toBe("Info");
      expect(messages).toHaveLength(2);
    }).pipe(Effect.provide(Logger.layer([logger])));
  }),
);

it.live("fails on exit before readiness even with zero status and rejects malformed event JSON", () =>
  Effect.gen(function* () {
    const exited = yield* fixture({ mode: "exit" });
    const failure = yield* Effect.scoped(exited.lark.open()).pipe(Effect.flip);
    expect(failure.operation).toBe("open");
    const malformed = yield* fixture({ mode: "malformed" });
    const decoded = yield* Effect.scoped(
      Effect.gen(function* () {
        const source = yield* malformed.lark.open();
        return yield* Stream.runCollect(source);
      }),
    ).pipe(Effect.flip);
    expect(decoded.operation).toBe("consume");
  }),
);

it.live("startup remains cancellable before the exact ready line and awaits descendant cleanup", () =>
  Effect.gen(function* () {
    const f = yield* fixture({ mode: "starting" });
    const opening = yield* Effect.scoped(f.lark.open()).pipe(Effect.forkScoped);
    yield* waitForFile(f.directory, "pids.json");
    expect(opening.pollUnsafe()).toBeUndefined();
    yield* Fiber.interrupt(opening);
    expect(yield* Effect.promise(() => readFile(join(f.directory, "descendant-terminated"), "utf8"))).toMatch(
      /^\d+$/,
    );
    expect(yield* Effect.promise(() => readFile(join(f.directory, "leader-terminated"), "utf8"))).toMatch(
      /^\d+$/,
    );
  }),
);

it.live("resolves the actual thread ID and returns chronological raw history with conservative hasMore", () =>
  Effect.gen(function* () {
    const f = yield* fixture({
      root: { ...rawRoot, thread_id: "actual-thread" },
      page: {
        items: [
          { message_id: "reply", root_id: "root", create_time: "20", body: { content: "raw reply" } },
          rawRoot,
        ],
        has_more: true,
      },
    });
    expect(yield* f.lark.history(root)).toEqual({
      messages: [
        { id: "root", content: '{"text":"root"}', timestamp: "10" },
        { id: "reply", content: "raw reply", timestamp: "20" },
      ],
      hasMore: true,
    });
    const argv = yield* Effect.promise(() => readFile(join(f.directory, "argv.ndjson"), "utf8"));
    const commands = yield* Schema.decodeUnknownEffect(Schema.Array(Schema.Array(Schema.String)))(
      argv
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line)),
    );
    expect(commands[0]).toEqual(["api", "GET", "/open-apis/im/v1/messages/root", "--as", "bot"]);
    expect(commands[1]).toEqual([
      "api",
      "GET",
      "/open-apis/im/v1/messages",
      "--params",
      JSON.stringify({
        container_id_type: "thread",
        container_id: "actual-thread",
        sort_type: "ByCreateTimeDesc",
        page_size: 50,
      }),
      "--as",
      "bot",
    ]);
  }),
);

it.live(
  "filters non-thread history by root, includes the fetched root, deduplicates, and sorts numerically",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture({
        root: rawRoot,
        page: {
          items: [
            { message_id: "unrelated", root_id: "other", create_time: "40" },
            { message_id: "reply", root_id: "root", create_time: "30", body: { content: "new" } },
            { message_id: "reply", root_id: "root", create_time: "30", body: { content: "old" } },
            { message_id: "earlier", root_id: "root", create_time: "9", sender: { id: "sender" } },
          ],
          has_more: true,
        },
      });
      expect(yield* f.lark.history(root)).toEqual({
        messages: [
          { id: "earlier", content: "", senderId: "sender", timestamp: "9" },
          { id: "root", content: '{"text":"root"}', timestamp: "10" },
          { id: "reply", content: "new", timestamp: "30" },
        ],
        hasMore: true,
      });
      const argv = yield* Effect.promise(() => readFile(join(f.directory, "argv.ndjson"), "utf8"));
      expect(argv).toContain('\\"container_id_type\\":\\"chat\\"');
      expect(argv).toContain('\\"only_thread_root_messages\\":false');
    }),
);

it.live("rejects CLI errors, malformed history JSON, and unsuccessful envelopes", () =>
  Effect.gen(function* () {
    for (const mode of ["failure", "bad-json", "bad-envelope"]) {
      const f = yield* fixture({ mode });
      expect((yield* f.lark.history(root).pipe(Effect.flip)).operation).toBe("history");
    }
  }),
);
