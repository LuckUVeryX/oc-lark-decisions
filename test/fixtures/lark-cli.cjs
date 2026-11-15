#!/usr/bin/env node
const { spawn } = require("node:child_process");
const { appendFileSync, readFileSync, writeFileSync } = require("node:fs");
const config = JSON.parse(readFileSync("fixture.json", "utf8"));

if (process.argv[2] === "descendant") {
  process.stdin.resume();
  process.on("SIGTERM", () => {
    writeFileSync("descendant-terminated", String(process.pid));
    process.exit(0);
  });
  process.send("ready");
} else {
  const args = process.argv.slice(2);
  appendFileSync("argv.ndjson", `${JSON.stringify(args)}\n`);
  if (args[0] === "event") {
    if (config.mode === "exit") process.exit(0);
    const descendant = spawn(process.execPath, [__filename, "descendant"], {
      stdio: ["pipe", "ignore", "ignore", "ipc"],
    });
    process.on("SIGTERM", () => {
      writeFileSync("leader-terminated", String(process.pid));
      descendant.once("exit", () => process.exit(0));
    });
    process.stdin.resume();
    descendant.once("message", () => {
      writeFileSync("pids.json", JSON.stringify([process.pid, descendant.pid]));
      if (config.mode === "starting") {
        process.stdout.write("starting\n");
        return;
      }
      process.stderr.write("[event] ready event_key=im.", () => {
        setImmediate(() => {
          process.stderr.write("message.receive_v1\n", () => {
            process.stderr.write("after readiness\n");
            if (config.mode === "malformed") {
              process.stdout.write("{broken\n");
              return;
            }
            process.stdout.write('{"message_id":"one","chat_id":"chat","root_id":"root",', () => {
              setImmediate(() => {
                process.stdout.write(
                  '"content":"{\\"interactive\\":true}","sender_id":"sender","create_time":"10"}\n',
                );
                process.stdout.write('{"message_id":"two","chat_id":"chat"}\n');
              });
            });
          });
        });
      });
    });
  } else {
    const path = args[2];
    if (config.mode === "failure") {
      process.stderr.write("fixture API failure\n");
      process.exitCode = 7;
    } else if (config.mode === "bad-json") {
      process.stdout.write("not JSON");
    } else if (config.mode === "bad-envelope") {
      process.stdout.write(JSON.stringify({ ok: false, error: "no access" }));
    } else {
      const data = path.endsWith("/messages") ? config.page : { items: [config.root] };
      process.stdout.write(JSON.stringify({ ok: true, data }));
    }
  }
}
