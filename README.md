# Lark watcher for OpenCode

`oc-lark-relay` watches Lark conversations and queues selected native decision results in the session that runs `/lark`.
It requires OpenCode 2.3.0, Effect 4.1.0, and `lark-cli` on the host's `PATH`.
Configure and authorize `lark-cli` for bot event consumption and message history before starting a watch.
The adapter targets the `lark-cli` 1.0.100 event and raw API contracts.

## Configure the plugin

Add this entry to your OpenCode configuration. Replace the package path with this checkout's absolute path.
Replace the dummy model and scoring instructions with your own.

```json
{
	"plugins": [
		{
			"package": "/absolute/path/to/oc-lark-decisions",
			"options": {
				"decisions": {
					"model": {
						"providerID": "dummy",
						"id": "dummy"
					},
					"input": [
						{
							"type": "text",
							"text": "Score whether the attached conversation needs attention."
						}
					],
					"questions": [
						{
							"type": "score",
							"name": "attention",
							"instructions": "Use the conversation and its recent history.",
							"levels": [
								{ "label": "ignore" },
								{ "label": "review" },
								{ "label": "urgent" }
							]
						}
					]
				},
				"forwarding": {
					"question": "attention",
					"threshold": 1.4
				},
				"debounceMs": 15000,
				"maxWorkers": 4
			}
		}
	]
}
```

`decisions` is the complete native `Generate.DecisionRequest`. The plugin does not choose a model or questions.
`forwarding.question` must name a configured score question. Both forwarding fields are required.
Invalid options fail plugin setup before command registration.

The native `score` uses the question's level scale, from zero through `levels.length - 1`.
For three levels, the scale is 0 through 2, and fractional scores are valid.
The example forwards scores at least 1.4. It does not compare `confidence` or predicate probability.

`debounceMs` defaults to 15000 and accepts finite nonnegative milliseconds.
`maxWorkers` defaults to 4 and accepts positive integers.
The CLI runs in the host plugin's invocation directory. There is no directory or destination option.

## Use `/lark`

Run `/lark` in a session to start watching. Startup returns before CLI readiness.
The watcher changes from `starting` to `watching` only after the exact CLI readiness marker.
Run `/lark` again in that session to stop, including during startup.
Run it in another existing session to switch destinations.
Switching cancels and awaits all pending and active work, but keeps the same Lark consumer.
An invalid destination leaves the existing watch unchanged.

OpenCode establishes the command's session before dispatch. From its home prompt, OpenCode can create that session normally.
The plugin never creates a session. It registers only a native server command, with no TUI extension or RPC.
The native command dispatch path does not require an idle session.

## Batching and delivery

Each conversation root is the pair `chatId` and `rootId`. A missing root ID defaults to the incoming message ID.
Each root has its own quiet-period timer and pending messages keyed by message ID.
A new pending ID resets that root's timer. A duplicate replaces its content without resetting the timer.
Arrivals after a batch detaches form a new batch, even while earlier work runs.

Each flushed batch starts an independent scoped worker when capacity is available.
At capacity, the watcher logs a warning and discards the batch. It never queues completed batches for later processing.

A worker fetches required history before calling native decisions.
It copies the configured input array and appends exactly one `{ "type": "json", "value": context }` part.
The context contains `root`, `incoming`, and `history`.
A successful selected score at or above the threshold queues JSON text with this shape:

```text
{
	context: { root, incoming, history: { messages, hasMore } },
	result: complete native DecisionResult
}
```

The plugin calls native `session.prompt` with `delivery: "queue"`.
It preserves the whole successful native result, including other answers' errors, cost, tokens, and native metadata.
The destination session decides what to do next. The watcher never replies to Lark.
Below-threshold scores skip normally. Missing, failed, or wrong-kind answers log and skip.
History, decision-call, and queue failures log and discard only that batch.

## History limits

History first fetches the root message to resolve its actual `thread_id`.
For a threaded root, it fetches one descending thread page of 50 messages.
For a standalone root, it fetches one recent chat page with `only_thread_root_messages=false`.
It keeps only the root and messages whose `root_id` matches, then includes the separately fetched root if absent.
History deduplicates message IDs and sorts by numeric creation time.

History contains at most 50 page messages plus the root. It is recent context, not a complete thread archive.
`hasMore` preserves the page's truncation flag. For a chat page, it does not claim that more matching replies exist.
Raw API `body.content` remains encoded text, which can differ from rendered incoming event content.
A failed history request prevents that batch's decision call.

## Ownership and accepted limits

```text
Host plugin scope
  Watcher and one lifecycle semaphore
  Session deletion listener
  Controller
    Watch scope
      One Lark CLI process group and stderr reader
      Replaceable work scope for one destination
        Root buffers and debounce timers
        Independent batch workers
```

Stop, source termination, destination deletion, and plugin disposal cancel and await watch resources.
The CLI process group receives graceful termination with no force-kill escalation.
Already admitted native prompts are not retracted.

There is one watch per plugin instance and host location. Results can arrive out of order.
Pending roots and messages are unbounded until they go quiet.
A continuously active root can wait indefinitely because there is no maximum-wait timer.
Completed batches at worker capacity are lost.
There are no retries, processing timeouts, persistence, or automatic restarts.

## Verify locally

Run `bun run check` for lint, typechecking, and tests.
The tests use Effect TestClock, Deferred, and Queue synchronization for watcher behavior.
CLI tests run real launcher and descendant processes through the Node child-process spawner.
They use a temporary fake `lark-cli`, with no authentication, Lark requests, or provider calls.

Native busy-command behavior is established from OpenCode's dispatch implementation.
These tests do not run an active provider turn or verify a real Lark account's permissions.
