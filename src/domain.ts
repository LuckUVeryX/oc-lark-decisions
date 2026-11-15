import { Generate } from "@opencode/schema/generate";
import { Session } from "@opencode/schema/session";
import { Effect, Schema } from "effect";

export const ChatId = Schema.NonEmptyString.pipe(Schema.brand("ChatId"));
export type ChatId = typeof ChatId.Type;
export const MessageId = Schema.NonEmptyString.pipe(Schema.brand("MessageId"));
export type MessageId = typeof MessageId.Type;

export const Root = Schema.Struct({ chatId: ChatId, rootId: MessageId });
export type Root = typeof Root.Type;
export const Message = Schema.Struct({
  id: MessageId,
  content: Schema.String,
  senderId: Schema.optionalKey(Schema.String),
  timestamp: Schema.optionalKey(Schema.String),
});
export type Message = typeof Message.Type;
export const Incoming = Schema.Struct({ root: Root, message: Message });
export type Incoming = typeof Incoming.Type;
export const Batch = Schema.Struct({ root: Root, messages: Schema.Array(Message) });
export type Batch = typeof Batch.Type;
export const History = Schema.Struct({ messages: Schema.Array(Message), hasMore: Schema.Boolean });
export type History = typeof History.Type;
export const ConversationContext = Schema.Struct({
  root: Root,
  incoming: Schema.Array(Message),
  history: History,
});
export type ConversationContext = typeof ConversationContext.Type;
export const Queued = Schema.Struct({ context: ConversationContext, result: Generate.DecisionResult });
export type Queued = typeof Queued.Type;
export type DecisionRequest = Generate.DecisionRequest;
export type DecisionResult = Generate.DecisionResult;

export const Options = Schema.Struct({
  decisions: Generate.DecisionRequest,
  forwarding: Schema.Struct({ question: Schema.NonEmptyString, threshold: Schema.Finite }),
  debounceMs: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(15_000)),
  ),
  maxWorkers: Schema.Int.check(Schema.isGreaterThan(0)).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(4)),
  ),
}).check(
  Schema.makeFilter(
    (options) =>
      options.decisions.questions.some(
        (question) => question.name === options.forwarding.question && question.type === "score",
      ) || "forwarding.question must name a configured score question",
  ),
);
export type Options = typeof Options.Type;

export const WatchStatus = Schema.Union([
  Schema.Struct({ state: Schema.Literal("stopped") }),
  Schema.Struct({ state: Schema.Literal("starting"), sessionID: Session.ID }),
  Schema.Struct({ state: Schema.Literal("watching"), sessionID: Session.ID }),
]);
export type WatchStatus = typeof WatchStatus.Type;
