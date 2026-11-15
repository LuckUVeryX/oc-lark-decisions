export const settings = {
  decisions: {
    model: { providerID: "dummy", id: "dummy", variant: "test" },
    input: [
      { type: "text", text: "Classify this conversation." },
      { type: "json", value: { preserved: true } },
      { type: "file", data: "YWJj", mime: "text/plain", name: "rubric.txt" },
    ],
    questions: [
      {
        type: "score",
        name: "attention",
        instructions: "Choose a level.",
        levels: [{ label: "ignore" }, { label: "review" }, { label: "urgent" }],
      },
    ],
  },
  forwarding: { question: "attention", threshold: 1.4 },
};
