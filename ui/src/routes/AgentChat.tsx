import React, { Suspense, useMemo, useRef, useState } from "react";
import { runAgent, submitFeedback } from "../api";
import TraceLink from "../components/common/TraceLink";
import { useConfirmation } from "../hooks/useConfirmation";
import { ConfirmationDialog } from "../components/common/ConfirmationDialog";

const VegaChart = React.lazy(() => import("../components/common/VegaChart"));

interface Message {
  role: "user" | "assistant";
  text?: string;
  sql?: string;
  result?: any;
  error?: string;
  interactionId?: string;
  fromCache?: boolean;
  vizSpec?: any;
  traceId?: string;
}

const LLM_PROVIDERS = [
  { value: "openai", label: "OpenAI" },
  { value: "anthropic", label: "Anthropic" },
  { value: "google", label: "Google" }
];

const LLM_MODELS: Record<string, { value: string; label: string }[]> = {
  openai: [
    { value: "gpt-4o", label: "GPT-4o" },
    { value: "gpt-4o-mini", label: "GPT-4o Mini" },
    { value: "gpt-4-turbo", label: "GPT-4 Turbo" }
  ],
  anthropic: [
    { value: "claude-sonnet-4-20250514", label: "Claude Sonnet 4" },
    { value: "claude-3-5-sonnet-20241022", label: "Claude 3.5 Sonnet" },
    { value: "claude-3-5-haiku-20241022", label: "Claude 3.5 Haiku" }
  ],
  google: [
    { value: "gemini-2.0-flash", label: "Gemini 2.0 Flash" },
    { value: "gemini-1.5-pro", label: "Gemini 1.5 Pro" },
    { value: "gemini-1.5-flash", label: "Gemini 1.5 Flash" }
  ]
};

function formatValue(value: any): string {
  if (value == null) return "\u2014";
  if (typeof value === "string") return value;
  return JSON.stringify(value, null, 2);
}

function ResultsTable({ rows }: { rows: any[] }) {
  const columns = useMemo(() => {
    const keys = new Set<string>();
    rows.forEach((row) => Object.keys(row || {}).forEach((key) => keys.add(key)));
    return Array.from(keys);
  }, [rows]);

  if (!columns.length) {
    return <div className="empty">No structured rows returned.</div>;
  }

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col}>{col}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, idx) => (
            <tr key={idx}>
              {columns.map((col) => (
                <td key={col}>{formatValue(row?.[col])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AgentChat() {
  const [tenantId, setTenantId] = useState<number>(1);
  const [llmProvider, setLlmProvider] = useState<string>("openai");
  const [llmModel, setLlmModel] = useState<string>("gpt-4o");
  const [question, setQuestion] = useState<string>("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [feedbackState, setFeedbackState] = useState<Record<string, string>>({});
  const threadIdRef = useRef<string>(crypto.randomUUID());

  const availableModels = LLM_MODELS[llmProvider] || [];
  const { confirm, dialogProps } = useConfirmation();

  const handleProviderChange = (provider: string) => {
    setLlmProvider(provider);
    const models = LLM_MODELS[provider];
    if (models && models.length > 0) {
      setLlmModel(models[0].value);
    }
  };

  const handleClearHistory = async () => {
    if (messages.length === 0) return;
    const isConfirmed = await confirm({
      title: "Clear History",
      description: "Clear all messages and start a new conversation? This action cannot be undone.",
      confirmText: "Clear History",
      danger: true
    });
    if (!isConfirmed) return;

    setMessages([]);
    setFeedbackState({});
    setError(null);
    threadIdRef.current = crypto.randomUUID();
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!question.trim()) {
      return;
    }

    const prompt = question.trim();
    setQuestion("");
    setError(null);

    setMessages((prev) => [...prev, { role: "user", text: prompt }]);

    setIsLoading(true);
    try {
      const result = await runAgent({
        question: prompt,
        tenant_id: tenantId,
        thread_id: threadIdRef.current
      });

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: result.response ?? undefined,
          sql: result.sql ?? undefined,
          result: result.result,
          error: result.error ?? undefined,
          interactionId: result.interaction_id ?? undefined,
          fromCache: result.from_cache,
          vizSpec: result.viz_spec,
          traceId: result.trace_id ?? undefined
        }
      ]);
    } catch (err: any) {
      setError(err.message || "Failed to run agent.");
    } finally {
      setIsLoading(false);
    }
  };

  const handleFeedback = async (
    interactionId: string,
    thumb: "UP" | "DOWN",
    comment?: string
  ) => {
    try {
      await submitFeedback({ interaction_id: interactionId, thumb, comment });
      setFeedbackState((prev) => ({ ...prev, [interactionId]: "done" }));
    } catch (err: any) {
      setFeedbackState((prev) => ({
        ...prev,
        [interactionId]: `error:${err.message || "Failed"}`
      }));
    }
  };

  const mcpUrl = import.meta.env.VITE_AGENT_SERVICE_URL || "http://localhost:8081";

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Text2SQL</p>
          <h1>Ask in natural language. Get SQL with confidence.</h1>
          <p className="subtitle">
            This React client mirrors the Streamlit chat while the agent continues to run
            server-side.
          </p>
        </div>
      </header>

      <div style={{ display: "grid", gridTemplateColumns: "280px 1fr", gap: "32px" }}>
        {/* Sidebar */}
        <aside>
          <div className="panel" style={{ marginBottom: "16px" }}>
            <label>
              Tenant ID
              <input
                type="number"
                min="1"
                value={tenantId}
                onChange={(event) => setTenantId(Number(event.target.value))}
              />
            </label>
          </div>

          <div className="panel" style={{ marginBottom: "16px" }}>
            <label style={{ marginBottom: "12px" }}>
              LLM Provider
              <select
                value={llmProvider}
                onChange={(e) => handleProviderChange(e.target.value)}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  fontSize: "1rem",
                  marginTop: "6px"
                }}
              >
                {LLM_PROVIDERS.map((p) => (
                  <option key={p.value} value={p.value}>
                    {p.label}
                  </option>
                ))}
              </select>
            </label>

            <label style={{ marginTop: "16px" }}>
              Model
              <select
                value={llmModel}
                onChange={(e) => setLlmModel(e.target.value)}
                style={{
                  width: "100%",
                  padding: "10px 12px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  fontSize: "1rem",
                  marginTop: "6px"
                }}
              >
                {availableModels.map((m) => (
                  <option key={m.value} value={m.value}>
                    {m.label}
                  </option>
                ))}
              </select>
            </label>
          </div>

          <div className="panel" style={{ marginBottom: "16px" }}>
            <div style={{ fontSize: "0.75rem", textTransform: "uppercase", letterSpacing: "0.08em", color: "var(--muted)", marginBottom: "12px" }}>
              System Status
            </div>
            <div style={{ fontSize: "0.85rem", display: "grid", gap: "8px" }}>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>Provider</span>
                <strong>{LLM_PROVIDERS.find((p) => p.value === llmProvider)?.label}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>Model</span>
                <strong style={{ fontSize: "0.8rem" }}>{llmModel}</strong>
              </div>
              <div style={{ display: "flex", justifyContent: "space-between" }}>
                <span style={{ color: "var(--muted)" }}>MCP URL</span>
                <strong style={{ fontSize: "0.75rem", wordBreak: "break-all" }}>{mcpUrl.replace(/^https?:\/\//, "")}</strong>
              </div>
            </div>
          </div>

          <div className="panel">
            <div className="meta" style={{ marginTop: 0 }}>
              <span>Thread</span>
              <strong>{threadIdRef.current.slice(0, 8)}</strong>
            </div>
            <button
              type="button"
              onClick={handleClearHistory}
              disabled={messages.length === 0}
              style={{
                width: "100%",
                marginTop: "12px",
                padding: "10px",
                borderRadius: "8px",
                border: "1px solid var(--border)",
                background: messages.length === 0 ? "var(--surface-muted)" : "var(--surface)",
                cursor: messages.length === 0 ? "not-allowed" : "pointer",
                color: messages.length === 0 ? "var(--muted)" : "var(--ink)",
                fontWeight: 500
              }}
            >
              Clear History
            </button>
          </div>
        </aside>

        {/* Main chat area */}
        <main>
          <section className="chat">
            {messages.map((msg, idx) => (
              <article
                key={`${msg.role}-${idx}`}
                className={`bubble ${msg.role} animate-in`}
              >
                <div className="bubble-header">
                  <span>{msg.role === "user" ? "You" : "Assistant"}</span>
                  {(msg.traceId || msg.interactionId) && (
                    <TraceLink
                      traceId={msg.traceId}
                      interactionId={msg.interactionId}
                      variant="icon"
                    />
                  )}
                </div>
                <div className="bubble-body">
                  {msg.text && <p>{msg.text}</p>}
                  {msg.error && <p className="error">Error: {msg.error}</p>}

                  {msg.sql && (
                    <details>
                      <summary>Generated SQL</summary>
                      <pre>{msg.sql}</pre>
                    </details>
                  )}

                  {Array.isArray(msg.result) && <ResultsTable rows={msg.result} />}
                  {msg.result && !Array.isArray(msg.result) && (
                    <pre className="result-block">{formatValue(msg.result)}</pre>
                  )}

                  {msg.vizSpec && (
                    <div style={{ marginTop: "16px" }}>
                      <Suspense fallback={<div className="loading">Loading chart...</div>}>
                        <VegaChart spec={msg.vizSpec} />
                      </Suspense>
                    </div>
                  )}

                  {msg.fromCache && <div className="pill" style={{ marginTop: "8px" }}>From cache</div>}
                </div>

                {msg.role === "assistant" && msg.interactionId && (
                  <div className="feedback">
                    {feedbackState[msg.interactionId] === "done" ? (
                      <span className="pill">Feedback submitted</span>
                    ) : (
                      <>
                        <button
                          type="button"
                          onClick={() => handleFeedback(msg.interactionId!, "UP")}
                        >
                          Helpful
                        </button>
                        <button
                          type="button"
                          onClick={() =>
                            setFeedbackState((prev) => ({
                              ...prev,
                              [msg.interactionId!]: "comment"
                            }))
                          }
                        >
                          Needs work
                        </button>
                        {feedbackState[msg.interactionId] === "comment" && (
                          <FeedbackForm
                            onSubmit={(comment) =>
                              handleFeedback(msg.interactionId!, "DOWN", comment)
                            }
                          />
                        )}
                        {feedbackState[msg.interactionId]?.startsWith("error") && (
                          <span className="error">Feedback failed</span>
                        )}
                      </>
                    )}
                  </div>
                )}
              </article>
            ))}

            {isLoading && <div className="loading">Thinking...</div>}
            {error && <div className="error-banner">{error}</div>}
          </section>

          <form className="composer" onSubmit={handleSubmit}>
            <input
              type="text"
              placeholder="Ask a question about your data"
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
            />
            <button type="submit" disabled={isLoading}>
              {isLoading ? "Running..." : "Run"}
            </button>
          </form>
        </main>
      </div>
      <ConfirmationDialog {...dialogProps} />
    </>
  );
}

function FeedbackForm({ onSubmit }: { onSubmit: (comment: string) => void }) {
  const [value, setValue] = useState<string>("");

  return (
    <div className="feedback-form">
      <textarea
        value={value}
        placeholder="Tell us what went wrong"
        onChange={(event) => setValue(event.target.value)}
      />
      <button
        type="button"
        onClick={() => {
          onSubmit(value);
          setValue("");
        }}
      >
        Send feedback
      </button>
    </div>
  );
}
