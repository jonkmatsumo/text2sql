import React, { useMemo, useRef, useState } from "react";
import { runAgent, submitFeedback } from "../api";

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

function formatValue(value: any): string {
  if (value == null) return "—";
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
  const [question, setQuestion] = useState<string>("");
  const [messages, setMessages] = useState<Message[]>([]);
  const [isLoading, setIsLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [feedbackState, setFeedbackState] = useState<Record<string, string>>({});
  const threadIdRef = useRef<string>(crypto.randomUUID());

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
        tenantId,
        threadId: threadIdRef.current
      });

      setMessages((prev) => [
        ...prev,
        {
          role: "assistant",
          text: result.response,
          sql: result.sql,
          result: result.result,
          error: result.error,
          interactionId: result.interaction_id,
          fromCache: result.from_cache,
          vizSpec: result.viz_spec,
          traceId: result.trace_id
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
      await submitFeedback({ interactionId, thumb, comment });
      setFeedbackState((prev) => ({ ...prev, [interactionId]: "done" }));
    } catch (err: any) {
      setFeedbackState((prev) => ({
        ...prev,
        [interactionId]: `error:${err.message || "Failed"}`
      }));
    }
  };

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
        <div className="panel">
          <label>
            Tenant ID
            <input
              type="number"
              min="1"
              value={tenantId}
              onChange={(event) => setTenantId(Number(event.target.value))}
            />
          </label>
          <div className="meta">
            <span>Thread</span>
            <strong>{threadIdRef.current.slice(0, 8)}</strong>
          </div>
        </div>
      </header>

      <section className="chat">
        {messages.map((msg, idx) => (
          <article
            key={`${msg.role}-${idx}`}
            className={`bubble ${msg.role} animate-in`}
          >
            <div className="bubble-header">
              <span>{msg.role === "user" ? "You" : "Assistant"}</span>
              {msg.traceId ? <span className="trace">Trace {msg.traceId}</span> : null}
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
                <details>
                  <summary>Visualization Spec</summary>
                  <pre>{formatValue(msg.vizSpec)}</pre>
                </details>
              )}

              {msg.fromCache && <div className="pill">✓ From cache</div>}
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
                      👍 Helpful
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
                      👎 Needs work
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

        {isLoading && <div className="loading">Thinking…</div>}
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
          {isLoading ? "Running…" : "Run"}
        </button>
      </form>
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
