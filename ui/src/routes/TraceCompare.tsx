import React, { useState } from "react";
import { useSearchParams } from "react-router-dom";

export default function TraceCompare() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialLeft = searchParams.get("left") || "";
  const initialRight = searchParams.get("right") || "";
  const [leftId, setLeftId] = useState(initialLeft);
  const [rightId, setRightId] = useState(initialRight);

  const handleSubmit = (event: React.FormEvent) => {
    event.preventDefault();
    const next = new URLSearchParams();
    if (leftId.trim()) next.set("left", leftId.trim());
    if (rightId.trim()) next.set("right", rightId.trim());
    setSearchParams(next, { replace: true });
  };

  return (
    <div className="page">
      <header className="hero">
        <div>
          <p className="kicker">Observability</p>
          <h1>Trace Comparison</h1>
          <p className="subtitle">Compare two traces side-by-side with aligned stages.</p>
        </div>
      </header>

      <div className="panel">
        <form onSubmit={handleSubmit} className="trace-compare__form">
          <label>
            Left trace ID
            <input
              type="text"
              value={leftId}
              onChange={(event) => setLeftId(event.target.value)}
              placeholder="Trace ID"
            />
          </label>
          <label>
            Right trace ID
            <input
              type="text"
              value={rightId}
              onChange={(event) => setRightId(event.target.value)}
              placeholder="Trace ID"
            />
          </label>
          <button type="submit">Compare</button>
        </form>
      </div>

      {!initialLeft || !initialRight ? (
        <div className="panel" style={{ textAlign: "center", color: "var(--muted)" }}>
          Enter two trace IDs to load the comparison.
        </div>
      ) : null}
    </div>
  );
}
