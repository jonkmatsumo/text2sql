import React, { useEffect, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { resolveTraceByInteraction } from "../api";

export default function TraceResolver() {
  const { interactionId } = useParams();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!interactionId) return;
    resolveTraceByInteraction(interactionId)
      .then((traceId) => {
        navigate(`/traces/${traceId}?interactionId=${interactionId}`, { replace: true });
      })
      .catch((err) => {
        setError(err.message || "Failed to resolve trace.");
      });
  }, [interactionId, navigate]);

  return (
    <div className="panel">
      <h2>Resolving trace...</h2>
      {error && <p className="error">{error}</p>}
      {!error && <p className="subtitle">Looking up trace for this interaction.</p>}
    </div>
  );
}
