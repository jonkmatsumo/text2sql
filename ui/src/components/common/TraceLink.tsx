import React, { useState } from "react";

export interface TraceLinkProps {
  traceId?: string;
  interactionId?: string;
  variant?: "icon" | "button" | "text";
  showCopy?: boolean;
}

const TRACE_ID_RE = /^[0-9a-f]{32}$/i;

export default function TraceLink({
  traceId,
  interactionId,
  variant = "icon",
  showCopy = true
}: TraceLinkProps) {
  const [copied, setCopied] = useState(false);
  const isValidTrace = traceId ? TRACE_ID_RE.test(traceId) : false;
  const href = isValidTrace
    ? `/traces/${traceId}`
    : interactionId
      ? `/traces/interaction/${interactionId}`
      : null;

  const handleCopy = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!traceId) return;
    try {
      await navigator.clipboard.writeText(traceId);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } catch {
      const textArea = document.createElement("textarea");
      textArea.value = traceId;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand("copy");
      document.body.removeChild(textArea);
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }
  };

  if (variant === "text") {
    return (
      <span className="trace-link trace-link--text">
        {href ? (
          <a href={href} onClick={(e) => e.stopPropagation()}>
            {(traceId || interactionId || "").slice(0, 8)}...
          </a>
        ) : (
          <span>{(traceId || interactionId || "").slice(0, 8)}...</span>
        )}
        {showCopy && traceId && (
          <button
            type="button"
            onClick={handleCopy}
            className="trace-link__copy"
            title="Copy trace ID"
          >
            {copied ? "Copied!" : "Copy"}
          </button>
        )}
      </span>
    );
  }

  if (variant === "button") {
    return (
      <span className="trace-link trace-link--button">
        {href ? (
          <a
            href={href}
            className="trace-link__btn"
            onClick={(e) => e.stopPropagation()}
          >
            View Trace
          </a>
        ) : (
          <span className="trace-link__btn trace-link__btn--disabled">Trace Unavailable</span>
        )}
        {showCopy && traceId && (
          <button
            type="button"
            onClick={handleCopy}
            className="trace-link__copy-btn"
            title="Copy trace ID"
          >
            {copied ? "Copied!" : "Copy ID"}
          </button>
        )}
      </span>
    );
  }

  // Default: icon variant
  return (
    <span className="trace-link trace-link--icon">
      {href ? (
        <a
          href={href}
          className="trace-link__icon"
          title={`View trace ${traceId || interactionId}`}
          onClick={(e) => e.stopPropagation()}
        >
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <polyline points="15 3 21 3 21 9" />
            <line x1="10" y1="14" x2="21" y2="3" />
          </svg>
        </a>
      ) : (
        <span className="trace-link__icon trace-link__icon--disabled" title="Trace unavailable">
          <svg
            width="16"
            height="16"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            strokeLinecap="round"
            strokeLinejoin="round"
          >
            <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
            <polyline points="15 3 21 3 21 9" />
            <line x1="10" y1="14" x2="21" y2="3" />
          </svg>
        </span>
      )}
      {showCopy && traceId && (
        <button
          type="button"
          onClick={handleCopy}
          className="trace-link__copy-icon"
          title="Copy trace ID"
        >
          {copied ? (
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <polyline points="20 6 9 17 4 12" />
            </svg>
          ) : (
            <svg
              width="14"
              height="14"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
              strokeLinecap="round"
              strokeLinejoin="round"
            >
              <rect x="9" y="9" width="13" height="13" rx="2" ry="2" />
              <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1" />
            </svg>
          )}
        </button>
      )}
    </span>
  );
}
