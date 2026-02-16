import React, { useEffect, useState } from "react";
import { CopyButton } from "../artifacts/CopyButton";
import { getErrorMapping, type ErrorSeverity } from "../../utils/errorMapping";

export interface ErrorCardProps {
  category?: string;
  message: string;
  requestId?: string;
  hint?: string;
  retryable?: boolean;
  retryAfterSeconds?: number;
  detailsSafe?: Record<string, unknown>;
  onRetry?: () => void;
  actions?: Array<{ label: string; href: string }>;
}

const SEVERITY_STYLES: Record<ErrorSeverity, { bg: string; border: string; labelColor: string }> = {
  error: {
    bg: "rgba(220, 53, 69, 0.06)",
    border: "rgba(220, 53, 69, 0.2)",
    labelColor: "var(--error, #dc3545)",
  },
  warn: {
    bg: "rgba(255, 193, 7, 0.08)",
    border: "rgba(255, 193, 7, 0.3)",
    labelColor: "#856404",
  },
  info: {
    bg: "rgba(13, 110, 253, 0.06)",
    border: "rgba(13, 110, 253, 0.2)",
    labelColor: "#084298",
  },
};

export function ErrorCard({
  category,
  message,
  requestId,
  hint,
  retryable,
  retryAfterSeconds,
  detailsSafe,
  onRetry,
  actions,
}: ErrorCardProps) {
  const [countdown, setCountdown] = useState(retryAfterSeconds ?? 0);

  useEffect(() => {
    if (!retryable || !retryAfterSeconds || retryAfterSeconds <= 0) {
      setCountdown(0);
      return;
    }
    setCountdown(retryAfterSeconds);
    const interval = setInterval(() => {
      setCountdown((prev) => {
        if (prev <= 1) {
          clearInterval(interval);
          return 0;
        }
        return prev - 1;
      });
    }, 1000);
    return () => clearInterval(interval);
  }, [retryable, retryAfterSeconds]);

  const canRetry = retryable && countdown === 0;
  const mapping = getErrorMapping(category);
  const severity = SEVERITY_STYLES[mapping.severity];

  return (
    <div
      className="error-card"
      style={{
        padding: "16px",
        borderRadius: "10px",
        background: severity.bg,
        border: `1px solid ${severity.border}`,
        fontSize: "0.9rem",
      }}
    >
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", gap: "12px" }}>
        <div style={{ flex: 1 }}>
          <div
            data-testid="error-category"
            style={{
              fontSize: "0.75rem",
              fontWeight: 600,
              textTransform: "uppercase",
              letterSpacing: "0.05em",
              color: severity.labelColor,
              marginBottom: "4px",
            }}
          >
            {mapping.title}
          </div>
          <div style={{ color: "var(--ink)", fontWeight: 500 }}>{message}</div>
        </div>
        {requestId && (
          <div style={{ display: "flex", alignItems: "center", gap: "6px", fontSize: "0.75rem", color: "var(--muted)" }}>
            <span>ID: {requestId.slice(0, 8)}</span>
            <CopyButton text={requestId} label="Copy" />
          </div>
        )}
      </div>

      {hint && (
        <div
          data-testid="error-hint"
          style={{
            marginTop: "12px",
            padding: "10px 12px",
            borderRadius: "6px",
            background: "rgba(255, 193, 7, 0.1)",
            borderLeft: "3px solid #ffc107",
            fontSize: "0.85rem",
            color: "#856404",
          }}
        >
          {hint}
        </div>
      )}

      {detailsSafe && Object.keys(detailsSafe).length > 0 && (
        <details style={{ marginTop: "12px" }}>
          <summary style={{ cursor: "pointer", fontSize: "0.8rem", color: "var(--muted)" }}>
            Technical Details
          </summary>
          <pre
            style={{
              marginTop: "8px",
              padding: "10px",
              borderRadius: "6px",
              background: "var(--surface-muted, #f8f9fa)",
              fontSize: "0.75rem",
              overflow: "auto",
              maxHeight: "200px",
            }}
          >
            {JSON.stringify(detailsSafe, null, 2)}
          </pre>
        </details>
      )}

      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", marginTop: "12px" }}>
        {retryable && onRetry && (
          <button
            type="button"
            onClick={onRetry}
            disabled={!canRetry}
            data-testid="retry-button"
            style={{
              padding: "8px 16px",
              borderRadius: "8px",
              border: "1px solid var(--border)",
              background: canRetry ? "var(--accent, #6366f1)" : "var(--surface-muted, #e9ecef)",
              color: canRetry ? "#fff" : "var(--muted)",
              cursor: canRetry ? "pointer" : "not-allowed",
              fontWeight: 500,
              fontSize: "0.85rem",
            }}
          >
            {countdown > 0 ? `Retry in ${countdown}s` : "Retry"}
          </button>
        )}
        {actions?.map((action) => (
          <a
            key={action.href}
            href={action.href}
            data-testid="error-action-link"
            style={{
              padding: "8px 16px",
              borderRadius: "8px",
              border: "1px solid var(--border)",
              background: "var(--surface, #fff)",
              color: "var(--accent, #6366f1)",
              textDecoration: "none",
              fontWeight: 500,
              fontSize: "0.85rem",
            }}
          >
            {action.label}
          </a>
        ))}
      </div>
    </div>
  );
}
