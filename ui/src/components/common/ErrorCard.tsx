import React, { useEffect, useState } from "react";
import { CopyButton } from "../artifacts/CopyButton";

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

const CATEGORY_LABELS: Record<string, string> = {
  auth: "Authentication Error",
  unauthorized: "Unauthorized",
  limit_exceeded: "Limit Exceeded",
  invalid_request: "Invalid Request",
  unsupported_capability: "Unsupported Capability",
  timeout: "Timeout",
  schema_drift: "Schema Mismatch",
  internal: "Internal Error",
  connectivity: "Connection Error",
  syntax: "SQL Syntax Error",
  deadlock: "Deadlock",
  serialization: "Serialization Error",
  throttling: "Rate Limited",
  resource_exhausted: "Resource Exhausted",
  budget_exceeded: "Budget Exceeded",
  transient: "Transient Error",
  dependency_failure: "Dependency Failure",
  mutation_blocked: "Mutation Blocked",
  tool_version_invalid: "Tool Version Invalid",
  tool_version_unsupported: "Tool Version Unsupported",
  tool_response_malformed: "Malformed Response",
  unknown: "Error",
};

function getCategoryLabel(category?: string): string {
  if (!category) return "Error";
  return CATEGORY_LABELS[category] || category.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

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

  return (
    <div
      className="error-card"
      style={{
        padding: "16px",
        borderRadius: "10px",
        background: "rgba(220, 53, 69, 0.06)",
        border: "1px solid rgba(220, 53, 69, 0.2)",
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
              color: "var(--error, #dc3545)",
              marginBottom: "4px",
            }}
          >
            {getCategoryLabel(category)}
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
