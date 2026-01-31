import React from "react";

interface ErrorStateProps {
  error: unknown;
  onRetry?: () => void;
}

export function ErrorState({ error, onRetry }: ErrorStateProps) {
  const errorMessage =
    error instanceof Error ? error.message : String(error || "Unknown error");

  return (
    <div className="error-banner" style={{ margin: "24px 0" }}>
      <div style={{ fontWeight: 600, marginBottom: "4px" }}>
        Something went wrong
      </div>
      <div>{errorMessage}</div>
      {onRetry && (
        <button
          onClick={onRetry}
          style={{
            marginTop: "12px",
            background: "transparent",
            border: "1px solid currentColor",
            padding: "4px 12px",
            borderRadius: "4px",
            cursor: "pointer",
            color: "inherit",
          }}
        >
          Try Again
        </button>
      )}
    </div>
  );
}
