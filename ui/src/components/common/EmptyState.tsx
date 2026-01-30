import React from "react";

interface EmptyStateProps {
  title?: string;
  description?: string;
  action?: React.ReactNode;
}

export function EmptyState({
  title = "No data found",
  description,
  action,
}: EmptyStateProps) {
  return (
    <div
      style={{
        padding: "48px 24px",
        textAlign: "center",
        color: "var(--muted)",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        gap: "12px",
      }}
    >
      <div style={{ fontWeight: 600, color: "var(--ink)" }}>{title}</div>
      {description && <div style={{ maxWidth: 400 }}>{description}</div>}
      {action && <div style={{ marginTop: "12px" }}>{action}</div>}
    </div>
  );
}
