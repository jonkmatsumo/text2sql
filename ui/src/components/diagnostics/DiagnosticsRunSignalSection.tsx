import React from "react";
import { Link } from "react-router-dom";
import { Interaction } from "../../types/admin";
import { formatRelativeTime } from "../../utils/observability";

interface DiagnosticsRunSignalSectionProps {
    title: string;
    runs: Interaction[];
    pillLabel: string;
    pillClass: string;
    emptyMessage: string;
    testId: string;
}

export const DiagnosticsRunSignalSection: React.FC<DiagnosticsRunSignalSectionProps> = ({
    title,
    runs,
    pillLabel,
    pillClass,
    emptyMessage,
    testId,
}) => {
    return (
        <section data-testid={testId}>
            <h4 style={{ margin: "0 0 10px 0", fontSize: "0.95rem", fontWeight: 600 }}>{title}</h4>
            {runs.length === 0 ? (
                <p style={{ color: "var(--muted)", fontSize: "0.85rem", fontStyle: "italic", margin: 0 }}>
                    {emptyMessage}
                </p>
            ) : (
                <div style={{ display: "grid", gap: "12px" }}>
                    {runs.map((run) => (
                        <div
                            key={`${testId}-${run.id}`}
                            style={{
                                display: "flex",
                                justifyContent: "space-between",
                                alignItems: "center",
                                padding: "12px",
                                background: "var(--surface-muted)",
                                borderRadius: "10px",
                                border: "1px solid var(--border)",
                            }}
                        >
                            <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{ display: "flex", alignItems: "center", gap: "8px", marginBottom: "4px" }}>
                                    <span
                                        className={`pill text-xs font-bold ${pillClass}`}
                                        style={{ padding: "2px 8px", borderRadius: "12px" }}
                                    >
                                        {pillLabel}
                                    </span>
                                    <span style={{ fontSize: "0.75rem", color: "var(--muted)", fontFamily: "monospace" }}>
                                        {run.id?.slice(0, 8) ?? "Unknown"}
                                    </span>
                                    <span
                                        style={{
                                            fontSize: "0.7rem",
                                            fontWeight: 500,
                                            color: "var(--muted)",
                                            backgroundColor: "var(--surface)",
                                            padding: "1px 6px",
                                            borderRadius: "4px",
                                            border: "1px solid var(--border)",
                                        }}
                                        title={run.created_at ? new Date(run.created_at).toLocaleString() : "Timestamp unavailable"}
                                    >
                                        {run.created_at ? formatRelativeTime(run.created_at) : "unknown time"}
                                    </span>
                                </div>
                                <p style={{ margin: "0 0 4px 0", fontSize: "0.8rem", fontWeight: 600, color: "var(--ink)" }}>
                                    {run.created_at ? new Date(run.created_at).toLocaleString() : "—"}
                                </p>
                                <p
                                    style={{
                                        margin: 0,
                                        fontSize: "0.9rem",
                                        color: "var(--ink)",
                                        whiteSpace: "nowrap",
                                        overflow: "hidden",
                                        textOverflow: "ellipsis",
                                    }}
                                >
                                    {run.user_nlq_text || "(No query text available)"}
                                </p>
                            </div>
                            <Link
                                to={`/admin/runs/${run.id}`}
                                className="button-link"
                                style={{
                                    marginLeft: "16px",
                                    padding: "6px 12px",
                                    background: "var(--surface)",
                                    border: "1px solid var(--border)",
                                    borderRadius: "8px",
                                    fontSize: "0.85rem",
                                    textDecoration: "none",
                                    color: "var(--accent)",
                                    fontWeight: 500,
                                }}
                            >
                                Inspect
                            </Link>
                        </div>
                    ))}
                </div>
            )}
        </section>
    );
};
