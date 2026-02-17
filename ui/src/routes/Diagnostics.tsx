import { useState, useEffect, useCallback, useRef } from "react";
import { getDiagnostics } from "../api";
import { DiagnosticsResponse } from "../types/diagnostics";
import { ErrorCard } from "../components/common/ErrorCard";
import { LoadingState } from "../components/common/LoadingState";
import { CopyButton } from "../components/artifacts/CopyButton";

export default function Diagnostics() {
    const [data, setData] = useState<DiagnosticsResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<any>(null);
    const [isDebug, setIsDebug] = useState(false);
    const [lastUpdatedAt, setLastUpdatedAt] = useState<number | null>(null);
    const isFetchingRef = useRef(false);

    const fetchDiagnostics = useCallback(async (debug = false) => {
        if (isFetchingRef.current) {
            return;
        }
        isFetchingRef.current = true;
        setLoading(true);
        setError(null);
        try {
            const resp = await getDiagnostics(debug);
            setData(resp);
            setLastUpdatedAt(Date.now());
        } catch (err: any) {
            setError(err);
        } finally {
            setLoading(false);
            isFetchingRef.current = false;
        }
    }, []);

    useEffect(() => {
        fetchDiagnostics(isDebug);
    }, [fetchDiagnostics, isDebug]);

    const runtimeIndicators = data?.runtime_indicators;
    const retryPolicy = data?.retry_policy;
    const enabledFlags = data?.enabled_flags ?? {};
    const rawJsonSnapshot = data ? JSON.stringify(data, null, 2) : "";

    if (loading && !data) {
        return (
            <div className="panel">
                <div data-testid="diagnostics-loading-indicator" style={{ padding: "40px", textAlign: "center", color: "var(--muted)" }}>
                    <LoadingState message="Loading system diagnostics..." />
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ maxWidth: "800px", margin: "0 auto" }}>
                <header style={{ marginBottom: "24px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <h1>System Diagnostics</h1>
                    <button onClick={() => fetchDiagnostics(isDebug)} className="button-primary">
                        Retry
                    </button>
                </header>

                <ErrorCard
                    category={error.code}
                    message={error?.message || "Failed to load diagnostics"}
                    requestId={error.requestId}
                    detailsSafe={error.details}
                    onRetry={() => fetchDiagnostics(isDebug)}
                />
            </div>
        );
    }

    return (
        <div className="diagnostics-page">
            <header style={{ marginBottom: "32px", display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                <div>
                    <h1 style={{ margin: 0, fontSize: "1.75rem" }}>System Diagnostics</h1>
                    <p style={{ margin: "4px 0 0 0", color: "var(--muted)", fontSize: "0.9rem" }}>
                        Real-time runtime health and configuration check.
                    </p>
                    <p data-testid="diagnostics-last-updated" style={{ margin: "6px 0 0 0", color: "var(--muted)", fontSize: "0.8rem" }}>
                        Last updated: {lastUpdatedAt ? new Date(lastUpdatedAt).toLocaleString() : "Not yet"}
                    </p>
                </div>
                <div style={{ display: "flex", gap: "12px", alignItems: "center" }}>
                    <label style={{ display: "flex", alignItems: "center", gap: "8px", fontSize: "0.85rem", cursor: "pointer" }}>
                        <input
                            type="checkbox"
                            checked={isDebug}
                            onChange={(e) => setIsDebug(e.target.checked)}
                        />
                        Verbose / Diagnostic View
                    </label>
                    <button
                        onClick={() => fetchDiagnostics(isDebug)}
                        disabled={loading}
                        className="button-primary"
                        style={{ padding: "8px 20px" }}
                    >
                        {loading ? "Refreshing..." : "Refresh"}
                    </button>
                </div>
            </header>

            {data && (
                <div style={{ display: "grid", gap: "16px" }}>
                    {loading && (
                        <div
                            data-testid="diagnostics-refreshing-indicator"
                            style={{
                                padding: "10px 12px",
                                borderRadius: "8px",
                                background: "var(--surface-muted)",
                                border: "1px solid var(--border-muted)",
                                color: "var(--muted)",
                                fontSize: "0.85rem",
                            }}
                        >
                            Refreshing diagnostics...
                        </div>
                    )}
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(400px, 1fr))", gap: "24px" }}>
                    {/* Runtime Indicators */}
                    <div className="panel" style={{ padding: "24px" }}>
                        <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>Runtime Indicators</h3>
                        <div style={{ display: "grid", gap: "16px" }}>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between", alignItems: "center" }}>
                                <span style={{ color: "var(--muted)" }}>Avg Query Complexity</span>
                                <span style={{ fontWeight: 600, fontSize: "1.1rem" }}>
                                    {runtimeIndicators?.avg_query_complexity ?? "—"}
                                </span>
                            </div>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between", alignItems: "center" }}>
                                <span style={{ color: "var(--muted)" }}>Schema Cache Size</span>
                                <span style={{ fontWeight: 600 }}>
                                    {runtimeIndicators?.active_schema_cache_size != null
                                        ? `${runtimeIndicators.active_schema_cache_size} items`
                                        : "—"}
                                </span>
                            </div>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between", alignItems: "center" }}>
                                <span style={{ color: "var(--muted)" }}>Truncation Events (Recent)</span>
                                <span style={{ fontWeight: 600, color: (runtimeIndicators?.recent_truncation_event_count ?? 0) > 0 ? "var(--error)" : "inherit" }}>
                                    {runtimeIndicators?.recent_truncation_event_count ?? "—"}
                                </span>
                            </div>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between", alignItems: "center" }}>
                                <span style={{ color: "var(--muted)" }}>Last Schema Refresh</span>
                                <span style={{ fontSize: "0.85rem" }}>
                                    {runtimeIndicators?.last_schema_refresh_timestamp
                                        ? new Date(runtimeIndicators.last_schema_refresh_timestamp * 1000).toLocaleString()
                                        : "Never"}
                                </span>
                            </div>
                        </div>
                    </div>

                    {/* Configuration & Policy */}
                    <div className="panel" style={{ padding: "24px" }}>
                        <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>Configuration & Policy</h3>
                        <div style={{ display: "grid", gap: "12px" }}>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between" }}>
                                <span style={{ color: "var(--muted)" }}>Database Provider</span>
                                <span style={{ fontWeight: 600, textTransform: "capitalize" }}>{data.active_database_provider || "Unknown"}</span>
                            </div>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between" }}>
                                <span style={{ color: "var(--muted)" }}>Retry Policy</span>
                                <span>
                                    {retryPolicy?.mode ? `${retryPolicy.mode} (max ${retryPolicy.max_retries ?? 0})` : "—"}
                                </span>
                            </div>
                            <div style={{ display: "flex", justifySelf: "stretch", justifyContent: "space-between" }}>
                                <span style={{ color: "var(--muted)" }}>Schema Cache TTL</span>
                                <span>{data.schema_cache_ttl_seconds}s</span>
                            </div>
                        </div>

                        <div style={{ marginTop: "20px", paddingTop: "20px", borderTop: "1px solid var(--border)" }}>
                            <h4 style={{ margin: "12px 0", fontSize: "0.9rem", color: "var(--muted)" }}>Feature Flags</h4>
                            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "8px", fontSize: "0.8rem" }}>
                                {Object.entries(enabledFlags).map(([key, val]) => (
                                    <div key={key} style={{ display: "flex", alignItems: "center", gap: "6px" }}>
                                        <span style={{
                                            width: "8px",
                                            height: "8px",
                                            borderRadius: "50%",
                                            backgroundColor: val === true ? "var(--success)" : val === false ? "var(--muted)" : "var(--accent)"
                                        }} />
                                        <span style={{ wordBreak: "break-all" }}>{key.replace(/_/g, " ")}: {String(val)}</span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>

                    {/* Latency Breakdown (Debug only) */}
                    {data.debug?.latency_breakdown_ms && (
                        <div className="panel" style={{ padding: "24px", gridColumn: "1 / -1" }}>
                            <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>Latency Breakdown</h3>
                            <div style={{ overflowX: "auto" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
                                    <thead>
                                        <tr style={{ borderBottom: "1px solid var(--border)" }}>
                                            <th style={{ textAlign: "left", padding: "12px" }}>Stage</th>
                                            <th style={{ textAlign: "right", padding: "12px" }}>Latency (ms)</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {Object.entries(data.debug.latency_breakdown_ms).map(([stage, ms]) => (
                                            <tr key={stage} style={{ borderBottom: "1px solid var(--border-muted)" }}>
                                                <td style={{ padding: "12px", textTransform: "capitalize" }}>{stage.replace(/_/g, " ")}</td>
                                                <td style={{ padding: "12px", textAlign: "right", fontWeight: 600 }}>{ms.toFixed(2)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Raw Snapshot (Verbose only) */}
                    {isDebug && (
                        <div className="panel" style={{ padding: "24px", gridColumn: "1 / -1", backgroundColor: "var(--surface-muted)" }}>
                            <details data-testid="diagnostics-raw-json-details">
                                <summary data-testid="diagnostics-raw-json-summary" style={{ cursor: "pointer", fontWeight: 600 }}>
                                    Raw Diagnostic Snapshot
                                </summary>
                                <div style={{ display: "flex", justifyContent: "flex-end", margin: "12px 0" }}>
                                    <CopyButton text={rawJsonSnapshot} label="Copy JSON" />
                                </div>
                                <pre data-testid="diagnostics-raw-json" style={{ fontSize: "0.75rem", overflowX: "auto", margin: 0 }}>
                                    {rawJsonSnapshot}
                                </pre>
                            </details>
                        </div>
                    )}
                </div>
                </div>
            )}
        </div>
    );
}
