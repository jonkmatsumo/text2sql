import { useState, useEffect, useCallback, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import { getDiagnostics } from "../api";
import { DiagnosticsResponse } from "../types/diagnostics";
import { ErrorCard } from "../components/common/ErrorCard";
import { LoadingState } from "../components/common/LoadingState";
import { CopyButton } from "../components/artifacts/CopyButton";
import { formatTimestamp, toPrettyJson } from "../utils/observability";
import {
    getDiagnosticsAnomalies,
    getDiagnosticsStatus,
} from "../utils/diagnosticsStatus";

type DiagnosticsSection = "all" | "runtime" | "config" | "latency" | "raw";

function parseSection(raw: string | null): DiagnosticsSection {
    if (raw === "runtime" || raw === "config" || raw === "latency" || raw === "raw") {
        return raw;
    }
    return "all";
}

export default function Diagnostics() {
    const [searchParams, setSearchParams] = useSearchParams();
    const [data, setData] = useState<DiagnosticsResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<any>(null);
    const [isDebug, setIsDebug] = useState(() => searchParams.get("debug") === "1");
    const [filterMode, setFilterMode] = useState<"all" | "anomalies">(
        () => (searchParams.get("filter") === "anomalies" ? "anomalies" : "all")
    );
    const [selectedSection, setSelectedSection] = useState<DiagnosticsSection>(
        () => parseSection(searchParams.get("section"))
    );
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

    useEffect(() => {
        const nextParams = new URLSearchParams(searchParams);
        if (isDebug) nextParams.set("debug", "1");
        else nextParams.delete("debug");

        if (filterMode === "anomalies") nextParams.set("filter", "anomalies");
        else nextParams.delete("filter");

        if (selectedSection !== "all") nextParams.set("section", selectedSection);
        else nextParams.delete("section");

        if (nextParams.toString() !== searchParams.toString()) {
            setSearchParams(nextParams, { replace: true });
        }
    }, [isDebug, filterMode, searchParams, selectedSection, setSearchParams]);

    const runtimeIndicators = data?.runtime_indicators;
    const retryPolicy = data?.retry_policy;
    const enabledFlags = data?.enabled_flags ?? {};
    const rawJsonSnapshot = toPrettyJson(data);
    const anomalies = getDiagnosticsAnomalies(data);
    const anomalyIds = new Set(anomalies.map((item) => item.id));
    const diagnosticsStatus = getDiagnosticsStatus(data);
    const statusLabel =
        diagnosticsStatus === "healthy"
            ? "Healthy"
            : diagnosticsStatus === "degraded"
                ? "Degraded"
                : "Unknown";
    const statusColor =
        diagnosticsStatus === "healthy"
            ? "var(--success)"
            : diagnosticsStatus === "degraded"
                ? "#f59e0b"
                : "var(--muted)";

    const runtimeRows = [
        {
            id: "avg_query_complexity",
            label: "Avg Query Complexity",
            value: runtimeIndicators?.avg_query_complexity ?? "—",
            isAnomaly: anomalyIds.has("avg_query_complexity"),
        },
        {
            id: "active_schema_cache_size",
            label: "Schema Cache Size",
            value: runtimeIndicators?.active_schema_cache_size != null
                ? `${runtimeIndicators.active_schema_cache_size} items`
                : "—",
            isAnomaly: anomalyIds.has("active_schema_cache_size"),
        },
        {
            id: "recent_truncation_event_count",
            label: "Truncation Events (Recent)",
            value: runtimeIndicators?.recent_truncation_event_count ?? "—",
            isAnomaly: anomalyIds.has("recent_truncation_event_count"),
        },
        {
            id: "last_schema_refresh_timestamp",
            label: "Last Schema Refresh",
            value: formatTimestamp(runtimeIndicators?.last_schema_refresh_timestamp, {
                inputInSeconds: true,
                fallback: "Never",
            }),
            isAnomaly: false,
        },
    ];
    const visibleRuntimeRows =
        filterMode === "anomalies"
            ? runtimeRows.filter((item) => item.isAnomaly)
            : runtimeRows;

    const latencyRows = Object.entries(data?.debug?.latency_breakdown_ms ?? {});
    const visibleLatencyRows =
        filterMode === "anomalies"
            ? latencyRows.filter(([stage]) => anomalyIds.has(`latency:${stage}`))
            : latencyRows;
    const showRuntimePanel = selectedSection === "all" || selectedSection === "runtime";
    const showConfigPanel =
        filterMode === "all" &&
        (selectedSection === "all" || selectedSection === "config");
    const showLatencyPanel =
        Boolean(data?.debug?.latency_breakdown_ms) &&
        (selectedSection === "all" || selectedSection === "latency");
    const showRawPanel =
        isDebug &&
        filterMode === "all" &&
        (selectedSection === "all" || selectedSection === "raw");

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
                        Last updated: {formatTimestamp(lastUpdatedAt, { fallback: "Not yet" })}
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
                    <div
                        data-testid="diagnostics-status-strip"
                        className="panel"
                        style={{
                            padding: "14px 16px",
                            display: "flex",
                            alignItems: "center",
                            justifyContent: "space-between",
                            gap: "12px",
                            flexWrap: "wrap",
                        }}
                    >
                        <div style={{ display: "flex", alignItems: "center", gap: "8px", flexWrap: "wrap" }}>
                            <span
                                style={{
                                    width: "10px",
                                    height: "10px",
                                    borderRadius: "50%",
                                    background: statusColor,
                                }}
                            />
                            <strong>System Status: {statusLabel}</strong>
                            <span style={{ color: "var(--muted)", fontSize: "0.85rem" }}>
                                {anomalies.length} {anomalies.length === 1 ? "anomaly" : "anomalies"} detected
                            </span>
                        </div>
                        <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
                            <label
                                style={{
                                    display: "flex",
                                    alignItems: "center",
                                    gap: "6px",
                                    color: "var(--muted)",
                                    fontSize: "0.82rem",
                                    marginRight: "8px",
                                }}
                            >
                                Section
                                <select
                                    data-testid="diagnostics-section-select"
                                    value={selectedSection}
                                    onChange={(event) => setSelectedSection(parseSection(event.target.value))}
                                    style={{
                                        borderRadius: "8px",
                                        border: "1px solid var(--border)",
                                        padding: "4px 8px",
                                        fontSize: "0.82rem",
                                    }}
                                >
                                    <option value="all">All</option>
                                    <option value="runtime">Runtime</option>
                                    <option value="config">Config</option>
                                    <option value="latency">Latency</option>
                                    <option value="raw">Raw JSON</option>
                                </select>
                            </label>
                            <button
                                type="button"
                                data-testid="diagnostics-filter-anomalies"
                                onClick={() => setFilterMode("anomalies")}
                                className="button-primary"
                                style={{
                                    opacity: filterMode === "anomalies" ? 1 : 0.75,
                                    padding: "6px 12px",
                                }}
                            >
                                Show only anomalies
                            </button>
                            <button
                                type="button"
                                data-testid="diagnostics-filter-all"
                                onClick={() => setFilterMode("all")}
                                className="button-primary"
                                style={{
                                    opacity: filterMode === "all" ? 1 : 0.75,
                                    padding: "6px 12px",
                                }}
                            >
                                Show all
                            </button>
                        </div>
                    </div>
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
                    {showRuntimePanel && (
                    <div className="panel" style={{ padding: "24px" }}>
                        <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>Runtime Indicators</h3>
                        {visibleRuntimeRows.length === 0 ? (
                            <div style={{ color: "var(--muted)", fontSize: "0.85rem" }}>
                                No runtime anomalies detected.
                            </div>
                        ) : (
                            <div style={{ display: "grid", gap: "16px" }}>
                                {visibleRuntimeRows.map((row) => (
                                    <div
                                        key={row.id}
                                        style={{
                                            display: "flex",
                                            justifySelf: "stretch",
                                            justifyContent: "space-between",
                                            alignItems: "center",
                                            padding: row.isAnomaly ? "6px 8px" : undefined,
                                            borderRadius: row.isAnomaly ? "8px" : undefined,
                                            background: row.isAnomaly ? "rgba(245, 158, 11, 0.08)" : undefined,
                                            border: row.isAnomaly ? "1px solid rgba(245, 158, 11, 0.25)" : undefined,
                                        }}
                                    >
                                        <span style={{ color: "var(--muted)" }}>{row.label}</span>
                                        <span style={{ fontWeight: 600, fontSize: row.id === "avg_query_complexity" ? "1.1rem" : undefined }}>
                                            {row.value}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                    )}

                    {/* Configuration & Policy */}
                    {showConfigPanel && (
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
                    )}

                    {/* Latency Breakdown (Debug only) */}
                    {showLatencyPanel && (
                        <div className="panel" style={{ padding: "24px", gridColumn: "1 / -1" }}>
                            <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>Latency Breakdown</h3>
                            {visibleLatencyRows.length === 0 && filterMode === "anomalies" ? (
                                <div style={{ color: "var(--muted)", fontSize: "0.85rem" }}>
                                    No latency anomalies detected.
                                </div>
                            ) : (
                                <div style={{ overflowX: "auto" }}>
                                    <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.9rem" }}>
                                        <thead>
                                            <tr style={{ borderBottom: "1px solid var(--border)" }}>
                                                <th style={{ textAlign: "left", padding: "12px" }}>Stage</th>
                                                <th style={{ textAlign: "right", padding: "12px" }}>Latency (ms)</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            {visibleLatencyRows.map(([stage, ms]) => (
                                                <tr
                                                    key={stage}
                                                    style={{
                                                        borderBottom: "1px solid var(--border-muted)",
                                                        background: anomalyIds.has(`latency:${stage}`) ? "rgba(245, 158, 11, 0.08)" : undefined,
                                                    }}
                                                >
                                                    <td style={{ padding: "12px", textTransform: "capitalize" }}>{stage.replace(/_/g, " ")}</td>
                                                    <td style={{ padding: "12px", textAlign: "right", fontWeight: 600 }}>{ms.toFixed(2)}</td>
                                                </tr>
                                            ))}
                                        </tbody>
                                    </table>
                                </div>
                            )}
                        </div>
                    )}

                    {/* Raw Snapshot (Verbose only) */}
                    {showRawPanel && (
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
