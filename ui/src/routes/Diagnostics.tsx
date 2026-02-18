import { useState, useEffect, useCallback, useRef } from "react";
import { getDiagnostics, OpsService } from "../api";
import { Link } from "react-router-dom";
import { Interaction } from "../types/admin";
import { DiagnosticsResponse } from "../types/diagnostics";
import { ErrorCard } from "../components/common/ErrorCard";
import { LoadingState } from "../components/common/LoadingState";
import { CopyButton } from "../components/artifacts/CopyButton";
import { formatTimestamp, toPrettyJson } from "../utils/observability";
import {
    DiagnosticsFilters,
    useDiagnosticsViewFilters,
} from "../components/diagnostics/DiagnosticsFilters";
import { DiagnosticsStatusStrip } from "../components/diagnostics/DiagnosticsStatusStrip";
import { DiagnosticsRunSignalSection } from "../components/diagnostics/DiagnosticsRunSignalSection";
import {
    getDiagnosticsAnomalies,
    getDiagnosticsStatus,
    normalizeNonNegativeMetric,
} from "../utils/diagnosticsStatus";

interface DiagnosticsError {
    code?: string;
    message?: string;
    requestId?: string;
    details?: Record<string, unknown>;
}

function readOptionalString(value: unknown): string | undefined {
    if (typeof value !== "string") return undefined;
    const trimmed = value.trim();
    return trimmed || undefined;
}

function formatMetricNumber(value: unknown, digits: number = 2): string {
    const normalized = normalizeNonNegativeMetric(value);
    if (normalized == null) return "—";
    return normalized.toFixed(digits).replace(/\.00$/, "").replace(/(\.\d*[1-9])0+$/, "$1");
}

function formatCountWithUnit(value: unknown, unit: string): string {
    const normalized = normalizeNonNegativeMetric(value);
    if (normalized == null) return "—";
    return `${Math.round(normalized)} ${unit}`;
}

function formatMilliseconds(value: unknown): string {
    const normalized = normalizeNonNegativeMetric(value);
    if (normalized == null) return "—";
    return normalized.toFixed(2);
}

export default function Diagnostics() {
    const {
        isDebug,
        setIsDebug,
        filterMode,
        setFilterMode,
        selectedSection,
        setSelectedSection,
    } = useDiagnosticsViewFilters();
    const [data, setData] = useState<DiagnosticsResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<DiagnosticsError | null>(null);
    const [recentFailures, setRecentFailures] = useState<Interaction[]>([]);
    const [recentLowRatings, setRecentLowRatings] = useState<Interaction[]>([]);
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
        } catch (err: unknown) {
            if (err && typeof err === "object") {
                setError(err as DiagnosticsError);
            } else {
                setError({ message: "Failed to load diagnostics" });
            }
        } finally {
            setLoading(false);
            isFetchingRef.current = false;
        }

        // Fetch degraded runs concurrently (failed + negatively rated)
        try {
            const [failed, negative] = await Promise.all([
                OpsService.listRuns(5, 0, "FAILED"),
                OpsService.listRuns(5, 0, "All", "DOWN"),
            ]);
            const normalizeCategory = (runs: Interaction[]) => {
                const seenIds = new Set<string>();
                return runs
                    .filter((run) => Boolean(run.created_at))
                    .filter((run) => {
                        if (seenIds.has(run.id)) return false;
                        seenIds.add(run.id);
                        return true;
                    })
                    .map((run) => ({
                        ...run,
                        user_nlq_text: run.user_nlq_text || "Unknown",
                        execution_status: run.execution_status || "UNKNOWN",
                    }))
                    .sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())
                    .slice(0, 5);
            };

            setRecentFailures(normalizeCategory(failed));
            setRecentLowRatings(normalizeCategory(negative));
        } catch (err) {
            console.error("Failed to load degraded runs", err);
        }
    }, []);

    useEffect(() => {
        fetchDiagnostics(isDebug);
    }, [fetchDiagnostics, isDebug]);

    const runtimeIndicators = data?.runtime_indicators;
    const retryPolicy = data?.retry_policy;
    const enabledFlags = data?.enabled_flags ?? {};
    const rawJsonSnapshot = toPrettyJson(data);
    const anomalies = getDiagnosticsAnomalies(data);
    const anomalyIds = new Set(anomalies.map((item) => item.id));
    const diagnosticsStatus = getDiagnosticsStatus(data);

    const runtimeRows = [
        {
            id: "avg_query_complexity",
            label: "Avg Query Complexity",
            value: formatMetricNumber(runtimeIndicators?.avg_query_complexity),
            isAnomaly: anomalyIds.has("avg_query_complexity"),
        },
        {
            id: "active_schema_cache_size",
            label: "Schema Cache Size",
            value: formatCountWithUnit(runtimeIndicators?.active_schema_cache_size, "items"),
            isAnomaly: anomalyIds.has("active_schema_cache_size"),
        },
        {
            id: "recent_truncation_event_count",
            label: "Truncation Events (Recent)",
            value: formatMetricNumber(runtimeIndicators?.recent_truncation_event_count, 0),
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

    const latencyRows = Object.entries(data?.debug?.latency_breakdown_ms ?? {}).map(([stage, ms]) => ({
        stage,
        value: normalizeNonNegativeMetric(ms),
    }));
    const visibleLatencyRows =
        filterMode === "anomalies"
            ? latencyRows.filter((item) => anomalyIds.has(`latency:${item.stage}`))
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
    const selectedPanelPayload =
        selectedSection === "runtime"
            ? {
                section: "runtime",
                runtime_indicators: data?.runtime_indicators ?? null,
                anomalies,
            }
            : selectedSection === "config"
                ? {
                    section: "config",
                    active_database_provider: data?.active_database_provider ?? null,
                    retry_policy: data?.retry_policy ?? null,
                    schema_cache_ttl_seconds: data?.schema_cache_ttl_seconds ?? null,
                    enabled_flags: data?.enabled_flags ?? {},
                }
                : selectedSection === "latency"
                    ? {
                        section: "latency",
                        latency_breakdown_ms: data?.debug?.latency_breakdown_ms ?? {},
                    }
                    : selectedSection === "raw"
                        ? { section: "raw", diagnostics: data }
                        : {
                            section: "all",
                            status: diagnosticsStatus,
                            anomalies,
                            diagnostics: data,
                        };
    const selectedPanelJson = toPrettyJson(selectedPanelPayload);
    const diagnosticsTraceId =
        readOptionalString(data?.trace_id) ??
        readOptionalString(data?.debug?.trace_id);
    const diagnosticsInteractionId =
        readOptionalString(data?.interaction_id) ??
        readOptionalString(data?.debug?.interaction_id);
    const diagnosticsRequestId =
        readOptionalString(data?.request_id) ??
        readOptionalString(data?.debug?.request_id);
    const schemaCacheTtlSeconds = normalizeNonNegativeMetric(data?.schema_cache_ttl_seconds);
    const schemaCacheTtlDisplay = schemaCacheTtlSeconds == null ? "—" : `${schemaCacheTtlSeconds}s`;

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
                    <DiagnosticsStatusStrip
                        status={diagnosticsStatus}
                        anomalyCount={anomalies.length}
                        filterMode={filterMode}
                        onFilterModeChange={setFilterMode}
                    >
                        <DiagnosticsFilters
                            selectedSection={selectedSection}
                            onSelectedSectionChange={setSelectedSection}
                            selectedPanelJson={selectedPanelJson}
                            identifiers={{
                                traceId: diagnosticsTraceId,
                                interactionId: diagnosticsInteractionId,
                                requestId: diagnosticsRequestId,
                            }}
                        />
                    </DiagnosticsStatusStrip>
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
                                        <span>{schemaCacheTtlDisplay}</span>
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
                                                {visibleLatencyRows.map((item) => (
                                                    <tr
                                                        key={item.stage}
                                                        style={{
                                                            borderBottom: "1px solid var(--border-muted)",
                                                            background: anomalyIds.has(`latency:${item.stage}`) ? "rgba(245, 158, 11, 0.08)" : undefined,
                                                        }}
                                                    >
                                                        <td style={{ padding: "12px", textTransform: "capitalize" }}>{item.stage.replace(/_/g, " ")}</td>
                                                        <td style={{ padding: "12px", textAlign: "right", fontWeight: 600 }}>
                                                            {formatMilliseconds(item.value)}
                                                        </td>
                                                    </tr>
                                                ))}
                                            </tbody>
                                        </table>
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Recent Degraded Runs Panel */}
                        <div className="panel" style={{ padding: "24px", gridColumn: "1 / -1" }}>
                            <h3 style={{ marginTop: 0, marginBottom: "20px", fontSize: "1.1rem" }}>
                                Recent Run Signals
                                {filterMode === "anomalies" && (
                                    <span style={{ fontSize: "0.8rem", fontWeight: 400, color: "var(--muted)", marginLeft: "8px", verticalAlign: "middle" }}>
                                        (Global status — non-anomaly derived)
                                    </span>
                                )}
                            </h3>
                            <p style={{ marginTop: "-12px", marginBottom: "16px", color: "var(--muted)", fontSize: "0.8rem" }}>
                                Showing latest 5 per category.
                            </p>
                            <div style={{ display: "grid", gap: "20px" }}>
                                <DiagnosticsRunSignalSection
                                    title="Recent failures"
                                    runs={recentFailures}
                                    pillLabel="FAILED"
                                    pillClass="bg-red-100 text-red-800"
                                    emptyMessage="No recent failures found."
                                    testId="diagnostics-failures-section"
                                />

                                <DiagnosticsRunSignalSection
                                    title="Recent low ratings"
                                    runs={recentLowRatings}
                                    pillLabel="LOW_RATING"
                                    pillClass="bg-amber-100 text-amber-800"
                                    emptyMessage="No recent low ratings found."
                                    testId="diagnostics-low-ratings-section"
                                />

                                <div style={{ marginTop: "4px", textAlign: "right" }}>
                                    <Link to="/admin/runs" style={{ fontSize: "0.85rem", color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
                                        View Full History &rarr;
                                    </Link>
                                </div>
                            </div>
                        </div>

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
