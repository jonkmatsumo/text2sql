import React, { useState } from "react";
import { Link } from "react-router-dom";
import Tabs from "../components/common/Tabs";
import TraceLink from "../components/common/TraceLink";
import { OpsService } from "../api";
import { PatternReloadResult } from "../types/admin";
import { useToast } from "../hooks/useToast";
import { grafanaBaseUrl, isGrafanaConfigured } from "../config";

export default function SystemOperations() {
    const [activeTab, setActiveTab] = useState("nlp");
    const [logs, setLogs] = useState<string[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [reloadResult, setReloadResult] = useState<PatternReloadResult | null>(null);
    const [traceId, setTraceId] = useState("");

    const { show: showToast } = useToast();

    const tabs = [
        { id: "nlp", label: "NLP Patterns" },
        { id: "schema", label: "Schema" },
        { id: "cache", label: "Semantic Cache" },
        { id: "obs", label: "Observability" }
    ];

    const addLog = (msg: string) => setLogs((prev) => [...prev, `${new Date().toLocaleTimeString()} - ${msg}`]);

    const runPatternGen = async () => {
        setIsLoading(true);
        setLogs(["Starting pattern generation..."]);
        try {
            const result = await OpsService.generatePatterns(false);
            if (result.success) {
                addLog(`Generation complete (ID: ${result.run_id})`);
                if (result.metrics) {
                    addLog(`Created: ${result.metrics.created_count}, Updated: ${result.metrics.updated_count}`);
                }
                showToast("Pattern generation completed successfully", "success");
            } else {
                addLog(`Generation failed: ${result.error}`);
                showToast("Pattern generation failed", "error");
            }
        } catch (err: any) {
            addLog(`Error: ${err.message}`);
            showToast("Pattern generation failed", "error");
        } finally {
            setIsLoading(false);
        }
    };

    const runReload = async () => {
        setIsLoading(true);
        try {
            const result = await OpsService.reloadPatterns();
            setReloadResult(result);
            if (result.success) {
                showToast("Patterns reloaded successfully", "success");
            } else {
                showToast("Pattern reload failed", "error");
            }
        } catch (err: any) {
            showToast("Reload failed", "error");
        } finally {
            setIsLoading(false);
        }
    };

    const handleTraceView = () => {
        if (!traceId.trim()) {
            showToast("Please enter a trace ID", "warning");
            return;
        }
    };

    return (
        <>
            <header className="hero">
                <div>
                    <p className="kicker">Maintenance & Ops</p>
                    <h1>System Operations</h1>
                    <p className="subtitle">
                        Perform maintenance tasks and update system state across the stack.
                    </p>
                </div>
            </header>

            <Tabs tabs={tabs} activeTab={activeTab} onChange={setActiveTab} />

            <div style={{ display: "grid", gap: "24px" }}>
                {activeTab === "nlp" && (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "24px" }}>
                        <div className="panel">
                            <h3>Generate Patterns</h3>
                            <p className="subtitle">Refresh entity patterns using DB values and LLM synonyms.</p>
                            <button
                                className="feedback button"
                                onClick={runPatternGen}
                                disabled={isLoading}
                                style={{ width: "100%", background: "var(--accent)", color: "#fff", border: "none", padding: "12px", borderRadius: "10px", cursor: "pointer", marginTop: "12px" }}
                            >
                                Run Generation
                            </button>
                            {logs.length > 0 && (
                                <div style={{ marginTop: "20px", padding: "12px", backgroundColor: "#1e1e20", color: "#fefefe", borderRadius: "12px", fontSize: "0.85rem", fontFamily: "monospace", maxHeight: "200px", overflow: "auto" }}>
                                    {logs.map((log, i) => <div key={i}>{log}</div>)}
                                </div>
                            )}
                        </div>

                        <div className="panel">
                            <h3>Reload Backend</h3>
                            <p className="subtitle">Reload NLP models into memory without system restart.</p>
                            <button
                                className="feedback button"
                                onClick={runReload}
                                disabled={isLoading}
                                style={{ width: "100%", background: "var(--accent)", color: "#fff", border: "none", padding: "12px", borderRadius: "10px", cursor: "pointer", marginTop: "12px" }}
                            >
                                Reload Patterns
                            </button>

                            {reloadResult && (
                                <div style={{ marginTop: "20px" }}>
                                    <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "8px" }}>
                                        <strong>Status</strong>
                                        <span style={{ color: reloadResult.success ? "#10b981" : "#ef4444" }}>{reloadResult.success ? "Success" : "Failed"}</span>
                                    </div>
                                    <div style={{ fontSize: "0.9rem", color: "var(--muted)" }}>
                                        Count: {reloadResult.pattern_count} | Timing: {reloadResult.duration_ms}ms
                                    </div>
                                </div>
                            )}
                        </div>
                    </div>
                )}

                {activeTab === "schema" && (
                    <div className="panel" style={{ textAlign: "center", padding: "40px" }}>
                        <h3 style={{ margin: 0 }}>Schema Hydration</h3>
                        <p className="subtitle" style={{ margin: "12px auto 24px" }}>Propagate structural metadata from Postgres to the graph index.</p>
                        <div style={{
                            display: "inline-block",
                            padding: "10px 20px",
                            background: "var(--surface-muted)",
                            borderRadius: "8px",
                            color: "var(--muted)",
                            fontWeight: 600,
                            border: "1px dashed var(--border)"
                        }}>
                            Coming Soon
                        </div>
                    </div>
                )}

                {activeTab === "cache" && (
                    <div className="panel" style={{ textAlign: "center", padding: "40px" }}>
                        <h3 style={{ margin: 0 }}>Semantic Cache</h3>
                        <p className="subtitle" style={{ margin: "12px auto 24px" }}>Re-index vector embeddings for natural language lookups.</p>
                        <div style={{
                            display: "inline-block",
                            padding: "10px 20px",
                            background: "var(--surface-muted)",
                            borderRadius: "8px",
                            color: "var(--muted)",
                            fontWeight: 600,
                            border: "1px dashed var(--border)"
                        }}>
                            Coming Soon
                        </div>
                    </div>
                )}

                {activeTab === "obs" && (
                    <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: "24px" }}>
                        <div className="panel">
                            <h3>Trace Explorer</h3>
                            <p className="subtitle">Browse traces, view span waterfalls, and explore execution details.</p>
                            <Link
                                to="/admin/traces"
                                style={{ display: "inline-block", backgroundColor: "var(--accent)", color: "#fff", textDecoration: "none", padding: "12px 24px", borderRadius: "10px", marginTop: "12px", fontWeight: 600 }}
                            >
                                Open Trace Explorer
                            </Link>

                            <div style={{ marginTop: "20px" }}>
                                <p className="subtitle" style={{ marginBottom: "8px" }}>Quick lookup by trace ID:</p>
                                <div style={{ display: "flex", gap: "10px" }}>
                                    <input
                                        type="text"
                                        placeholder="Paste Trace ID..."
                                        value={traceId}
                                        onChange={(e) => setTraceId(e.target.value)}
                                        style={{ flex: 1, padding: "10px", borderRadius: "8px", border: "1px solid var(--border)" }}
                                    />
                                </div>
                                {traceId.trim() && (
                                    <div style={{ marginTop: "12px" }}>
                                        <TraceLink traceId={traceId.trim()} variant="button" />
                                    </div>
                                )}
                            </div>
                        </div>

                        <div className="panel">
                            <h3>Metrics (Preview)</h3>
                            <p className="subtitle">View basic metrics derived from trace data, including trace volume and error rates.</p>
                            <Link
                                to="/admin/observability/metrics"
                                style={{ display: "inline-block", backgroundColor: "var(--accent)", color: "#fff", textDecoration: "none", padding: "12px 24px", borderRadius: "10px", marginTop: "12px", fontWeight: 600 }}
                            >
                                Open Metrics Preview
                            </Link>
                            {isGrafanaConfigured() && (
                                <div style={{ marginTop: "16px" }}>
                                    <a
                                        href={`${grafanaBaseUrl}/d/text2sql-traces/text2sql-trace-metrics`}
                                        target="_blank"
                                        rel="noreferrer"
                                        style={{
                                            display: "inline-block",
                                            backgroundColor: "transparent",
                                            border: "1px solid var(--border)",
                                            color: "var(--ink)",
                                            textDecoration: "none",
                                            padding: "10px 20px",
                                            borderRadius: "8px",
                                            fontSize: "0.9rem"
                                        }}
                                    >
                                        Open Grafana Dashboards
                                    </a>
                                </div>
                            )}
                            {!isGrafanaConfigured() && (
                                <div style={{
                                    marginTop: "16px",
                                    padding: "10px 12px",
                                    backgroundColor: "var(--surface-muted)",
                                    borderRadius: "6px",
                                    fontSize: "0.85rem",
                                    color: "var(--muted)"
                                }}>
                                    Set <code>VITE_GRAFANA_BASE_URL</code> for advanced Grafana dashboards.
                                </div>
                            )}
                        </div>
                    </div>
                )}
            </div>
        </>
    );
}
