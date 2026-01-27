import React, { useEffect, useState } from "react";
import Tabs from "../components/common/Tabs";
import DataTable from "../components/common/DataTable";
import { AdminService, OpsService } from "../api";
import { PinRule, RecommendationResult } from "../types/admin";

export default function Recommendations() {
    const [activeTab, setActiveTab] = useState("pinned");
    const [tenantId, setTenantId] = useState(1);
    const [pins, setPins] = useState<PinRule[]>([]);
    const [isLoading, setIsLoading] = useState(false);

    // Playground state
    const [query, setQuery] = useState("");
    const [limit, setLimit] = useState(3);
    const [enableFallback, setEnableFallback] = useState(true);
    const [recoResult, setRecoResult] = useState<RecommendationResult | null>(null);

    const tabs = [
        { id: "pinned", label: "📌 Pinned Rules" },
        { id: "playground", label: "🧪 Playground" }
    ];

    const loadPins = async () => {
        setIsLoading(true);
        try {
            const data = await AdminService.listPins(tenantId);
            setPins(data);
        } catch (err) {
            console.error(err);
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        if (activeTab === "pinned") {
            loadPins();
        }
    }, [activeTab, tenantId]);

    const handleTogglePin = async (rule: PinRule) => {
        try {
            await AdminService.upsertPin({
                id: rule.id,
                tenant_id: tenantId,
                enabled: !rule.enabled
            });
            loadPins();
        } catch (err) {
            alert("Failed to toggle pin");
        }
    };

    const handleDeletePin = async (id: string) => {
        if (!confirm("Are you sure?")) return;
        try {
            await AdminService.deletePin(id, tenantId);
            loadPins();
        } catch (err) {
            alert("Failed to delete pin");
        }
    };

    const runPlayground = async () => {
        if (!query.trim()) return;
        setIsLoading(true);
        try {
            const result = await OpsService.runRecommendations(query, tenantId, limit, enableFallback);
            setRecoResult(result);
        } catch (err) {
            alert("Failed to run playground");
        } finally {
            setIsLoading(false);
        }
    };

    return (
        <>
            <header className="hero">
                <div>
                    <p className="kicker">Context Engineering</p>
                    <h1>Recommendations</h1>
                    <p className="subtitle">
                        Optimize few-shot context retrieval with pinned rules and ranker inspection.
                    </p>
                </div>
                <div className="panel" style={{ minWidth: "180px" }}>
                    <label>
                        Tenant Context
                        <input
                            type="number"
                            value={tenantId}
                            onChange={(e) => setTenantId(Number(e.target.value))}
                        />
                    </label>
                </div>
            </header>

            <Tabs tabs={tabs} activeTab={activeTab} onChange={setActiveTab} />

            {activeTab === "pinned" && (
                <>
                    <div style={{ padding: "24px", background: "var(--surface-muted)", borderRadius: "16px", marginBottom: "24px", border: "1px solid var(--border)" }}>
                        <h3 style={{ margin: "0 0 8px" }}>Forcefully Include Examples</h3>
                        <p style={{ margin: 0, color: "var(--muted)", fontSize: "0.9rem" }}>
                            Define rules to pin specific examples to the top of the context for specific queries.
                        </p>
                    </div>

                    <DataTable
                        data={pins}
                        isLoading={isLoading}
                        columns={[
                            { header: "Type", key: "match_type", render: (r) => <strong>{r.match_type.toUpperCase()}</strong> },
                            { header: "Value", key: "match_value", render: (r) => <code style={{ color: "var(--accent)" }}>{r.match_value}</code> },
                            { header: "Pins", key: "registry_example_ids", render: (r) => `${r.registry_example_ids.length} items` },
                            { header: "Priority", key: "priority" },
                            {
                                header: "Status", key: "enabled", render: (r) => (
                                    <span className="pill" style={{ backgroundColor: r.enabled ? "#ecfdf3" : "#fef3f2", color: r.enabled ? "#0f5132" : "#b42318" }}>
                                        {r.enabled ? "Enabled" : "Disabled"}
                                    </span>
                                )
                            },
                            {
                                header: "Actions", key: "id", render: (r) => (
                                    <div style={{ display: "flex", gap: "8px" }}>
                                        <button onClick={() => handleTogglePin(r)} style={{ border: "1px solid var(--border)", background: "var(--surface)", cursor: "pointer", borderRadius: "8px", padding: "4px 8px" }}>
                                            {r.enabled ? "Disable" : "Enable"}
                                        </button>
                                        <button onClick={() => handleDeletePin(r.id)} style={{ border: "1px solid var(--border)", background: "#fff5f5", color: "#b42318", cursor: "pointer", borderRadius: "8px", padding: "4px 8px" }}>
                                            Delete
                                        </button>
                                    </div>
                                )
                            }
                        ]}
                    />
                </>
            )}

            {activeTab === "playground" && (
                <div style={{ display: "grid", gap: "32px" }}>
                    <div className="panel" style={{ display: "grid", gap: "16px" }}>
                        <label>
                            Natural Language Query
                            <textarea
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                placeholder="e.g. show me total revenue by month"
                                style={{ width: "100%", padding: "12px", borderRadius: "10px", border: "1px solid var(--border)", minHeight: "80px" }}
                            />
                        </label>
                        <div style={{ display: "flex", gap: "24px", alignItems: "center" }}>
                            <label style={{ flexDirection: "row", alignItems: "center", gap: "8px" }}>
                                <input type="checkbox" checked={enableFallback} onChange={(e) => setEnableFallback(e.target.checked)} />
                                Enable Fallback
                            </label>
                            <label style={{ flexDirection: "row", alignItems: "center", gap: "8px" }}>
                                Rank Limit
                                <input type="number" value={limit} onChange={(e) => setLimit(Number(e.target.value))} style={{ width: "60px" }} />
                            </label>
                            <button
                                className="feedback button"
                                onClick={runPlayground}
                                disabled={isLoading}
                                style={{ marginLeft: "auto", background: "var(--accent)", color: "#fff", padding: "10px 24px", borderRadius: "999px", border: "none", cursor: "pointer", fontWeight: 600 }}
                            >
                                {isLoading ? "Analyzing..." : "Run Inspection"}
                            </button>
                        </div>
                    </div>

                    {recoResult && (
                        <div className="animate-in" style={{ display: "grid", gap: "24px" }}>
                            <div>
                                <h3 style={{ marginBottom: "16px" }}>Selection Summary</h3>
                                <div style={{ display: "flex", gap: "16px", flexWrap: "wrap" }}>
                                    <div className="panel" style={{ flex: 1, textAlign: "center", padding: "16px" }}>
                                        <div style={{ color: "var(--muted)", fontSize: "0.8rem", textTransform: "uppercase" }}>Selected</div>
                                        <div style={{ fontSize: "1.5rem", fontWeight: 700 }}>{recoResult.metadata.count_total}</div>
                                    </div>
                                    <div className="panel" style={{ flex: 1, textAlign: "center", padding: "16px" }}>
                                        <div style={{ color: "var(--muted)", fontSize: "0.8rem", textTransform: "uppercase" }}>Pinned</div>
                                        <div style={{ fontSize: "1.5rem", fontWeight: 700 }}>{recoResult.metadata.pins_selected_count}</div>
                                    </div>
                                    <div className="panel" style={{ flex: 1, textAlign: "center", padding: "16px" }}>
                                        <div style={{ color: "var(--muted)", fontSize: "0.8rem", textTransform: "uppercase" }}>Fallback</div>
                                        <div style={{ fontSize: "1.5rem", fontWeight: 700 }}>{recoResult.metadata.count_fallback}</div>
                                    </div>
                                </div>
                            </div>

                            <div>
                                <h3 style={{ marginBottom: "16px" }}>Retrieved Examples</h3>
                                <div style={{ display: "grid", gap: "12px" }}>
                                    {recoResult.examples.map((ex, idx) => (
                                        <div key={idx} className="bubble" style={{ padding: "16px", borderLeft: ex.metadata.pinned ? "4px solid var(--accent)" : "1px solid var(--border)" }}>
                                            <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "8px", fontSize: "0.8rem" }}>
                                                <span style={{ color: "var(--muted)" }}>{ex.source.toUpperCase()} • {ex.metadata.status.toUpperCase()}</span>
                                                {ex.metadata.pinned && <span style={{ color: "var(--accent)", fontWeight: 600 }}>📌 PINNED</span>}
                                            </div>
                                            <div style={{ fontWeight: 500 }}>{ex.question}</div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </>
    );
}
