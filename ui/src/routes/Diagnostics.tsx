import { useState, useEffect, useCallback } from "react";
import { getDiagnostics } from "../api";
import { DiagnosticsResponse } from "../types/diagnostics";
import { ErrorCard } from "../components/common/ErrorCard";

export default function Diagnostics() {
    const [data, setData] = useState<DiagnosticsResponse | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<any>(null);
    const [isDebug, setIsDebug] = useState(false);

    const fetchDiagnostics = useCallback(async (debug = false) => {
        setLoading(true);
        setError(null);
        try {
            const resp = await getDiagnostics(debug);
            setData(resp);
        } catch (err: any) {
            setError(err);
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        fetchDiagnostics(isDebug);
    }, [fetchDiagnostics, isDebug]);

    if (loading && !data) {
        return (
            <div className="panel animate-pulse">
                <div style={{ padding: "40px", textAlign: "center", color: "var(--muted)" }}>
                    Loading system diagnostics...
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
                    message={error.message || "Failed to load diagnostics"}
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
                <div className="panel" style={{ padding: "24px" }}>
                    <pre style={{ fontSize: "0.8rem", overflowX: "auto" }}>
                        {JSON.stringify(data, null, 2)}
                    </pre>
                </div>
            )}
        </div>
    );
}
