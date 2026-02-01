import React, { useState, useEffect } from "react";
import { SynthService } from "../../api";
import { SynthRunSummary } from "../../types/admin";
import { getErrorMessage } from "../../api";
import { useToast } from "../../hooks/useToast";

interface SynthDashboardProps {
    onStartWizard: () => void;
    onViewRun: (runId: string) => void;
}

export default function SynthDashboard({ onStartWizard, onViewRun }: SynthDashboardProps) {
    const [runs, setRuns] = useState<SynthRunSummary[]>([]);
    const [isLoading, setIsLoading] = useState(true);
    const { show: showToast } = useToast();

    const fetchRuns = async () => {
        setIsLoading(true);
        try {
            const data = await SynthService.listRuns();
            setRuns(data);
        } catch (err) {
            showToast(getErrorMessage(err), "error");
        } finally {
            setIsLoading(false);
        }
    };

    useEffect(() => {
        fetchRuns();
    }, []);

    return (
        <div style={{ display: "grid", gap: "24px" }}>
            <div className="panel">
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <div>
                        <h3>Synthetic Data Generation</h3>
                        <p className="subtitle">Generate deterministic synthetic datasets for testing.</p>
                    </div>
                    <button
                        onClick={onStartWizard}
                        style={{
                            padding: "10px 20px",
                            background: "#10b981",
                            color: "#fff",
                            border: "none",
                            borderRadius: "10px",
                            fontWeight: 600,
                            cursor: "pointer"
                        }}
                    >
                        Start New Generation
                    </button>
                </div>
            </div>

            <div className="panel">
                <h3>Recent Runs</h3>
                {isLoading ? (
                    <p>Loading runs...</p>
                ) : runs.length === 0 ? (
                    <p className="subtitle">No runs found.</p>
                ) : (
                    <table style={{ width: "100%", borderCollapse: "collapse", marginTop: "12px" }}>
                        <thead>
                            <tr style={{ textAlign: "left", borderBottom: "1px solid var(--border)" }}>
                                <th style={{ padding: "12px" }}>Started</th>
                                <th style={{ padding: "12px" }}>Status</th>
                                <th style={{ padding: "12px" }}>Job ID</th>
                                <th style={{ padding: "12px" }}>Action</th>
                            </tr>
                        </thead>
                        <tbody>
                            {runs.map((run) => (
                                <tr key={run.id} style={{ borderBottom: "1px solid var(--border)" }}>
                                    <td style={{ padding: "12px" }}>
                                        {new Date(run.started_at).toLocaleString()}
                                    </td>
                                    <td style={{ padding: "12px" }}>
                                        <span style={{
                                            padding: "2px 8px",
                                            borderRadius: "4px",
                                            fontSize: "0.8rem",
                                            fontWeight: 600,
                                            backgroundColor: run.status === "COMPLETED" ? "#10b981" : run.status === "FAILED" ? "#ef4444" : "#f59e0b",
                                            color: "#fff"
                                        }}>
                                            {run.status}
                                        </span>
                                    </td>
                                    <td style={{ padding: "12px", fontFamily: "monospace", fontSize: "0.8rem" }}>
                                        {run.job_id || "-"}
                                    </td>
                                    <td style={{ padding: "12px" }}>
                                        <button
                                            className="text-button"
                                            onClick={() => onViewRun(run.id)}
                                            style={{ color: "var(--accent)", cursor: "pointer", background: "none", border: "none", fontWeight: 600 }}
                                        >
                                            View
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                )}
            </div>
        </div>
    );
}
