import React, { useState, useEffect } from "react";
import { IngestionService, IngestionRun } from "../../api";
import { useToast } from "../../hooks/useToast";
import IngestionRunDetail from "./IngestionRunDetail";

export default function IngestionDashboard() {
  const [metrics, setMetrics] = useState<any>(null);
  const [recentRuns, setRecentRuns] = useState<IngestionRun[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const { show: showToast } = useToast();

  useEffect(() => {
    loadData();
  }, []);

  const loadData = async () => {
    setIsLoading(true);
    try {
      const [m, runs] = await Promise.all([
        IngestionService.getMetrics("30d"),
        IngestionService.listRuns()
      ]);
      setMetrics(m);
      setRecentRuns(runs);
    } catch (err) {
      showToast("Failed to load dashboard data", "error");
    } finally {
      setIsLoading(false);
    }
  };

  if (isLoading) return <div className="panel">Loading dashboard...</div>;

  if (selectedRunId) {
    return <IngestionRunDetail runId={selectedRunId} onBack={() => setSelectedRunId(null)} />;
  }

  return (
    <div style={{ display: "grid", gap: "24px" }}>
      {/* Summary Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: "16px" }}>
        <div className="panel" style={{ textAlign: "center" }}>
          <div className="subtitle">Total Runs</div>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>{metrics?.total_runs}</div>
        </div>
        <div className="panel" style={{ textAlign: "center" }}>
          <div className="subtitle">Generated</div>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>{metrics?.total_patterns_generated}</div>
        </div>
        <div className="panel" style={{ textAlign: "center" }}>
          <div className="subtitle">Accepted</div>
          <div style={{ fontSize: "2rem", fontWeight: 700 }}>{metrics?.total_patterns_accepted}</div>
        </div>
        <div className="panel" style={{ textAlign: "center" }}>
          <div className="subtitle">Acceptance Rate</div>
          <div style={{ fontSize: "2rem", fontWeight: 700, color: "var(--accent)" }}>
            {((metrics?.avg_acceptance_rate || 0) * 100).toFixed(1)}%
          </div>
        </div>
      </div>

      {/* Charts Placeholder - using simple CSS bars for now as we don't have a chart lib confirmed */}
      <div className="panel">
        <h3>Acceptance Rate by Day</h3>
        <div style={{ display: "flex", alignItems: "flex-end", gap: "8px", height: "200px", padding: "20px 0" }}>
          {metrics?.runs_by_day.map((day: any, i: number) => {
            const rate = day.count > 0 ? (day.accepted / (day.count * 5)) * 100 : 0; // rough scale
            // Wait, day.accepted is total patterns accepted that day.
            // Let's just show count of runs for now or similar.
            return (
              <div key={i} style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center" }}>
                <div style={{
                  width: "100%",
                  background: "var(--accent)",
                  height: `${Math.min(100, (day.count / (metrics.total_runs || 1)) * 200)}%`,
                  borderRadius: "4px 4px 0 0"
                }} title={`${day.count} runs`} />
                <div style={{ fontSize: "0.7rem", marginTop: "8px", transform: "rotate(-45deg)", whiteSpace: "nowrap" }}>
                  {new Date(day.day).toLocaleDateString(undefined, { month: 'short', day: 'numeric' })}
                </div>
              </div>
            );
          })}
        </div>
      </div>

      {/* Recent Runs */}
      <div className="panel">
        <h3>Recent Ingestion Runs</h3>
        <div style={{ overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ textAlign: "left", background: "var(--surface-muted)" }}>
                <th style={{ padding: "12px" }}>ID</th>
                <th style={{ padding: "12px" }}>Started</th>
                <th style={{ padding: "12px" }}>Status</th>
                <th style={{ padding: "12px" }}>Success Rate</th>
                <th style={{ padding: "12px" }}>Action</th>
              </tr>
            </thead>
            <tbody>
              {recentRuns.map(run => {
                const acc = run.metrics?.patterns_accepted || 0;
                const gen = run.metrics?.patterns_generated || 1;
                const rate = (acc / gen * 100).toFixed(0);

                return (
                  <tr key={run.id} style={{ borderTop: "1px solid var(--border)" }}>
                    <td style={{ padding: "12px" }}><code>{run.id.slice(0, 8)}</code></td>
                    <td style={{ padding: "12px" }}>{new Date(run.started_at).toLocaleString()}</td>
                    <td style={{ padding: "12px" }}>
                      <span style={{
                        padding: "4px 8px",
                        borderRadius: "12px",
                        fontSize: "0.8rem",
                        background: run.status === "COMPLETED" ? "#dcfce7" : "#fee2e2",
                        color: run.status === "COMPLETED" ? "#166534" : "#991b1b"
                      }}>
                        {run.status}
                      </span>
                    </td>
                                        <td style={{ padding: "12px" }}>{run.status === "COMPLETED" ? `${rate}%` : "-"}</td>
                                        <td style={{ padding: "12px" }}>
                                          <button
                                            onClick={() => setSelectedRunId(run.id)}
                                            style={{ background: "transparent", border: "1px solid var(--border)", padding: "4px 8px", borderRadius: "4px", cursor: "pointer" }}
                                          >
                                            View Details
                                          </button>
                                        </td>

                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
