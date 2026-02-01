import React, { useState, useEffect } from "react";
import { SynthService, getErrorMessage } from "../../api";
import { SynthRun } from "../../types/admin";
import { useToast } from "../../hooks/useToast";

interface Props {
  runId: string;
  onBack: () => void;
}

export default function SynthRunDetail({ runId, onBack }: Props) {
  const [run, setRun] = useState<SynthRun | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const { show: showToast } = useToast();

  useEffect(() => {
    loadRun();
  }, [runId]);

  const loadRun = async () => {
    setIsLoading(true);
    try {
      const data = await SynthService.getRun(runId);
      setRun(data);
    } catch (err) {
      showToast(getErrorMessage(err), "error");
    } finally {
      setIsLoading(false);
    }
  };

  if (isLoading) return <div className="panel">Loading run details...</div>;
  if (!run) return <div className="panel">Run not found.</div>;

  const manifest = run.manifest;
  const tables = manifest?.tables || {};
  const metrics = run.metrics || {};

  return (
    <div style={{ display: "grid", gap: "24px" }}>
      <div className="panel">
        <button onClick={onBack} style={{ background: "transparent", border: "none", color: "var(--accent)", cursor: "pointer", padding: 0, marginBottom: "8px", fontWeight: 600 }}>
          ← Back to Dashboard
        </button>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start" }}>
          <div>
            <h2 style={{ margin: 0 }}>Run: <code>{runId}</code></h2>
            <p className="subtitle" style={{ marginTop: "4px" }}>
              Started on {new Date(run.started_at).toLocaleString()}
            </p>
          </div>
          <span style={{
            padding: "4px 12px",
            borderRadius: "12px",
            fontSize: "0.85rem",
            fontWeight: 600,
            backgroundColor: run.status === "COMPLETED" ? "#10b981" : run.status === "FAILED" ? "#ef4444" : "#f59e0b",
            color: "#fff"
          }}>
            {run.status}
          </span>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "1fr 2fr", gap: "24px" }}>
        <div className="panel">
          <h3>Run Metadata</h3>
          <div style={{ display: "grid", gap: "16px", marginTop: "16px" }}>
            <div>
              <label style={{ fontSize: "0.8rem", color: "var(--muted)", display: "block" }}>Output Path</label>
              <code style={{ fontSize: "0.85rem", wordBreak: "break-all" }}>{run.output_path || "N/A"}</code>
            </div>
            <div>
              <label style={{ fontSize: "0.8rem", color: "var(--muted)", display: "block" }}>Job ID</label>
              <code style={{ fontSize: "0.85rem" }}>{run.job_id || "N/A"}</code>
            </div>
            <div>
              <label style={{ fontSize: "0.8rem", color: "var(--muted)", display: "block" }}>Duration</label>
              <span style={{ fontSize: "0.9rem" }}>
                {run.completed_at ?
                  `${Math.round((new Date(run.completed_at).getTime() - new Date(run.started_at).getTime()) / 1000)} seconds`
                  : "N/A"}
              </span>
            </div>
          </div>

          <h3 style={{ marginTop: "32px" }}>Metrics</h3>
          <div style={{ display: "grid", gap: "16px", marginTop: "16px" }}>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>Tables:</span>
              <span style={{ fontWeight: 600 }}>{metrics.tables_generated || 0}</span>
            </div>
            <div style={{ display: "flex", justifyContent: "space-between" }}>
              <span>Total Rows:</span>
              <span style={{ fontWeight: 600 }}>{metrics.total_rows?.toLocaleString() || 0}</span>
            </div>
          </div>
        </div>

        <div className="panel">
          <h3>Generated Tables</h3>
          <p className="subtitle">Extracted from <code>manifest.json</code></p>

          <div style={{ marginTop: "20px" }}>
            {Object.keys(tables).length === 0 ? (
              <p>No table information available in manifest.</p>
            ) : (
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ textAlign: "left", borderBottom: "1px solid var(--border)" }}>
                    <th style={{ padding: "12px" }}>Table Name</th>
                    <th style={{ padding: "12px" }}>Rows</th>
                    <th style={{ padding: "12px" }}>Files</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(tables).map(([name, details]: [string, any]) => (
                    <tr key={name} style={{ borderBottom: "1px solid var(--border)" }}>
                      <td style={{ padding: "12px" }}><code>{name}</code></td>
                      <td style={{ padding: "12px" }}>{details.row_count?.toLocaleString()}</td>
                      <td style={{ padding: "12px" }}>
                        {details.files?.map((f: any, idx: number) => (
                          <div key={idx} style={{ fontSize: "0.8rem", color: "var(--muted)" }}>
                            {f.format.toUpperCase()}: <code>{f.path}</code>
                          </div>
                        ))}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>

          {run.error_message && (
            <div style={{ marginTop: "32px", padding: "16px", background: "#fee2e2", border: "1px solid #fecaca", borderRadius: "8px", color: "#991b1b" }}>
              <h4 style={{ margin: "0 0 8px" }}>Error Message</h4>
              <pre style={{ margin: 0, whiteSpace: "pre-wrap", fontSize: "0.85rem" }}>{run.error_message}</pre>
            </div>
          )}
        </div>
      </div>

      <div className="panel">
        <h3>Configuration Snapshot</h3>
        <pre style={{
          marginTop: "16px",
          padding: "16px",
          background: "var(--surface-muted)",
          borderRadius: "8px",
          fontSize: "0.85rem",
          overflow: "auto",
          maxHeight: "300px"
        }}>
          {JSON.stringify(run.config_snapshot, null, 2)}
        </pre>
      </div>
    </div>
  );
}
