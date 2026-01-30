import React, { useState, useEffect } from "react";
import { IngestionService } from "../../api";
import { useToast } from "../../hooks/useToast";
import { useConfirmation } from "../../hooks/useConfirmation";
import { ConfirmationDialog } from "../common/ConfirmationDialog";

interface Props {
  runId: string;
  onBack: () => void;
}

export default function IngestionRunDetail({ runId, onBack }: Props) {
  const [patterns, setPatterns] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [isRollingBack, setIsRollingBack] = useState(false);
  const { show: showToast } = useToast();
  const { confirm, dialogProps } = useConfirmation();

  useEffect(() => {
    loadPatterns();
  }, [runId]);

  const loadPatterns = async () => {
    setIsLoading(true);
    try {
      const data = await IngestionService.getRunPatterns(runId);
      setPatterns(data);
    } catch (err) {
      showToast("Failed to load patterns", "error");
    } finally {
      setIsLoading(false);
    }
  };

  const handleRollback = async () => {
    const isConfirmed = await confirm({
      title: "Rollback Run",
      description: `Are you sure you want to roll back this run? All ${patterns.length} patterns created will be soft-deleted.`,
      confirmText: "Rollback",
      danger: true
    });
    if (!isConfirmed) return;

    const typedId = window.prompt(`Please type the run ID to confirm: ${runId}`);
    if (typedId !== runId) {
      showToast("ID mismatch, rollback cancelled", "error");
      return;
    }

    setIsRollingBack(true);
    try {
      await IngestionService.rollbackRun(runId);
      showToast("Rollback successful", "success");
      onBack();
    } catch (err) {
      showToast("Rollback failed", "error");
    } finally {
      setIsRollingBack(false);
    }
  };

  if (isLoading) return <div className="panel">Loading run details...</div>;

  return (
    <div style={{ display: "grid", gap: "24px" }}>
      <div className="panel" style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
        <div>
          <button onClick={onBack} style={{ background: "transparent", border: "none", color: "var(--accent)", cursor: "pointer", padding: 0, marginBottom: "8px" }}>
            ← Back to Dashboard
          </button>
          <h2 style={{ margin: 0 }}>Run Detail: <code>{runId}</code></h2>
        </div>
        <button
          onClick={handleRollback}
          disabled={isRollingBack || patterns.length === 0}
          style={{ background: "#fee2e2", color: "#991b1b", border: "1px solid #fecaca", padding: "10px 20px", borderRadius: "8px", cursor: "pointer", fontWeight: 600 }}
        >
          {isRollingBack ? "Rolling back..." : "Rollback Run"}
        </button>
      </div>

      <div className="panel">
        <h3>Generated Patterns</h3>
        <p className="subtitle">{patterns.length} patterns recorded for this run.</p>

        <div style={{ marginTop: "20px", overflowX: "auto" }}>
          <table style={{ width: "100%", borderCollapse: "collapse" }}>
            <thead>
              <tr style={{ textAlign: "left", background: "var(--surface-muted)" }}>
                <th style={{ padding: "12px" }}>Label</th>
                <th style={{ padding: "12px" }}>Pattern</th>
                <th style={{ padding: "12px" }}>Action</th>
              </tr>
            </thead>
            <tbody>
              {patterns.map((p, i) => (
                <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                  <td style={{ padding: "12px" }}><code>{p.label}</code></td>
                  <td style={{ padding: "12px" }}>{p.pattern}</td>
                  <td style={{ padding: "12px" }}>
                    <span style={{
                      padding: "2px 6px",
                      borderRadius: "4px",
                      fontSize: "0.75rem",
                      background: p.action === "CREATED" ? "#dcfce7" : p.action === "DELETED" ? "#fee2e2" : "var(--surface-muted)",
                      color: p.action === "CREATED" ? "#166534" : p.action === "DELETED" ? "#991b1b" : "var(--muted)"
                    }}>
                      {p.action}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
      <ConfirmationDialog {...dialogProps} />
    </div>
  );
}
