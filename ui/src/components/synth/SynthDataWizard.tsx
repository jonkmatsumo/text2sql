import React, { useReducer, useEffect, useCallback } from "react";
import { SynthService, getErrorMessage } from "../../api";
import { SynthGenerateResponse, OpsJobResponse } from "../../types/admin";
import { useToast } from "../../hooks/useToast";
import { useJobPolling } from "../../hooks/useJobPolling";

interface Props {
  onExit: () => void;
}

type WizardStep = "configure" | "generating" | "complete";

interface WizardState {
  step: WizardStep;
  runId: string | null;
  jobId: string | null;
  activeJob: OpsJobResponse | null;
  error: string | null;
  isLoading: boolean;
  preset: string;
  outputPath: string;
}

const initialState: WizardState = {
  step: "configure",
  runId: null,
  jobId: null,
  activeJob: null,
  error: null,
  isLoading: false,
  preset: "mvp",
  outputPath: "",
};

type WizardAction =
  | { type: "SET_STEP"; step: WizardStep }
  | { type: "SET_LOADING"; isLoading: boolean }
  | { type: "SET_ERROR"; error: string | null }
  | { type: "START_GENERATION_SUCCESS"; runId: string; jobId: string }
  | { type: "SET_ACTIVE_JOB"; job: OpsJobResponse | null }
  | { type: "SET_PRESET"; preset: string }
  | { type: "SET_OUTPUT_PATH"; path: string };

function wizardReducer(state: WizardState, action: WizardAction): WizardState {
  switch (action.type) {
    case "SET_STEP":
      return { ...state, step: action.step };
    case "SET_LOADING":
      return { ...state, isLoading: action.isLoading };
    case "SET_ERROR":
      return { ...state, error: action.error, isLoading: false };
    case "START_GENERATION_SUCCESS":
      return { ...state, runId: action.runId, jobId: action.jobId, step: "generating", isLoading: false, error: null };
    case "SET_ACTIVE_JOB":
      return { ...state, activeJob: action.job };
    case "SET_PRESET":
      return { ...state, preset: action.preset };
    case "SET_OUTPUT_PATH":
      return { ...state, outputPath: action.path };
    default:
      return state;
  }
}

export default function SynthDataWizard({ onExit }: Props) {
  const [state, dispatch] = useReducer(wizardReducer, initialState);
  const { show: showToast } = useToast();

  const handleJobComplete = useCallback((job: OpsJobResponse) => {
    dispatch({ type: "SET_STEP", step: "complete" });
    showToast("Synthetic data generation complete!", "success");
  }, [showToast]);

  const handleJobFailed = useCallback((job: OpsJobResponse) => {
    dispatch({ type: "SET_ERROR", error: job.error_message || "Generation failed" });
    showToast("Generation failed", "error");
  }, [showToast]);

  const { job: activeJob } = useJobPolling({
    jobId: state.jobId,
    enabled: state.step === "generating",
    onComplete: handleJobComplete,
    onFailed: handleJobFailed,
  });

  useEffect(() => {
    if (activeJob) {
      dispatch({ type: "SET_ACTIVE_JOB", job: activeJob });
    }
  }, [activeJob]);

  const handleStart = async () => {
    dispatch({ type: "SET_LOADING", isLoading: true });
    try {
      const response = await SynthService.generate({
        preset: state.preset,
        output_path: state.outputPath || undefined,
      });
      dispatch({ type: "START_GENERATION_SUCCESS", runId: response.run_id, jobId: response.job_id });
    } catch (err) {
      dispatch({ type: "SET_ERROR", error: getErrorMessage(err) });
    }
  };

  const progress = state.activeJob?.result?.percent || 0;
  const currentTable = state.activeJob?.result?.current_table || "";

  return (
    <div className="wizard-container" style={{ maxWidth: "800px", margin: "0 auto", padding: "40px 0" }}>
      {/* Progress Stepper */}
      <div style={{ display: "flex", justifyContent: "space-between", marginBottom: "40px", position: "relative" }}>
        <div style={{ position: "absolute", top: "15px", left: "0", right: "0", height: "2px", background: "var(--border)", zIndex: 0 }} />
        {["Configure", "Generating", "Complete"].map((label, i) => {
          const stepIdx = i;
          const currentIdx = ["configure", "generating", "complete"].indexOf(state.step);
          const isCompleted = currentIdx > stepIdx;
          const isActive = currentIdx === stepIdx;

          return (
            <div key={label} style={{ position: "relative", zIndex: 1, display: "flex", flexDirection: "column", alignItems: "center", width: "100px" }}>
              <div style={{
                width: "32px",
                height: "32px",
                borderRadius: "50%",
                background: isCompleted ? "#10b981" : isActive ? "var(--accent)" : "var(--surface-muted)",
                color: (isActive || isCompleted) ? "#fff" : "var(--muted)",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontWeight: 600,
                border: isActive ? "4px solid #e0e7ff" : "none",
                marginBottom: "8px"
              }}>
                {isCompleted ? "✓" : i + 1}
              </div>
              <span style={{ fontSize: "0.8rem", fontWeight: isActive ? 600 : 400, color: isActive ? "var(--ink)" : "var(--muted)" }}>{label}</span>
            </div>
          );
        })}
      </div>

      <div className="panel" style={{ padding: "32px" }}>
        {state.error && (
          <div style={{ padding: "12px", backgroundColor: "#fef2f2", color: "#b91c1c", borderRadius: "8px", marginBottom: "24px", fontSize: "0.9rem" }}>
            <strong>Error:</strong> {state.error}
          </div>
        )}

        {state.step === "configure" && (
          <div>
            <h3>Generation Settings</h3>
            <p className="subtitle">Select a scale preset and optional output path.</p>

            <div style={{ display: "grid", gap: "20px", marginTop: "24px" }}>
              <div>
                <label style={{ display: "block", marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600 }}>Scale Preset</label>
                <select
                  value={state.preset}
                  onChange={(e) => dispatch({ type: "SET_PRESET", preset: e.target.value })}
                  style={{ width: "100%", padding: "10px", borderRadius: "8px", border: "1px solid var(--border)" }}
                >
                  <option value="small">Small (few rows per table, fast)</option>
                  <option value="mvp">MVP (balanced for testing)</option>
                  <option value="medium">Medium (larger dataset)</option>
                </select>
              </div>

              <div>
                <label style={{ display: "block", marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600 }}>Output Directory (Optional)</label>
                <input
                  type="text"
                  placeholder="/tmp/synth_data"
                  value={state.outputPath}
                  onChange={(e) => dispatch({ type: "SET_OUTPUT_PATH", path: e.target.value })}
                  style={{ width: "100%", padding: "10px", borderRadius: "8px", border: "1px solid var(--border)" }}
                />
              </div>
            </div>

            <div style={{ marginTop: "32px", display: "flex", justifyContent: "flex-end", gap: "12px" }}>
              <button className="text-button" onClick={onExit}>Cancel</button>
              <button
                onClick={handleStart}
                disabled={state.isLoading}
                style={{
                  padding: "10px 24px",
                  background: "var(--accent)",
                  color: "#fff",
                  border: "none",
                  borderRadius: "8px",
                  fontWeight: 600,
                  cursor: "pointer"
                }}
              >
                {state.isLoading ? "Starting..." : "Start Generation"}
              </button>
            </div>
          </div>
        )}

        {state.step === "generating" && (
          <div style={{ textAlign: "center", padding: "20px 0" }}>
            <h3>Generating Synthetic Data...</h3>
            <p className="subtitle">This may take a minute depending on the selected scale.</p>

            <div style={{ margin: "40px 0" }}>
              <div style={{ height: "12px", width: "100%", background: "var(--surface-muted)", borderRadius: "6px", overflow: "hidden", marginBottom: "12px" }}>
                <div style={{ height: "100%", width: `${progress}%`, background: "var(--accent)", transition: "width 0.3s ease" }} />
              </div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: "0.85rem", color: "var(--muted)" }}>
                <span>{currentTable ? `Generating ${currentTable}...` : "Initializing..."}</span>
                <span>{progress}%</span>
              </div>
            </div>

            <div style={{ fontSize: "0.85rem", color: "var(--muted)", fontFamily: "monospace", padding: "12px", background: "var(--surface-muted)", borderRadius: "8px" }}>
               Job ID: {state.jobId}
            </div>
          </div>
        )}

        {state.step === "complete" && (
          <div style={{ textAlign: "center", padding: "20px 0" }}>
            <div style={{ width: "64px", height: "64px", background: "#10b981", color: "#fff", borderRadius: "50%", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "32px", margin: "0 auto 24px" }}>
              ✓
            </div>
            <h3>Generation Successful</h3>
            <p className="subtitle">Your synthetic dataset has been generated and is ready for use.</p>

            <div style={{ marginTop: "32px", display: "flex", justifyContent: "center", gap: "12px" }}>
              <button
                onClick={onExit}
                style={{
                  padding: "10px 24px",
                  background: "var(--accent)",
                  color: "#fff",
                  border: "none",
                  borderRadius: "8px",
                  fontWeight: 600,
                  cursor: "pointer"
                }}
              >
                Done
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
