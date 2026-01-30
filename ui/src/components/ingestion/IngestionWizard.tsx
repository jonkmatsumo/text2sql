import React, { useReducer, useEffect, useCallback } from "react";
import { useSearchParams } from "react-router-dom";
import { IngestionService, IngestionCandidate, Suggestion, IngestionRun, IngestionTemplate, getErrorMessage } from "../../api";
import { OpsJobResponse } from "../../types/admin";
import { useToast } from "../../hooks/useToast";
import { useJobPolling } from "../../hooks/useJobPolling";
import { useConfirmation } from "../../hooks/useConfirmation";
import { ConfirmationDialog } from "../common/ConfirmationDialog";

interface Props {
  onExit: () => void;
}

type WizardStep =
  | "intro"
  | "analyzing"
  | "review_candidates"
  | "enriching"
  | "review_suggestions"
  | "confirmation"
  | "committing"
  | "complete";

// ---------------------------------------------------------------------------
// Wizard State
// ---------------------------------------------------------------------------

interface WizardState {
  step: WizardStep;
  runId: string | null;
  candidates: IngestionCandidate[];
  suggestions: Suggestion[];
  originalSuggestions: Suggestion[];
  hydrationJobId: string | null;
  activeJob: OpsJobResponse | null;
  enrichJobId: string | null;
  enrichJobStatus: OpsJobResponse | null;
  error: string | null;
  isLoading: boolean;
  recentRuns: IngestionRun[];
  templates: IngestionTemplate[];
  selectedTemplateId: string;
  saveAsTemplate: boolean;
  newTemplateName: string;
  activeCandidateIdx: number;
  newSynonymInputs: Record<string, string>;
  reviewedCandidates: number[];
  modifiedCandidates: number[];
}

const initialState: WizardState = {
  step: "intro",
  runId: null,
  candidates: [],
  suggestions: [],
  originalSuggestions: [],
  hydrationJobId: null,
  activeJob: null,
  enrichJobId: null,
  enrichJobStatus: null,
  error: null,
  isLoading: false,
  recentRuns: [],
  templates: [],
  selectedTemplateId: "",
  saveAsTemplate: false,
  newTemplateName: "",
  activeCandidateIdx: 0,
  newSynonymInputs: {},
  reviewedCandidates: [],
  modifiedCandidates: [],
};

// ---------------------------------------------------------------------------
// Wizard Actions
// ---------------------------------------------------------------------------

type WizardAction =
  | { type: "SET_STEP"; step: WizardStep }
  | { type: "SET_LOADING"; isLoading: boolean }
  | { type: "SET_ERROR"; error: string | null }
  | { type: "SET_RUN_ID"; runId: string | null }
  | { type: "SET_RECENT_RUNS"; runs: IngestionRun[] }
  | { type: "SET_TEMPLATES"; templates: IngestionTemplate[] }
  | { type: "SET_SELECTED_TEMPLATE"; templateId: string }
  | { type: "SET_SAVE_AS_TEMPLATE"; enabled: boolean }
  | { type: "SET_NEW_TEMPLATE_NAME"; name: string }
  | { type: "LOAD_RUN_SUCCESS"; runId: string; candidates: IngestionCandidate[]; suggestions: Suggestion[]; step: WizardStep }
  | { type: "ANALYZE_SUCCESS"; runId: string; candidates: IngestionCandidate[] }
  | { type: "SET_ENRICH_JOB"; jobId: string }
  | { type: "SET_ENRICH_JOB_STATUS"; status: OpsJobResponse | null }
  | { type: "ENRICH_COMPLETE"; suggestions: Suggestion[] }
  | { type: "SET_HYDRATION_JOB"; jobId: string }
  | { type: "SET_ACTIVE_JOB"; job: OpsJobResponse | null }
  | { type: "TOGGLE_CANDIDATE"; index: number }
  | { type: "SET_ACTIVE_CANDIDATE_IDX"; index: number }
  | { type: "MARK_REVIEWED"; index: number }
  | { type: "TOGGLE_SUGGESTION"; suggIndex: number }
  | { type: "ADD_SYNONYM"; canonicalId: string; label: string; pattern: string }
  | { type: "UPDATE_SYNONYM_INPUT"; key: string; value: string }
  | { type: "BULK_ACCEPT"; label: string }
  | { type: "BULK_REJECT"; label: string }
  | { type: "BULK_RESET"; label: string };

// ---------------------------------------------------------------------------
// Reducer
// ---------------------------------------------------------------------------

function addToArray(arr: number[], value: number): number[] {
  if (arr.includes(value)) return arr;
  return [...arr, value];
}

function removeFromArray(arr: number[], value: number): number[] {
  return arr.filter((v) => v !== value);
}

function wizardReducer(state: WizardState, action: WizardAction): WizardState {
  switch (action.type) {
    case "SET_STEP":
      return { ...state, step: action.step };

    case "SET_LOADING":
      return { ...state, isLoading: action.isLoading };

    case "SET_ERROR":
      return { ...state, error: action.error };

    case "SET_RUN_ID":
      return { ...state, runId: action.runId };

    case "SET_RECENT_RUNS":
      return { ...state, recentRuns: action.runs };

    case "SET_TEMPLATES":
      return { ...state, templates: action.templates };

    case "SET_SELECTED_TEMPLATE":
      return { ...state, selectedTemplateId: action.templateId };

    case "SET_SAVE_AS_TEMPLATE":
      return { ...state, saveAsTemplate: action.enabled };

    case "SET_NEW_TEMPLATE_NAME":
      return { ...state, newTemplateName: action.name };

    case "LOAD_RUN_SUCCESS": {
      const clonedSuggestions = structuredClone(action.suggestions);
      return {
        ...state,
        runId: action.runId,
        candidates: action.candidates,
        suggestions: action.suggestions,
        originalSuggestions: clonedSuggestions,
        step: action.step,
        isLoading: false,
        error: null,
      };
    }

    case "ANALYZE_SUCCESS":
      return {
        ...state,
        runId: action.runId,
        candidates: action.candidates.map((c) => ({ ...c, selected: true })),
        step: "review_candidates",
        error: null,
      };

    case "SET_ENRICH_JOB":
      return { ...state, enrichJobId: action.jobId };

    case "SET_ENRICH_JOB_STATUS":
      return { ...state, enrichJobStatus: action.status };

    case "ENRICH_COMPLETE": {
      const clonedSuggestions = structuredClone(action.suggestions);
      return {
        ...state,
        suggestions: action.suggestions,
        originalSuggestions: clonedSuggestions,
        activeCandidateIdx: 0,
        step: "review_suggestions",
      };
    }

    case "SET_HYDRATION_JOB":
      return { ...state, hydrationJobId: action.jobId };

    case "SET_ACTIVE_JOB":
      return { ...state, activeJob: action.job };

    case "TOGGLE_CANDIDATE": {
      const newCandidates = state.candidates.map((c, i) =>
        i === action.index ? { ...c, selected: !c.selected } : c
      );
      return { ...state, candidates: newCandidates };
    }

    case "SET_ACTIVE_CANDIDATE_IDX":
      return {
        ...state,
        activeCandidateIdx: action.index,
        reviewedCandidates: addToArray(state.reviewedCandidates, action.index),
      };

    case "MARK_REVIEWED":
      return {
        ...state,
        reviewedCandidates: addToArray(state.reviewedCandidates, action.index),
      };

    case "TOGGLE_SUGGESTION": {
      const newSuggestions = state.suggestions.map((s, i) =>
        i === action.suggIndex ? { ...s, accepted: !s.accepted } : s
      );
      return {
        ...state,
        suggestions: newSuggestions,
        modifiedCandidates: addToArray(state.modifiedCandidates, state.activeCandidateIdx),
        reviewedCandidates: addToArray(state.reviewedCandidates, state.activeCandidateIdx),
      };
    }

    case "ADD_SYNONYM": {
      const newSug: Suggestion = {
        id: action.canonicalId,
        label: action.label,
        pattern: action.pattern.toLowerCase(),
        accepted: true,
        is_new: true,
      };
      const key = `${action.label}:${action.canonicalId}`;
      return {
        ...state,
        suggestions: [...state.suggestions, newSug],
        newSynonymInputs: { ...state.newSynonymInputs, [key]: "" },
        modifiedCandidates: addToArray(state.modifiedCandidates, state.activeCandidateIdx),
        reviewedCandidates: addToArray(state.reviewedCandidates, state.activeCandidateIdx),
      };
    }

    case "UPDATE_SYNONYM_INPUT":
      return {
        ...state,
        newSynonymInputs: { ...state.newSynonymInputs, [action.key]: action.value },
      };

    case "BULK_ACCEPT": {
      const newSuggestions = state.suggestions.map((s) =>
        s.label === action.label ? { ...s, accepted: true } : s
      );
      return {
        ...state,
        suggestions: newSuggestions,
        modifiedCandidates: addToArray(state.modifiedCandidates, state.activeCandidateIdx),
        reviewedCandidates: addToArray(state.reviewedCandidates, state.activeCandidateIdx),
      };
    }

    case "BULK_REJECT": {
      const newSuggestions = state.suggestions.map((s) =>
        s.label === action.label ? { ...s, accepted: false } : s
      );
      return {
        ...state,
        suggestions: newSuggestions,
        modifiedCandidates: addToArray(state.modifiedCandidates, state.activeCandidateIdx),
        reviewedCandidates: addToArray(state.reviewedCandidates, state.activeCandidateIdx),
      };
    }

    case "BULK_RESET": {
      const resetSuggestions = state.originalSuggestions.filter((s) => s.label === action.label);
      const otherSuggestions = state.suggestions.filter((s) => s.label !== action.label);
      const clonedReset = structuredClone(resetSuggestions);
      return {
        ...state,
        suggestions: [...otherSuggestions, ...clonedReset],
        modifiedCandidates: removeFromArray(state.modifiedCandidates, state.activeCandidateIdx),
        reviewedCandidates: addToArray(state.reviewedCandidates, state.activeCandidateIdx),
      };
    }

    default:
      return state;
  }
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function IngestionWizard({ onExit }: Props) {
  const [searchParams, setSearchParams] = useSearchParams();
  const [state, dispatch] = useReducer(wizardReducer, initialState);
  const { show: showToast } = useToast();
  const { confirm, dialogProps } = useConfirmation();

  const {
    step,
    runId,
    candidates,
    suggestions,
    hydrationJobId,
    activeJob,
    enrichJobId,
    enrichJobStatus,
    error,
    isLoading,
    recentRuns,
    templates,
    selectedTemplateId,
    saveAsTemplate,
    newTemplateName,
    activeCandidateIdx,
    newSynonymInputs,
    reviewedCandidates,
    modifiedCandidates,
  } = state;

  const runIdFromUrl = searchParams.get("run_id");

  // Load run from URL if present
  useEffect(() => {
    if (runIdFromUrl && !runId) {
      loadRun(runIdFromUrl);
    }
  }, [runIdFromUrl, runId]);

  // Load recent runs and templates on intro step
  useEffect(() => {
    if (step === "intro") {
      IngestionService.listRuns("AWAITING_REVIEW")
        .then((runs) => dispatch({ type: "SET_RECENT_RUNS", runs }))
        .catch(console.error);
      IngestionService.listTemplates()
        .then((t) => dispatch({ type: "SET_TEMPLATES", templates: t }))
        .catch(console.error);
    }
  }, [step]);

  const loadRun = useCallback(async (id: string) => {
    dispatch({ type: "SET_LOADING", isLoading: true });
    dispatch({ type: "SET_ERROR", error: null });
    try {
      const run = await IngestionService.getRun(id);
      const snapshot = run.config_snapshot || {};
      const uiState = snapshot.ui_state || {};
      const targetStep = (uiState.current_step as WizardStep) || "review_candidates";

      dispatch({
        type: "LOAD_RUN_SUCCESS",
        runId: run.id,
        candidates: snapshot.candidates || [],
        suggestions: snapshot.draft_patterns || [],
        step: targetStep,
      });
      setSearchParams({ run_id: id });
    } catch (err: unknown) {
      dispatch({ type: "SET_ERROR", error: getErrorMessage(err) });
      dispatch({ type: "SET_LOADING", isLoading: false });
      showToast("Failed to load run", "error");
    }
  }, [setSearchParams, showToast]);

  // Hydration Job Polling
  const handleHydrationComplete = useCallback((job: OpsJobResponse) => {
    dispatch({ type: "SET_ACTIVE_JOB", job });
  }, []);

  const { job: hydrationJob } = useJobPolling({
    jobId: hydrationJobId,
    enabled: step === "complete",
    onComplete: handleHydrationComplete,
    onFailed: handleHydrationComplete,
  });

  // Sync hydration job to state
  useEffect(() => {
    if (hydrationJob) {
      dispatch({ type: "SET_ACTIVE_JOB", job: hydrationJob });
    }
  }, [hydrationJob]);

  // Enrichment Job Polling
  const handleEnrichComplete = useCallback(async (job: OpsJobResponse) => {
    if (runId) {
      try {
        const run = await IngestionService.getRun(runId);
        const initialSuggestions = run.config_snapshot?.draft_patterns || [];
        dispatch({ type: "ENRICH_COMPLETE", suggestions: initialSuggestions });
      } catch (e) {
        console.error(e);
      }
    }
  }, [runId]);

  const handleEnrichFailed = useCallback((job: OpsJobResponse) => {
    dispatch({ type: "SET_ERROR", error: job.error_message || "Enrichment job failed" });
    showToast("Enrichment failed", "error");
  }, [showToast]);

  const { job: enrichJob } = useJobPolling({
    jobId: enrichJobId,
    enabled: step === "enriching",
    onComplete: handleEnrichComplete,
    onFailed: handleEnrichFailed,
  });

  // Sync enrich job status to state for progress display
  useEffect(() => {
    if (enrichJob) {
      dispatch({ type: "SET_ENRICH_JOB_STATUS", status: enrichJob });
    }
  }, [enrichJob]);

  const handleAnalyze = async () => {
    dispatch({ type: "SET_STEP", step: "analyzing" });
    dispatch({ type: "SET_ERROR", error: null });
    try {
      const res = await IngestionService.analyze(undefined, selectedTemplateId || undefined);
      dispatch({ type: "ANALYZE_SUCCESS", runId: res.run_id, candidates: res.candidates });
      setSearchParams({ run_id: res.run_id });
    } catch (err: unknown) {
      dispatch({ type: "SET_ERROR", error: getErrorMessage(err) });
      dispatch({ type: "SET_STEP", step: "intro" });
      showToast("Analysis failed", "error");
    }
  };

  const handleEnrich = async () => {
    if (!runId) return;
    dispatch({ type: "SET_STEP", step: "enriching" });
    dispatch({ type: "SET_ERROR", error: null });
    dispatch({ type: "SET_ENRICH_JOB_STATUS", status: null });
    try {
      const selected = candidates.filter((c) => c.selected);
      const res = await IngestionService.enrich(runId, selected);
      dispatch({ type: "SET_ENRICH_JOB", jobId: res.job_id });
    } catch (err: unknown) {
      dispatch({ type: "SET_ERROR", error: getErrorMessage(err) });
      dispatch({ type: "SET_STEP", step: "review_candidates" });
      showToast("Enrichment failed", "error");
    }
  };

  const handleCommit = async () => {
    if (!runId) return;
    const approved = suggestions.filter((s) => s.accepted);

    if (approved.length === 0) {
      showToast("Cannot commit empty pattern set. Accept at least one suggestion.", "error");
      return;
    }

    const suspicious = approved.filter((s) => s.pattern.length > 50 || s.pattern.trim() === "");
    if (suspicious.length > 0) {
      const isConfirmed = await confirm({
        title: "Suspicious Patterns Detected",
        description: `Warning: ${suspicious.length} patterns are unusually long or empty. Proceed anyway?`,
        confirmText: "Proceed",
        danger: true
      });
      if (!isConfirmed) return;
    }

    if (saveAsTemplate && newTemplateName) {
      try {
        await IngestionService.createTemplate({
          name: newTemplateName,
          config: { target_tables: candidates.filter((c) => c.selected).map((c) => c.table) },
        });
        showToast("Template saved", "success");
      } catch (e) {
        console.error("Failed to save template", e);
      }
    }

    dispatch({ type: "SET_STEP", step: "committing" });
    dispatch({ type: "SET_ERROR", error: null });
    try {
      const res = await IngestionService.commit(runId, approved);
      dispatch({ type: "SET_HYDRATION_JOB", jobId: res.hydration_job_id });
      dispatch({ type: "SET_STEP", step: "complete" });
      showToast("Ingestion committed successfully", "success");
    } catch (err: unknown) {
      dispatch({ type: "SET_ERROR", error: getErrorMessage(err) });
      dispatch({ type: "SET_STEP", step: "review_suggestions" });
      showToast("Commit failed", "error");
    }
  };

  // --- Handlers ---

  const toggleCandidate = (index: number) => {
    dispatch({ type: "TOGGLE_CANDIDATE", index });
  };

  const toggleSuggestion = (suggIndex: number) => {
    dispatch({ type: "TOGGLE_SUGGESTION", suggIndex });
  };

  const addSynonym = (canonicalId: string, label: string) => {
    const key = `${label}:${canonicalId}`;
    const pattern = newSynonymInputs[key]?.trim();
    if (!pattern) return;
    dispatch({ type: "ADD_SYNONYM", canonicalId, label, pattern });
  };

  const bulkAction = (action: "accept" | "reject" | "reset") => {
    const selectedCandidates = candidates.filter((c) => c.selected);
    const activeCandidate = selectedCandidates[activeCandidateIdx];
    if (!activeCandidate) return;

    if (action === "accept") {
      dispatch({ type: "BULK_ACCEPT", label: activeCandidate.label });
    } else if (action === "reject") {
      dispatch({ type: "BULK_REJECT", label: activeCandidate.label });
    } else {
      dispatch({ type: "BULK_RESET", label: activeCandidate.label });
    }
  };

  // Convert arrays to Sets for quick lookup in render
  const reviewedSet = new Set(reviewedCandidates);
  const modifiedSet = new Set(modifiedCandidates);

  return (
    <div className="panel" style={{ minHeight: "600px", border: "1px solid var(--accent)", position: "relative" }}>
      {/* Header */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "24px", borderBottom: "1px solid var(--border)", paddingBottom: "16px" }}>
        <div>
          <h2 style={{ margin: 0 }}>Ingestion Wizard</h2>
          <p className="subtitle" style={{ margin: 0 }}>Run ID: {runId || "Pending"}</p>
        </div>
        <button onClick={onExit} style={{ background: "transparent", border: "1px solid var(--border)", padding: "8px 16px", borderRadius: "8px", cursor: "pointer", color: "var(--muted)" }}>
          Exit
        </button>
      </div>

      {error && (
        <div style={{ padding: "12px", backgroundColor: "#fee2e2", color: "#b91c1c", borderRadius: "8px", marginBottom: "16px" }}>
          {error}
        </div>
      )}

      {/* Steps Content */}
      <div style={{ padding: "12px" }}>
        {step === "intro" && (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <p>Start a new pattern ingestion run by analyzing the database schema.</p>

            <div style={{ margin: "24px auto", maxWidth: "400px", textAlign: "left" }}>
              <label style={{ display: "block", marginBottom: "8px", fontSize: "0.9rem", fontWeight: 600 }}>Use Template (Optional)</label>
              <select
                value={selectedTemplateId}
                onChange={(e) => dispatch({ type: "SET_SELECTED_TEMPLATE", templateId: e.target.value })}
                style={{ width: "100%", padding: "10px", borderRadius: "8px", border: "1px solid var(--border)" }}
              >
                <option value="">No Template (Default)</option>
                {templates.map((t) => (
                  <option key={t.id} value={t.id}>{t.name}</option>
                ))}
              </select>
            </div>

            <button
              onClick={handleAnalyze}
              className="button"
              disabled={isLoading}
              style={{ background: "var(--accent)", color: "#fff", padding: "12px 24px", borderRadius: "8px", border: "none", fontSize: "1rem", cursor: "pointer", marginTop: "16px" }}
            >
              {isLoading ? "Loading..." : "Start Analysis"}
            </button>

            {recentRuns.length > 0 && (
              <div style={{ marginTop: "40px", borderTop: "1px solid var(--border)", paddingTop: "24px" }}>
                <h4 style={{ marginBottom: "12px" }}>Resume Previous Run</h4>
                <div style={{ display: "flex", flexDirection: "column", gap: "8px", maxWidth: "400px", margin: "0 auto" }}>
                  {recentRuns.map((run) => (
                    <button
                      key={run.id}
                      onClick={() => loadRun(run.id)}
                      style={{ background: "var(--surface-muted)", border: "1px solid var(--border)", padding: "10px", borderRadius: "6px", cursor: "pointer", textAlign: "left", fontSize: "0.9rem" }}
                    >
                      <div style={{ fontWeight: 600 }}>ID: {run.id.slice(0, 8)}...</div>
                      <div style={{ fontSize: "0.8rem", color: "var(--muted)" }}>Started: {new Date(run.started_at).toLocaleString()}</div>
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}

        {step === "analyzing" && (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <p>Analyzing schema and detecting enum candidates...</p>
            <div style={{ marginTop: "16px" }}>Loading...</div>
          </div>
        )}

        {step === "review_candidates" && (
          <div>
            <h3>Step 1: Review Candidates</h3>
            <p className="subtitle">Select columns to enrich with synonyms.</p>

            <div style={{ overflowX: "auto", border: "1px solid var(--border)", borderRadius: "8px" }}>
              <table style={{ width: "100%", borderCollapse: "collapse" }}>
                <thead>
                  <tr style={{ background: "var(--surface-muted)", textAlign: "left" }}>
                    <th style={{ padding: "12px", width: "40px" }}></th>
                    <th style={{ padding: "12px" }}>Table</th>
                    <th style={{ padding: "12px" }}>Column</th>
                    <th style={{ padding: "12px" }}>Distinct Values</th>
                    <th style={{ padding: "12px" }}>Label</th>
                  </tr>
                </thead>
                <tbody>
                  {candidates.map((c, i) => (
                    <tr key={i} style={{ borderTop: "1px solid var(--border)" }}>
                      <td style={{ padding: "12px", textAlign: "center" }}>
                        <input type="checkbox" checked={c.selected} onChange={() => toggleCandidate(i)} />
                      </td>
                      <td style={{ padding: "12px" }}>{c.table}</td>
                      <td style={{ padding: "12px" }}>{c.column}</td>
                      <td style={{ padding: "12px" }}>
                        <div style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: "0.85rem", color: "var(--muted)" }}>
                          {c.values.join(", ")}
                        </div>
                      </td>
                      <td style={{ padding: "12px" }}>
                        <code>{c.label}</code>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
              <button
                onClick={handleEnrich}
                disabled={!candidates.some((c) => c.selected)}
                style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer", opacity: !candidates.some((c) => c.selected) ? 0.5 : 1 }}
              >
                Generate Suggestions
              </button>
            </div>
          </div>
        )}

        {step === "enriching" && (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <p>Generating synonyms with LLM...</p>
            {enrichJobStatus && (
              <div style={{ marginTop: "20px", maxWidth: "400px", margin: "20px auto" }}>
                <div style={{ width: "100%", background: "var(--surface-muted)", height: "8px", borderRadius: "4px", overflow: "hidden" }}>
                  <div
                    style={{
                      width: `${((Number(enrichJobStatus.result?.processed) || 0) / (Number(enrichJobStatus.result?.total) || 1)) * 100}%`,
                      background: "var(--accent)",
                      height: "100%",
                      transition: "width 0.3s ease",
                    }}
                  />
                </div>
                <p style={{ marginTop: "8px", fontSize: "0.9rem", color: "var(--muted)" }}>Processed {Number(enrichJobStatus.result?.processed) || 0} of {Number(enrichJobStatus.result?.total) || 0} candidates</p>
              </div>
            )}
            <div style={{ marginTop: "16px" }}>Loading...</div>
          </div>
        )}

        {step === "review_suggestions" && (
          <div style={{ display: "grid", gridTemplateColumns: "250px 1fr", gap: "24px" }}>
            {/* Left: Candidate List */}
            <div style={{ borderRight: "1px solid var(--border)", paddingRight: "16px" }}>
              <h4 style={{ margin: "0 0 12px 0" }}>Candidates</h4>
              <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                {candidates
                  .filter((c) => c.selected)
                  .map((c, i) => {
                    const isActive = i === activeCandidateIdx;
                    const isModified = modifiedSet.has(i);
                    const isReviewed = reviewedSet.has(i);

                    return (
                      <button
                        key={i}
                        onClick={() => dispatch({ type: "SET_ACTIVE_CANDIDATE_IDX", index: i })}
                        style={{
                          textAlign: "left",
                          padding: "8px 12px",
                          background: isActive ? "var(--surface-muted)" : "transparent",
                          border: isActive ? "1px solid var(--accent)" : "1px solid transparent",
                          borderRadius: "6px",
                          cursor: "pointer",
                          fontWeight: isActive ? 600 : 400,
                          color: isActive ? "var(--ink)" : "var(--muted)",
                          display: "flex",
                          justifyContent: "space-between",
                          alignItems: "center",
                        }}
                      >
                        <span>
                          {c.table}.{c.column}
                        </span>
                        {isModified ? <span title="Modified" style={{ color: "#f59e0b" }}>●</span> : isReviewed ? <span title="Reviewed" style={{ color: "#10b981" }}>✓</span> : null}
                      </button>
                    );
                  })}
              </div>
            </div>

            {/* Right: Details */}
            <div>
              {(() => {
                const selectedCandidates = candidates.filter((c) => c.selected);
                const activeCandidate = selectedCandidates[activeCandidateIdx];

                if (!activeCandidate) return <div>Select a candidate</div>;

                return (
                  <div>
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: "16px" }}>
                      <div>
                        <h3 style={{ marginTop: 0, marginBottom: "4px" }}>
                          {activeCandidate.table}.{activeCandidate.column}
                        </h3>
                        <p className="subtitle" style={{ margin: 0 }}>
                          Entity Label: <code>{activeCandidate.label}</code>
                        </p>
                      </div>
                      <div style={{ display: "flex", gap: "8px" }}>
                        <button onClick={() => bulkAction("accept")} style={{ fontSize: "0.8rem", padding: "4px 8px", borderRadius: "4px", border: "1px solid var(--border)", cursor: "pointer" }}>Accept All</button>
                        <button onClick={() => bulkAction("reject")} style={{ fontSize: "0.8rem", padding: "4px 8px", borderRadius: "4px", border: "1px solid var(--border)", cursor: "pointer" }}>Reject All</button>
                        <button onClick={() => bulkAction("reset")} style={{ fontSize: "0.8rem", padding: "4px 8px", borderRadius: "4px", border: "1px solid var(--border)", cursor: "pointer" }}>Reset</button>
                      </div>
                    </div>

                    <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
                      {activeCandidate.values.map((val) => {
                        const relatedSuggestions = suggestions.map((s, idx) => ({ s, idx })).filter((item) => item.s.id === val && item.s.label === activeCandidate.label);
                        const inputKey = `${activeCandidate.label}:${val}`;

                        return (
                          <div key={val} style={{ border: "1px solid var(--border)", borderRadius: "8px", padding: "16px" }}>
                            <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "12px" }}>
                              <span style={{ background: "#e0e7ff", color: "#4338ca", padding: "4px 8px", borderRadius: "4px", fontSize: "0.85rem", fontWeight: 600 }}>{val}</span>
                              <span style={{ fontSize: "0.8rem", color: "var(--muted)" }}>(Canonical)</span>
                            </div>

                            <div style={{ display: "grid", gap: "8px" }}>
                              {relatedSuggestions.map(({ s, idx }) => (
                                <label key={idx} style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer" }}>
                                  <input type="checkbox" checked={s.accepted} onChange={() => toggleSuggestion(idx)} />
                                  <span style={{ textDecoration: s.accepted ? "none" : "line-through", color: s.accepted ? "var(--ink)" : "var(--muted)" }}>{s.pattern}</span>
                                  {s.is_new && (
                                    <span style={{ fontSize: "0.7rem", background: "#dcfce7", color: "#166534", padding: "2px 6px", borderRadius: "4px" }}>NEW</span>
                                  )}
                                  {s.pattern.length > 50 && <span title="Unusually long pattern" style={{ color: "#ef4444" }}>⚠</span>}
                                </label>
                              ))}
                            </div>

                            <div style={{ marginTop: "12px", display: "flex", gap: "8px" }}>
                              <input
                                type="text"
                                placeholder="Add synonym..."
                                value={newSynonymInputs[inputKey] || ""}
                                onChange={(e) => dispatch({ type: "UPDATE_SYNONYM_INPUT", key: inputKey, value: e.target.value })}
                                onKeyDown={(e) => {
                                  if (e.key === "Enter") {
                                    addSynonym(val, activeCandidate.label);
                                  }
                                }}
                                style={{ flex: 1, padding: "6px 10px", borderRadius: "4px", border: "1px solid var(--border)", fontSize: "0.9rem" }}
                              />
                              <button onClick={() => addSynonym(val, activeCandidate.label)} style={{ background: "transparent", border: "1px solid var(--border)", borderRadius: "4px", cursor: "pointer" }}>+</button>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                );
              })()}

              <div style={{ marginTop: "32px", borderTop: "1px solid var(--border)", paddingTop: "16px", display: "flex", gap: "12px" }}>
                <button
                  onClick={() => dispatch({ type: "SET_STEP", step: "confirmation" })}
                  style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}
                >
                  Next: Confirmation
                </button>
                <button
                  onClick={() => dispatch({ type: "SET_STEP", step: "review_candidates" })}
                  style={{ background: "transparent", border: "1px solid var(--border)", padding: "10px 20px", borderRadius: "6px", cursor: "pointer" }}
                >
                  Back
                </button>
              </div>
            </div>
          </div>
        )}

        {step === "confirmation" && (
          <div style={{ maxWidth: "600px", margin: "0 auto", padding: "24px" }}>
            <h3>Step 3: Confirmation</h3>
            <p>Review summary before committing.</p>

            <div style={{ background: "var(--surface-muted)", padding: "20px", borderRadius: "8px" }}>
              <ul style={{ margin: 0, paddingLeft: "20px" }}>
                <li>Candidates processed: <strong>{candidates.filter((c) => c.selected).length}</strong></li>
                <li>Total Patterns to Insert: <strong>{suggestions.filter((s) => s.accepted).length}</strong></li>
                <li>New Patterns: <strong>{suggestions.filter((s) => s.accepted && s.is_new).length}</strong></li>
              </ul>
            </div>

            <div style={{ marginTop: "24px", border: "1px solid var(--border)", padding: "16px", borderRadius: "8px" }}>
              <label style={{ display: "flex", alignItems: "center", gap: "10px", cursor: "pointer" }}>
                <input type="checkbox" checked={saveAsTemplate} onChange={(e) => dispatch({ type: "SET_SAVE_AS_TEMPLATE", enabled: e.target.checked })} />
                <span>Save this configuration as a template</span>
              </label>
              {saveAsTemplate && (
                <div style={{ marginTop: "12px" }}>
                  <input
                    type="text"
                    placeholder="Template Name (e.g. Sales Schema Baseline)"
                    value={newTemplateName}
                    onChange={(e) => dispatch({ type: "SET_NEW_TEMPLATE_NAME", name: e.target.value })}
                    style={{ width: "100%", padding: "8px", borderRadius: "4px", border: "1px solid var(--border)" }}
                  />
                </div>
              )}
            </div>

            <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
              <button
                onClick={handleCommit}
                style={{ background: "#10b981", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}
              >
                Commit & Hydrate
              </button>
              <button
                onClick={() => dispatch({ type: "SET_STEP", step: "review_suggestions" })}
                style={{ background: "transparent", border: "1px solid var(--border)", padding: "10px 20px", borderRadius: "6px", cursor: "pointer" }}
              >
                Back
              </button>
            </div>
          </div>
        )}

        {step === "committing" && (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <p>Committing patterns and triggering hydration...</p>
            <div>Processing...</div>
          </div>
        )}

        {step === "complete" && (
          <div style={{ textAlign: "center", padding: "40px" }}>
            <h3 style={{ color: "#10b981" }}>Success!</h3>
            <p>Patterns committed and hydration job started.</p>
            <div style={{ margin: "24px 0", background: "var(--surface-muted)", padding: "16px", borderRadius: "8px", display: "inline-block", textAlign: "left" }}>
              <div style={{ marginBottom: "8px" }}>
                <strong>Hydration Job:</strong> {activeJob ? activeJob.status : "Loading..."}
              </div>
              <div style={{ fontSize: "0.85rem", color: "var(--muted)" }}>ID: {hydrationJobId}</div>
              {activeJob?.error_message && <div style={{ color: "red", marginTop: "8px" }}>Error: {activeJob.error_message}</div>}
            </div>
            <div style={{ marginTop: "24px" }}>
              <button
                onClick={onExit}
                style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}
              >
                Done
              </button>
            </div>
          </div>
        )}
      </div>
      <ConfirmationDialog {...dialogProps} />
    </div>
  );
}
