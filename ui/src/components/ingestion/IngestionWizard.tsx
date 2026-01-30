import React, { useState } from "react";
import { IngestionService, IngestionCandidate, Suggestion } from "../../api";
import { useToast } from "../../hooks/useToast";

interface Props {
  onExit: () => void;
}

type WizardStep =
  | "intro"
  | "analyzing"
  | "review_candidates"
  | "enriching"
  | "review_suggestions"
  | "committing"
  | "complete";

export default function IngestionWizard({ onExit }: Props) {
  const [step, setStep] = useState<WizardStep>("intro");
  const [runId, setRunId] = useState<string | null>(null);
  const [candidates, setCandidates] = useState<IngestionCandidate[]>([]);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [hydrationJobId, setHydrationJobId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const { show: showToast } = useToast();

  const handleAnalyze = async () => {
    setStep("analyzing");
    setError(null);
    try {
      const res = await IngestionService.analyze();
      setRunId(res.run_id);
      setCandidates(res.candidates.map(c => ({ ...c, selected: true })));
      setStep("review_candidates");
    } catch (err: any) {
      setError(err.message || "Analysis failed");
      setStep("intro");
      showToast("Analysis failed", "error");
    }
  };

  const handleEnrich = async () => {
    if (!runId) return;
    setStep("enriching");
    setError(null);
    try {
      const selected = candidates.filter(c => c.selected);
      const res = await IngestionService.enrich(runId, selected);
      setSuggestions(res.suggestions.map(s => ({ ...s, accepted: true })));
      setStep("review_suggestions");
    } catch (err: any) {
      setError(err.message || "Enrichment failed");
      setStep("review_candidates");
      showToast("Enrichment failed", "error");
    }
  };

  const handleCommit = async () => {
    if (!runId) return;
    setStep("committing");
    setError(null);
    try {
      const approved = suggestions.filter(s => s.accepted);
      const res = await IngestionService.commit(runId, approved);
      setHydrationJobId(res.hydration_job_id);
      setStep("complete");
      showToast("Ingestion committed successfully", "success");
    } catch (err: any) {
      setError(err.message || "Commit failed");
      setStep("review_suggestions");
      showToast("Commit failed", "error");
    }
  };

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
                <button
                  onClick={handleAnalyze}
                  className="button"
                  style={{ background: "var(--accent)", color: "#fff", padding: "12px 24px", borderRadius: "8px", border: "none", fontSize: "1rem", cursor: "pointer", marginTop: "16px" }}
                >
                    Start Analysis
                </button>
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
                <p>Select columns to enrich with synonyms.</p>
                <pre style={{ background: "#f1f1f1", padding: "12px", borderRadius: "8px", maxHeight: "300px", overflow: "auto" }}>
                  {JSON.stringify(candidates, null, 2)}
                </pre>

                <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
                    <button onClick={handleEnrich} style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}>
                        Generate Suggestions
                    </button>
                </div>
            </div>
        )}

        {step === "enriching" && (
            <div style={{ textAlign: "center", padding: "40px" }}>
                <p>Generating synonyms with LLM...</p>
                <div>Loading...</div>
            </div>
        )}

        {step === "review_suggestions" && (
            <div>
                <h3>Step 2: Review Suggestions</h3>
                <p>Review and curate generated synonyms.</p>
                <pre style={{ background: "#f1f1f1", padding: "12px", borderRadius: "8px", maxHeight: "300px", overflow: "auto" }}>
                  {JSON.stringify(suggestions, null, 2)}
                </pre>

                <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
                    <button onClick={handleCommit} style={{ background: "#10b981", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}>
                        Commit & Hydrate
                    </button>
                    <button onClick={() => setStep("review_candidates")} style={{ background: "transparent", border: "1px solid var(--border)", padding: "10px 20px", borderRadius: "6px", cursor: "pointer" }}>
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
                <p>Job ID: {hydrationJobId}</p>
                <div style={{ marginTop: "24px" }}>
                    <button onClick={onExit} style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}>
                        Done
                    </button>
                </div>
            </div>
        )}
      </div>
    </div>
  );
}
