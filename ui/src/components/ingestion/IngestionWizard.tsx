import React, { useState, useEffect } from "react";
import { IngestionService, IngestionCandidate, Suggestion, OpsService } from "../../api";
import { OpsJobResponse } from "../../types/admin";
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
  | "confirmation"
  | "committing"
  | "complete";

export default function IngestionWizard({ onExit }: Props) {
  const [step, setStep] = useState<WizardStep>("intro");
  const [runId, setRunId] = useState<string | null>(null);
  const [candidates, setCandidates] = useState<IngestionCandidate[]>([]);
  const [suggestions, setSuggestions] = useState<Suggestion[]>([]);
  const [hydrationJobId, setHydrationJobId] = useState<string | null>(null);
  const [activeJob, setActiveJob] = useState<OpsJobResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Step 2 State
  const [activeCandidateIdx, setActiveCandidateIdx] = useState<number>(0);
  const [newSynonymInputs, setNewSynonymInputs] = useState<Record<string, string>>({});

  const { show: showToast } = useToast();

  // Polling Effect for Hydration Job
  useEffect(() => {
    let timer: any;
    if (hydrationJobId && step === "complete") {
        // Initial fetch
        OpsService.getJobStatus(hydrationJobId).then(setActiveJob).catch(console.error);

        timer = setInterval(async () => {
            try {
                const status = await OpsService.getJobStatus(hydrationJobId);
                setActiveJob(status);
                if (status.status === "COMPLETED" || status.status === "FAILED") {
                    clearInterval(timer);
                }
            } catch (e) { console.error(e); }
        }, 2000);
    }
    return () => clearInterval(timer);
  }, [hydrationJobId, step]);

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
      setActiveCandidateIdx(0); // Reset to first candidate
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

  // --- Handlers ---

  const toggleCandidate = (index: number) => {
    const newCandidates = [...candidates];
    newCandidates[index].selected = !newCandidates[index].selected;
    setCandidates(newCandidates);
  };

  const toggleSuggestion = (suggIndex: number) => {
      const newSuggestions = [...suggestions];
      newSuggestions[suggIndex].accepted = !newSuggestions[suggIndex].accepted;
      setSuggestions(newSuggestions);
  };

  const addSynonym = (canonicalId: string, label: string) => {
      const key = `${label}:${canonicalId}`;
      const pattern = newSynonymInputs[key]?.trim();
      if (!pattern) return;

      const newSug: Suggestion = {
          id: canonicalId,
          label: label,
          pattern: pattern.toLowerCase(),
          accepted: true,
          is_new: true
      };
      setSuggestions([...suggestions, newSug]);
      setNewSynonymInputs({ ...newSynonymInputs, [key]: "" });
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
                                        <input
                                            type="checkbox"
                                            checked={c.selected}
                                            onChange={() => toggleCandidate(i)}
                                        />
                                    </td>
                                    <td style={{ padding: "12px" }}>{c.table}</td>
                                    <td style={{ padding: "12px" }}>{c.column}</td>
                                    <td style={{ padding: "12px" }}>
                                        <div style={{ maxWidth: "200px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", fontSize: "0.85rem", color: "var(--muted)" }}>
                                            {c.values.join(", ")}
                                        </div>
                                    </td>
                                    <td style={{ padding: "12px" }}><code>{c.label}</code></td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>

                <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
                    <button
                        onClick={handleEnrich}
                        disabled={!candidates.some(c => c.selected)}
                        style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer", opacity: !candidates.some(c => c.selected) ? 0.5 : 1 }}
                    >
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
            <div style={{ display: "grid", gridTemplateColumns: "250px 1fr", gap: "24px" }}>
                {/* Left: Candidate List */}
                <div style={{ borderRight: "1px solid var(--border)", paddingRight: "16px" }}>
                    <h4 style={{ margin: "0 0 12px 0" }}>Candidates</h4>
                    <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                        {candidates.filter(c => c.selected).map((c, i) => {
                             const isActive = i === activeCandidateIdx;
                             return (
                                <button
                                    key={i}
                                    onClick={() => setActiveCandidateIdx(i)}
                                    style={{
                                        textAlign: "left",
                                        padding: "8px 12px",
                                        background: isActive ? "var(--surface-muted)" : "transparent",
                                        border: "none",
                                        borderRadius: "6px",
                                        cursor: "pointer",
                                        fontWeight: isActive ? 600 : 400,
                                        color: isActive ? "var(--ink)" : "var(--muted)"
                                    }}
                                >
                                    {c.table}.{c.column}
                                </button>
                             );
                        })}
                    </div>
                </div>

                {/* Right: Details */}
                <div>
                   {(() => {
                       const selectedCandidates = candidates.filter(c => c.selected);
                       const activeCandidate = selectedCandidates[activeCandidateIdx];

                       if (!activeCandidate) return <div>Select a candidate</div>;

                       return (
                           <div>
                               <h3 style={{ marginTop: 0 }}>{activeCandidate.table}.{activeCandidate.column}</h3>
                               <p className="subtitle">Entity Label: <code>{activeCandidate.label}</code></p>

                               <div style={{ display: "flex", flexDirection: "column", gap: "24px" }}>
                                   {activeCandidate.values.map(val => {
                                       const relatedSuggestions = suggestions.map((s, idx) => ({ s, idx })).filter(item => item.s.id === val && item.s.label === activeCandidate.label);
                                       const inputKey = `${activeCandidate.label}:${val}`;

                                       return (
                                           <div key={val} style={{ border: "1px solid var(--border)", borderRadius: "8px", padding: "16px" }}>
                                               <div style={{ display: "flex", alignItems: "center", gap: "12px", marginBottom: "12px" }}>
                                                   <span style={{ background: "#e0e7ff", color: "#4338ca", padding: "4px 8px", borderRadius: "4px", fontSize: "0.85rem", fontWeight: 600 }}>
                                                       {val}
                                                   </span>
                                                   <span style={{ fontSize: "0.8rem", color: "var(--muted)" }}>(Canonical)</span>
                                               </div>

                                               <div style={{ display: "grid", gap: "8px" }}>
                                                   {relatedSuggestions.map(({ s, idx }) => (
                                                       <label key={idx} style={{ display: "flex", alignItems: "center", gap: "8px", cursor: "pointer" }}>
                                                           <input
                                                               type="checkbox"
                                                               checked={s.accepted}
                                                               onChange={() => toggleSuggestion(idx)}
                                                           />
                                                           <span style={{ textDecoration: s.accepted ? "none" : "line-through", color: s.accepted ? "var(--ink)" : "var(--muted)" }}>
                                                               {s.pattern}
                                                           </span>
                                                           {s.is_new && <span style={{ fontSize: "0.7rem", background: "#dcfce7", color: "#166534", padding: "2px 6px", borderRadius: "4px" }}>NEW</span>}
                                                       </label>
                                                   ))}
                                               </div>

                                               <div style={{ marginTop: "12px", display: "flex", gap: "8px" }}>
                                                   <input
                                                       type="text"
                                                       placeholder="Add synonym..."
                                                       value={newSynonymInputs[inputKey] || ""}
                                                       onChange={(e) => setNewSynonymInputs({ ...newSynonymInputs, [inputKey]: e.target.value })}
                                                       onKeyDown={(e) => {
                                                           if (e.key === "Enter") {
                                                               addSynonym(val, activeCandidate.label);
                                                           }
                                                       }}
                                                       style={{ flex: 1, padding: "6px 10px", borderRadius: "4px", border: "1px solid var(--border)", fontSize: "0.9rem" }}
                                                   />
                                                   <button
                                                        onClick={() => addSynonym(val, activeCandidate.label)}
                                                        style={{ background: "transparent", border: "1px solid var(--border)", borderRadius: "4px", cursor: "pointer" }}
                                                    >
                                                        +
                                                    </button>
                                               </div>
                                           </div>
                                       );
                                   })}
                               </div>
                           </div>
                       );
                   })()}

                    <div style={{ marginTop: "32px", borderTop: "1px solid var(--border)", paddingTop: "16px", display: "flex", gap: "12px" }}>
                        <button onClick={() => setStep("confirmation")} style={{ background: "var(--accent)", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}>
                            Next: Confirmation
                        </button>
                        <button onClick={() => setStep("review_candidates")} style={{ background: "transparent", border: "1px solid var(--border)", padding: "10px 20px", borderRadius: "6px", cursor: "pointer" }}>
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
                        <li>Candidates processed: <strong>{candidates.filter(c => c.selected).length}</strong></li>
                        <li>Total Patterns to Insert: <strong>{suggestions.filter(s => s.accepted).length}</strong></li>
                        <li>New Patterns: <strong>{suggestions.filter(s => s.accepted && s.is_new).length}</strong></li>
                    </ul>
                </div>

                <div style={{ marginTop: "24px", display: "flex", gap: "12px" }}>
                    <button onClick={handleCommit} style={{ background: "#10b981", color: "#fff", padding: "10px 20px", borderRadius: "6px", border: "none", cursor: "pointer" }}>
                        Commit & Hydrate
                    </button>
                    <button onClick={() => setStep("review_suggestions")} style={{ background: "transparent", border: "1px solid var(--border)", padding: "10px 20px", borderRadius: "6px", cursor: "pointer" }}>
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
                    <div style={{ marginBottom: "8px" }}><strong>Hydration Job:</strong> {activeJob ? activeJob.status : "Loading..."}</div>
                    <div style={{ fontSize: "0.85rem", color: "var(--muted)" }}>ID: {hydrationJobId}</div>
                    {activeJob?.error_message && <div style={{ color: "red", marginTop: "8px" }}>Error: {activeJob.error_message}</div>}
                </div>
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
