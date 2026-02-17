import { useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { CopyButton } from "../artifacts/CopyButton";
import RunIdentifiers from "../common/RunIdentifiers";

export type DiagnosticsFilterMode = "all" | "anomalies";
export type DiagnosticsSection = "all" | "runtime" | "config" | "latency" | "raw";

export interface DiagnosticsIdentifiers {
  traceId?: string;
  interactionId?: string;
  requestId?: string;
}

interface DiagnosticsFiltersProps {
  selectedSection: DiagnosticsSection;
  onSelectedSectionChange: (section: DiagnosticsSection) => void;
  selectedPanelJson: string;
  identifiers?: DiagnosticsIdentifiers;
}

export function parseDiagnosticsSection(raw: string | null): DiagnosticsSection {
  if (raw === "runtime" || raw === "config" || raw === "latency" || raw === "raw") {
    return raw;
  }
  return "all";
}

export function useDiagnosticsViewFilters() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [isDebug, setIsDebug] = useState(() => searchParams.get("debug") === "1");
  const [filterMode, setFilterMode] = useState<DiagnosticsFilterMode>(
    () => (searchParams.get("filter") === "anomalies" ? "anomalies" : "all")
  );
  const [selectedSection, setSelectedSection] = useState<DiagnosticsSection>(
    () => parseDiagnosticsSection(searchParams.get("section"))
  );

  useEffect(() => {
    const nextParams = new URLSearchParams(searchParams);
    if (isDebug) nextParams.set("debug", "1");
    else nextParams.delete("debug");

    if (filterMode === "anomalies") nextParams.set("filter", "anomalies");
    else nextParams.delete("filter");

    if (selectedSection !== "all") nextParams.set("section", selectedSection);
    else nextParams.delete("section");

    if (nextParams.toString() !== searchParams.toString()) {
      setSearchParams(nextParams, { replace: true });
    }
  }, [filterMode, isDebug, searchParams, selectedSection, setSearchParams]);

  return {
    isDebug,
    setIsDebug,
    filterMode,
    setFilterMode,
    selectedSection,
    setSelectedSection,
  };
}

export function DiagnosticsFilters({
  selectedSection,
  onSelectedSectionChange,
  selectedPanelJson,
  identifiers,
}: DiagnosticsFiltersProps) {
  return (
    <div style={{ display: "flex", gap: "8px", alignItems: "center" }}>
      <RunIdentifiers
        traceId={identifiers?.traceId}
        interactionId={identifiers?.interactionId}
        requestId={identifiers?.requestId}
      />
      <Link
        to="/admin/traces/search"
        data-testid="diagnostics-open-trace-search"
        style={{ fontSize: "0.82rem", color: "var(--accent)" }}
      >
        Open Trace Search
      </Link>
      <Link
        to="/admin/jobs"
        data-testid="diagnostics-open-jobs-dashboard"
        style={{ fontSize: "0.82rem", color: "var(--accent)" }}
      >
        Open Jobs Dashboard
      </Link>
      <CopyButton text={selectedPanelJson} label="Copy selected panel" />
      <label
        style={{
          display: "flex",
          alignItems: "center",
          gap: "6px",
          color: "var(--muted)",
          fontSize: "0.82rem",
          marginRight: "8px",
        }}
      >
        Section
        <select
          data-testid="diagnostics-section-select"
          value={selectedSection}
          onChange={(event) => onSelectedSectionChange(parseDiagnosticsSection(event.target.value))}
          style={{
            borderRadius: "8px",
            border: "1px solid var(--border)",
            padding: "4px 8px",
            fontSize: "0.82rem",
          }}
        >
          <option value="all">All</option>
          <option value="runtime">Runtime</option>
          <option value="config">Config</option>
          <option value="latency">Latency</option>
          <option value="raw">Raw JSON</option>
        </select>
      </label>
    </div>
  );
}
