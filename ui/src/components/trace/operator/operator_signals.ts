export type OperatorSignalTone = "ok" | "warn" | "error" | "info";

export interface OperatorSignalItem {
  label: string;
  value: string;
}

export interface OperatorSignalSection {
  id: string;
  title: string;
  tooltip: string;
  tone: OperatorSignalTone;
  items: OperatorSignalItem[];
}

function readBool(attrs: Record<string, unknown>, key: string): boolean | null {
  const value = attrs[key];
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true") return true;
    if (normalized === "false") return false;
  }
  return null;
}

function readNumber(attrs: Record<string, unknown>, key: string): number | null {
  const value = attrs[key];
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function readString(attrs: Record<string, unknown>, key: string): string | null {
  const value = attrs[key];
  if (typeof value === "string" && value.trim() !== "") return value;
  return null;
}

function boolText(value: boolean | null): string {
  if (value === null) return "—";
  return value ? "Yes" : "No";
}

function numberText(value: number | null, suffix = ""): string {
  if (value === null) return "—";
  return `${value}${suffix}`;
}

export function extractOperatorSignals(
  attrs: Record<string, unknown>
): OperatorSignalSection[] {
  const sections: OperatorSignalSection[] = [];

  const rowsReturned = readNumber(attrs, "result.rows_returned");
  const isTruncated = readBool(attrs, "result.is_truncated");
  const partialReason = readString(attrs, "result.partial_reason");
  const capDetected = readBool(attrs, "result.cap_detected");
  const capMitigationMode = readString(attrs, "result.cap_mitigation_mode");
  if (
    rowsReturned !== null ||
    isTruncated !== null ||
    partialReason !== null ||
    capDetected !== null ||
    capMitigationMode !== null
  ) {
    sections.push({
      id: "completeness",
      title: "Completeness Envelope",
      tooltip: "Rows, truncation, and provider-cap signals for this span.",
      tone: isTruncated ? "warn" : "ok",
      items: [
        { label: "Rows Returned", value: numberText(rowsReturned) },
        { label: "Truncated", value: boolText(isTruncated) },
        { label: "Partial Reason", value: partialReason ?? "—" },
        { label: "Provider Cap Detected", value: boolText(capDetected) },
        { label: "Cap Mitigation", value: capMitigationMode ?? "—" }
      ]
    });
  }

  const retryAttempt = readNumber(attrs, "retry.attempt_number");
  const retryBudgetLeft = readNumber(attrs, "retry.remaining_budget_seconds");
  const retryStoppedBudget = readBool(attrs, "retry.stopped_due_to_budget");
  const retryStoppedCapability = readBool(attrs, "retry.stopped_due_to_capability");
  if (
    retryAttempt !== null ||
    retryBudgetLeft !== null ||
    retryStoppedBudget !== null ||
    retryStoppedCapability !== null
  ) {
    const retryTone: OperatorSignalTone =
      retryStoppedBudget || retryStoppedCapability ? "warn" : "info";
    sections.push({
      id: "retries",
      title: "Retry Summary",
      tooltip: "Retry attempt counters and budget stop signals.",
      tone: retryTone,
      items: [
        { label: "Attempt", value: numberText(retryAttempt) },
        { label: "Remaining Budget", value: numberText(retryBudgetLeft, "s") },
        { label: "Stopped by Budget", value: boolText(retryStoppedBudget) },
        { label: "Stopped by Capability", value: boolText(retryStoppedCapability) }
      ]
    });
  }

  const preExecPassed = readBool(attrs, "validation.pre_exec_check_passed");
  const preExecMissing = readNumber(attrs, "validation.pre_exec_missing_tables");
  const schemaBoundEnabled = readBool(attrs, "validation.schema_bound_enabled");
  if (preExecPassed !== null || preExecMissing !== null || schemaBoundEnabled !== null) {
    sections.push({
      id: "validation",
      title: "Schema Validation",
      tooltip: "Schema-bound and pre-execution validation outcomes.",
      tone: preExecPassed === false ? "error" : "ok",
      items: [
        { label: "Pre-Exec Check Passed", value: boolText(preExecPassed) },
        { label: "Missing Tables", value: numberText(preExecMissing) },
        { label: "Schema-Bound Validation", value: boolText(schemaBoundEnabled) }
      ]
    });
  }

  const capabilityRequired = readString(attrs, "capability.required");
  const capabilitySupported = readBool(attrs, "capability.supported");
  const fallbackApplied = readBool(attrs, "capability.fallback_applied");
  const fallbackMode = readString(attrs, "capability.fallback_mode");
  const fallbackPolicy = readString(attrs, "capability.fallback_policy");
  if (
    capabilityRequired !== null ||
    capabilitySupported !== null ||
    fallbackApplied !== null ||
    fallbackMode !== null ||
    fallbackPolicy !== null
  ) {
    const isSuggest = fallbackPolicy === "suggest";
    const capabilityTone: OperatorSignalTone =
      fallbackApplied === true ? "warn" : capabilitySupported === false ? (isSuggest ? "info" : "error") : "info";

    sections.push({
      id: "capability",
      title: "Capability Fallback",
      tooltip: "Required capability support and explicit fallback decisions.",
      tone: capabilityTone,
      items: [
        { label: "Required Capability", value: capabilityRequired ?? "—" },
        { label: "Capability Supported", value: boolText(capabilitySupported) },
        { label: "Fallback Policy", value: (fallbackPolicy ?? "—") + (isSuggest ? " (Advisory)" : "") },
        { label: "Fallback Applied", value: boolText(fallbackApplied) },
        { label: "Fallback Mode", value: fallbackMode ?? "—" }
      ]
    });
  }

  const autoPaginated = readBool(attrs, "pagination.auto_paginated");
  const pagesFetched = readNumber(attrs, "pagination.pages_fetched");
  const autoStopReason = readString(attrs, "pagination.auto_stopped_reason");
  const prefetchEnabled = readBool(attrs, "prefetch.enabled");
  const prefetchScheduled = readBool(attrs, "prefetch.scheduled");
  const prefetchReason = readString(attrs, "prefetch.reason");
  if (
    autoPaginated !== null ||
    pagesFetched !== null ||
    autoStopReason !== null ||
    prefetchEnabled !== null ||
    prefetchScheduled !== null ||
    prefetchReason !== null
  ) {
    sections.push({
      id: "pagination",
      title: "Pagination & Prefetch",
      tooltip: "Automatic pagination and background prefetch execution decisions.",
      tone: autoPaginated || prefetchScheduled ? "info" : "ok",
      items: [
        { label: "Auto Paginated", value: boolText(autoPaginated) },
        { label: "Pages Fetched", value: numberText(pagesFetched) },
        { label: "Auto Stop Reason", value: autoStopReason ?? "—" },
        { label: "Prefetch Enabled", value: boolText(prefetchEnabled) },
        { label: "Prefetch Scheduled", value: boolText(prefetchScheduled) },
        { label: "Prefetch Reason", value: prefetchReason ?? "—" }
      ]
    });
  }

  return sections;
}
