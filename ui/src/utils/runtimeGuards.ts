import { Interaction, OpsJobStatus, JobStatusResponse } from "../types/admin";
import { RunDiagnosticsResponse } from "../types/diagnostics";

/**
 * Lightweight runtime check for Interaction array.
 * Validates only the presence of minimal required keys.
 */
export function isInteractionArray(value: unknown): value is Interaction[] {
    if (!Array.isArray(value)) return false;
    if (value.length === 0) return true;

    // Just check the first element for structural integrity if array is large,
    // or check all if small. For safety, we check all since these are small lists.
    return value.every(item =>
        item &&
        typeof item === "object" &&
        typeof item.id === "string" &&
        typeof item.execution_status === "string"
    );
}

/**
 * Lightweight runtime check for JobStatusResponse.
 */
export function isJobStatusResponse(value: unknown): value is JobStatusResponse {
    const v = value as any;
    return !!(
        v &&
        typeof v === "object" &&
        typeof v.id === "string" &&
        typeof v.status === "string" &&
        typeof v.job_type === "string" &&
        typeof v.started_at === "string" &&
        ["PENDING", "RUNNING", "CANCELLING", "CANCELLED", "COMPLETED", "FAILED"].includes(v.status)
    );
}

/**
 * Lightweight runtime check for RunDiagnosticsResponse.
 */
export function isRunDiagnosticsResponse(value: unknown): value is RunDiagnosticsResponse {
    const v = value as any;
    return !!(
        v &&
        typeof v === "object" &&
        typeof v.diagnostics_schema_version === "number" &&
        v.enabled_flags &&
        typeof v.enabled_flags === "object"
    );
}
