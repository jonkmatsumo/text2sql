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

/**
 * Lightweight runtime check for InteractionListResponse.
 */
export function isInteractionListResponse(value: unknown): value is { data: Interaction[], has_more?: boolean } {
    const v = value as any;
    return !!(
        v &&
        typeof v === "object" &&
        Array.isArray(v.data) &&
        (v.has_more === undefined || typeof v.has_more === "boolean")
    );
}

/**
 * Validates and extracts common identifiers from an unexpected response payload.
 */
export function extractIdentifiers(value: unknown): Record<string, string> {
    const ids: Record<string, string> = {};
    if (value && typeof value === "object" && !Array.isArray(value)) {
        const v = value as Record<string, unknown>;
        const keys = ["request_id", "trace_id", "run_id", "job_id", "id"];
        for (const key of keys) {
            if (typeof v[key] === "string") {
                ids[key] = v[key] as string;
            }
        }
    }
    return ids;
}

/**
 * Produces a bounded, safe string summary of an unexpected response payload.
 * Handles circular references and truncates to maxChars.
 */
export function summarizeUnexpectedResponse(value: unknown, maxChars = 8000): string {
    if (value === null) return "null";
    if (value === undefined) return "undefined";

    try {
        const cache = new Set();
        const json = JSON.stringify(value, (key, val) => {
            if (typeof val === "object" && val !== null) {
                if (cache.has(val)) return "[Circular]";
                cache.add(val);
            }
            return val;
        }, 2);

        if (json.length > maxChars) {
            return json.substring(0, maxChars) + "... [truncated]";
        }
        return json;
    } catch (err) {
        return `[Serialization Error: ${err}]`;
    }
}
