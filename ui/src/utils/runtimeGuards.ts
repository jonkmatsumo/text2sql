import { Interaction, OpsJobResponse, OpsJobStatus, JobStatusResponse, CancelJobResponse } from "../types/admin";
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
 * Lightweight runtime check for CancelJobResponse.
 */
export function isCancelJobResponse(value: unknown): value is CancelJobResponse {
    const v = value as any;
    return !!(
        v &&
        typeof v === "object" &&
        typeof v.success === "boolean"
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
 * Lightweight runtime check for OpsJobResponse array payloads.
 */
export function isOpsJobResponseArray(value: unknown): value is OpsJobResponse[] {
    if (!Array.isArray(value)) return false;
    if (value.length === 0) return true;

    return value.every((item) => {
        const v = item as any;
        return !!(
            v &&
            typeof v === "object" &&
            typeof v.id === "string" &&
            typeof v.job_type === "string" &&
            typeof v.started_at === "string" &&
            typeof v.status === "string" &&
            ["PENDING", "RUNNING", "CANCELLING", "CANCELLED", "COMPLETED", "FAILED"].includes(v.status)
        );
    });
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
        const keys = ["request_id", "trace_id", "run_id", "job_id", "interaction_id", "id"];
        for (const key of keys) {
            if (typeof v[key] === "string") {
                ids[key] = v[key] as string;
            }
        }
    }
    return ids;
}

interface ResponseSummaryOptions {
    maxChars: number;
    maxArrayItems: number;
    maxObjectKeys: number;
    maxDepth: number;
}

type ResponseSummaryOptionInput = number | Partial<ResponseSummaryOptions>;

const DEFAULT_SUMMARY_OPTIONS: ResponseSummaryOptions = {
    maxChars: 8000,
    maxArrayItems: 20,
    maxObjectKeys: 40,
    maxDepth: 6,
};

function normalizeSummaryOptions(input?: ResponseSummaryOptionInput): ResponseSummaryOptions {
    if (typeof input === "number") {
        return { ...DEFAULT_SUMMARY_OPTIONS, maxChars: input };
    }

    return {
        maxChars: input?.maxChars ?? DEFAULT_SUMMARY_OPTIONS.maxChars,
        maxArrayItems: input?.maxArrayItems ?? DEFAULT_SUMMARY_OPTIONS.maxArrayItems,
        maxObjectKeys: input?.maxObjectKeys ?? DEFAULT_SUMMARY_OPTIONS.maxObjectKeys,
        maxDepth: input?.maxDepth ?? DEFAULT_SUMMARY_OPTIONS.maxDepth,
    };
}

function toBoundedSerializable(
    value: unknown,
    options: ResponseSummaryOptions,
    depth: number,
    seen: Set<unknown>
): unknown {
    if (value == null || typeof value === "boolean" || typeof value === "number" || typeof value === "string") {
        return value;
    }

    if (typeof value === "bigint") {
        return `${value.toString()}n`;
    }

    if (typeof value !== "object") {
        return String(value);
    }

    if (seen.has(value)) {
        return "[Circular]";
    }

    if (depth >= options.maxDepth) {
        return "[MaxDepth]";
    }

    seen.add(value);

    if (Array.isArray(value)) {
        const limited = value
            .slice(0, options.maxArrayItems)
            .map((item) => toBoundedSerializable(item, options, depth + 1, seen));

        if (value.length > options.maxArrayItems) {
            limited.push(`[+${value.length - options.maxArrayItems} more items]`);
        }

        return limited;
    }

    const source = value as Record<string, unknown>;
    const sortedKeys = Object.keys(source).sort((left, right) => left.localeCompare(right));
    const limitedKeys = sortedKeys.slice(0, options.maxObjectKeys);
    const normalized: Record<string, unknown> = {};

    for (const key of limitedKeys) {
        normalized[key] = toBoundedSerializable(source[key], options, depth + 1, seen);
    }

    if (sortedKeys.length > options.maxObjectKeys) {
        normalized.__truncated_keys__ = `[+${sortedKeys.length - options.maxObjectKeys} more keys]`;
    }

    return normalized;
}

export interface ContractMismatchReport {
    surface: string;
    ids: Record<string, string>;
    request_context?: Record<string, unknown>;
    response_preview: string;
}

export function buildContractMismatchReport(
    surface: string,
    value: unknown,
    requestContext?: Record<string, unknown>,
    summaryOptions?: ResponseSummaryOptionInput
): ContractMismatchReport {
    return {
        surface,
        ids: extractIdentifiers(value),
        request_context: requestContext,
        response_preview: summarizeUnexpectedResponse(value, summaryOptions),
    };
}

/**
 * Produces a bounded, safe string summary of an unexpected response payload.
 * Handles circular references and bounds object breadth/depth for stable logs.
 */
export function summarizeUnexpectedResponse(value: unknown, options?: ResponseSummaryOptionInput): string {
    if (value === null) return "null";
    if (value === undefined) return "undefined";

    const normalizedOptions = normalizeSummaryOptions(options);

    try {
        const bounded = toBoundedSerializable(value, normalizedOptions, 0, new Set<unknown>());
        const json = JSON.stringify(bounded, null, 2);

        if (json.length > normalizedOptions.maxChars) {
            return `${json.substring(0, normalizedOptions.maxChars)}... [truncated, total_size=${json.length}chars]`;
        }
        return json;
    } catch (err) {
        return `[Serialization Error: ${err}]`;
    }
}
