export interface RunContextInput {
    runId?: string;
    traceId?: string;
    requestId?: string;
    userQuery?: string;
    generatedSql?: string;
    validationStatus?: string;
    validationErrors?: string[];
    executionStatus?: string;
    isComplete?: boolean;
}

/**
 * Builds a human-readable, copy-pasteable context bundle for a run.
 * Useful for sharing with support, filing bugs, or attaching to incident reports.
 *
 * This text bundle intentionally differs from `buildCopyBundlePayload`, which emits
 * a structured JSON payload for AgentChat export and downstream parsing.
 */
export function buildRunContextBundle(input: RunContextInput): string {
    const lines: string[] = ["=== Run Context Bundle ===", "Bundle-Version: 1"];

    if (input.runId) lines.push(`Run ID:           ${input.runId}`);
    if (input.traceId) lines.push(`Trace ID:         ${input.traceId}`);
    if (input.requestId) lines.push(`Request ID:       ${input.requestId}`);
    if (input.executionStatus) lines.push(`Execution Status: ${input.executionStatus}`);
    if (input.isComplete !== undefined) lines.push(`Complete:         ${input.isComplete ? "yes" : "no"}`);

    if (input.userQuery) {
        lines.push("");
        lines.push("--- User Query ---");
        lines.push(input.userQuery);
    }

    if (input.generatedSql) {
        lines.push("");
        lines.push("--- Generated SQL ---");
        lines.push(input.generatedSql);
    }

    if (input.validationStatus) {
        lines.push("");
        lines.push("--- Validation ---");
        lines.push(`Status: ${input.validationStatus}`);
        if (input.validationErrors && input.validationErrors.length > 0) {
            lines.push("Errors:");
            input.validationErrors.forEach(e => lines.push(`  - ${e}`));
        }
    }

    lines.push("");
    lines.push(`Generated at: ${new Date().toISOString()}`);
    lines.push("=========================");

    return lines.join("\n");
}
