export interface RunContextInput {
    runId?: string;
    traceId?: string;
    requestId?: string;
    interactionId?: string;
    userQuery?: string;
    generatedSql?: string;
    validationStatus?: string;
    validationErrors?: string[];
    executionStatus?: string;
    isComplete?: boolean;
    environment?: string;
}

/**
 * Shared identifier block builder for consistency across different bundle types (text/JSON).
 */
export function buildIdentifierBlock(input: RunContextInput): Record<string, string> {
    const block: Record<string, string> = {};
    if (input.runId) block["Run ID"] = input.runId;
    if (input.traceId) block["Trace ID"] = input.traceId;
    if (input.interactionId) block["Interaction ID"] = input.interactionId;
    if (input.requestId) block["Request ID"] = input.requestId;
    return block;
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

    const identifiers = buildIdentifierBlock(input);
    Object.entries(identifiers).forEach(([key, value]) => {
        lines.push(`${key.padEnd(16)}: ${value}`);
    });

    if (input.executionStatus) lines.push(`${"Execution Status".padEnd(16)}: ${input.executionStatus}`);
    if (input.isComplete !== undefined) lines.push(`${"Complete".padEnd(16)}: ${input.isComplete ? "yes" : "no"}`);

    const env = input.environment || (import.meta as any).env?.MODE || "development";
    lines.push(`${"Environment".padEnd(16)}: ${env}`);
    lines.push(`${"Generated at".padEnd(16)}: ${new Date().toISOString()}`);

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
    lines.push("=========================");

    return lines.join("\n");
}
