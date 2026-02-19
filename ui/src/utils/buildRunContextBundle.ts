import { buildRunIdentifierBlock, RunIdentifierInput } from "./copyBundles";

export interface RunContextInput extends RunIdentifierInput {
    userQuery?: string;
    generatedSql?: string;
    validationStatus?: string;
    validationErrors?: string[];
    executionStatus?: string;
    isComplete?: boolean;
}

/**
 * Builds a human-readable context bundle for operators/support workflows.
 * This output is intentionally optimized for copy/paste in tickets and chats.
 *
 * This text bundle intentionally differs from `buildCopyBundlePayload`, which emits
 * a structured JSON payload for AgentChat export and downstream parsing.
 * Stability contract: the text header carries `Bundle-Version`, while schemaed JSON
 * versioning remains at `schema_version` in `buildCopyBundlePayload`.
 */
export function buildRunContextBundle(input: RunContextInput): string {
    const lines: string[] = ["=== Run Context Bundle ===", "Bundle-Version: 1"];

    const identifiers = buildRunIdentifierBlock(input);
    Object.entries(identifiers).forEach(([key, value]) => {
        lines.push(`${key.padEnd(16)}: ${value}`);
    });

    const env = input.environment || (import.meta as any).env?.MODE || "development";
    lines.push(`${"Environment".padEnd(16)}: ${env}`);
    lines.push(`${"Generated-At".padEnd(16)}: ${new Date().toISOString()}`);

    if (input.executionStatus) lines.push(`${"Execution Status".padEnd(16)}: ${input.executionStatus}`);
    if (input.isComplete !== undefined) lines.push(`${"Complete".padEnd(16)}: ${input.isComplete ? "yes" : "no"}`);

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
