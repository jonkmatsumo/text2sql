/**
 * Shared input for run identification across different export/copy formats.
 */
export interface RunIdentifierInput {
    runId?: string;
    traceId?: string;
    requestId?: string;
    interactionId?: string;
    environment?: string;
}

/**
 * Shared identifier block builder for consistency across different bundle types (text/JSON).
 * Maps internal field names to canonical user-facing labels.
 */
export function buildRunIdentifierBlock(input: RunIdentifierInput): Record<string, string> {
    const block: Record<string, string> = {};
    if (input.runId) block["Run ID"] = input.runId;
    if (input.traceId) block["Trace ID"] = input.traceId;
    if (input.interactionId) block["Interaction ID"] = input.interactionId;
    if (input.requestId) block["Request ID"] = input.requestId;
    return block;
}
