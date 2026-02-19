/**
 * Generates a stable dedupe key for toasts to prevent spamming the user.
 */
export interface ToastDedupeContext {
    surface?: string;
    identifiers?: Record<string, string | number | boolean | null | undefined>;
}

function hashMessage(message: string): number {
    let hash = 0;
    for (let i = 0; i < message.length; i++) {
        const char = message.charCodeAt(i);
        hash = ((hash << 5) - hash) + char;
        hash |= 0; // Convert to 32-bit integer
    }
    return hash;
}

export function makeToastDedupeKey(
    scope: string,
    category: string,
    message?: string,
    context?: ToastDedupeContext
): string {
    const segments = [scope, category];

    if (context?.surface) {
        segments.push(`surface=${context.surface}`);
    }

    if (context?.identifiers) {
        const identifierSegments = Object.entries(context.identifiers)
            .filter(([, value]) => value !== undefined && value !== null && String(value).trim() !== "")
            .sort(([left], [right]) => left.localeCompare(right))
            .map(([key, value]) => `${key}=${String(value)}`);
        segments.push(...identifierSegments);
    }

    if (message) {
        segments.push(`message_hash=${hashMessage(message)}`);
    }

    return segments.join(":");
}
