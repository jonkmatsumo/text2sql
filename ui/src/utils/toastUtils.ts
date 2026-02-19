/**
 * Generates a stable dedupe key for toasts to prevent spamming the user.
 */
export function makeToastDedupeKey(scope: string, category: string, message?: string): string {
    if (message) {
        // Basic hash function for a string
        let hash = 0;
        for (let i = 0; i < message.length; i++) {
            const char = message.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash |= 0; // Convert to 32bit integer
        }
        return `${scope}:${category}:${hash}`;
    }
    return `${scope}:${category}`;
}
