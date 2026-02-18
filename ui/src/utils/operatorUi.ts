import { InteractionStatus } from "../types/admin";

export type StatusTone = "success" | "neutral" | "danger";

/**
 * Returns a semantic tone for a given interaction or job status.
 * Used to unify coloring across RunHistory and RunDetails status badges.
 */
export function getInteractionStatusTone(status?: string): StatusTone {
    if (!status) return "neutral";

    const s = status.toUpperCase();

    switch (s) {
        case "SUCCESS":
        case "APPROVED":
        case "COMPLETED":
            return "success";
        case "FAILED":
        case "REJECTED":
        case "ERROR":
            return "danger";
        case "PENDING":
        case "RUNNING":
        case "CANCELLING":
        case "CANCELLED":
        case "UNKNOWN":
        default:
            return "neutral";
    }
}
