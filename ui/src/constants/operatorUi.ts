export const COPY_SQL_METADATA_LABEL = "Copy SQL + metadata";
export const DECISION_LOG_SEARCH_ARIA_LABEL = "Search decision events";
export const DECISION_LOG_PHASE_ARIA_LABEL = "Filter decision events by phase";
export const DIAGNOSTICS_SECTION_ARIA_LABEL = "Select diagnostics section";
export const RUN_HISTORY_PAGE_SIZE = 50;

export function formatRunHistoryRange(
    offset: number,
    runsLength: number,
    totalCount?: number
): string {
    if (runsLength <= 0) {
        return "No results for this page";
    }

    const start = offset + 1;
    const end = offset + runsLength;

    if (typeof totalCount === "number") {
        return `Showing ${start}\u2013${end} of ${totalCount}`;
    }

    return `Showing ${start}\u2013${end}`;
}
