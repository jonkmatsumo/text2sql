export const COPY_SQL_METADATA_LABEL = "Copy SQL + metadata";
export const DECISION_LOG_SEARCH_ARIA_LABEL = "Search decision events";
export const DECISION_LOG_PHASE_ARIA_LABEL = "Filter decision events by phase";
export const DIAGNOSTICS_SECTION_ARIA_LABEL = "Select diagnostics section";
export const RUN_HISTORY_PAGE_SIZE = 50;

/**
 * Determines whether RunHistory can navigate forward.
 * Uses explicit has_more contract when present; otherwise falls back to page-size heuristic.
 */
export function hasRunHistoryNextPage(
    hasMore: boolean | undefined,
    runsLength: number,
    pageSize: number
): boolean {
    const normalizedRunsLength = Number.isFinite(runsLength) ? Math.max(0, Math.trunc(runsLength)) : 0;
    const normalizedPageSize = Number.isFinite(pageSize) ? Math.max(1, Math.trunc(pageSize)) : 1;

    if (normalizedRunsLength === 0) {
        return false;
    }

    if (typeof hasMore === "boolean") {
        return hasMore;
    }

    return normalizedRunsLength === normalizedPageSize;
}

export function formatRunHistoryRange(
    offset: number,
    runsLength: number,
    totalCount?: number
): string {
    const normalizedOffset = Number.isFinite(offset) ? Math.max(0, Math.trunc(offset)) : 0;
    const normalizedRunsLength = Number.isFinite(runsLength) ? Math.max(0, Math.trunc(runsLength)) : 0;

    if (normalizedRunsLength <= 0) {
        return "No results on this page";
    }

    const start = normalizedOffset + 1;
    const rawEnd = normalizedOffset + normalizedRunsLength;
    const normalizedTotalCount =
        typeof totalCount === "number" && Number.isFinite(totalCount)
            ? Math.max(0, Math.trunc(totalCount))
            : undefined;
    const boundedEnd = normalizedTotalCount === undefined
        ? rawEnd
        : Math.min(rawEnd, Math.max(normalizedTotalCount, start));
    const end = Math.max(start, boundedEnd);

    if (normalizedTotalCount !== undefined) {
        return `Showing ${start}\u2013${end} of ${normalizedTotalCount}`;
    }

    return `Showing ${start}\u2013${end}`;
}
