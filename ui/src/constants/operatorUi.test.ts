import { describe, it, expect } from "vitest";
import { formatRunHistoryRange, hasRunHistoryNextPage } from "./operatorUi";

describe("formatRunHistoryRange", () => {
    it("returns deterministic empty-page copy when runs length is zero", () => {
        expect(formatRunHistoryRange(100, 0)).toBe("No results on this page");
    });

    it("never returns an inverted range when total_count is smaller than offset", () => {
        expect(formatRunHistoryRange(100, 1, 100)).toBe("Showing 101\u2013101 of 100");
    });

    it("normalizes invalid offsets to zero", () => {
        expect(formatRunHistoryRange(-10, 2)).toBe("Showing 1\u20132");
    });
});

describe("hasRunHistoryNextPage", () => {
    it("uses has_more=true even for short pages", () => {
        expect(hasRunHistoryNextPage(true, 5, 50)).toBe(true);
    });

    it("uses has_more=false even for full pages", () => {
        expect(hasRunHistoryNextPage(false, 50, 50)).toBe(false);
    });

    it("falls back to page-size heuristic when has_more is undefined", () => {
        expect(hasRunHistoryNextPage(undefined, 50, 50)).toBe(true);
        expect(hasRunHistoryNextPage(undefined, 20, 50)).toBe(false);
    });
});
