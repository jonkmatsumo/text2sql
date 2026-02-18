import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { formatRelativeTime } from "../observability";

describe("formatRelativeTime", () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(new Date("2026-02-18T12:00:00Z"));
    });

    afterEach(() => {
        vi.useRealTimers();
    });

    it("renders seconds ago for very recent timestamps", () => {
        const past = new Date("2026-02-18T11:59:30Z").getTime();
        expect(formatRelativeTime(past)).toBe("30s ago");
    });

    it("renders minutes ago for recent timestamps", () => {
        const past = new Date("2026-02-18T11:55:00Z").getTime();
        expect(formatRelativeTime(past)).toBe("5m ago");
    });

    it("renders hours ago for today's timestamps", () => {
        const past = new Date("2026-02-18T09:00:00Z").getTime();
        expect(formatRelativeTime(past)).toBe("3h ago");
    });

    it("falls back to absolute date for timestamps older than 24 hours", () => {
        const past = new Date("2026-02-17T11:00:00Z").getTime();
        // Just verify it doesn't contain "ago" and looks like a date
        const result = formatRelativeTime(past);
        expect(result).not.toContain("ago");
        expect(result).toMatch(/\//); // e.g. 2/17/2026
    });

    it("handles malformed timestamp gracefully", () => {
        expect(formatRelativeTime("invalid-date", { fallback: "N/A" })).toBe("N/A");
        expect(formatRelativeTime(null)).toBe("—");
    });

    it("handles future dates as 'just now'", () => {
        const future = new Date("2026-02-18T12:05:00Z").getTime();
        expect(formatRelativeTime(future)).toBe("just now");
    });
});
