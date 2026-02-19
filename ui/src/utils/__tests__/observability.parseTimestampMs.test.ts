import { describe, it, expect } from "vitest";
import { parseTimestampMs } from "../observability";

describe("parseTimestampMs", () => {
    it("returns null for missing or invalid values", () => {
        expect(parseTimestampMs(undefined)).toBeNull();
        expect(parseTimestampMs(null)).toBeNull();
        expect(parseTimestampMs("not-a-timestamp")).toBeNull();
    });

    it("parses ISO timestamps and unix milliseconds", () => {
        const iso = "2026-02-19T00:00:00Z";
        expect(parseTimestampMs(iso)).toBe(Date.parse(iso));
        expect(parseTimestampMs(1708300800000)).toBe(1708300800000);
    });

    it("supports unix seconds with inputInSeconds option", () => {
        expect(parseTimestampMs(1708300800, { inputInSeconds: true })).toBe(1708300800000);
    });
});
