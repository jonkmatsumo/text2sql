import { describe, it, expect } from "vitest";
import { makeToastDedupeKey } from "../toastUtils";

describe("makeToastDedupeKey", () => {
    it("includes surface and sorted identifiers in the dedupe key", () => {
        const key = makeToastDedupeKey("run-history", "MALFORMED_RESPONSE", "unexpected payload", {
            surface: "RunHistory.fetchRuns",
            identifiers: {
                status: "FAILED",
                offset: 100,
            },
        });

        expect(key).toContain("surface=RunHistory.fetchRuns");
        expect(key).toContain("offset=100");
        expect(key).toContain("status=FAILED");
        expect(key).toContain("message_hash=");
    });

    it("keeps distinct surfaces from sharing the same dedupe key", () => {
        const left = makeToastDedupeKey("ops", "MALFORMED_RESPONSE", "same message", {
            surface: "RunHistory.fetchRuns",
        });
        const right = makeToastDedupeKey("ops", "MALFORMED_RESPONSE", "same message", {
            surface: "RunDetails.fetchDetails",
        });

        expect(left).not.toBe(right);
    });
});
