import { describe, it, expect } from "vitest";
import { getInteractionStatusTone } from "../operatorUi";

describe("getInteractionStatusTone", () => {
    it("returns success for terminal positive states", () => {
        expect(getInteractionStatusTone("APPROVED")).toBe("success");
        expect(getInteractionStatusTone("SUCCESS")).toBe("success");
        expect(getInteractionStatusTone("COMPLETED")).toBe("success");
    });

    it("returns danger for terminal negative states", () => {
        expect(getInteractionStatusTone("FAILED")).toBe("danger");
        expect(getInteractionStatusTone("REJECTED")).toBe("danger");
        expect(getInteractionStatusTone("ERROR")).toBe("danger");
    });

    it("returns neutral for pending, unknown, or intermediate states", () => {
        expect(getInteractionStatusTone("PENDING")).toBe("neutral");
        expect(getInteractionStatusTone("RUNNING")).toBe("neutral");
        expect(getInteractionStatusTone("CANCELLING")).toBe("neutral");
        expect(getInteractionStatusTone("CANCELLED")).toBe("neutral");
        expect(getInteractionStatusTone("UNKNOWN")).toBe("neutral");
        expect(getInteractionStatusTone(undefined)).toBe("neutral");
        expect(getInteractionStatusTone("")).toBe("neutral");
    });

    it("is case-insensitive", () => {
        expect(getInteractionStatusTone("approved")).toBe("success");
        expect(getInteractionStatusTone("failed")).toBe("danger");
    });

    it("defaults to neutral for unknown strings", () => {
        expect(getInteractionStatusTone("SOME_RANDOM_STATUS")).toBe("neutral");
    });
});
