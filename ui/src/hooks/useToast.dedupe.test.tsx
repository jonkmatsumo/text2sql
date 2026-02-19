import { renderHook, act } from "@testing-library/react";
import React from "react";
import { describe, it, expect, vi } from "vitest";
import { ToastProvider } from "../context/ToastContext";
import { useToast } from "./useToast";
import { makeToastDedupeKey } from "../utils/toastUtils";

const wrapper = ({ children }: { children: React.ReactNode }) => (
    <ToastProvider>{children}</ToastProvider>
);

describe("useToast deduplication", () => {
    it("deduplicates toasts with the same key", () => {
        const { result } = renderHook(() => useToast(), { wrapper });

        act(() => {
            result.current.show("Error 1", "error", { dedupeKey: "same-key" });
        });

        act(() => {
            result.current.show("Error 2", "error", { dedupeKey: "same-key" });
        });

        expect(result.current.toasts).toHaveLength(1);
        expect(result.current.toasts[0].message).toBe("Error 1");
    });

    it("allows multiple toasts with different keys", () => {
        const { result } = renderHook(() => useToast(), { wrapper });

        act(() => {
            result.current.show("Error 1", "error", { dedupeKey: "key-1" });
        });

        act(() => {
            result.current.show("Error 2", "error", { dedupeKey: "key-2" });
        });

        expect(result.current.toasts).toHaveLength(2);
    });

    it("should allow same message after TTL expires", () => {
        vi.useFakeTimers();
        const { result } = renderHook(() => useToast(), { wrapper });

        act(() => {
            result.current.show("Error", "error", { dedupeKey: "key1" });
        });
        expect(result.current.toasts).toHaveLength(1);

        // Advance time by 6 minutes (TTL is 5 mins)
        act(() => {
            vi.advanceTimersByTime(6 * 60 * 1000);
        });

        act(() => {
            result.current.show("Error Again", "error", { dedupeKey: "key1" });
        });

        // Both should be visible (if we didn't advance too much for duration)
        // Duration is 4000ms. 6 minutes > 4000ms, so the first one should be gone.
        expect(result.current.toasts).toHaveLength(1);
        expect(result.current.toasts[0].message).toBe("Error Again");
        vi.useRealTimers();
    });

    it("should enforce cache bounding (max 50)", () => {
        const { result } = renderHook(() => useToast(), { wrapper });

        // Fill cache with 50 entries
        act(() => {
            for (let i = 0; i < 50; i++) {
                result.current.show(`Msg ${i}`, "error", { dedupeKey: `key-${i}` });
            }
        });

        // Add 51st entry
        act(() => {
            result.current.show("Msg 51", "error", { dedupeKey: "key-51" });
        });

        // Now key-0 should have been evicted.
        // If we try to show key-0 again, it should be allowed (i.e. not return empty string)
        let id0 = "";
        act(() => {
            id0 = result.current.show("Msg 0", "error", { dedupeKey: "key-0" });
        });
        expect(id0).not.toBe("");

        // Whereas key-51 should still be in cache and return empty string
        let id51 = "init";
        act(() => {
            id51 = result.current.show("Msg 51 Duplicate", "error", { dedupeKey: "key-51" });
        });
        expect(id51).toBe("");
    });

    it("dedupes identical diagnostics failure keys but keeps distinct panel failures separate", () => {
        const { result } = renderHook(() => useToast(), { wrapper });
        const failedRunsKey = makeToastDedupeKey(
            "diagnostics",
            "RUN_SIGNALS_FETCH_FAILED",
            "Failed to load failed run signals. Refresh to retry.",
            {
                surface: "Diagnostics.runSignals",
                identifiers: { panels: "failed-runs" },
            }
        );
        const lowRatingsKey = makeToastDedupeKey(
            "diagnostics",
            "RUN_SIGNALS_FETCH_FAILED",
            "Failed to load low-rated run signals. Refresh to retry.",
            {
                surface: "Diagnostics.runSignals",
                identifiers: { panels: "low-ratings" },
            }
        );

        act(() => {
            result.current.show("Failed to load failed run signals. Refresh to retry.", "error", { dedupeKey: failedRunsKey });
            result.current.show("Failed to load failed run signals. Refresh to retry.", "error", { dedupeKey: failedRunsKey });
            result.current.show("Failed to load low-rated run signals. Refresh to retry.", "error", { dedupeKey: lowRatingsKey });
        });

        expect(result.current.toasts).toHaveLength(2);
    });
});
