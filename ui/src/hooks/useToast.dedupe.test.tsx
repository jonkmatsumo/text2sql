import { renderHook, act } from "@testing-library/react";
import React from "react";
import { describe, it, expect } from "vitest";
import { ToastProvider } from "../context/ToastContext";
import { useToast } from "./useToast";

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
});
