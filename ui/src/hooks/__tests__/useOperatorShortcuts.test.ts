import { renderHook } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { useOperatorShortcuts } from "../useOperatorShortcuts";

function fireKey(key: string) {
    window.dispatchEvent(new KeyboardEvent("keydown", { key, bubbles: true }));
}

function focusElement(tag: string) {
    const el = document.createElement(tag as any);
    document.body.appendChild(el);
    el.focus();
    return el;
}

describe("useOperatorShortcuts", () => {
    beforeEach(() => {
        // Ensure no element is focused
        (document.activeElement as HTMLElement)?.blur?.();
    });

    afterEach(() => {
        // Clean up any appended elements
        document.body.innerHTML = "";
    });

    it("fires handler when key matches and no input is focused", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        fireKey("r");
        expect(handler).toHaveBeenCalledTimes(1);
    });

    it("does not fire when a different key is pressed", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        fireKey("x");
        expect(handler).not.toHaveBeenCalled();
    });

    it("skips handler when an INPUT is focused", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        focusElement("input");
        fireKey("r");
        expect(handler).not.toHaveBeenCalled();
    });

    it("skips handler when a TEXTAREA is focused", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        focusElement("textarea");
        fireKey("r");
        expect(handler).not.toHaveBeenCalled();
    });

    it("skips handler when a SELECT is focused", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        focusElement("select");
        fireKey("r");
        expect(handler).not.toHaveBeenCalled();
    });

    it("fires handler in input when allowInInput is true", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "Escape", label: "Clear", handler, allowInInput: true }],
            })
        );

        focusElement("input");
        fireKey("Escape");
        expect(handler).toHaveBeenCalledTimes(1);
    });

    it("suppresses all shortcuts when disabled is true", () => {
        const handler = vi.fn();
        renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
                disabled: true,
            })
        );

        fireKey("r");
        expect(handler).not.toHaveBeenCalled();
    });

    it("cleans up listener on unmount", () => {
        const handler = vi.fn();
        const { unmount } = renderHook(() =>
            useOperatorShortcuts({
                shortcuts: [{ key: "r", label: "Refresh", handler }],
            })
        );

        unmount();
        fireKey("r");
        expect(handler).not.toHaveBeenCalled();
    });
});
