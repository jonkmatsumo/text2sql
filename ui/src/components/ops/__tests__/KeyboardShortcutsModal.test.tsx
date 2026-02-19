import { render, screen, fireEvent } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { KeyboardShortcutsModal } from "../KeyboardShortcutsModal";

const shortcuts = [
    { key: "r", label: "Refresh list", handler: vi.fn() },
    { key: "/", label: "Focus search", handler: vi.fn() },
    { key: "?", label: "Show shortcuts", handler: vi.fn() },
];

describe("KeyboardShortcutsModal", () => {
    it("renders nothing when closed", () => {
        render(
            <KeyboardShortcutsModal isOpen={false} onClose={vi.fn()} shortcuts={shortcuts} />
        );
        expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    });

    it("renders shortcut rows when open", () => {
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={vi.fn()} shortcuts={shortcuts} />
        );
        expect(screen.getByRole("dialog")).toBeInTheDocument();
        expect(screen.getByText("Refresh list")).toBeInTheDocument();
        expect(screen.getByText("Focus search")).toBeInTheDocument();
        expect(screen.getByText("Show shortcuts")).toBeInTheDocument();
    });

    it("renders kbd elements for each shortcut key", () => {
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={vi.fn()} shortcuts={shortcuts} />
        );
        const kbdElements = screen.getAllByRole("table")[0].querySelectorAll("kbd");
        expect(kbdElements).toHaveLength(shortcuts.length);
    });

    it("calls onClose when close button is clicked", () => {
        const onClose = vi.fn();
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={onClose} shortcuts={shortcuts} />
        );
        fireEvent.click(screen.getByRole("button", { name: "Close shortcuts modal" }));
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it("calls onClose when backdrop is clicked", () => {
        const onClose = vi.fn();
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={onClose} shortcuts={shortcuts} />
        );
        // Click the outer backdrop div (the dialog role element)
        fireEvent.click(screen.getByRole("dialog"));
        expect(onClose).toHaveBeenCalledTimes(1);
    });

    it("does not close when inner content is clicked", () => {
        const onClose = vi.fn();
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={onClose} shortcuts={shortcuts} />
        );
        fireEvent.click(screen.getByText("Keyboard Shortcuts"));
        expect(onClose).not.toHaveBeenCalled();
    });

    it("calls onClose and stops propagation when Escape is pressed", () => {
        const onClose = vi.fn();
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={onClose} shortcuts={shortcuts} />
        );

        const event = new KeyboardEvent("keydown", { key: "Escape", bubbles: true });
        const stopSpy = vi.spyOn(event, "stopPropagation");
        const stopImmediateSpy = vi.spyOn(event, "stopImmediatePropagation");
        const preventSpy = vi.spyOn(event, "preventDefault");

        window.dispatchEvent(event);

        expect(onClose).toHaveBeenCalledTimes(1);
        expect(stopSpy).toHaveBeenCalled();
        expect(stopImmediateSpy).toHaveBeenCalled();
        expect(preventSpy).toHaveBeenCalled();
    });

    it("focuses the close button when opened", () => {
        render(
            <KeyboardShortcutsModal isOpen={true} onClose={vi.fn()} shortcuts={shortcuts} />
        );
        expect(document.activeElement).toBe(
            screen.getByRole("button", { name: "Close shortcuts modal" })
        );
    });
});
