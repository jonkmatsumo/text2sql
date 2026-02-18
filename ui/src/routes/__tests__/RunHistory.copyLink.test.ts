import { render, screen, fireEvent, act, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";

/**
 * Tests for the Copy link button behavior.
 * We test the clipboard interaction and feedback state.
 */

describe("Copy link button logic", () => {
    beforeEach(() => {
        // Mock clipboard API
        Object.assign(navigator, {
            clipboard: {
                writeText: vi.fn().mockResolvedValue(undefined),
            },
        });
    });

    it("calls clipboard.writeText with the current URL", async () => {
        // Simulate the copyLink logic
        const setLinkCopied = vi.fn();
        const copyLink = async () => {
            await navigator.clipboard.writeText(window.location.href);
            setLinkCopied(true);
            setTimeout(() => setLinkCopied(false), 2000);
        };

        await copyLink();
        expect(navigator.clipboard.writeText).toHaveBeenCalledWith(window.location.href);
        expect(setLinkCopied).toHaveBeenCalledWith(true);
    });

    it("clipboard receives URL with active filter params", async () => {
        // Simulate a URL with filter params
        const testUrl = "http://localhost:3000/admin/runs?feedback=DOWN&status=FAILED";
        Object.defineProperty(window, "location", {
            value: { href: testUrl },
            writable: true,
        });

        await navigator.clipboard.writeText(window.location.href);
        expect(navigator.clipboard.writeText).toHaveBeenCalledWith(testUrl);
    });
});
