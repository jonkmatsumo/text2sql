import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { SQLPreviewCard } from "./SQLPreviewCard";
import React from "react";
import "@testing-library/jest-dom";

describe("SQLPreviewCard", () => {
    it("renders the SQL and buttons", () => {
        const onRun = vi.fn();
        const onBack = vi.fn();
        const sql = "SELECT * FROM users";

        render(<SQLPreviewCard sql={sql} onRun={onRun} onBack={onBack} />);

        expect(screen.getByText(sql)).toBeInTheDocument();
        expect(screen.getByText("Run SQL")).toBeInTheDocument();
        expect(screen.getByText("Back")).toBeInTheDocument();
    });

    it("calls handlers on click", () => {
        const onRun = vi.fn();
        const onBack = vi.fn();
        render(<SQLPreviewCard sql="SELECT 1" onRun={onRun} onBack={onBack} />);

        fireEvent.click(screen.getByText("Run SQL"));
        expect(onRun).toHaveBeenCalled();

        fireEvent.click(screen.getByText("Back"));
        expect(onBack).toHaveBeenCalled();
    });

    it("disables buttons when executing", () => {
        render(<SQLPreviewCard sql="SELECT 1" onRun={() => { }} onBack={() => { }} isExecuting={true} />);
        expect(screen.getByText("Running...")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /running/i })).toBeDisabled();
        expect(screen.getByRole("button", { name: /back/i })).toBeDisabled();
    });
});
