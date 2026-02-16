import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { SQLPreviewCard } from "./SQLPreviewCard";
import React from "react";
import "@testing-library/jest-dom";

describe("SQLPreviewCard", () => {
    it("renders the SQL in a textarea", () => {
        const onRun = vi.fn();
        const onBack = vi.fn();
        const sql = "SELECT * FROM users";

        render(<SQLPreviewCard sql={sql} onRun={onRun} onBack={onBack} />);

        expect(screen.getByDisplayValue(sql)).toBeInTheDocument();
    });

    it("calls onSqlChange when edited", () => {
        const onRun = vi.fn();
        const onBack = vi.fn();
        const onSqlChange = vi.fn();
        render(<SQLPreviewCard sql="SELECT 1" onRun={onRun} onBack={onBack} onSqlChange={onSqlChange} />);

        const textarea = screen.getByDisplayValue("SELECT 1");
        fireEvent.change(textarea, { target: { value: "SELECT 2" } });
        expect(onSqlChange).toHaveBeenCalledWith("SELECT 2");
    });

    it("disables textarea when executing or not editable", () => {
        const { rerender } = render(<SQLPreviewCard sql="SELECT 1" onRun={() => { }} onBack={() => { }} isExecuting={true} />);
        expect(screen.getByDisplayValue("SELECT 1")).toHaveAttribute("readonly");

        rerender(<SQLPreviewCard sql="SELECT 1" onRun={() => { }} onBack={() => { }} isEditable={false} />);
        expect(screen.getByDisplayValue("SELECT 1")).toHaveAttribute("readonly");
    });
});
