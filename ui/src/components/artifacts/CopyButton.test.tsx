import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { describe, it, expect, vi } from "vitest";
import { CopyButton } from "./CopyButton";

describe("CopyButton", () => {
  it("uses an accessible label and announces successful copy", async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(window.navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });

    render(<CopyButton text="hello" label="Copy diagnostics" />);
    const button = screen.getByRole("button", { name: "Copy diagnostics" });
    fireEvent.click(button);

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledWith("hello");
    });
    expect(screen.getByText("Copied to clipboard")).toBeInTheDocument();
  });

  it("allows overriding aria-label independently from button text", () => {
    render(<CopyButton text="raw-json" label="Copy JSON" ariaLabel="Copy raw diagnostics JSON" />);
    expect(screen.getByRole("button", { name: "Copy raw diagnostics JSON" })).toBeInTheDocument();
  });
});
