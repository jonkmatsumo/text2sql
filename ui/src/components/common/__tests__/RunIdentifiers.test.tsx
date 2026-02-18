import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect } from "vitest";
import RunIdentifiers from "../RunIdentifiers";

describe("RunIdentifiers", () => {
  it("renders trace link plus trace/request copy controls", () => {
    const traceId = "0123456789abcdef0123456789abcdef";
    render(
      <MemoryRouter>
        <RunIdentifiers traceId={traceId} requestId="req-12345" />
      </MemoryRouter>
    );

    const viewTrace = screen.getByRole("link", { name: "View Trace" });
    expect(viewTrace).toHaveAttribute("href", `/traces/${traceId}`);
    expect(screen.getByText("trace: 01234567...")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy trace id" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy request id" })).toBeInTheDocument();
  });

  it("uses interaction trace link when trace id is unavailable", () => {
    render(
      <MemoryRouter>
        <RunIdentifiers interactionId="interaction-1" />
      </MemoryRouter>
    );

    const viewTrace = screen.getByRole("link", { name: "View Trace" });
    expect(viewTrace).toHaveAttribute("href", "/traces/interaction/interaction-1");
    expect(screen.queryByRole("button", { name: "Copy trace id" })).not.toBeInTheDocument();
  });

  it("renders nothing when no identifiers are provided", () => {
    const { container } = render(
      <MemoryRouter>
        <RunIdentifiers />
      </MemoryRouter>
    );
    expect(container).toBeEmptyDOMElement();
  });
});
