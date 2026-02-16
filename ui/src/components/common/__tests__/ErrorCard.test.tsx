import { render, screen, fireEvent, act } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { ErrorCard } from "../ErrorCard";

describe("ErrorCard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders category label and message", () => {
    render(<ErrorCard category="auth" message="Invalid credentials" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Authentication Error");
    expect(screen.getByText("Invalid credentials")).toBeInTheDocument();
  });

  it("renders fallback label for unknown category", () => {
    render(<ErrorCard category="some_new_thing" message="Something happened" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Some New Thing");
  });

  it("renders 'Error' when no category provided", () => {
    render(<ErrorCard message="Something went wrong" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Error");
  });

  it("shows copy button for requestId", () => {
    render(<ErrorCard message="fail" requestId="abc-123-def-456" />);
    expect(screen.getByText(/abc-123-/)).toBeInTheDocument();
    expect(screen.getByText("Copy")).toBeInTheDocument();
  });

  it("renders hint in callout", () => {
    render(<ErrorCard message="fail" hint="Try refreshing the schema" />);
    expect(screen.getByTestId("error-hint")).toHaveTextContent("Try refreshing the schema");
  });

  it("shows countdown then enables retry button", () => {
    vi.useFakeTimers();
    const onRetry = vi.fn();

    render(
      <ErrorCard
        message="fail"
        retryable={true}
        retryAfterSeconds={2}
        onRetry={onRetry}
      />
    );

    const button = screen.getByTestId("retry-button");
    expect(button).toHaveTextContent("Retry in 2s");
    expect(button).toBeDisabled();

    act(() => { vi.advanceTimersByTime(1000); });
    expect(button).toHaveTextContent("Retry in 1s");
    expect(button).toBeDisabled();

    act(() => { vi.advanceTimersByTime(1000); });
    expect(button).toHaveTextContent("Retry");
    expect(button).not.toBeDisabled();

    fireEvent.click(button);
    expect(onRetry).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
  });

  it("renders action links", () => {
    render(
      <ErrorCard
        message="Schema mismatch"
        actions={[
          { label: "Open Ingestion Wizard", href: "/admin/operations" },
          { label: "Check Settings", href: "/admin/settings/query-target" },
        ]}
      />
    );

    const links = screen.getAllByTestId("error-action-link");
    expect(links).toHaveLength(2);
    expect(links[0]).toHaveTextContent("Open Ingestion Wizard");
    expect(links[0]).toHaveAttribute("href", "/admin/operations");
    expect(links[1]).toHaveTextContent("Check Settings");
  });

  it("renders collapsible details", () => {
    render(
      <ErrorCard
        message="fail"
        detailsSafe={{ sql: "SELECT *", error_code: "42P01" }}
      />
    );

    expect(screen.getByText("Technical Details")).toBeInTheDocument();
  });

  it("maps schema_drift category correctly", () => {
    render(<ErrorCard category="schema_drift" message="Tables changed" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Schema Mismatch");
  });

  it("maps connectivity category correctly", () => {
    render(<ErrorCard category="connectivity" message="Cannot reach database" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Connection Error");
  });

  it("renders retry button without countdown when retryAfterSeconds is 0", () => {
    const onRetry = vi.fn();
    render(
      <ErrorCard message="fail" retryable={true} retryAfterSeconds={0} onRetry={onRetry} />
    );
    const button = screen.getByTestId("retry-button");
    expect(button).toHaveTextContent("Retry");
    expect(button).not.toBeDisabled();
  });
});
