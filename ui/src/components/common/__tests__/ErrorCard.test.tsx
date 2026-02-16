import { render, screen, fireEvent, act } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { ErrorCard } from "../ErrorCard";

function renderWithRouter(ui: React.ReactElement) {
  return render(<MemoryRouter>{ui}</MemoryRouter>);
}

describe("ErrorCard", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("renders category label and message", () => {
    renderWithRouter(<ErrorCard category="auth" message="Invalid credentials" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Authentication Error");
    expect(screen.getByText("Invalid credentials")).toBeInTheDocument();
  });

  it("renders fallback label for unknown category", () => {
    renderWithRouter(<ErrorCard category="some_new_thing" message="Something happened" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Some New Thing");
  });

  it("renders 'Error' when no category provided", () => {
    renderWithRouter(<ErrorCard message="Something went wrong" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Error");
  });

  it("shows copy button for requestId", () => {
    renderWithRouter(<ErrorCard message="fail" requestId="abc-123-def-456" />);
    expect(screen.getByText(/abc-123-/)).toBeInTheDocument();
    expect(screen.getByText("Copy")).toBeInTheDocument();
  });

  it("renders hint in callout", () => {
    renderWithRouter(<ErrorCard message="fail" hint="Try refreshing the schema" />);
    expect(screen.getByTestId("error-hint")).toHaveTextContent("Try refreshing the schema");
  });

  it("shows countdown then enables retry button", () => {
    vi.useFakeTimers();
    const onRetry = vi.fn();

    renderWithRouter(
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
    renderWithRouter(
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

  it("renders collapsible details and shows JSON when opened", () => {
    renderWithRouter(
      <ErrorCard
        message="fail"
        detailsSafe={{ sql: "SELECT *", error_code: "42P01" }}
      />
    );

    const summary = screen.getByText("Technical Details");
    expect(summary).toBeInTheDocument();

    // Open the details
    fireEvent.click(summary);
    expect(screen.getByText(/"error_code": "42P01"/)).toBeInTheDocument();
    expect(screen.getByText(/"sql": "SELECT \*"/)).toBeInTheDocument();
  });

  it("does not render details section when detailsSafe is empty", () => {
    renderWithRouter(
      <ErrorCard message="fail" detailsSafe={{}} />
    );

    expect(screen.queryByText("Technical Details")).not.toBeInTheDocument();
  });

  it("maps schema_drift category correctly", () => {
    renderWithRouter(<ErrorCard category="schema_drift" message="Tables changed" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Schema Mismatch");
  });

  it("maps connectivity category correctly", () => {
    renderWithRouter(<ErrorCard category="connectivity" message="Cannot reach database" />);
    expect(screen.getByTestId("error-category")).toHaveTextContent("Connection Error");
  });

  it("shows 'Retry now' button during countdown for manual skip", () => {
    vi.useFakeTimers();
    const onRetry = vi.fn();

    renderWithRouter(
      <ErrorCard
        message="rate limited"
        retryable={true}
        retryAfterSeconds={5}
        onRetry={onRetry}
      />
    );

    // "Retry now" should appear during countdown
    const retryNow = screen.getByTestId("retry-now-button");
    expect(retryNow).toHaveTextContent("Retry now");

    // Click "Retry now" skips countdown and fires onRetry
    fireEvent.click(retryNow);
    expect(onRetry).toHaveBeenCalledTimes(1);

    // "Retry now" should disappear since countdown is now 0
    expect(screen.queryByTestId("retry-now-button")).not.toBeInTheDocument();

    vi.useRealTimers();
  });

  it("renders correct deep links for schema_missing category", () => {
    renderWithRouter(
      <ErrorCard
        category="schema_missing"
        message="Schema not found"
        actions={[{ label: "Open Ingestion Wizard", href: "/admin/operations" }]}
      />
    );
    const link = screen.getByTestId("error-action-link");
    expect(link).toHaveTextContent("Open Ingestion Wizard");
    expect(link).toHaveAttribute("href", "/admin/operations");
  });

  it("renders correct deep links for permission_denied category", () => {
    renderWithRouter(
      <ErrorCard
        category="permission_denied"
        message="Access denied"
        actions={[{ label: "Check Permissions", href: "/admin/settings/query-target" }]}
      />
    );
    const link = screen.getByTestId("error-action-link");
    expect(link).toHaveTextContent("Check Permissions");
    expect(link).toHaveAttribute("href", "/admin/settings/query-target");
  });

  it("renders retry button without countdown when retryAfterSeconds is 0", () => {
    const onRetry = vi.fn();
    renderWithRouter(
      <ErrorCard message="fail" retryable={true} retryAfterSeconds={0} onRetry={onRetry} />
    );
    const button = screen.getByTestId("retry-button");
    expect(button).toHaveTextContent("Retry");
    expect(button).not.toBeDisabled();
  });
});
