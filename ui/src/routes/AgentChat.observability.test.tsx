import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgentStream, fetchAvailableModels } from "../api";
import { resetAvailableModelsCache } from "../hooks/useAvailableModels";

vi.mock("../api", () => ({
  runAgent: vi.fn(),
  runAgentStream: vi.fn(),
  submitFeedback: vi.fn(),
  fetchAvailableModels: vi.fn(),
  fetchQueryTargetSettings: vi.fn().mockResolvedValue({ active: { id: "cfg-1" } }),
  ApiError: class extends Error {
    status: number;
    code: string;
    details: Record<string, unknown>;
    constructor(message: string, status: number, code = "UNKNOWN", details = {}) {
      super(message);
      this.name = "ApiError";
      this.status = status;
      this.code = code;
      this.details = details;
    }
    get displayMessage() { return this.message; }
  },
}));

async function* makeStream(events: Array<{ event: string; data: any }>) {
  for (const e of events) {
    yield e;
  }
}

describe("AgentChat observability", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    (fetchAvailableModels as ReturnType<typeof vi.fn>).mockResolvedValue([
      { value: "gpt-4o", label: "GPT-4o" },
    ]);
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("renders decision events in deterministic timestamp order", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Done",
            decision_events: [
              { timestamp: 1700000003, node: "execute", decision: "third", reason: "r3" },
              { timestamp: 1700000001, node: "router", decision: "first", reason: "r1" },
              { timestamp: 1700000002, node: "plan", decision: "second", reason: "r2" },
            ],
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "show decision order" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Done")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Decision Log \(3 events\)/));
    expect(screen.getByRole("button", { name: "Copy decision log" })).toBeInTheDocument();

    const orderedDecisions = screen
      .getAllByTestId("decision-event-decision")
      .map((node) => node.textContent);
    expect(orderedDecisions).toEqual(["first", "second", "third"]);
  });

  it("shows first 10 decision events with show-all toggle", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const manyEvents = Array.from({ length: 12 }, (_, index) => ({
      timestamp: 1700000000 + index,
      node: "execute",
      decision: `event-${index + 1}`,
      reason: `reason-${index + 1}`,
    }));

    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Many events",
            decision_events: manyEvents,
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "show many events" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Many events")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Decision Log \(12 events\)/));

    await waitFor(() => {
      expect(screen.getAllByTestId("decision-event-item")).toHaveLength(10);
    });

    const toggle = screen.getByTestId("decision-log-show-all");
    expect(toggle).toHaveTextContent("Show all 12 events");

    fireEvent.click(toggle);

    await waitFor(() => {
      expect(screen.getAllByTestId("decision-event-item")).toHaveLength(12);
    });
    expect(screen.getByTestId("decision-log-show-all")).toHaveTextContent("Show first 10 events");
  });

  it("hides decision-log copy button when there are no decision events", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "No decisions",
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "no decision events" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("No decisions")).toBeInTheDocument();
    });

    expect(screen.queryByRole("button", { name: "Copy decision log" })).not.toBeInTheDocument();
  });

  it("shows cartesian join warning in SQL validation highlights", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Validation complete",
            sql: "SELECT * FROM a, b",
            validation_report: {
              affected_tables: ["a", "b"],
              query_complexity_score: 14,
              detected_cartesian_flag: true,
              has_aggregation: false,
              has_subquery: false,
              has_window_function: false,
            },
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "cartesian test" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Validation complete")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Generated SQL"));

    await waitFor(() => {
      expect(screen.getByTestId("validation-cartesian-warning")).toBeInTheDocument();
    });
    expect(screen.getByText(/Potential cartesian join detected/i)).toBeInTheDocument();
  });

  it("renders concise validation failure guidance", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Validation failed",
            sql: "SELEC bad FROM orders",
            validation_summary: {
              ast_valid: false,
              syntax_errors: ["Unexpected keyword near 'SELEC'"],
            },
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "bad sql validation" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Validation failed")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText("Generated SQL"));

    await waitFor(() => {
      expect(screen.getByTestId("validation-failure-guidance")).toBeInTheDocument();
    });
    expect(screen.getByText(/Validation failed due to SQL syntax issues/i)).toBeInTheDocument();
  });

  it("renders auto-pagination and prefetch metadata when completeness includes it", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Paginated response",
            result: [{ id: 1 }],
            result_completeness: {
              rows_returned: 1,
              next_page_token: "page-2",
              auto_paginated: true,
              pages_fetched: 3,
              auto_pagination_stopped_reason: "max_pages",
              prefetch_enabled: true,
              prefetch_scheduled: false,
              prefetch_reason: "manual_trigger",
            },
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "pagination metadata" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Paginated response")).toBeInTheDocument();
    });

    const metadata = screen.getByTestId("completeness-metadata");
    expect(metadata).toHaveTextContent("auto_paginated: true");
    expect(metadata).toHaveTextContent("pages_fetched: 3");
    expect(metadata).toHaveTextContent("stopped_reason: max_pages");
    expect(metadata).toHaveTextContent("prefetch_enabled: true");
    expect(metadata).toHaveTextContent("prefetch_scheduled: false");
    expect(metadata).toHaveTextContent("prefetch_reason: manual_trigger");
  });

  it("filters decision events by search text and phase while preserving order", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Filtered events",
            decision_events: [
              { timestamp: 1700000001, node: "router", decision: "route request", reason: "router step", type: "info" },
              { timestamp: 1700000002, node: "execute", decision: "retry query", reason: "network timeout", type: "warn" },
              { timestamp: 1700000003, node: "execute", decision: "query failed", reason: "permission denied", type: "error" },
            ],
          },
        },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "filter decisions" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Filtered events")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByText(/Decision Log \(3 events\)/));

    fireEvent.change(screen.getByTestId("decision-log-search"), { target: { value: "retry" } });
    await waitFor(() => {
      const filtered = screen.getAllByTestId("decision-event-decision").map((node) => node.textContent);
      expect(filtered).toEqual(["retry query"]);
    });

    fireEvent.change(screen.getByTestId("decision-log-search"), { target: { value: "" } });
    fireEvent.change(screen.getByTestId("decision-log-phase-filter"), { target: { value: "execute" } });

    await waitFor(() => {
      const executeOnly = screen.getAllByTestId("decision-event-decision").map((node) => node.textContent);
      expect(executeOnly).toEqual(["retry query", "query failed"]);
    });
  });
});
