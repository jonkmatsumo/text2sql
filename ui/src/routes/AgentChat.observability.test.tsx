import { render, screen, fireEvent, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgentStream, fetchAvailableModels } from "../api";
import { resetAvailableModelsCache } from "../hooks/useAvailableModels";
import {
  COPY_SQL_METADATA_LABEL,
  DECISION_LOG_PHASE_ARIA_LABEL,
  DECISION_LOG_SEARCH_ARIA_LABEL,
} from "../constants/operatorUi";

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

    fireEvent.click(screen.getByTestId("decision-log-toggle"));
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

    fireEvent.click(screen.getByTestId("decision-log-toggle"));

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

  it("renders compact validation/completeness summary and expands SQL details", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Footer summary",
            sql: "SELECT * FROM orders",
            validation_summary: {
              ast_valid: false,
            },
            validation_report: {
              detected_cartesian_flag: true,
            },
            result_completeness: {
              is_truncated: true,
              pages_fetched: 2,
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
    fireEvent.change(input, { target: { value: "summary footer" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Footer summary")).toBeInTheDocument();
    });

    const summaryRow = screen.getByTestId("validation-completeness-summary");
    expect(summaryRow).toHaveTextContent("Validation: fail");
    expect(summaryRow).toHaveTextContent("Cartesian: risk");
    expect(summaryRow).toHaveTextContent("Completeness: truncated");
    expect(summaryRow).toHaveTextContent("Pages: 2");

    expect(screen.getByTestId("validation-key-signals")).not.toBeVisible();
    fireEvent.click(summaryRow);

    await waitFor(() => {
      expect(screen.getByTestId("validation-key-signals")).toBeInTheDocument();
    });
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

  it("renders copy SQL + metadata action and copies expected bundle payload", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(window.navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Bundle ready",
            sql: "SELECT * FROM orders",
            trace_id: "trace-bundle-1",
            validation_summary: {
              ast_valid: true,
            },
            result_completeness: {
              pages_fetched: 3,
              next_page_token: "token-2",
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
    fireEvent.change(input, { target: { value: "bundle copy test" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Bundle ready")).toBeInTheDocument();
    });

    const copyButton = screen.getByRole("button", { name: COPY_SQL_METADATA_LABEL });
    expect(copyButton).toBeInTheDocument();
    fireEvent.click(copyButton);

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });

    const copiedPayload = JSON.parse(String(writeText.mock.calls[0][0]));
    expect(copiedPayload.sql).toBe("SELECT * FROM orders");
    expect(copiedPayload.trace_id).toBe("trace-bundle-1");
    expect(copiedPayload.validation.status).toBe("pass");
    expect(copiedPayload.validation.validation_summary.ast_valid).toBe(true);
    expect(copiedPayload.completeness.status).toBe("paginated");
    expect(copiedPayload.completeness.pages_fetched).toBe(3);
  });

  it("omits trace_id from copy bundle payload when trace id is unavailable", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.defineProperty(window.navigator, "clipboard", {
      value: { writeText },
      configurable: true,
    });
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Bundle without trace",
            sql: "SELECT 1",
            validation_summary: {
              ast_valid: true,
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
    fireEvent.change(input, { target: { value: "bundle no trace" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Bundle without trace")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByRole("button", { name: COPY_SQL_METADATA_LABEL }));

    await waitFor(() => {
      expect(writeText).toHaveBeenCalledTimes(1);
    });

    const copiedPayload = JSON.parse(String(writeText.mock.calls[0][0]));
    expect(copiedPayload.sql).toBe("SELECT 1");
    expect(copiedPayload).not.toHaveProperty("trace_id");
  });

  it("renders trace and request identifier controls in assistant message footer", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Identifiers ready",
            trace_id: "0123456789abcdef0123456789abcdef",
            request_id: "req-42",
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
    fireEvent.change(input, { target: { value: "id parity" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Identifiers ready")).toBeInTheDocument();
    });

    const viewTrace = screen.getByRole("link", { name: "View Trace" });
    expect(viewTrace).toHaveAttribute("href", "/traces/0123456789abcdef0123456789abcdef");
    expect(screen.getByRole("button", { name: "Copy trace id" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Copy request id" })).toBeInTheDocument();
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

    fireEvent.click(screen.getByTestId("decision-log-toggle"));

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

  it("combines event-type search with phase filtering", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Type plus phase filters",
            decision_events: [
              { timestamp: 1700000001, node: "router", decision: "route request", reason: "router", type: "info" },
              { timestamp: 1700000002, node: "execute", decision: "retry query", reason: "execute", type: "warn" },
              { timestamp: 1700000003, node: "execute", decision: "query failed", reason: "execute", type: "error" },
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
    fireEvent.change(input, { target: { value: "type and phase filtering" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Type plus phase filters")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("decision-log-toggle"));
    fireEvent.change(screen.getByTestId("decision-log-search"), { target: { value: "error" } });
    fireEvent.change(screen.getByTestId("decision-log-phase-filter"), { target: { value: "execute" } });

    await waitFor(() => {
      const filtered = screen.getAllByTestId("decision-event-decision").map((node) => node.textContent);
      expect(filtered).toEqual(["query failed"]);
    });
  });

  it("keeps filtered ordering stable when toggling show-all in large decision logs", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const manyEvents = [
      ...Array.from({ length: 12 }, (_, index) => ({
        timestamp: 1700000000 + index,
        node: "execute",
        decision: `execute-${index + 1}`,
        reason: `execute reason ${index + 1}`,
      })),
      ...Array.from({ length: 8 }, (_, index) => ({
        timestamp: 1700000100 + index,
        node: "router",
        decision: `router-${index + 1}`,
        reason: `router reason ${index + 1}`,
      })),
    ];

    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Many filtered events",
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
    fireEvent.change(input, { target: { value: "large decision filtering" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Many filtered events")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("decision-log-toggle"));
    fireEvent.change(screen.getByTestId("decision-log-phase-filter"), { target: { value: "execute" } });

    await waitFor(() => {
      expect(screen.getByTestId("decision-log-show-all")).toHaveTextContent("Show all 12 events");
    });

    const firstWindowDecisions = screen
      .getAllByTestId("decision-event-decision")
      .map((node) => node.textContent);
    expect(firstWindowDecisions).toEqual([
      "execute-1",
      "execute-2",
      "execute-3",
      "execute-4",
      "execute-5",
      "execute-6",
      "execute-7",
      "execute-8",
      "execute-9",
      "execute-10",
    ]);

    fireEvent.click(screen.getByTestId("decision-log-show-all"));

    await waitFor(() => {
      expect(screen.getAllByTestId("decision-event-item")).toHaveLength(12);
    });

    const expandedDecisions = screen
      .getAllByTestId("decision-event-decision")
      .map((node) => node.textContent);
    expect(expandedDecisions).toEqual([
      "execute-1",
      "execute-2",
      "execute-3",
      "execute-4",
      "execute-5",
      "execute-6",
      "execute-7",
      "execute-8",
      "execute-9",
      "execute-10",
      "execute-11",
      "execute-12",
    ]);
  });

  it("applies severity styling from event metadata when available", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Severity events",
            decision_events: [
              { timestamp: 1700000001, node: "execute", decision: "failed run", reason: "fatal", level: "error" },
              { timestamp: 1700000002, node: "execute", decision: "retry run", reason: "timeout", type: "warn" },
              { timestamp: 1700000003, node: "plan", decision: "planned path", reason: "normal" },
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
    fireEvent.change(input, { target: { value: "severity decisions" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Severity events")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("decision-log-toggle"));

    const cards = screen.getAllByTestId("decision-event-item");
    const severityByDecision = new Map(
      cards.map((card) => [
        within(card).getByTestId("decision-event-decision").textContent,
        card.getAttribute("data-severity"),
      ])
    );
    expect(severityByDecision.get("failed run")).toBe("error");
    expect(severityByDecision.get("retry run")).toBe("warn");
    expect(severityByDecision.get("planned path")).toBe("neutral");
  });

  it("shows compact decision summary and keeps details hidden by default", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Summary events",
            decision_events: [
              { timestamp: 1700000001, node: "execute", decision: "warned", reason: "timeout", type: "warn" },
              { timestamp: 1700000002, node: "execute", decision: "errored", reason: "failed", level: "error" },
              { timestamp: 1700000003, node: "plan", decision: "neutral", reason: "normal" },
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
    fireEvent.change(input, { target: { value: "decision summary" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Summary events")).toBeInTheDocument();
    });

    expect(screen.getByTestId("decision-log-summary")).toHaveTextContent("Decision log: 3 events (2 warnings)");
    expect(screen.queryByTestId("decision-log-search")).not.toBeInTheDocument();

    fireEvent.click(screen.getByTestId("decision-log-toggle"));
    expect(screen.getByTestId("decision-log-search")).toBeInTheDocument();
  });

  it("exposes accessible labels for decision search and phase filters", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "A11y filters",
            decision_events: [
              { timestamp: 1700000001, node: "router", decision: "route request", reason: "router step", type: "info" },
              { timestamp: 1700000002, node: "execute", decision: "run query", reason: "execute step", type: "warn" },
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
    fireEvent.change(input, { target: { value: "decision filter accessibility" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("A11y filters")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("decision-log-toggle"));
    expect(screen.getByLabelText(DECISION_LOG_SEARCH_ARIA_LABEL)).toBeInTheDocument();
    expect(screen.getByLabelText(DECISION_LOG_PHASE_ARIA_LABEL)).toBeInTheDocument();
  });

  it("renders decision events with unknown shapes and missing timestamps safely", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        {
          event: "result",
          data: {
            response: "Unknown decisions",
            decision_events: [
              { node: "router", decision: "route request", reason: "router" },
              { payload: { foo: "bar" } },
              "raw decision note",
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
    fireEvent.change(input, { target: { value: "unknown decision shapes" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Unknown decisions")).toBeInTheDocument();
    });

    fireEvent.click(screen.getByTestId("decision-log-toggle"));

    await waitFor(() => {
      expect(screen.getAllByTestId("decision-event-item")).toHaveLength(3);
    });

    const decisions = screen.getAllByTestId("decision-event-decision").map((node) => node.textContent);
    expect(decisions).toContain("route request");
    expect(decisions).toContain("(no decision recorded)");
    expect(decisions).toContain("raw decision note");
    expect(screen.getAllByText("No timestamp").length).toBeGreaterThan(0);
  });
});
