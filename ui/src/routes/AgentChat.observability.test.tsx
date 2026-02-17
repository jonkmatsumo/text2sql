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

    const orderedDecisions = screen
      .getAllByTestId("decision-event-decision")
      .map((node) => node.textContent);
    expect(orderedDecisions).toEqual(["first", "second", "third"]);
  });
});
