import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgent, runAgentStream, fetchAvailableModels, ApiError } from "../api";
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
    requestId?: string;
    constructor(message: string, status: number, code = "UNKNOWN", details = {}, requestId?: string) {
      super(message);
      this.name = "ApiError";
      this.status = status;
      this.code = code;
      this.details = details;
      this.requestId = requestId;
    }
    get displayMessage() { return this.message; }
  },
}));

function mockModels() {
  (fetchAvailableModels as ReturnType<typeof vi.fn>).mockResolvedValue([
    { value: "gpt-4o", label: "GPT-4o" },
  ]);
}

async function* makeStream(events: Array<{ event: string; data: any }>) {
  for (const e of events) {
    yield e;
  }
}

describe("AgentChat streaming", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    mockModels();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("streams progress events and renders result", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        { event: "progress", data: { phase: "router" } },
        { event: "progress", data: { phase: "plan" } },
        { event: "progress", data: { phase: "execute" } },
        { event: "result", data: { response: "Here are the results", sql: "SELECT 1", result: [{ count: 42 }] } },
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "How many users?" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(mockedStream).toHaveBeenCalled();
    });

    await waitFor(() => {
      expect(screen.getByText("Here are the results")).toBeInTheDocument();
    });

    expect(screen.getByText("42")).toBeInTheDocument();
  });

  it("falls back to runAgent when stream fails", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    mockedStream.mockReturnValue(
      (async function* () {
        throw new Error("Stream endpoint not available");
      })()
    );

    mockedRunAgent.mockResolvedValue({
      response: "Fallback response",
      from_cache: false,
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "test fallback" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(mockedRunAgent).toHaveBeenCalled();
    });

    await waitFor(() => {
      expect(screen.getByText("Fallback response")).toBeInTheDocument();
    });
  });

  it("shows ErrorCard on error", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    mockedStream.mockReturnValue(
      (async function* () {
        throw new Error("Stream failed");
      })()
    );

    mockedRunAgent.mockRejectedValue(new Error("Agent also failed"));

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "trigger error" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Agent also failed")).toBeInTheDocument();
    });

    // Should show the error card
    expect(screen.getByText("Error")).toBeInTheDocument();
  });
});
