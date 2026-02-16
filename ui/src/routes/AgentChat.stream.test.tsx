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

  it("shows structured ErrorCard with ApiError details", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    mockedStream.mockReturnValue(
      (async function* () {
        throw new Error("Stream failed");
      })()
    );

    const apiErr = new (ApiError as any)(
      "Schema tables have changed",
      400,
      "schema_drift",
      { error_category: "schema_drift", hint: "Re-run ingestion" },
      "req-abc-123"
    );
    mockedRunAgent.mockRejectedValue(apiErr);

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "broken query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByTestId("error-category")).toHaveTextContent("Schema Mismatch");
    });

    expect(screen.getByText("Schema tables have changed")).toBeInTheDocument();
    expect(screen.getByTestId("error-hint")).toHaveTextContent("Re-run ingestion");
    expect(screen.getByText(/req-abc-/)).toBeInTheDocument();
    // Should show action link for schema_drift
    expect(screen.getByText("Open Ingestion Wizard")).toBeInTheDocument();
  });

  it("ignores out-of-order phase events (monotonic advance)", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;

    // Create a stream that emits phases out of order
    let resolveBlock: () => void;
    const blocked = new Promise<void>((r) => { resolveBlock = r; });

    mockedStream.mockReturnValue(
      (async function* () {
        yield { event: "progress", data: { phase: "plan" } };
        yield { event: "progress", data: { phase: "execute" } };
        // Out-of-order: router comes after execute — should be ignored
        yield { event: "progress", data: { phase: "router" } };
        // Hold stream open so we can inspect phase state
        await blocked;
        yield { event: "result", data: { response: "Done", result: [] } };
      })()
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "test monotonic" } });
    fireEvent.submit(input.closest("form")!);

    // Wait for the execute phase to be active (not regressed to router)
    await waitFor(() => {
      expect(screen.getByText("Executing SQL...")).toBeInTheDocument();
    });

    // Router should NOT be the current phase
    expect(screen.queryByText("Routing...")).not.toBeInTheDocument();

    // Unblock to finish
    resolveBlock!();

    await waitFor(() => {
      expect(screen.getByText("Done")).toBeInTheDocument();
    });
  });

  it("falls back to runAgent on stream timeout", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    // Stream that hangs on second event — first progress arrives but then
    // the iterator never resolves again, triggering the 30s timeout.
    // We patch setTimeout to use a short timeout for this test.
    const origSetTimeout = globalThis.setTimeout;
    globalThis.setTimeout = ((fn: (...args: any[]) => void, ms?: number, ...args: any[]) => {
      // Shorten the 30s stream timeout to 50ms for testing
      const adjusted = ms && ms >= 30_000 ? 50 : ms;
      return origSetTimeout(fn, adjusted, ...args);
    }) as typeof setTimeout;

    mockedStream.mockReturnValue({
      [Symbol.asyncIterator]() {
        let first = true;
        return {
          next() {
            if (first) {
              first = false;
              return Promise.resolve({
                done: false,
                value: { event: "progress", data: { phase: "router" } },
              });
            }
            // Hang forever on second iteration
            return new Promise(() => {});
          },
        };
      },
    });

    mockedRunAgent.mockResolvedValue({
      response: "Timeout fallback response",
      from_cache: false,
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "slow query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(mockedRunAgent).toHaveBeenCalledTimes(1);
    });

    await waitFor(() => {
      expect(screen.getByText("Timeout fallback response")).toBeInTheDocument();
    });

    globalThis.setTimeout = origSetTimeout;
  });

  it("aborts previous stream when new query is submitted", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    let callCount = 0;

    mockedStream.mockImplementation(() => ({
      [Symbol.asyncIterator]() {
        callCount++;
        const myCallNum = callCount;
        let yielded = false;
        return {
          next() {
            if (myCallNum === 1) {
              // First stream: hang forever (will be aborted)
              return new Promise<IteratorResult<any>>(() => {});
            }
            // Second stream: return result then done
            if (!yielded) {
              yielded = true;
              return Promise.resolve({
                done: false,
                value: { event: "result", data: { response: "Second query result" } },
              });
            }
            return Promise.resolve({ done: true, value: undefined });
          },
        };
      },
    }));

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);

    // Submit first query (stream will hang)
    fireEvent.change(input, { target: { value: "first query" } });
    fireEvent.submit(input.closest("form")!);

    // Wait a tick for the stream to start
    await waitFor(() => { expect(callCount).toBe(1); });

    // Submit second query (should abort first and start new stream)
    fireEvent.change(input, { target: { value: "second query" } });
    fireEvent.submit(input.closest("form")!);

    // Second query result should render
    await waitFor(() => {
      expect(screen.getByText("Second query result")).toBeInTheDocument();
    });

    // Both streams were created
    expect(callCount).toBe(2);
  });

  it("does not call runAgent fallback when stream already produced a result", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    // Stream produces a result then errors on iteration
    mockedStream.mockReturnValue(
      (async function* () {
        yield { event: "result", data: { response: "Streamed result", sql: "SELECT 1" } };
      })()
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "stream with result" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("Streamed result")).toBeInTheDocument();
    });

    // runAgent should NOT have been called since stream produced a result
    expect(mockedRunAgent).not.toHaveBeenCalled();
  });
});
