import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgent, runAgentStream, fetchAvailableModels } from "../api";
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

describe("AgentChat pagination", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    mockModels();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("shows Load more button when next_page_token exists", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        { event: "result", data: {
          response: "Results",
          result: [{ id: 1 }, { id: 2 }],
          result_completeness: {
            next_page_token: "token-abc",
            rows_returned: 2,
            is_truncated: false,
            is_limited: false,
          },
        }},
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "get data" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByTestId("load-more-button")).toBeInTheDocument();
    });

    expect(screen.getByText("Load more rows")).toBeInTheDocument();
  });

  it("does not show Load more when no next_page_token", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    mockedStream.mockReturnValue(
      makeStream([
        { event: "result", data: {
          response: "All results",
          result: [{ id: 1 }],
          result_completeness: {
            rows_returned: 1,
            is_truncated: false,
            is_limited: false,
          },
        }},
      ])
    );

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "get all" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByText("All results")).toBeInTheDocument();
    });

    expect(screen.queryByTestId("load-more-button")).not.toBeInTheDocument();
  });

  it("clicking Load more appends rows", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    mockedStream.mockReturnValue(
      makeStream([
        { event: "result", data: {
          response: "Page 1",
          result: [{ name: "Alice" }],
          result_completeness: {
            next_page_token: "page2-token",
            rows_returned: 1,
          },
        }},
      ])
    );

    // Mock the pagination call
    mockedRunAgent.mockResolvedValue({
      result: [{ name: "Bob" }],
      result_completeness: {
        rows_returned: 1,
        is_truncated: false,
      },
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "paginated query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByTestId("load-more-button")).toBeInTheDocument();
    });

    // Click load more
    fireEvent.click(screen.getByTestId("load-more-button"));

    await waitFor(() => {
      expect(mockedRunAgent).toHaveBeenCalledWith(
        expect.objectContaining({ page_token: "page2-token" })
      );
    });

    // Both rows should now be visible
    await waitFor(() => {
      expect(screen.getByText("Alice")).toBeInTheDocument();
      expect(screen.getByText("Bob")).toBeInTheDocument();
    });
  });

  it("disables Load more button and shows spinner during fetch", async () => {
    const mockedStream = runAgentStream as ReturnType<typeof vi.fn>;
    const mockedRunAgent = runAgent as ReturnType<typeof vi.fn>;

    mockedStream.mockReturnValue(
      makeStream([
        { event: "result", data: {
          response: "Page 1",
          result: [{ name: "Alice" }],
          result_completeness: {
            next_page_token: "page2-token",
            rows_returned: 1,
          },
        }},
      ])
    );

    // Make pagination request hang so we can assert disabled state
    let resolvePagination: (v: any) => void;
    mockedRunAgent.mockReturnValue(new Promise((r) => { resolvePagination = r; }));

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "paginated query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(screen.getByTestId("load-more-button")).toBeInTheDocument();
    });

    // Click load more
    fireEvent.click(screen.getByTestId("load-more-button"));

    // Button should be disabled and show "Loading..."
    await waitFor(() => {
      const btn = screen.getByTestId("load-more-button");
      expect(btn).toBeDisabled();
      expect(btn).toHaveTextContent("Loading...");
    });

    // Resolve pagination
    resolvePagination!({
      result: [{ name: "Bob" }],
      result_completeness: { rows_returned: 1, is_truncated: false },
    });

    await waitFor(() => {
      expect(screen.getByText("Bob")).toBeInTheDocument();
    });
  });
});
