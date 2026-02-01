import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgent, fetchAvailableModels } from "../api";
import { resetAvailableModelsCache } from "../hooks/useAvailableModels";

vi.mock("../api", () => ({
  runAgent: vi.fn(),
  submitFeedback: vi.fn(),
  fetchAvailableModels: vi.fn(),
}));

describe("AgentChat cache rendering", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("renders cache similarity in cached responses", async () => {
    const mockedRunAgent = runAgent as unknown as ReturnType<typeof vi.fn>;
    const mockedFetchModels = fetchAvailableModels as unknown as ReturnType<typeof vi.fn>;

    mockedFetchModels.mockResolvedValue([{ value: "gpt-4o", label: "GPT-4o" }]);
    mockedRunAgent.mockResolvedValue({
      response: "cached response",
      from_cache: true,
      cache_similarity: 0.953,
    });

    render(<AgentChat />);

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "test query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(mockedRunAgent).toHaveBeenCalled();
    });

    expect(
      screen.getByText("From cache (similarity ≥ 95%)")
    ).toBeInTheDocument();
  });
});
