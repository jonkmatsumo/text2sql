import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { runAgent, fetchAvailableModels } from "../api";
import { resetAvailableModelsCache } from "../hooks/useAvailableModels";

vi.mock("../api", () => ({
  runAgent: vi.fn(),
  submitFeedback: vi.fn(),
  fetchAvailableModels: vi.fn(),
}));

describe("AgentChat chart rendering", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("renders charts from schema responses", async () => {
    const mockedRunAgent = runAgent as unknown as ReturnType<typeof vi.fn>;
    const mockedFetchModels = fetchAvailableModels as unknown as ReturnType<typeof vi.fn>;

    mockedFetchModels.mockResolvedValue([{ value: "gpt-4o", label: "GPT-4o" }]);
    mockedRunAgent.mockResolvedValue({
      response: "here is a chart",
      viz_spec: {
        chartType: "line",
        series: [
          {
            name: "Requests",
            points: [
              { x: "2024-01-01T00:00:00Z", y: 1 },
              { x: "2024-01-01T00:05:00Z", y: 2 }
            ]
          }
        ]
      }
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    const input = screen.getByPlaceholderText(/ask a question/i);
    fireEvent.change(input, { target: { value: "test query" } });
    fireEvent.submit(input.closest("form")!);

    await waitFor(() => {
      expect(mockedRunAgent).toHaveBeenCalled();
    });

    expect(await screen.findByTestId("line-chart")).toBeInTheDocument();
  });
});
