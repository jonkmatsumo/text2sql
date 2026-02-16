import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { fetchAvailableModels } from "../api";
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

describe("AgentChat models", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("updates model options when provider changes", async () => {
    const mockedFetchModels = fetchAvailableModels as unknown as ReturnType<typeof vi.fn>;

    mockedFetchModels.mockImplementation((provider: string) => {
      if (provider === "anthropic") {
        return Promise.resolve([{ value: "claude-test", label: "Claude Test" }]);
      }
      return Promise.resolve([{ value: "gpt-4o", label: "GPT-4o" }]);
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(mockedFetchModels).toHaveBeenCalledWith("openai");
    });

    const providerSelect = screen.getByLabelText("LLM Provider");
    const modelSelect = screen.getByLabelText("Model");

    expect(modelSelect).toHaveDisplayValue("GPT-4o");

    fireEvent.change(providerSelect, { target: { value: "anthropic" } });

    await waitFor(() => {
      expect(mockedFetchModels).toHaveBeenCalledWith("anthropic");
    });

    expect(screen.getByLabelText("Model")).toHaveDisplayValue("Claude Test");
  });
});
