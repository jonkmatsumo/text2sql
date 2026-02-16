import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import AgentChat from "./AgentChat";
import { fetchQueryTargetSettings, fetchAvailableModels } from "../api";
import { resetAvailableModelsCache } from "../hooks/useAvailableModels";

vi.mock("../api", () => ({
  runAgent: vi.fn(),
  runAgentStream: vi.fn(),
  submitFeedback: vi.fn(),
  fetchAvailableModels: vi.fn(),
  fetchQueryTargetSettings: vi.fn(),
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

describe("AgentChat onboarding", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
    mockModels();
    if (!(globalThis as any).crypto) {
      (globalThis as any).crypto = { randomUUID: () => "test-uuid" };
    }
  });

  it("shows onboarding panel when no active config", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: null,
      pending: null,
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("onboarding-panel")).toBeInTheDocument();
    });

    expect(screen.getByText("Welcome to Text2SQL")).toBeInTheDocument();
    expect(screen.getByText("Configure Data Source")).toBeInTheDocument();
  });

  it("disables input when unconfigured", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: null,
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("onboarding-panel")).toBeInTheDocument();
    });

    const input = screen.getByPlaceholderText(/configure a data source first/i);
    expect(input).toBeDisabled();
  });

  it("shows normal chat when configured", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: { id: "cfg-1", provider: "postgres", status: "active" },
    });

    render(
      <MemoryRouter>
        <AgentChat />
      </MemoryRouter>
    );

    await waitFor(() => {
      const input = screen.getByPlaceholderText(/ask a question/i);
      expect(input).not.toBeDisabled();
    });

    expect(screen.queryByTestId("onboarding-panel")).not.toBeInTheDocument();
  });
});
