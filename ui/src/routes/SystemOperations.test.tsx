import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import SystemOperations from "./SystemOperations";
import { OpsService } from "../api";

vi.mock("../api", () => ({
  OpsService: {
    generatePatterns: vi.fn(),
  },
  getErrorMessage: vi.fn((err: any) => err?.message || "error"),
}));

vi.mock("../hooks/useToast", () => ({
  useToast: () => ({ show: vi.fn() }),
}));

vi.mock("../hooks/useJobPolling", () => ({
  useJobPolling: () => ({ job: null }),
}));

class MockEventSource {
  static instances: MockEventSource[] = [];
  url: string;
  onmessage: ((event: MessageEvent) => void) | null = null;
  onerror: (() => void) | null = null;
  listeners: Record<string, Array<(event: MessageEvent) => void>> = {};
  close = vi.fn();

  constructor(url: string) {
    this.url = url;
    MockEventSource.instances.push(this);
  }

  addEventListener(type: string, callback: (event: MessageEvent) => void) {
    this.listeners[type] = this.listeners[type] || [];
    this.listeners[type].push(callback);
  }

  emitMessage(payload: any) {
    this.onmessage?.({ data: JSON.stringify(payload) } as MessageEvent);
  }

  emitEvent(type: string, payload: any) {
    const event = { data: JSON.stringify(payload) } as MessageEvent;
    (this.listeners[type] || []).forEach((cb) => cb(event));
  }
}

describe("SystemOperations", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (globalThis as any).EventSource = MockEventSource;
    MockEventSource.instances.length = 0;
  });

  it("streams pattern generation logs", async () => {
    render(<SystemOperations />);

    const runButton = screen.getByText("Run Legacy Generation (Auto)");
    fireEvent.click(runButton);

    const stream = MockEventSource.instances[0];
    expect(stream).toBeTruthy();

    stream.emitMessage({ message: "log line 1" });

    await waitFor(() => {
      expect(screen.getByText(/log line 1/i)).toBeInTheDocument();
    });

    stream.emitEvent("complete", {
      success: true,
      run_id: "run-1",
      metrics: { created_count: 1, updated_count: 2 },
    });

    await waitFor(() => {
      expect(stream.close).toHaveBeenCalled();
    });

    expect(screen.getByText(/Generation complete/i)).toBeInTheDocument();
    expect(OpsService.generatePatterns).not.toHaveBeenCalled();
  });
});
