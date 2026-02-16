import { render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, it, expect, vi, beforeEach } from "vitest";
import Layout from "../Layout";
import { fetchQueryTargetSettings } from "../../../api";

vi.mock("../../../api", () => ({
  fetchQueryTargetSettings: vi.fn(),
}));

vi.mock("../OtelHealthBanner", () => ({
  default: () => null,
}));

vi.mock("../../../context/OtelHealthContext", () => ({
  useOtelHealth: () => ({
    health: { isHealthy: true, lastChecked: null, lastError: null, consecutiveFailures: 0 },
    checkHealth: vi.fn(),
    reportFailure: vi.fn(),
    reportSuccess: vi.fn(),
  }),
}));

describe("Layout health badge", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("shows System Online when active config exists", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: { id: "cfg-1", provider: "postgres", status: "active" },
    });

    render(
      <MemoryRouter>
        <Layout><div>content</div></Layout>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("health-badge")).toHaveTextContent("System Online");
    });
  });

  it("shows Not Configured when no active config", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: null,
    });

    render(
      <MemoryRouter>
        <Layout><div>content</div></Layout>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("health-badge")).toHaveTextContent("Not Configured");
    });
  });

  it("shows Connection Failed on API error", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockRejectedValue(
      new Error("Network error")
    );

    render(
      <MemoryRouter>
        <Layout><div>content</div></Layout>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("health-badge")).toHaveTextContent("Connection Failed");
    });
  });

  it("shows Checking... during initial load", () => {
    // Mock that never resolves
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockReturnValue(
      new Promise(() => {})
    );

    render(
      <MemoryRouter>
        <Layout><div>content</div></Layout>
      </MemoryRouter>
    );

    expect(screen.getByTestId("health-badge")).toHaveTextContent("Checking...");
  });

  it("badge links to settings page", async () => {
    (fetchQueryTargetSettings as ReturnType<typeof vi.fn>).mockResolvedValue({
      active: { id: "cfg-1" },
    });

    render(
      <MemoryRouter>
        <Layout><div>content</div></Layout>
      </MemoryRouter>
    );

    await waitFor(() => {
      expect(screen.getByTestId("health-badge")).toHaveTextContent("System Online");
    });

    const badge = screen.getByTestId("health-badge");
    expect(badge).toHaveAttribute("href", "/admin/settings/query-target");
  });
});
