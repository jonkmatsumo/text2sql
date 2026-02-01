import { render, screen, waitFor } from "@testing-library/react";
import { describe, it, expect, vi, beforeEach } from "vitest";
import { useAvailableModels, resetAvailableModelsCache } from "./useAvailableModels";
import { fetchAvailableModels } from "../api";

vi.mock("../api", () => ({
  fetchAvailableModels: vi.fn(),
}));

function TestComponent({
  provider,
  fallback = [],
}: {
  provider: string;
  fallback?: Array<{ value: string; label: string }>;
}) {
  const { models, isLoading, error } = useAvailableModels(provider, fallback);
  return (
    <div>
      <div data-testid="loading">{isLoading ? "loading" : "idle"}</div>
      <div data-testid="error">{error ?? ""}</div>
      <ul data-testid="models">
        {models.map((model) => (
          <li key={model.value}>{model.value}</li>
        ))}
      </ul>
    </div>
  );
}

describe("useAvailableModels", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAvailableModelsCache();
  });

  it("fetches and caches models per provider", async () => {
    const mockedFetch = fetchAvailableModels as unknown as ReturnType<typeof vi.fn>;
    mockedFetch.mockResolvedValueOnce([{ value: "m1", label: "M1" }]);

    const { rerender } = render(<TestComponent provider="cache-test" />);

    await waitFor(() => {
      expect(mockedFetch).toHaveBeenCalledWith("cache-test");
    });
    await waitFor(() => {
      expect(screen.getByText("m1")).toBeInTheDocument();
    });

    rerender(<TestComponent provider="cache-test" />);
    expect(mockedFetch).toHaveBeenCalledTimes(1);
  });

  it("falls back to defaults on error", async () => {
    const mockedFetch = fetchAvailableModels as unknown as ReturnType<typeof vi.fn>;
    mockedFetch.mockRejectedValueOnce(new Error("boom"));

    render(
      <TestComponent
        provider="error-test"
        fallback={[{ value: "fallback-model", label: "Fallback" }]}
      />
    );

    await waitFor(() => {
      expect(screen.getByText("fallback-model")).toBeInTheDocument();
    });
  });
});
