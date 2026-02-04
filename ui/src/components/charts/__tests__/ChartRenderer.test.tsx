import React, { Suspense } from "react";
import { describe, it, expect, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { ChartRenderer } from "../ChartRenderer";
import { ChartSchema } from "../../../types/charts";

vi.mock("../../common/VegaChart", () => ({
  default: ({ spec }: { spec: Record<string, unknown> }) => (
    <div data-testid="chart-vega">{JSON.stringify(spec)}</div>
  )
}));

describe("ChartRenderer", () => {
  it("dispatches based on schema chartType", () => {
    const schema: ChartSchema = {
      chartType: "line",
      series: [
        {
          name: "Requests",
          points: [
            { x: "2024-01-01T00:00:00Z", y: 10 },
            { x: "2024-01-01T00:05:00Z", y: 12 }
          ]
        }
      ]
    };

    render(<ChartRenderer schema={schema} />);
    expect(screen.getByTestId("chart-line")).toBeInTheDocument();
  });

  it("falls back to Vega when legacy spec present", async () => {
    render(
      <Suspense fallback={<div>Loading</div>}>
        <ChartRenderer legacySpec={{ mark: "line" }} />
      </Suspense>
    );

    expect(await screen.findByTestId("chart-vega")).toBeInTheDocument();
  });

  it("renders empty state when no chart data", () => {
    render(<ChartRenderer />);
    expect(screen.getByText("No chart data")).toBeInTheDocument();
  });
});
