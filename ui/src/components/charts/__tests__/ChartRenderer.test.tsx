import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { ChartRenderer } from "../ChartRenderer";
import { ChartSchema } from "../../../types/charts";

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
    expect(screen.getByTestId("line-chart")).toBeInTheDocument();
  });

  it("renders empty state when no chart data", () => {
    render(<ChartRenderer />);
    expect(screen.getByText("No chart data")).toBeInTheDocument();
  });
});
