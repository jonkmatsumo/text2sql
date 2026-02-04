import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { ScatterChart } from "../ScatterChart";
import { ScatterChartSchema } from "../../../../types/charts";

describe("ScatterChart", () => {
  it("renders scatter points", () => {
    const schema: ScatterChartSchema = {
      chartType: "scatter",
      series: [
        {
          name: "Points",
          points: [
            { x: 1, y: 2 },
            { x: 2, y: 3 }
          ]
        }
      ]
    };

    const { container } = render(<ScatterChart schema={schema} />);
    expect(container.querySelectorAll("circle").length).toBe(2);
  });

  it("renders empty state when no numeric data", () => {
    const schema: ScatterChartSchema = {
      chartType: "scatter",
      series: [
        {
          name: "Empty",
          points: []
        }
      ]
    };

    render(<ScatterChart schema={schema} />);
    expect(screen.getByText("No chart data")).toBeInTheDocument();
  });
});
