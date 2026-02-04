import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen } from "@testing-library/react";
import { BarChart } from "../BarChart";
import { BarChartSchema } from "../../../../types/charts";

describe("BarChart", () => {
  it("renders bars for categorical data", () => {
    const schema: BarChartSchema = {
      chartType: "bar",
      series: [
        {
          name: "Counts",
          points: [
            { x: "A", y: 5 },
            { x: "B", y: 8 }
          ]
        }
      ]
    };

    const { container } = render(<BarChart schema={schema} />);
    expect(container.querySelectorAll("rect").length).toBe(2);
  });

  it("renders empty state when no categories", () => {
    const schema: BarChartSchema = {
      chartType: "bar",
      series: [
        {
          name: "Empty",
          points: []
        }
      ]
    };

    render(<BarChart schema={schema} />);
    expect(screen.getByText("No chart data")).toBeInTheDocument();
  });
});
