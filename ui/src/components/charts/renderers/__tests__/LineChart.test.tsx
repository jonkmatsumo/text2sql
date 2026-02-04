import React from "react";
import { describe, it, expect } from "vitest";
import { render } from "@testing-library/react";
import { LineChart } from "../LineChart";
import { LineChartSchema } from "../../../../types/charts";

describe("LineChart", () => {
  it("renders a line path", () => {
    const schema: LineChartSchema = {
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
    };

    const { container } = render(<LineChart schema={schema} />);
    expect(container.querySelector("path")).toBeInTheDocument();
  });

  it("renders markers when enabled", () => {
    const schema: LineChartSchema = {
      chartType: "line",
      showMarkers: true,
      series: [
        {
          name: "Latency",
          points: [
            { x: "2024-01-01T00:00:00Z", y: 1 },
            { x: "2024-01-01T00:05:00Z", y: 2 }
          ]
        }
      ]
    };

    const { container } = render(<LineChart schema={schema} />);
    expect(container.querySelectorAll("circle").length).toBeGreaterThan(0);
  });
});
