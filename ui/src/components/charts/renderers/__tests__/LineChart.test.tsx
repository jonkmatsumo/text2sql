import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
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

  it("shows tooltip on hover", () => {
    const schema: LineChartSchema = {
      chartType: "line",
      series: [
        {
          name: "Latency",
          points: [
            { x: 1, y: 5 },
            { x: 2, y: 7 }
          ]
        }
      ],
      yAxis: { label: "ms" }
    };

    const { container } = render(<LineChart schema={schema} />);
    const wrapper = container.firstElementChild as HTMLElement;
    wrapper.getBoundingClientRect = () =>
      ({
        left: 0,
        top: 0,
        width: 640,
        height: 220,
        right: 640,
        bottom: 220,
        x: 0,
        y: 0,
        toJSON: () => ({})
      }) as DOMRect;

    const svg = screen.getByTestId("line-chart");
    fireEvent.mouseMove(svg, { clientX: 100, clientY: 80 });
    expect(screen.getByTestId("chart-tooltip")).toBeInTheDocument();
    expect(screen.getByText("ms:")).toBeInTheDocument();
  });
});
