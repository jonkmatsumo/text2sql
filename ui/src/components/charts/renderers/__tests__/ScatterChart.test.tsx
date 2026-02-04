import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
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

  it("shows tooltip on point hover", () => {
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
      ],
      xAxis: { label: "x" },
      yAxis: { label: "y" }
    };

    const { container } = render(<ScatterChart schema={schema} />);
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

    const circles = container.querySelectorAll("circle");
    fireEvent.mouseEnter(circles[0], { clientX: 40, clientY: 40 });
    expect(screen.getByTestId("chart-tooltip")).toBeInTheDocument();
    expect(screen.getByText("x:")).toBeInTheDocument();
  });
});
