import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
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

  it("shows tooltip on bar hover", () => {
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
      ],
      yAxis: { label: "count" }
    };

    const { container } = render(<BarChart schema={schema} />);
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

    const rects = container.querySelectorAll("rect");
    fireEvent.mouseEnter(rects[0], { clientX: 50, clientY: 50 });
    expect(screen.getByTestId("chart-tooltip")).toBeInTheDocument();
    expect(screen.getByText("count:")).toBeInTheDocument();
  });
});
