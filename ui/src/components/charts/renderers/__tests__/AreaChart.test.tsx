import React from "react";
import { describe, it, expect } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { AreaChart } from "../AreaChart";
import { AreaChartSchema } from "../../../../types/charts";

describe("AreaChart", () => {
  it("renders an area path for a single series", () => {
    const schema: AreaChartSchema = {
      chartType: "area",
      series: [
        {
          name: "Traces",
          points: [
            { x: "2024-01-01T00:00:00Z", y: 10 },
            { x: "2024-01-01T00:05:00Z", y: 15 }
          ]
        }
      ]
    };

    const { container } = render(<AreaChart schema={schema} />);
    const path = container.querySelector("path");
    expect(path?.getAttribute("d")).toContain("M");
  });

  it("emits a linear gradient when gradient is enabled", () => {
    const schema: AreaChartSchema = {
      chartType: "area",
      series: [
        {
          name: "Errors",
          gradient: true,
          points: [
            { x: "2024-01-01T00:00:00Z", y: 2 },
            { x: "2024-01-01T00:05:00Z", y: 5 }
          ]
        }
      ]
    };

    const { container } = render(<AreaChart schema={schema} />);
    expect(container.querySelector("linearGradient")).toBeInTheDocument();
  });

  it("breaks area path across null values", () => {
    const schema: AreaChartSchema = {
      chartType: "area",
      series: [
        {
          name: "Latency",
          points: [
            { x: "2024-01-01T00:00:00Z", y: 10 },
            { x: "2024-01-01T00:05:00Z", y: null },
            { x: "2024-01-01T00:10:00Z", y: 12 }
          ]
        }
      ]
    };

    const { container } = render(<AreaChart schema={schema} />);
    const areaPath = container.querySelector("path");
    const pathData = areaPath?.getAttribute("d") ?? "";
    const moveCount = pathData.split("M").length - 1;
    expect(moveCount).toBeGreaterThan(1);
  });

  it("shows and hides tooltip on hover", () => {
    const schema: AreaChartSchema = {
      chartType: "area",
      series: [
        {
          name: "Traces",
          points: [
            { x: "2024-01-01T00:00:00Z", y: 10 },
            { x: "2024-01-01T00:05:00Z", y: 12 }
          ]
        }
      ],
      yAxis: {
        label: "traces"
      }
    };

    const { container } = render(<AreaChart schema={schema} />);
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

    const svg = screen.getByTestId("area-chart");
    fireEvent.mouseMove(svg, { clientX: 80, clientY: 80 });

    expect(screen.getByTestId("chart-tooltip")).toBeInTheDocument();
    expect(screen.getByText("traces:")).toBeInTheDocument();

    fireEvent.mouseLeave(svg);
    expect(screen.queryByTestId("chart-tooltip")).not.toBeInTheDocument();
  });
});
