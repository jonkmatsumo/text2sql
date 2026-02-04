import React from "react";
import { describe, it, expect, beforeEach } from "vitest";
import { render, screen } from "@testing-library/react";
import { ChartRenderer } from "../ChartRenderer";
import { ChartSchema } from "../../../types/charts";

describe("ChartRenderer", () => {
  beforeEach(() => {
    const memoryStorage = (() => {
      let store: Record<string, string> = {};
      return {
        getItem: (key: string) => store[key] ?? null,
        setItem: (key: string, value: string) => {
          store[key] = value;
        },
        removeItem: (key: string) => {
          delete store[key];
        },
        clear: () => {
          store = {};
        }
      };
    })();

    Object.defineProperty(window, "localStorage", {
      value: memoryStorage,
      writable: true
    });

    window.localStorage.removeItem("CHART_RENDERER_V2");
  });

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

  it("renders area charts via the renderer", () => {
    const schema: ChartSchema = {
      chartType: "area",
      series: [
        {
          name: "Latency",
          gradient: true,
          points: [
            { x: "2024-01-01T00:00:00Z", y: 5 },
            { x: "2024-01-01T00:05:00Z", y: 8 }
          ]
        }
      ]
    };

    render(<ChartRenderer schema={schema} />);
    expect(screen.getByTestId("area-chart")).toBeInTheDocument();
  });

  it("renders bar charts via the renderer", () => {
    const schema: ChartSchema = {
      chartType: "bar",
      series: [
        {
          name: "Counts",
          points: [
            { x: "A", y: 1 },
            { x: "B", y: 2 }
          ]
        }
      ]
    };

    render(<ChartRenderer schema={schema} />);
    expect(screen.getByTestId("bar-chart")).toBeInTheDocument();
  });

  it("renders scatter charts via the renderer", () => {
    const schema: ChartSchema = {
      chartType: "scatter",
      series: [
        {
          name: "Scatter",
          points: [
            { x: 1, y: 2 },
            { x: 2, y: 3 }
          ]
        }
      ]
    };

    render(<ChartRenderer schema={schema} />);
    expect(screen.getByTestId("scatter-chart")).toBeInTheDocument();
  });

  it("renders empty state when no chart data", () => {
    render(<ChartRenderer />);
    expect(screen.getByText("No chart data")).toBeInTheDocument();
  });

  it("renders error state when schema is required but missing", () => {
    window.localStorage.setItem("CHART_RENDERER_V2", "true");
    render(<ChartRenderer />);
    expect(
      screen.getByText(
        "Chart schema is required when CHART_RENDERER_V2 is enabled."
      )
    ).toBeInTheDocument();
  });
});
