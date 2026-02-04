import React from "react";
import { ScatterChartSchema } from "../../../types/charts";
import { Axis } from "../primitives/Axis";
import { EmptyState } from "../../common/EmptyState";
import { formatNumber } from "../utils/formatters";
import { getNumericExtent, linearScale } from "../utils/scales";

const DEFAULT_HEIGHT = 220;
const DEFAULT_WIDTH = 640;
const DEFAULT_MARGIN = {
  top: 12,
  right: 16,
  bottom: 36,
  left: 48
};

function buildTicks(domain: [number, number], count = 5) {
  const [min, max] = domain;
  if (count <= 1) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, idx) => min + idx * step);
}

export function ScatterChart({ schema }: { schema: ScatterChartSchema }) {
  const width = schema.meta?.width ?? DEFAULT_WIDTH;
  const height = schema.meta?.height ?? DEFAULT_HEIGHT;
  const margin = DEFAULT_MARGIN;

  const allPoints = schema.series.flatMap((series) => series.points);
  const xValues = allPoints
    .map((point) => (typeof point.x === "number" ? point.x : null))
    .filter((value): value is number => value != null);
  const yValues = allPoints.map((point) => point.y);
  const xDomain = getNumericExtent(xValues);
  const yDomain = getNumericExtent(yValues);

  if (!xDomain || !yDomain) {
    return <EmptyState title="No chart data" />;
  }

  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xScale = linearScale(xDomain, [0, innerWidth]);
  const yScale = linearScale(yDomain, [innerHeight, 0]);
  const xTicks = buildTicks(xDomain, schema.xAxis?.tickCount ?? 5);
  const yTicks = buildTicks(yDomain, schema.yAxis?.tickCount ?? 4);

  return (
    <svg
      data-testid="scatter-chart"
      viewBox={`0 0 ${width} ${height}`}
      width="100%"
      height={height}
    >
      <g transform={`translate(${margin.left}, ${margin.top})`}>
        {schema.series.map((series) => {
          const color = series.color || "var(--accent)";
          return series.points.map((point, idx) => {
            if (point.y == null || typeof point.x !== "number") return null;
            return (
              <circle
                key={`${series.name}-${idx}`}
                cx={xScale(point.x)}
                cy={yScale(point.y)}
                r={4}
                fill={color}
                opacity={0.8}
              />
            );
          });
        })}
        <Axis
          orientation="bottom"
          ticks={xTicks}
          scale={xScale}
          length={innerWidth}
          offset={innerHeight}
          tickFormat={(value) => formatNumber(value, 2)}
          label={schema.xAxis?.label}
        />
        <Axis
          orientation="left"
          ticks={yTicks}
          scale={yScale}
          length={innerHeight}
          tickFormat={(value) => formatNumber(value, 2)}
          label={schema.yAxis?.label}
        />
      </g>
    </svg>
  );
}
