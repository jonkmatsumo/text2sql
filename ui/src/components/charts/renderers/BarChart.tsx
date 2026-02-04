import React from "react";
import { BarChartSchema, Point } from "../../../types/charts";
import { Axis } from "../primitives/Axis";
import { EmptyState } from "../../common/EmptyState";
import { formatNumber } from "../utils/formatters";
import { bandScale, getNumericExtent, linearScale } from "../utils/scales";

const DEFAULT_HEIGHT = 220;
const DEFAULT_WIDTH = 640;
const DEFAULT_MARGIN = {
  top: 12,
  right: 16,
  bottom: 36,
  left: 48
};

function buildCategoryDomain(series: { points: Point[] }[]) {
  const domain: string[] = [];
  series.forEach((item) => {
    item.points.forEach((point) => {
      const key = String(point.x);
      if (!domain.includes(key)) domain.push(key);
    });
  });
  return domain;
}

function buildValueMap(points: Point[]) {
  const map: Record<string, number> = {};
  points.forEach((point) => {
    if (point.y == null) return;
    map[String(point.x)] = point.y;
  });
  return map;
}

function buildTicks(domain: [number, number], count = 5) {
  const [min, max] = domain;
  if (count <= 1) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, idx) => min + idx * step);
}

export function BarChart({ schema }: { schema: BarChartSchema }) {
  const width = schema.meta?.width ?? DEFAULT_WIDTH;
  const height = schema.meta?.height ?? DEFAULT_HEIGHT;
  const margin = DEFAULT_MARGIN;

  const categories = buildCategoryDomain(schema.series);
  if (!categories.length) {
    return <EmptyState title="No chart data" />;
  }

  const seriesValues = schema.series.map((series) => ({
    name: series.name,
    color: series.color || "var(--accent)",
    values: buildValueMap(series.points)
  }));

  const stackedTotals = categories.map((category) =>
    seriesValues.reduce(
      (sum, series) => sum + (series.values[category] ?? 0),
      0
    )
  );

  const flatValues = seriesValues.flatMap((series) => Object.values(series.values));
  const yMax = Math.max(
    0,
    ...(schema.stacked ? stackedTotals : flatValues).filter(
      (value) => typeof value === "number"
    )
  );
  const yDomain = getNumericExtent([0, yMax]);

  if (!yDomain) {
    return <EmptyState title="No chart data" />;
  }

  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xScale = bandScale(categories, [0, innerWidth], 0.2);
  const yScale = linearScale(yDomain, [innerHeight, 0]);
  const yTicks = buildTicks(yDomain, schema.yAxis?.tickCount ?? 4);

  const innerBand = schema.stacked
    ? null
    : bandScale(
        seriesValues.map((series) => series.name),
        [0, xScale.bandwidth],
        0.1
      );

  return (
    <svg
      data-testid="bar-chart"
      viewBox={`0 0 ${width} ${height}`}
      width="100%"
      height={height}
    >
      <g transform={`translate(${margin.left}, ${margin.top})`}>
        {categories.map((category) => {
          let stackOffset = 0;

          return seriesValues.map((series) => {
            const value = series.values[category] ?? 0;
            if (schema.stacked) {
              const barHeight = innerHeight - yScale(value + stackOffset);
              const y = yScale(value + stackOffset);
              const x = xScale(category);
              const rect = (
                <rect
                  key={`${series.name}-${category}`}
                  x={x}
                  y={y}
                  width={xScale.bandwidth}
                  height={barHeight}
                  fill={series.color}
                  opacity={0.85}
                />
              );
              stackOffset += value;
              return rect;
            }

            const x =
              xScale(category) + (innerBand ? innerBand(series.name) : 0);
            const widthValue = innerBand ? innerBand.bandwidth : xScale.bandwidth;
            const y = yScale(value);
            const barHeight = innerHeight - y;

            return (
              <rect
                key={`${series.name}-${category}`}
                x={x}
                y={y}
                width={widthValue}
                height={barHeight}
                fill={series.color}
                opacity={0.85}
              />
            );
          });
        })}
        <Axis
          orientation="bottom"
          ticks={categories.map((_, index) => index)}
          scale={(value) => {
            const category = categories[value] ?? categories[0];
            return xScale(category) + xScale.bandwidth / 2;
          }}
          length={innerWidth}
          offset={innerHeight}
          tickFormat={(value) => categories[value] ?? ""}
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
