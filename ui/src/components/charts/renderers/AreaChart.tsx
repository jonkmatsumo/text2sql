import React from "react";
import { AreaChartSchema, Point } from "../../../types/charts";
import { Axis } from "../primitives/Axis";
import { EmptyState } from "../../common/EmptyState";
import { formatNumber, formatTime } from "../utils/formatters";
import {
  getNumericExtent,
  getTimeExtent,
  linearScale,
  timeScale
} from "../utils/scales";

const DEFAULT_HEIGHT = 220;
const DEFAULT_WIDTH = 640;

const DEFAULT_MARGIN = {
  top: 12,
  right: 16,
  bottom: 36,
  left: 48
};

function buildSegments(points: Point[]) {
  const segments: Point[][] = [];
  let current: Point[] = [];

  points.forEach((point) => {
    if (point.y == null || Number.isNaN(point.y)) {
      if (current.length) {
        segments.push(current);
        current = [];
      }
      return;
    }

    current.push(point);
  });

  if (current.length) {
    segments.push(current);
  }

  return segments;
}

function buildAreaPath(
  segments: Point[][],
  xScale: (value: string | number | Date) => number,
  yScale: (value: number) => number,
  baseline: number
) {
  return segments
    .map((segment) => {
      const line = segment
        .map((point, idx) => {
          const x = xScale(point.x);
          const y = yScale(point.y ?? 0);
          return `${idx === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ");

      const start = segment[0];
      const end = segment[segment.length - 1];
      const closing = `L${xScale(end.x)},${baseline} L${xScale(
        start.x
      )},${baseline} Z`;

      return `${line} ${closing}`;
    })
    .join(" ");
}

function buildLinePath(
  segments: Point[][],
  xScale: (value: string | number | Date) => number,
  yScale: (value: number) => number
) {
  return segments
    .map((segment) =>
      segment
        .map((point, idx) => {
          const x = xScale(point.x);
          const y = yScale(point.y ?? 0);
          return `${idx === 0 ? "M" : "L"}${x},${y}`;
        })
        .join(" ")
    )
    .join(" ");
}

function buildTicks(domain: [number, number], count = 5) {
  const [min, max] = domain;
  if (count <= 1) return [min];
  const step = (max - min) / (count - 1);
  return Array.from({ length: count }, (_, idx) => min + idx * step);
}

export function AreaChart({ schema }: { schema: AreaChartSchema }) {
  const width = schema.meta?.width ?? DEFAULT_WIDTH;
  const height = schema.meta?.height ?? DEFAULT_HEIGHT;
  const margin = DEFAULT_MARGIN;

  const allPoints = schema.series.flatMap((series) => series.points);
  const xDomain = getTimeExtent(allPoints.map((point) => point.x));
  const yDomain = getNumericExtent(allPoints.map((point) => point.y));

  if (!xDomain || !yDomain) {
    return <EmptyState title="No chart data" />;
  }

  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  const xScale = timeScale(xDomain, [0, innerWidth]);
  const yScale = linearScale(yDomain, [innerHeight, 0]);

  const xTicks = buildTicks(xDomain, schema.xAxis?.tickCount ?? 5);
  const yTicks = buildTicks(yDomain, schema.yAxis?.tickCount ?? 4);

  return (
    <svg
      data-testid="area-chart"
      viewBox={`0 0 ${width} ${height}`}
      width="100%"
      height={height}
    >
      <g transform={`translate(${margin.left}, ${margin.top})`}>
        {schema.series.map((series, index) => {
          const segments = buildSegments(series.points);
          if (!segments.length) return null;

          const stroke = series.stroke || series.color || "var(--accent)";
          const fill = series.fill || series.color || "var(--accent)";
          const gradientId = `area-gradient-${index}`;
          const useGradient = !!series.gradient;

          return (
            <g key={series.name}>
              {useGradient && (
                <defs>
                  <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                    <stop offset="0%" stopColor={fill} stopOpacity={0.35} />
                    <stop offset="100%" stopColor={fill} stopOpacity={0} />
                  </linearGradient>
                </defs>
              )}
              <path
                d={buildAreaPath(segments, xScale, yScale, innerHeight)}
                fill={useGradient ? `url(#${gradientId})` : fill}
                fillOpacity={useGradient ? 1 : 0.15}
                stroke="none"
              />
              <path
                d={buildLinePath(segments, xScale, yScale)}
                fill="none"
                stroke={stroke}
                strokeWidth={2}
                vectorEffect="non-scaling-stroke"
              />
            </g>
          );
        })}
        <Axis
          orientation="bottom"
          ticks={xTicks}
          scale={xScale}
          length={innerWidth}
          offset={innerHeight}
          tickFormat={(value) =>
            formatTime(value, schema.xAxis?.format ?? "%m/%d %H:%M")
          }
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
