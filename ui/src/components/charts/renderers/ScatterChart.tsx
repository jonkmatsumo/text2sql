import React, { useRef, useState } from "react";
import { ScatterChartSchema } from "../../../types/charts";
import { Axis } from "../primitives/Axis";
import { EmptyState } from "../../common/EmptyState";
import { formatNumber } from "../utils/formatters";
import { getNumericExtent, linearScale } from "../utils/scales";
import { Tooltip } from "../primitives/Tooltip";

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
  const wrapperRef = useRef<HTMLDivElement | null>(null);
  const [tooltip, setTooltip] = useState<{
    x: number;
    y: number;
    visible: boolean;
    items: { label: string; value: string }[];
  }>({ x: 0, y: 0, visible: false, items: [] });

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

  const showTooltip = (
    event: React.MouseEvent<SVGCircleElement>,
    xValue: number,
    yValue: number,
    seriesName: string
  ) => {
    if (!wrapperRef.current) return;
    const rect = wrapperRef.current.getBoundingClientRect();
    const xLabel = schema.xAxis?.label || "X";
    const yLabel = schema.yAxis?.label || seriesName;
    setTooltip({
      x: event.clientX - rect.left + 12,
      y: event.clientY - rect.top + 12,
      visible: true,
      items: [
        { label: xLabel, value: formatNumber(xValue, 2) },
        { label: yLabel, value: formatNumber(yValue, 2) }
      ]
    });
  };

  const hideTooltip = () => {
    setTooltip((prev) => ({ ...prev, visible: false }));
  };

  return (
    <div ref={wrapperRef} style={{ position: "relative" }}>
      <svg
        data-testid="scatter-chart"
        viewBox={`0 0 ${width} ${height}`}
        width="100%"
        height={height}
        onMouseLeave={hideTooltip}
      >
        <g transform={`translate(${margin.left}, ${margin.top})`}>
          {schema.series.map((series) => {
            const color = series.color || "var(--accent)";
          return series.points.map((point, idx) => {
              const xValue = point.x;
              if (point.y == null || typeof xValue !== "number") return null;
              return (
                <circle
                  key={`${series.name}-${idx}`}
                  cx={xScale(xValue)}
                  cy={yScale(point.y)}
                  r={4}
                  fill={color}
                  opacity={0.8}
                  onMouseEnter={(event) =>
                    showTooltip(event, xValue, point.y ?? 0, series.name)
                  }
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
      <Tooltip
        x={tooltip.x}
        y={tooltip.y}
        items={tooltip.items}
        visible={tooltip.visible}
      />
    </div>
  );
}
