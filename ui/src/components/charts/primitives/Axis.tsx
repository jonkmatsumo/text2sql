import React from "react";

interface AxisProps {
  orientation: "bottom" | "left";
  ticks: number[];
  scale: (value: number) => number;
  length: number;
  tickFormat?: (value: number) => string;
  label?: string;
  offset?: number;
}

export function Axis({
  orientation,
  ticks,
  scale,
  length,
  tickFormat,
  label,
  offset = 0
}: AxisProps) {
  const isBottom = orientation === "bottom";
  const lineProps = isBottom
    ? { x1: 0, x2: length, y1: 0, y2: 0 }
    : { x1: 0, x2: 0, y1: 0, y2: length };

  return (
    <g transform={isBottom ? `translate(0, ${offset})` : `translate(${offset}, 0)`}>
      <line {...lineProps} stroke="var(--border)" strokeWidth={1} />
      {ticks.map((tick) => {
        const pos = scale(tick);
        const x = isBottom ? pos : 0;
        const y = isBottom ? 0 : pos;
        const labelText = tickFormat ? tickFormat(tick) : String(tick);

        return (
          <g key={tick} transform={`translate(${x}, ${y})`}>
            <line
              x1={isBottom ? 0 : -4}
              x2={isBottom ? 0 : 0}
              y1={isBottom ? 0 : 0}
              y2={isBottom ? 4 : 0}
              stroke="var(--border)"
              strokeWidth={1}
            />
            <text
              x={isBottom ? 0 : -8}
              y={isBottom ? 14 : 4}
              textAnchor={isBottom ? "middle" : "end"}
              fontSize={10}
              fill="var(--muted)"
            >
              {labelText}
            </text>
          </g>
        );
      })}
      {label && (
        <text
          x={isBottom ? length / 2 : -length / 2}
          y={isBottom ? 32 : -32}
          textAnchor="middle"
          fontSize={11}
          fill="var(--muted)"
          transform={isBottom ? undefined : "rotate(-90)"}
        >
          {label}
        </text>
      )}
    </g>
  );
}
