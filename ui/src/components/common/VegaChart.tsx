import React, { Suspense } from "react";

const VegaLite = React.lazy(() =>
  import("react-vega").then((module) => ({ default: module.VegaLite }))
);

export interface VegaChartProps {
  spec: Record<string, unknown>;
  width?: number | "container";
  height?: number;
  className?: string;
}

export default function VegaChart({
  spec,
  width = "container",
  height,
  className
}: VegaChartProps) {
  if (!spec || typeof spec !== "object") {
    return null;
  }

  const chartSpec = {
    ...spec,
    width: width === "container" ? undefined : width,
    height: height ?? (spec.height as number | undefined),
    autosize: width === "container" ? { type: "fit", contains: "padding" } : undefined
  };

  return (
    <Suspense
      fallback={
        <div className="loading" style={{ padding: "20px" }}>
          Loading chart...
        </div>
      }
    >
      <div className={className} style={{ width: "100%" }}>
        <VegaLite
          spec={chartSpec as any}
          actions={false}
        />
      </div>
    </Suspense>
  );
}
