import React from "react";
import { EmptyState } from "../common/EmptyState";
import { ErrorState } from "../common/ErrorState";
import { ChartSchema, ChartMeta } from "../../types/charts";
import { AreaChart } from "./renderers/AreaChart";
import { BarChart } from "./renderers/BarChart";
import { LineChart } from "./renderers/LineChart";
import { ScatterChart } from "./renderers/ScatterChart";

const CHART_RENDERER_V2_KEY = "CHART_RENDERER_V2";

function isChartRendererV2Enabled(): boolean {
  if (typeof window === "undefined") return false;

  const envValue = import.meta.env.VITE_CHART_RENDERER_V2;
  if (envValue != null && envValue !== "") {
    return envValue !== "false";
  }

  try {
    const stored = window.localStorage?.getItem(CHART_RENDERER_V2_KEY);
    if (stored == null) return false;
    return stored === "true" || stored === "1";
  } catch {
    return false;
  }
}

interface ChartRendererProps {
  schema?: ChartSchema;
  title?: string;
  meta?: ChartMeta;
}

function renderSchemaChart(schema: ChartSchema) {
  switch (schema.chartType) {
    case "line":
      return <LineChart schema={schema} />;
    case "area":
      return <AreaChart schema={schema} />;
    case "bar":
      return <BarChart schema={schema} />;
    case "scatter":
      return <ScatterChart schema={schema} />;
    default:
      return <ErrorState error="Unsupported chart type" />;
  }
}

export function ChartRenderer({
  schema,
  title,
  meta
}: ChartRendererProps) {
  const forceSchema = isChartRendererV2Enabled();
  const emptyTitle = title ? `${title} unavailable` : "No chart data";

  if (forceSchema && !schema?.chartType) {
    return (
      <ErrorState
        error="Chart schema is required when CHART_RENDERER_V2 is enabled."
      />
    );
  }

  if (schema?.chartType) {
    return renderSchemaChart(schema);
  }

  void meta;
  return <EmptyState title={emptyTitle} />;
}
