import React from "react";
import { ScatterChartSchema } from "../../../types/charts";

export function ScatterChart({ schema }: { schema: ScatterChartSchema }) {
  return (
    <div data-testid="chart-scatter">
      Scatter chart placeholder ({schema.series.length} series)
    </div>
  );
}
