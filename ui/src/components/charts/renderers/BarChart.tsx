import React from "react";
import { BarChartSchema } from "../../../types/charts";

export function BarChart({ schema }: { schema: BarChartSchema }) {
  return (
    <div data-testid="chart-bar">
      Bar chart placeholder ({schema.series.length} series)
    </div>
  );
}
