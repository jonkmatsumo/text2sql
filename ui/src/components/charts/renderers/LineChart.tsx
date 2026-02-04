import React from "react";
import { LineChartSchema } from "../../../types/charts";

export function LineChart({ schema }: { schema: LineChartSchema }) {
  return (
    <div data-testid="chart-line">
      Line chart placeholder ({schema.series.length} series)
    </div>
  );
}
