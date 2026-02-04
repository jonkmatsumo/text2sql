import React from "react";
import { AreaChartSchema } from "../../../types/charts";

export function AreaChart({ schema }: { schema: AreaChartSchema }) {
  return (
    <div data-testid="chart-area">
      Area chart placeholder ({schema.series.length} series)
    </div>
  );
}
