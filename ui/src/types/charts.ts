export type ChartType = "line" | "area" | "bar" | "scatter";

export interface Point {
  x: string | number;
  y: number | null;
}

export interface Series {
  name: string;
  points: Point[];
  color?: string;
  stroke?: string;
  fill?: string;
  gradient?: boolean;
}

export interface AxisSpec {
  label?: string;
  format?: string;
  tickCount?: number;
  min?: number;
  max?: number;
}

export interface ChartMeta {
  title?: string;
  description?: string;
  width?: number;
  height?: number;
}

export interface LineChartSchema {
  chartType: "line";
  series: Series[];
  xAxis?: AxisSpec;
  yAxis?: AxisSpec;
  meta?: ChartMeta;
}

export interface AreaChartSchema {
  chartType: "area";
  series: Series[];
  xAxis?: AxisSpec;
  yAxis?: AxisSpec;
  meta?: ChartMeta;
}

export interface BarChartSchema {
  chartType: "bar";
  series: Series[];
  xAxis?: AxisSpec;
  yAxis?: AxisSpec;
  meta?: ChartMeta;
}

export interface ScatterChartSchema {
  chartType: "scatter";
  series: Series[];
  xAxis?: AxisSpec;
  yAxis?: AxisSpec;
  meta?: ChartMeta;
}

export type ChartSchema =
  | LineChartSchema
  | AreaChartSchema
  | BarChartSchema
  | ScatterChartSchema;
