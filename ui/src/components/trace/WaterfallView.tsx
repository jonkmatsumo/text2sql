import React from "react";
import { WaterfallRow } from "./waterfall_model";
import { GroupedWaterfallView } from "./waterfall/GroupedWaterfallView";

export { type WaterfallRow };

interface WaterfallViewProps {
  rows: WaterfallRow[];
  traceStart: number;
  traceDurationMs: number;
  onSelect: (spanId: string) => void;
  criticalPath?: Set<string>;
  showCriticalPath?: boolean;
}

export default function WaterfallView(props: WaterfallViewProps) {
  return <GroupedWaterfallView {...props} />;
}
