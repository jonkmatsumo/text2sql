import React from "react";
import { WaterfallRow } from "./waterfall/waterfall_model";
import { GroupedWaterfallView, GroupedWaterfallHandle } from "./waterfall/GroupedWaterfallView";

export { type WaterfallRow };

interface WaterfallViewProps {
  rows: WaterfallRow[];
  traceStart: number;
  traceDurationMs: number;
  onSelect: (spanId: string) => void;
  criticalPath?: Set<string>;
  showCriticalPath?: boolean;
  selectedSpanId?: string | null;
  showEvents?: boolean;
  matchIds?: Set<string>;
}

export interface WaterfallViewHandle {
  scrollToSpanId: (spanId: string) => void;
}

const WaterfallView = React.forwardRef<WaterfallViewHandle, WaterfallViewProps>((props, ref) => {
  return <GroupedWaterfallView {...props} ref={ref as React.Ref<GroupedWaterfallHandle>} />;
});

export default WaterfallView;
