import React, { useEffect, useImperativeHandle, useMemo, useRef, useState } from "react";
import VirtualList, { VirtualListHandle } from "../../common/VirtualList";
import { WaterfallRow, WaterfallGroup, groupWaterfallRows } from "./waterfall_model";
import { GroupHeaderRow } from "./GroupHeaderRow";
import { WaterfallSpanRow } from "./WaterfallSpanRow";

interface GroupedWaterfallViewProps {
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

export interface GroupedWaterfallHandle {
  scrollToSpanId: (spanId: string) => void;
}

type RenderableItem =
  | { type: "group"; group: WaterfallGroup }
  | { type: "span"; row: WaterfallRow; groupKey: string };

export const GroupedWaterfallView = React.forwardRef<GroupedWaterfallHandle, GroupedWaterfallViewProps>(({
  rows,
  traceStart,
  traceDurationMs,
  onSelect,
  criticalPath,
  showCriticalPath,
  selectedSpanId,
  showEvents = true,
  matchIds
}, ref) => {
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [pendingScrollSpanId, setPendingScrollSpanId] = useState<string | null>(null);
  const listRef = useRef<VirtualListHandle | null>(null);

  const groups = useMemo(() => groupWaterfallRows(rows), [rows]);

  const toggleGroup = (groupId: string) => {
    setCollapsedGroups(prev => {
      const next = new Set(prev);
      if (next.has(groupId)) {
        next.delete(groupId);
      } else {
        next.add(groupId);
      }
      return next;
    });
  };

  const flattenedItems = useMemo(() => {
    const items: RenderableItem[] = [];
    groups.forEach(group => {
      items.push({ type: "group", group });
      if (!collapsedGroups.has(group.id)) {
        group.rows.forEach(row => {
          items.push({ type: "span", row, groupKey: group.id });
        });
      }
    });
    return items;
  }, [groups, collapsedGroups]);

  const spanToGroup = useMemo(() => {
    const mapping = new Map<string, string>();
    groups.forEach(group => {
      group.rows.forEach(row => mapping.set(row.span.span_id, group.id));
    });
    return mapping;
  }, [groups]);

  useImperativeHandle(ref, () => ({
    scrollToSpanId(spanId: string) {
      const groupId = spanToGroup.get(spanId);
      if (groupId && collapsedGroups.has(groupId)) {
        setCollapsedGroups(prev => {
          const next = new Set(prev);
          next.delete(groupId);
          return next;
        });
        setPendingScrollSpanId(spanId);
        return;
      }
      setPendingScrollSpanId(spanId);
    }
  }), [collapsedGroups, spanToGroup]);

  useEffect(() => {
    if (!pendingScrollSpanId) return;
    const index = flattenedItems.findIndex(
      (item) => item.type === "span" && item.row.span.span_id === pendingScrollSpanId
    );
    if (index >= 0) {
      listRef.current?.scrollToIndex(index);
      setPendingScrollSpanId(null);
    }
  }, [flattenedItems, pendingScrollSpanId]);

  const totalDuration = traceDurationMs || 1;
  const height = Math.min(600, Math.max(300, flattenedItems.length * 32));

  return (
    <div className="trace-waterfall">
      <div className="trace-waterfall__header">
        <span>Span Hierarchy</span>
        <span>Timeline / Duration</span>
      </div>
      <VirtualList
        items={flattenedItems}
        rowHeight={32}
        height={height}
        ref={listRef}
        renderRow={(item) => {
          if (item.type === "group") {
            const criticalPathSpanCount = showCriticalPath && criticalPath
              ? item.group.rows.filter(r => criticalPath.has(r.span.span_id)).length
              : 0;
            return (
              <GroupHeaderRow
                label={item.group.label}
                spanCount={item.group.rows.length}
                totalDurationMs={item.group.totalDurationMs}
                isExpanded={!collapsedGroups.has(item.group.id)}
                onToggle={() => toggleGroup(item.group.id)}
                criticalPathSpanCount={criticalPathSpanCount}
              />
            );
          } else {
            const isCritical = !!(showCriticalPath && criticalPath?.has(item.row.span.span_id));
            const isMatch = matchIds ? matchIds.has(item.row.span.span_id) : false;
            return (
              <WaterfallSpanRow
                row={item.row}
                traceStart={traceStart}
                totalDuration={totalDuration}
                onSelect={onSelect}
                isCriticalPath={isCritical}
                isSelected={selectedSpanId === item.row.span.span_id}
                showEvents={showEvents}
                isMatch={isMatch}
              />
            );
          }
        }}
      />
    </div>
  );
});
