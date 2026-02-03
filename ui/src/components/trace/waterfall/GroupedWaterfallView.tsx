import React, { useEffect, useImperativeHandle, useMemo, useRef, useState } from "react";
import VirtualList, { VirtualListHandle } from "../../common/VirtualList";
import { WaterfallRow, WaterfallGroup, StageGroup, groupStageRows, groupWaterfallRows } from "./waterfall_model";
import { GroupHeaderRow } from "./GroupHeaderRow";
import { StageHeaderRow } from "./StageHeaderRow";
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
  | { type: "stage"; stage: StageGroup }
  | { type: "group"; group: WaterfallGroup; stageId: string }
  | { type: "span"; row: WaterfallRow; groupKey: string; stageId: string };

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
  const [collapsedStages, setCollapsedStages] = useState<Set<string>>(new Set());
  const [pendingScrollSpanId, setPendingScrollSpanId] = useState<string | null>(null);
  const listRef = useRef<VirtualListHandle | null>(null);

  const stageGroups = useMemo(() => groupStageRows(rows), [rows]);

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

  const toggleStage = (stageId: string) => {
    setCollapsedStages((prev) => {
      const next = new Set(prev);
      if (next.has(stageId)) {
        next.delete(stageId);
      } else {
        next.add(stageId);
      }
      return next;
    });
  };

  const groupsByStage = useMemo(() => {
    return stageGroups.map((stage) => ({
      stage,
      groups: groupWaterfallRows(stage.rows),
    }));
  }, [stageGroups]);

  const flattenedItems = useMemo(() => {
    const items: RenderableItem[] = [];
    groupsByStage.forEach(({ stage, groups }) => {
      items.push({ type: "stage", stage });
      if (collapsedStages.has(stage.id)) {
        return;
      }
      groups.forEach((group) => {
        const groupKey = `${stage.id}::${group.id}`;
        items.push({ type: "group", group, stageId: stage.id });
        if (!collapsedGroups.has(groupKey)) {
          group.rows.forEach((row) => {
            items.push({ type: "span", row, groupKey, stageId: stage.id });
          });
        }
      });
    });
    return items;
  }, [groupsByStage, collapsedGroups, collapsedStages]);

  const spanToGroup = useMemo(() => {
    const mapping = new Map<string, string>();
    groupsByStage.forEach(({ stage, groups }) => {
      groups.forEach((group) => {
        const groupKey = `${stage.id}::${group.id}`;
        group.rows.forEach((row) => mapping.set(row.span.span_id, groupKey));
      });
    });
    return mapping;
  }, [groupsByStage]);

  useImperativeHandle(ref, () => ({
    scrollToSpanId(spanId: string) {
      const groupId = spanToGroup.get(spanId);
      const stageId = groupId ? groupId.split("::")[0] : null;
      if (stageId && collapsedStages.has(stageId)) {
        setCollapsedStages((prev) => {
          const next = new Set(prev);
          next.delete(stageId);
          return next;
        });
      }
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
  }), [collapsedGroups, collapsedStages, spanToGroup]);

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
          if (item.type === "stage") {
            return (
              <StageHeaderRow
                label={item.stage.label}
                spanCount={item.stage.rows.length}
                totalDurationMs={item.stage.totalDurationMs}
                totalSelfTimeMs={item.stage.totalSelfTimeMs}
                isExpanded={!collapsedStages.has(item.stage.id)}
                onToggle={() => toggleStage(item.stage.id)}
              />
            );
          } else if (item.type === "group") {
            const criticalPathSpanCount = showCriticalPath && criticalPath
              ? item.group.rows.filter(r => criticalPath.has(r.span.span_id)).length
              : 0;
            const groupKey = `${item.stageId}::${item.group.id}`;
            return (
              <GroupHeaderRow
                label={item.group.label}
                spanCount={item.group.rows.length}
                totalDurationMs={item.group.totalDurationMs}
                totalSelfTimeMs={item.group.totalSelfTimeMs}
                isExpanded={!collapsedGroups.has(groupKey)}
                onToggle={() => toggleGroup(groupKey)}
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
