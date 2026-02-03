import React, { useMemo, useState } from "react";
import { SpanSummary } from "../../../types";
import { buildTraceGraph, TraceGraphNode } from "./trace_graph_model";

interface TraceGraphViewProps {
  spans: SpanSummary[];
  onSelect: (spanId: string) => void;
  selectedSpanId?: string | null;
}

const COLUMN_WIDTH = 220;
const ROW_HEIGHT = 70;
const MAX_VISIBLE_NODES = 200;

function layoutNodes(nodes: TraceGraphNode[]) {
  const byDepth = new Map<number, TraceGraphNode[]>();
  nodes.forEach((node) => {
    const depth = node.depth ?? 0;
    if (!byDepth.has(depth)) byDepth.set(depth, []);
    byDepth.get(depth)!.push(node);
  });

  const positioned = new Map<string, { x: number; y: number }>();
  let maxDepth = 0;
  let maxCount = 0;
  Array.from(byDepth.entries()).forEach(([depth, list]) => {
    maxDepth = Math.max(maxDepth, depth);
    maxCount = Math.max(maxCount, list.length);
    list.forEach((node, index) => {
      positioned.set(node.id, {
        x: depth * COLUMN_WIDTH,
        y: index * ROW_HEIGHT
      });
    });
  });

  return {
    positions: positioned,
    width: Math.max(1, (maxDepth + 1) * COLUMN_WIDTH),
    height: Math.max(1, maxCount * ROW_HEIGHT)
  };
}

export const TraceGraphView: React.FC<TraceGraphViewProps> = ({
  spans,
  onSelect,
  selectedSpanId
}) => {
  const [isExpanded, setIsExpanded] = useState(false);

  const graph = useMemo(() => buildTraceGraph(spans), [spans]);
  const sortedNodes = useMemo(
    () => [...graph.nodes].sort((a, b) => a.startOffsetMs - b.startOffsetMs),
    [graph.nodes]
  );
  const visibleNodes = isExpanded
    ? sortedNodes
    : sortedNodes.slice(0, MAX_VISIBLE_NODES);
  const visibleIds = new Set(visibleNodes.map((n) => n.id));

  const edges = graph.edges.filter(
    (edge) => visibleIds.has(edge.from) && visibleIds.has(edge.to)
  );

  const { positions, width, height } = useMemo(
    () => layoutNodes(visibleNodes),
    [visibleNodes]
  );

  if (spans.length === 0) {
    return <div className="trace-graph__empty">No spans available.</div>;
  }

  return (
    <div className="trace-graph">
      {sortedNodes.length > MAX_VISIBLE_NODES && !isExpanded && (
        <div className="trace-graph__limit">
          Showing first {MAX_VISIBLE_NODES} nodes.
          <button type="button" onClick={() => setIsExpanded(true)}>
            Expand graph
          </button>
        </div>
      )}
      <div
        className="trace-graph__canvas"
        style={{ width, height }}
      >
        <svg className="trace-graph__edges" width={width} height={height}>
          {edges.map((edge) => {
            const from = positions.get(edge.from);
            const to = positions.get(edge.to);
            if (!from || !to) return null;
            return (
              <line
                key={`${edge.from}-${edge.to}`}
                x1={from.x + 180}
                y1={from.y + 24}
                x2={to.x + 20}
                y2={to.y + 24}
                stroke="var(--border)"
                strokeWidth={1}
              />
            );
          })}
        </svg>
        {visibleNodes.map((node) => {
          const pos = positions.get(node.id);
          if (!pos) return null;
          return (
            <button
              key={node.id}
              type="button"
              className={`trace-graph__node${selectedSpanId === node.id ? " trace-graph__node--selected" : ""}${node.isError ? " trace-graph__node--error" : ""}`}
              style={{ left: pos.x, top: pos.y }}
              onClick={() => onSelect(node.id)}
            >
              <div className="trace-graph__node-title">{node.name}</div>
              <div className="trace-graph__node-meta">
                {node.serviceName || "unknown"} · {node.durationMs} ms
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
};
