import React, { useMemo, useRef, useState } from "react";

interface VirtualListProps<T> {
  items: T[];
  rowHeight: number;
  height: number;
  overscan?: number;
  renderRow: (item: T, index: number) => React.ReactNode;
}

export default function VirtualList<T>({
  items,
  rowHeight,
  height,
  overscan = 6,
  renderRow
}: VirtualListProps<T>) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [scrollTop, setScrollTop] = useState(0);

  const totalHeight = items.length * rowHeight;
  const startIndex = Math.max(0, Math.floor(scrollTop / rowHeight) - overscan);
  const endIndex = Math.min(
    items.length,
    Math.ceil((scrollTop + height) / rowHeight) + overscan
  );

  const visibleItems = useMemo(
    () => items.slice(startIndex, endIndex),
    [items, startIndex, endIndex]
  );

  return (
    <div
      ref={containerRef}
      style={{ height, overflowY: "auto", position: "relative" }}
      onScroll={(event) => setScrollTop(event.currentTarget.scrollTop)}
    >
      <div style={{ height: totalHeight, position: "relative" }}>
        {visibleItems.map((item, idx) => {
          const itemIndex = startIndex + idx;
          return (
            <div
              key={itemIndex}
              style={{
                position: "absolute",
                top: itemIndex * rowHeight,
                left: 0,
                right: 0,
                height: rowHeight
              }}
            >
              {renderRow(item, itemIndex)}
            </div>
          );
        })}
      </div>
    </div>
  );
}
