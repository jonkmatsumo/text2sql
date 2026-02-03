import React, { useImperativeHandle, useMemo, useRef, useState } from "react";

export interface VirtualListHandle {
  scrollToIndex: (index: number) => void;
}

interface VirtualListProps<T> {
  items: T[];
  rowHeight: number;
  height: number;
  overscan?: number;
  renderRow: (item: T, index: number) => React.ReactNode;
}

function VirtualListInner<T>({
  items,
  rowHeight,
  height,
  overscan = 6,
  renderRow
}: VirtualListProps<T>, ref: React.Ref<VirtualListHandle>) {
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

  useImperativeHandle(ref, () => ({
    scrollToIndex(index: number) {
      const next = Math.max(0, Math.min(index, items.length - 1));
      if (containerRef.current) {
        containerRef.current.scrollTop = next * rowHeight;
        setScrollTop(containerRef.current.scrollTop);
      }
    }
  }), [items.length, rowHeight]);

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

const VirtualList = React.forwardRef(VirtualListInner) as <T>(
  props: VirtualListProps<T> & { ref?: React.Ref<VirtualListHandle> }
) => React.ReactElement;

export default VirtualList;
