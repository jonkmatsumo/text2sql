import React from "react";

interface TooltipItem {
  label: string;
  value: string;
}

interface TooltipProps {
  x: number;
  y: number;
  items: TooltipItem[];
  visible: boolean;
}

export function Tooltip({ x, y, items, visible }: TooltipProps) {
  if (!visible) return null;

  return (
    <div
      style={{
        position: "absolute",
        top: y,
        left: x,
        pointerEvents: "none",
        background: "rgba(17, 24, 39, 0.9)",
        color: "white",
        padding: "8px 10px",
        borderRadius: "6px",
        fontSize: "12px",
        lineHeight: 1.4,
        zIndex: 20
      }}
    >
      {items.map((item) => (
        <div key={`${item.label}-${item.value}`}>
          <strong style={{ fontWeight: 600 }}>{item.label}:</strong> {item.value}
        </div>
      ))}
    </div>
  );
}
