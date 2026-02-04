import React from "react";

interface LegendItem {
  label: string;
  color?: string;
}

interface LegendProps {
  items: LegendItem[];
}

export function Legend({ items }: LegendProps) {
  if (!items.length) return null;

  return (
    <div style={{ display: "flex", flexWrap: "wrap", gap: "12px" }}>
      {items.map((item) => (
        <div
          key={item.label}
          style={{ display: "flex", alignItems: "center", gap: "6px" }}
        >
          <span
            style={{
              width: 10,
              height: 10,
              borderRadius: "50%",
              backgroundColor: item.color || "var(--accent)"
            }}
          />
          <span style={{ fontSize: "0.85rem", color: "var(--muted)" }}>
            {item.label}
          </span>
        </div>
      ))}
    </div>
  );
}
