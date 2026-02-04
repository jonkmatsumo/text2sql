import React from "react";

interface FacetPanelProps {
  title: string;
  description?: string;
  children: React.ReactNode;
}

export const FacetPanel: React.FC<FacetPanelProps> = ({ title, description, children }) => (
  <section className="facet-panel">
    <div className="facet-panel__header">
      <div>
        <h3>{title}</h3>
        {description && <p>{description}</p>}
      </div>
    </div>
    <div className="facet-panel__body">{children}</div>
  </section>
);

interface StatusDistributionProps {
  counts: Record<string, number>;
  totalCount?: number;
}

export const StatusDistribution: React.FC<StatusDistributionProps> = ({ counts, totalCount }) => {
  const entries = Object.entries(counts)
    .filter(([_, value]) => value > 0)
    .sort((a, b) => b[1] - a[1]);
  const total = totalCount ?? Object.values(counts).reduce((sum, value) => sum + value, 0);

  if (entries.length === 0) {
    return <div className="status-distribution__empty">No status data available.</div>;
  }

  return (
    <div className="status-distribution">
      <div className="status-distribution__header">
        <h4>Status distribution</h4>
        <span className="status-distribution__meta">{total} traces</span>
      </div>
      <div className="status-distribution__rows">
        {entries.map(([status, count]) => {
          const percent = total ? Math.round((count / total) * 100) : undefined;
          return (
            <div className="status-distribution__row" key={status}>
              <span className="status-distribution__label">{status.toUpperCase()}</span>
              <div className="status-distribution__bar">
                <div
                  className="status-distribution__bar-fill"
                  style={{ width: `${percent ?? 0}%` }}
                />
              </div>
              <span className="status-distribution__value">
                {count} {percent != null ? `(${percent}%)` : ""}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
};
