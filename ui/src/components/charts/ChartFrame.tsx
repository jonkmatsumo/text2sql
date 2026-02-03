import React from "react";
import { EmptyState } from "../common/EmptyState";
import { ErrorState } from "../common/ErrorState";
import { LoadingState } from "../common/LoadingState";

interface ChartFrameMeta {
  asOf?: string;
  window?: string;
  sampled?: boolean;
  truncated?: boolean;
  missingBuckets?: boolean;
}

interface ChartFrameProps {
  title: string;
  description?: string;
  isLoading?: boolean;
  error?: unknown;
  isEmpty?: boolean;
  meta?: ChartFrameMeta;
  children: React.ReactNode;
}

function renderBody({
  isLoading,
  error,
  isEmpty,
  children,
}: Pick<ChartFrameProps, "isLoading" | "error" | "isEmpty" | "children">) {
  if (isLoading) {
    return <LoadingState message="Loading chart..." />;
  }

  if (error) {
    return <ErrorState error={error} />;
  }

  if (isEmpty) {
    return <EmptyState title="No data in this window" />;
  }

  return children;
}

export function ChartFrame({
  title,
  description,
  isLoading,
  error,
  isEmpty,
  meta,
  children,
}: ChartFrameProps) {
  const badges: { label: string; tone: "info" | "warning" | "danger" }[] = [];

  if (meta?.sampled) badges.push({ label: "Sampled", tone: "info" });
  if (meta?.truncated) badges.push({ label: "Truncated", tone: "danger" });
  if (meta?.missingBuckets) badges.push({ label: "Missing buckets", tone: "warning" });

  const showMeta =
    meta?.asOf ||
    meta?.window ||
    meta?.sampled ||
    meta?.truncated ||
    meta?.missingBuckets;

  return (
    <div className="chart-frame">
      <div className="chart-frame__header">
        <div>
          <h3 className="chart-frame__title">{title}</h3>
          {description && <p className="chart-frame__description">{description}</p>}
        </div>
      </div>
      <div className="chart-frame__body">
        {renderBody({ isLoading, error, isEmpty, children })}
      </div>
      {showMeta && (
        <div className="chart-frame__meta">
          <div className="chart-frame__meta-items">
            {meta?.window && (
              <div className="chart-frame__meta-item">
                <span>Window</span>
                <span>{meta.window}</span>
              </div>
            )}
            {meta?.asOf && (
              <div className="chart-frame__meta-item">
                <span>As of</span>
                <span>{meta.asOf}</span>
              </div>
            )}
          </div>
          {badges.length > 0 && (
            <div className="chart-frame__badges">
              {badges.map((badge) => (
                <span
                  key={badge.label}
                  className={`artifact-badge artifact-badge--${badge.tone}`}
                >
                  {badge.label}
                </span>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
