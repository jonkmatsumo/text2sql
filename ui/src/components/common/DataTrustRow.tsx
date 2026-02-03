import React from "react";

interface DataTrustRowProps {
  scopeLabel: string;
  totalCount?: number | null;
  filteredCount?: number | null;
  isSampled?: boolean | null;
  sampleRate?: number | null;
  isTruncated?: boolean | null;
  asOf?: string | null;
  windowStart?: string | null;
  windowEnd?: string | null;
}

function formatNumber(value?: number | null) {
  if (value == null) return "Unknown";
  return value.toLocaleString();
}

function formatTimestamp(value?: string | null) {
  if (!value) return "Unknown";
  try {
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "Unknown";
    return parsed.toLocaleString();
  } catch {
    return "Unknown";
  }
}

export const DataTrustRow: React.FC<DataTrustRowProps> = ({
  scopeLabel,
  totalCount,
  filteredCount,
  isSampled,
  sampleRate,
  isTruncated,
  asOf,
  windowStart,
  windowEnd
}) => {
  const sampleText =
    isSampled == null
      ? "Unknown"
      : isSampled
      ? `Sampled (${sampleRate != null ? `${Math.round(sampleRate * 100)}%` : "rate unknown"})`
      : "Full dataset";
  const truncText =
    isTruncated == null ? "Unknown" : isTruncated ? "Truncated" : "Complete";

  return (
    <div className="data-trust-row">
      <div className="data-trust-row__scope">{scopeLabel}</div>
      <div className="data-trust-row__items">
        <div>
          <span className="data-trust-row__label">Total (dataset)</span>
          <strong>{formatNumber(totalCount)}</strong>
        </div>
        <div>
          <span className="data-trust-row__label">Filtered / loaded</span>
          <strong>{formatNumber(filteredCount)}</strong>
        </div>
        <div>
          <span className="data-trust-row__label">Sampling</span>
          <strong>{sampleText}</strong>
        </div>
        <div>
          <span className="data-trust-row__label">Truncation</span>
          <strong>{truncText}</strong>
        </div>
        <div>
          <span className="data-trust-row__label">As of</span>
          <strong>{formatTimestamp(asOf)}</strong>
        </div>
        <div>
          <span className="data-trust-row__label">Window</span>
          <strong>
            {windowStart && windowEnd
              ? `${formatTimestamp(windowStart)} → ${formatTimestamp(windowEnd)}`
              : "Unknown"}
          </strong>
        </div>
      </div>
    </div>
  );
};
