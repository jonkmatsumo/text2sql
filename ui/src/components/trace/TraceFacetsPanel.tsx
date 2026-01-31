import React from "react";
import { FacetFilters, DURATION_BUCKETS, DurationBucket } from "../../hooks/useTraceSearch";

interface Props {
  facets: FacetFilters;
  onFacetsChange: (f: FacetFilters) => void;
  activeFacetCount: number;
  availableStatuses: string[];
  statusCounts: Record<string, number>;
  durationBucketCounts: Record<string, number>;
  totalCount: number;
  hasErrorCountField?: boolean; // Assume true for now or pass it
}

function FacetChip({
  label,
  count,
  active,
  onClick
}: {
  label: string;
  count?: number;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        display: "flex",
        alignItems: "center",
        gap: "6px",
        padding: "6px 12px",
        borderRadius: "999px",
        border: active ? "1px solid var(--accent)" : "1px solid var(--border)",
        backgroundColor: active ? "var(--accent)" : "var(--surface)",
        color: active ? "#fff" : "var(--ink)",
        fontSize: "0.85rem",
        cursor: "pointer",
        transition: "all 0.1s"
      }}
    >
      <span>{label}</span>
      {count !== undefined && (
        <span
          style={{
            fontSize: "0.75rem",
            opacity: active ? 0.9 : 0.6,
            backgroundColor: active ? "rgba(255,255,255,0.2)" : "var(--surface-muted)",
            padding: "2px 6px",
            borderRadius: "999px"
          }}
        >
          {count}
        </span>
      )}
    </button>
  );
}

export function TraceFacetsPanel({
  facets,
  onFacetsChange,
  activeFacetCount,
  availableStatuses,
  statusCounts,
  durationBucketCounts,
  totalCount,
  hasErrorCountField = true
}: Props) {

  if (totalCount === 0) return null;

  return (
    <div className="panel">
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "12px" }}>
        <h3 style={{ margin: 0 }}>
          Facets
          {activeFacetCount > 0 && (
            <span style={{ marginLeft: "8px", fontSize: "0.85rem", color: "var(--accent)" }}>
              ({activeFacetCount} active)
            </span>
          )}
        </h3>
        {activeFacetCount > 0 && (
          <button
            type="button"
            onClick={() => onFacetsChange({ status: "all", durationBucket: "all", hasErrors: "all" })}
            style={{
              padding: "4px 12px",
              borderRadius: "6px",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "var(--muted)",
              fontSize: "0.85rem",
              cursor: "pointer"
            }}
          >
            Clear facets
          </button>
        )}
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: "16px" }}>
        {/* Status facet */}
        {availableStatuses.length > 0 && (
          <div>
            <div style={{ fontSize: "0.85rem", fontWeight: 500, marginBottom: "8px", color: "var(--muted)" }}>
              Status
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
              <FacetChip
                label="All"
                count={totalCount}
                active={facets.status === "all"}
                onClick={() => onFacetsChange({ ...facets, status: "all" })}
              />
              {availableStatuses.map((status) => (
                <FacetChip
                  key={status}
                  label={status.toUpperCase()}
                  count={statusCounts[status]}
                  active={facets.status === status}
                  onClick={() => onFacetsChange({ ...facets, status })}
                />
              ))}
            </div>
          </div>
        )}

        {/* Duration bucket facet */}
        <div>
          <div style={{ fontSize: "0.85rem", fontWeight: 500, marginBottom: "8px", color: "var(--muted)" }}>
            Duration
          </div>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
            {DURATION_BUCKETS.map((bucket) => (
              <FacetChip
                key={bucket.value}
                label={bucket.label}
                count={durationBucketCounts[bucket.value]}
                active={facets.durationBucket === bucket.value}
                onClick={() => onFacetsChange({ ...facets, durationBucket: bucket.value })}
              />
            ))}
          </div>
        </div>

        {/* Error count facet */}
        {hasErrorCountField && (
          <div>
            <div style={{ fontSize: "0.85rem", fontWeight: 500, marginBottom: "8px", color: "var(--muted)" }}>
              Errors
            </div>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
              <FacetChip
                label="All"
                active={facets.hasErrors === "all"}
                onClick={() => onFacetsChange({ ...facets, hasErrors: "all" })}
              />
              <FacetChip
                label="Has errors"
                active={facets.hasErrors === "yes"}
                onClick={() => onFacetsChange({ ...facets, hasErrors: "yes" })}
              />
              <FacetChip
                label="No errors"
                active={facets.hasErrors === "no"}
                onClick={() => onFacetsChange({ ...facets, hasErrors: "no" })}
              />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
