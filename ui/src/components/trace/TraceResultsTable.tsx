import React from "react";
import { Link } from "react-router-dom";
import { TraceSummary } from "../../types";
import { SortState, SortKey, SortDirection } from "../../hooks/useTraceSearch";
import { EmptyState } from "../common/EmptyState";
import { LoadingState } from "../common/LoadingState";
import { ErrorState } from "../common/ErrorState";

interface Props {
  traces: TraceSummary[];
  isLoading: boolean;
  error: string | null;
  onRetry: () => void;
  sort: SortState;
  onSort: (key: SortKey) => void;
  onLoadMore?: () => void;
  hasMore?: boolean;
  totalCount: number;
  filteredCount: number;
  onClearFilters: () => void;
  compareTarget?: "left" | "right";
  onSelectForCompare?: (traceId: string) => void;
}

// Helpers
function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(2)}s`;
  return `${(ms / 60000).toFixed(2)}m`;
}

function formatDateTime(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString();
  } catch {
    return iso;
  }
}

function getStatusColor(status: string): string {
  const s = status.toLowerCase();
  if (s === "ok" || s === "unset") return "var(--success-text)";
  if (s === "error") return "var(--error-text)";
  return "var(--muted)";
}

function getStatusBg(status: string): string {
  const s = status.toLowerCase();
  if (s === "ok" || s === "unset") return "var(--success-bg)";
  if (s === "error") return "var(--error-bg)";
  return "var(--surface-muted)";
}


/** Sort indicator arrow component */
function SortArrow({ direction, active }: { direction: SortDirection; active: boolean }) {
  return (
    <span
      style={{
        marginLeft: "4px",
        opacity: active ? 1 : 0.3,
        display: "inline-block",
        transition: "opacity 0.15s"
      }}
    >
      {direction === "asc" ? "▲" : "▼"}
    </span>
  );
}

/** Sortable column header component */
function SortableHeader({
  label,
  sortKey,
  currentSort,
  onSort
}: {
  label: string;
  sortKey: SortKey;
  currentSort: SortState;
  onSort: (key: SortKey) => void;
}) {
  const isActive = currentSort.key === sortKey;
  return (
    <th
      style={{
        padding: "12px 8px",
        fontWeight: 600,
        cursor: "pointer",
        userSelect: "none",
        borderBottom: "1px solid var(--border)",
        textAlign: "left"
      }}
      onClick={() => onSort(sortKey)}
    >
      {label}
      <SortArrow direction={currentSort.direction} active={isActive} />
    </th>
  );
}

export function TraceResultsTable({
  traces,
  isLoading,
  error,
  onRetry,
  sort,
  onSort,
  onLoadMore,
  totalCount,
  filteredCount,
  onClearFilters,
  compareTarget,
  onSelectForCompare
}: Props) {

  // Logic to show/hide different states
  // Error
  if (error) {
     return <ErrorState error={error} onRetry={onRetry} />;
  }

  // Loading initial
  if (isLoading && totalCount === 0) {
      return <LoadingState message="Loading traces..." />;
  }

  // Empty initial
  if (!isLoading && totalCount === 0) {
      return (
          <EmptyState
            title="No traces found"
            description="No traces match your current filters. Try adjusting your search criteria or time range."
            action={
                <button
                type="button"
                onClick={onClearFilters}
                style={{
                  padding: "10px 20px",
                  borderRadius: "8px",
                  border: "1px solid var(--border)",
                  backgroundColor: "transparent",
                  color: "var(--ink)",
                  fontWeight: 500,
                  cursor: "pointer"
                }}
              >
                Clear all filters
              </button>
            }
          />
      );
  }

  // Filtered empty
  if (traces.length === 0 && totalCount > 0) {
      return (
        <div className="panel">
            <div style={{ textAlign: "center", padding: "40px", color: "var(--muted)" }}>
              No traces match the selected facets. Try adjusting your facet filters.
            </div>
        </div>
      );
  }

  return (
    <div className="panel">
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          marginBottom: "16px"
        }}
      >
        <h3 style={{ margin: 0 }}>Results</h3>
        <span style={{ color: "var(--muted)", fontSize: "0.9rem" }}>
          {traces.length === totalCount
            ? `${totalCount} trace${totalCount !== 1 ? "s" : ""}`
            : `${traces.length} of ${totalCount} traces`}
        </span>
      </div>

      <div style={{ overflowX: "auto" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr
              style={{
                borderBottom: "1px solid var(--border)",
                textAlign: "left"
              }}
            >
              <th style={{ padding: "12px 8px", fontWeight: 600 }}>Trace ID</th>
              <th style={{ padding: "12px 8px", fontWeight: 600 }}>Service</th>
              <SortableHeader
                label="Start Time"
                sortKey="start_time"
                currentSort={sort}
                onSort={onSort}
              />
              <SortableHeader
                label="Duration"
                sortKey="duration_ms"
                currentSort={sort}
                onSort={onSort}
              />
              <SortableHeader
                label="Spans"
                sortKey="span_count"
                currentSort={sort}
                onSort={onSort}
              />
              <SortableHeader
                label="Status"
                sortKey="status"
                currentSort={sort}
                onSort={onSort}
              />
              {onSelectForCompare && (
                <th style={{ padding: "12px 8px", fontWeight: 600 }}>Compare</th>
              )}
            </tr>
          </thead>
          <tbody>
            {traces.map((trace) => (
              <tr
                key={trace.trace_id}
                style={{ borderBottom: "1px solid var(--border)" }}
              >
                <td style={{ padding: "12px 8px" }}>
                  <Link
                    to={`/traces/${trace.trace_id}`}
                    style={{
                      fontFamily: "monospace",
                      fontSize: "0.85rem",
                      color: "var(--accent)"
                    }}
                  >
                    {trace.trace_id.slice(0, 16)}...
                  </Link>
                </td>
                <td style={{ padding: "12px 8px" }}>{trace.service_name}</td>
                <td style={{ padding: "12px 8px", fontSize: "0.9rem" }}>
                  {formatDateTime(trace.start_time)}
                </td>
                <td style={{ padding: "12px 8px", fontFamily: "monospace" }}>
                  {formatDuration(trace.duration_ms)}
                </td>
                <td style={{ padding: "12px 8px" }}>{trace.span_count}</td>
                <td style={{ padding: "12px 8px" }}>
                  <span
                    style={{
                      display: "inline-block",
                      padding: "4px 8px",
                      borderRadius: "4px",
                      fontSize: "0.8rem",
                      fontWeight: 500,
                      backgroundColor: getStatusBg(trace.status),
                      color: getStatusColor(trace.status)
                    }}
                  >
                    {trace.status}
                  </span>
                </td>
                {onSelectForCompare && (
                  <td style={{ padding: "12px 8px" }}>
                    <button
                      type="button"
                      onClick={() => onSelectForCompare(trace.trace_id)}
                      style={{
                        padding: "4px 8px",
                        borderRadius: "8px",
                        border: "1px solid var(--border)",
                        background: "#fff",
                        fontSize: "0.75rem",
                        cursor: "pointer"
                      }}
                    >
                      Compare as {compareTarget ?? "right"}
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {onLoadMore && (
        <div style={{ textAlign: "center", marginTop: "20px" }}>
          <button
            onClick={onLoadMore}
            disabled={isLoading}
            style={{
              padding: "12px 24px",
              borderRadius: "10px",
              border: "1px solid var(--border)",
              backgroundColor: "transparent",
              color: "var(--ink)",
              fontWeight: 500,
              cursor: isLoading ? "wait" : "pointer"
            }}
          >
            {isLoading ? "Loading..." : "Load More"}
          </button>
        </div>
      )}
    </div>
  );
}
