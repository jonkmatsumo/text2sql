import React, { useCallback, useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { listTraces } from "../api";
import { TraceSummary, ListTracesParams } from "../types";

const DEFAULT_LIMIT = 50;

type SortKey = "start_time" | "duration_ms" | "span_count" | "status";
type SortDirection = "asc" | "desc";

/** Duration bucket definitions for client-side filtering */
type DurationBucket = "all" | "fast" | "medium" | "slow" | "very_slow";

const DURATION_BUCKETS: { value: DurationBucket; label: string; min: number; max: number }[] = [
  { value: "all", label: "All durations", min: 0, max: Infinity },
  { value: "fast", label: "< 100ms", min: 0, max: 100 },
  { value: "medium", label: "100ms - 1s", min: 100, max: 1000 },
  { value: "slow", label: "1s - 10s", min: 1000, max: 10000 },
  { value: "very_slow", label: "> 10s", min: 10000, max: Infinity }
];

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
  if (s === "ok" || s === "unset") return "#10b981";
  if (s === "error") return "#ef4444";
  return "var(--muted)";
}

interface TraceFilters {
  service: string;
  traceId: string;
  startTimeGte: string;
  startTimeLte: string;
}

interface FacetFilters {
  status: string; // "all" or specific status
  durationBucket: DurationBucket;
  hasErrors: "all" | "yes" | "no";
}

interface SortState {
  key: SortKey;
  direction: SortDirection;
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
        whiteSpace: "nowrap"
      }}
      onClick={() => onSort(sortKey)}
      role="columnheader"
      aria-sort={isActive ? (currentSort.direction === "asc" ? "ascending" : "descending") : "none"}
    >
      {label}
      <SortArrow direction={isActive ? currentSort.direction : "desc"} active={isActive} />
    </th>
  );
}

/** Facet chip for quick filtering */
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
        padding: "6px 12px",
        borderRadius: "16px",
        border: active ? "1px solid var(--accent)" : "1px solid var(--border)",
        backgroundColor: active ? "var(--accent)" : "transparent",
        color: active ? "#fff" : "var(--ink)",
        fontSize: "0.85rem",
        fontWeight: 500,
        cursor: "pointer",
        display: "inline-flex",
        alignItems: "center",
        gap: "6px",
        transition: "all 0.15s"
      }}
    >
      {label}
      {count !== undefined && (
        <span
          style={{
            backgroundColor: active ? "rgba(255,255,255,0.2)" : "var(--surface-muted)",
            padding: "2px 6px",
            borderRadius: "10px",
            fontSize: "0.75rem"
          }}
        >
          {count}
        </span>
      )}
    </button>
  );
}

export default function TraceSearch() {
  const [traces, setTraces] = useState<TraceSummary[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [nextOffset, setNextOffset] = useState<number | null>(null);
  const [filters, setFilters] = useState<TraceFilters>({
    service: "",
    traceId: "",
    startTimeGte: "",
    startTimeLte: ""
  });
  const [facets, setFacets] = useState<FacetFilters>({
    status: "all",
    durationBucket: "all",
    hasErrors: "all"
  });
  const [sort, setSort] = useState<SortState>({
    key: "start_time",
    direction: "desc"
  });

  const loadTraces = useCallback(
    async (append: boolean = false) => {
      setIsLoading(true);
      setError(null);

      const params: ListTracesParams = {
        limit: DEFAULT_LIMIT,
        offset: append && nextOffset ? nextOffset : 0,
        order: "desc" // Always fetch newest first from API, sort client-side
      };

      if (filters.service.trim()) {
        params.service = filters.service.trim();
      }
      if (filters.traceId.trim()) {
        params.trace_id = filters.traceId.trim();
      }
      if (filters.startTimeGte) {
        params.start_time_gte = new Date(filters.startTimeGte).toISOString();
      }
      if (filters.startTimeLte) {
        params.start_time_lte = new Date(filters.startTimeLte).toISOString();
      }

      try {
        const result = await listTraces(params);
        if (append) {
          setTraces((prev) => [...prev, ...result.items]);
        } else {
          setTraces(result.items);
        }
        setNextOffset(result.next_offset ?? null);
      } catch (err: any) {
        setError(err.message || "Failed to load traces");
      } finally {
        setIsLoading(false);
      }
    },
    [filters, nextOffset]
  );

  useEffect(() => {
    loadTraces(false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    loadTraces(false);
  };

  const handleLoadMore = () => {
    if (nextOffset !== null) {
      loadTraces(true);
    }
  };

  const handleClearFilters = () => {
    setFilters({
      service: "",
      traceId: "",
      startTimeGte: "",
      startTimeLte: ""
    });
    setFacets({
      status: "all",
      durationBucket: "all",
      hasErrors: "all"
    });
  };

  const handleSort = (key: SortKey) => {
    setSort((prev) => ({
      key,
      direction: prev.key === key && prev.direction === "desc" ? "asc" : "desc"
    }));
  };

  // Compute available statuses from loaded traces
  const statusCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    traces.forEach((t) => {
      const s = t.status.toLowerCase();
      counts[s] = (counts[s] || 0) + 1;
    });
    return counts;
  }, [traces]);

  const availableStatuses = useMemo(() => Object.keys(statusCounts).sort(), [statusCounts]);

  // Compute duration bucket counts
  const durationBucketCounts = useMemo(() => {
    const counts: Record<DurationBucket, number> = {
      all: traces.length,
      fast: 0,
      medium: 0,
      slow: 0,
      very_slow: 0
    };
    traces.forEach((t) => {
      if (t.duration_ms < 100) counts.fast++;
      else if (t.duration_ms < 1000) counts.medium++;
      else if (t.duration_ms < 10000) counts.slow++;
      else counts.very_slow++;
    });
    return counts;
  }, [traces]);

  // Check if any trace has error_count field
  const hasErrorCountField = useMemo(
    () => traces.some((t) => t.error_count !== undefined && t.error_count !== null),
    [traces]
  );

  // Apply facet filters client-side
  const filteredTraces = useMemo(() => {
    return traces.filter((trace) => {
      // Status filter
      if (facets.status !== "all" && trace.status.toLowerCase() !== facets.status) {
        return false;
      }

      // Duration bucket filter
      if (facets.durationBucket !== "all") {
        const bucket = DURATION_BUCKETS.find((b) => b.value === facets.durationBucket);
        if (bucket && (trace.duration_ms < bucket.min || trace.duration_ms >= bucket.max)) {
          return false;
        }
      }

      // Error count filter (only if field exists)
      if (hasErrorCountField && facets.hasErrors !== "all") {
        const hasErrors = (trace.error_count ?? 0) > 0;
        if (facets.hasErrors === "yes" && !hasErrors) return false;
        if (facets.hasErrors === "no" && hasErrors) return false;
      }

      return true;
    });
  }, [traces, facets, hasErrorCountField]);

  // Client-side sorting
  const sortedTraces = useMemo(() => {
    const data = [...filteredTraces];
    const { key, direction } = sort;
    const multiplier = direction === "asc" ? 1 : -1;

    return data.sort((a, b) => {
      switch (key) {
        case "start_time":
          return multiplier * (new Date(a.start_time).getTime() - new Date(b.start_time).getTime());
        case "duration_ms":
          return multiplier * (a.duration_ms - b.duration_ms);
        case "span_count":
          return multiplier * (a.span_count - b.span_count);
        case "status":
          return multiplier * a.status.localeCompare(b.status);
        default:
          return 0;
      }
    });
  }, [filteredTraces, sort]);

  const activeFacetCount =
    (facets.status !== "all" ? 1 : 0) +
    (facets.durationBucket !== "all" ? 1 : 0) +
    (facets.hasErrors !== "all" ? 1 : 0);

  return (
    <>
      <header className="hero">
        <div>
          <p className="kicker">Observability</p>
          <h1>Trace Search</h1>
          <p className="subtitle">
            Browse and filter traces from the telemetry store.
          </p>
        </div>
      </header>

      <div style={{ display: "grid", gap: "24px" }}>
        <div className="panel">
          <h3>Filters</h3>
          <form onSubmit={handleSearch}>
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))",
                gap: "16px",
                marginTop: "12px"
              }}
            >
              <div>
                <label
                  htmlFor="filter-service"
                  style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
                >
                  Service Name
                </label>
                <input
                  id="filter-service"
                  type="text"
                  placeholder="e.g., text2sql-agent"
                  value={filters.service}
                  onChange={(e) =>
                    setFilters((prev) => ({ ...prev, service: e.target.value }))
                  }
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "8px",
                    border: "1px solid var(--border)"
                  }}
                />
              </div>

              <div>
                <label
                  htmlFor="filter-trace-id"
                  style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
                >
                  Trace ID (exact)
                </label>
                <input
                  id="filter-trace-id"
                  type="text"
                  placeholder="32 hex characters"
                  value={filters.traceId}
                  onChange={(e) =>
                    setFilters((prev) => ({ ...prev, traceId: e.target.value }))
                  }
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "8px",
                    border: "1px solid var(--border)",
                    fontFamily: "monospace"
                  }}
                />
              </div>

              <div>
                <label
                  htmlFor="filter-start-gte"
                  style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
                >
                  Start Time (from)
                </label>
                <input
                  id="filter-start-gte"
                  type="datetime-local"
                  value={filters.startTimeGte}
                  onChange={(e) =>
                    setFilters((prev) => ({ ...prev, startTimeGte: e.target.value }))
                  }
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "8px",
                    border: "1px solid var(--border)"
                  }}
                />
              </div>

              <div>
                <label
                  htmlFor="filter-start-lte"
                  style={{ display: "block", marginBottom: "6px", fontWeight: 500 }}
                >
                  Start Time (to)
                </label>
                <input
                  id="filter-start-lte"
                  type="datetime-local"
                  value={filters.startTimeLte}
                  onChange={(e) =>
                    setFilters((prev) => ({ ...prev, startTimeLte: e.target.value }))
                  }
                  style={{
                    width: "100%",
                    padding: "10px 12px",
                    borderRadius: "8px",
                    border: "1px solid var(--border)"
                  }}
                />
              </div>
            </div>

            <div style={{ display: "flex", gap: "12px", marginTop: "20px" }}>
              <button
                type="submit"
                disabled={isLoading}
                style={{
                  padding: "12px 24px",
                  borderRadius: "10px",
                  border: "none",
                  backgroundColor: "var(--accent)",
                  color: "#fff",
                  fontWeight: 600,
                  cursor: isLoading ? "wait" : "pointer"
                }}
              >
                {isLoading ? "Searching..." : "Search"}
              </button>
              <button
                type="button"
                onClick={handleClearFilters}
                style={{
                  padding: "12px 24px",
                  borderRadius: "10px",
                  border: "1px solid var(--border)",
                  backgroundColor: "transparent",
                  color: "var(--ink)",
                  fontWeight: 500,
                  cursor: "pointer"
                }}
              >
                Clear Filters
              </button>
            </div>
          </form>
        </div>

        {/* Facets panel - only show when we have traces */}
        {traces.length > 0 && (
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
                  onClick={() => setFacets({ status: "all", durationBucket: "all", hasErrors: "all" })}
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
                      count={traces.length}
                      active={facets.status === "all"}
                      onClick={() => setFacets((prev) => ({ ...prev, status: "all" }))}
                    />
                    {availableStatuses.map((status) => (
                      <FacetChip
                        key={status}
                        label={status.toUpperCase()}
                        count={statusCounts[status]}
                        active={facets.status === status}
                        onClick={() => setFacets((prev) => ({ ...prev, status }))}
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
                      onClick={() => setFacets((prev) => ({ ...prev, durationBucket: bucket.value }))}
                    />
                  ))}
                </div>
              </div>

              {/* Error count facet - only show if API provides error_count */}
              {hasErrorCountField && (
                <div>
                  <div style={{ fontSize: "0.85rem", fontWeight: 500, marginBottom: "8px", color: "var(--muted)" }}>
                    Errors
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                    <FacetChip
                      label="All"
                      active={facets.hasErrors === "all"}
                      onClick={() => setFacets((prev) => ({ ...prev, hasErrors: "all" }))}
                    />
                    <FacetChip
                      label="Has errors"
                      active={facets.hasErrors === "yes"}
                      onClick={() => setFacets((prev) => ({ ...prev, hasErrors: "yes" }))}
                    />
                    <FacetChip
                      label="No errors"
                      active={facets.hasErrors === "no"}
                      onClick={() => setFacets((prev) => ({ ...prev, hasErrors: "no" }))}
                    />
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

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
              {filteredTraces.length === traces.length
                ? `${traces.length} trace${traces.length !== 1 ? "s" : ""}`
                : `${filteredTraces.length} of ${traces.length} traces`}
            </span>
          </div>

          {error && (
            <div
              style={{
                padding: "16px",
                backgroundColor: "rgba(239, 68, 68, 0.1)",
                border: "1px solid rgba(239, 68, 68, 0.3)",
                borderRadius: "8px",
                color: "var(--error)",
                marginBottom: "16px"
              }}
            >
              {error}
            </div>
          )}

          {isLoading && traces.length === 0 && (
            <div style={{ textAlign: "center", padding: "40px", color: "var(--muted)" }}>
              Loading traces...
            </div>
          )}

          {!isLoading && traces.length === 0 && !error && (
            <div style={{ textAlign: "center", padding: "40px", color: "var(--muted)" }}>
              No traces found. Try adjusting your filters.
            </div>
          )}

          {traces.length > 0 && filteredTraces.length === 0 && (
            <div style={{ textAlign: "center", padding: "40px", color: "var(--muted)" }}>
              No traces match the selected facets. Try adjusting your facet filters.
            </div>
          )}

          {sortedTraces.length > 0 && (
            <>
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
                        onSort={handleSort}
                      />
                      <SortableHeader
                        label="Duration"
                        sortKey="duration_ms"
                        currentSort={sort}
                        onSort={handleSort}
                      />
                      <SortableHeader
                        label="Spans"
                        sortKey="span_count"
                        currentSort={sort}
                        onSort={handleSort}
                      />
                      <SortableHeader
                        label="Status"
                        sortKey="status"
                        currentSort={sort}
                        onSort={handleSort}
                      />
                    </tr>
                  </thead>
                  <tbody>
                    {sortedTraces.map((trace) => (
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
                              backgroundColor: `${getStatusColor(trace.status)}20`,
                              color: getStatusColor(trace.status)
                            }}
                          >
                            {trace.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>

              {nextOffset !== null && (
                <div style={{ textAlign: "center", marginTop: "20px" }}>
                  <button
                    onClick={handleLoadMore}
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
            </>
          )}
        </div>

        <div style={{ textAlign: "center" }}>
          <Link
            to="/admin/traces"
            style={{ color: "var(--accent)", textDecoration: "none" }}
          >
            Back to Trace Explorer
          </Link>
        </div>
      </div>
    </>
  );
}
