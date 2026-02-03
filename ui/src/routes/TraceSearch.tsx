import React, { useEffect } from "react";
import { useOtelHealth } from "../hooks/useOtelHealth";
import { useTraceSearch } from "../hooks/useTraceSearch";
import { TraceFiltersPanel } from "../components/trace/TraceFiltersPanel";
import { TraceFacetsPanel } from "../components/trace/TraceFacetsPanel";
import { TraceResultsTable } from "../components/trace/TraceResultsTable";
import { DurationHistogram } from "../components/trace/search/DurationHistogram";

export default function TraceSearch() {
  const {
    filters,
    setFilters,
    facets,
    setFacets,
    sort,
    setSort,
    traces,
    loadTraces,
    isLoading,
    error,
    filteredTraces,
    sortedTraces,
    facetSource,
    facetSampleCount,
    facetTotalCount,
    facetMeta,
    statusCounts,
    availableStatuses,
    durationBucketCounts,
    durationHistogram,
    aggregationAsOf,
    aggregationWindow,
    activeFacetCount,
    handleClearFilters
  } = useTraceSearch();

  const { health } = useOtelHealth();
  const { isHealthy, lastError: healthError } = health;
  const isHealthLoading = false; // Context doesn't expose loading state

  const handleLoadMore = () => {
    loadTraces(true);
  };

  return (
    <div className="page">
        {/* Header */}
        <header className="hero">
          <div>
            <p className="kicker">Observability</p>
            <h1>Trace Explorer</h1>
            <p className="subtitle">
              Search and analyze request traces to understand system performance and agent behavior.
            </p>
          </div>

          {/* Health Indicator */}
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <div
              style={{
                width: "10px",
                height: "10px",
                borderRadius: "50%",
                backgroundColor: isHealthLoading
                  ? "var(--muted)"
                  : isHealthy
                  ? "var(--accent)"
                  : "#ef4444"
              }}
            />
            <span style={{ fontSize: "0.85rem", color: "var(--muted)" }}>
              {isHealthLoading
                ? "Checking Telemetry..."
                : isHealthy
                ? "Telemetry System Online"
                : "Telemetry System Unreachable"}
            </span>
          </div>
        </header>

        {healthError && (
          <div className="error-banner" style={{ marginBottom: "24px" }}>
             Telemetry Service Error: {healthError}
          </div>
        )}

        <TraceFiltersPanel
          filters={filters}
          onFiltersChange={setFilters}
          onSearch={() => loadTraces(false)}
          isLoading={isLoading}
        />

        <DurationHistogram
          traces={traces}
          bins={durationHistogram}
          scopeLabel={
            facetSource === "server"
              ? facetMeta?.isSampled || facetMeta?.isTruncated
                ? "Server sample (approximate)"
                : "Dataset-wide (server)"
              : `Subset of loaded results (${facetSampleCount})`
          }
          range={{
            min: facets.durationMinMs ?? null,
            max: facets.durationMaxMs ?? null
          }}
          onRangeChange={(next) => setFacets({ ...facets, durationMinMs: next.min, durationMaxMs: next.max })}
        />

        {facetSource === "server" && (
          <div className="trace-search__trust">
            <span>Total: {facetTotalCount.toLocaleString()} traces</span>
            {facetMeta?.isSampled && (
              <span>Sampled ({facetMeta.sampleRate ? `${Math.round(facetMeta.sampleRate * 100)}%` : "rate unknown"})</span>
            )}
            {facetMeta?.isTruncated && <span>Truncated results</span>}
            {aggregationAsOf && <span>As of {new Date(aggregationAsOf).toLocaleString()}</span>}
            {aggregationWindow?.start && aggregationWindow?.end && (
              <span>
                Window: {new Date(aggregationWindow.start).toLocaleString()} → {new Date(aggregationWindow.end).toLocaleString()}
              </span>
            )}
          </div>
        )}

        <TraceFacetsPanel
          facets={facets}
          onFacetsChange={setFacets}
          activeFacetCount={activeFacetCount}
          availableStatuses={availableStatuses}
          statusCounts={statusCounts}
          durationBucketCounts={durationBucketCounts}
          totalCount={facetTotalCount}
          facetDisclaimer={
            facetSource === "client"
              ? `Facet counts reflect only the currently loaded results (${facetSampleCount}), not the full dataset.`
              : undefined
          }
        />

        <TraceResultsTable
           traces={sortedTraces}
           isLoading={isLoading}
           error={error}
           onRetry={() => loadTraces(false)}
           sort={sort}
           onSort={(key) => setSort(prev => ({
               key,
               direction: prev.key === key && prev.direction === 'desc' ? 'asc' : 'desc'
           }))}
           onLoadMore={handleLoadMore}
           totalCount={facetTotalCount}
           filteredCount={filteredTraces.length}
           onClearFilters={handleClearFilters}
        />
    </div>
  );
}
