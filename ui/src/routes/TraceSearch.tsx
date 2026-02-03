import React, { useEffect, useState } from "react";
import { useOtelHealth } from "../hooks/useOtelHealth";
import { useTraceSearch } from "../hooks/useTraceSearch";
import { TraceFiltersPanel } from "../components/trace/TraceFiltersPanel";
import { TraceFacetsPanel } from "../components/trace/TraceFacetsPanel";
import { TraceResultsTable } from "../components/trace/TraceResultsTable";
import { DurationHistogram } from "../components/trace/search/DurationHistogram";
import { useNavigate, useSearchParams } from "react-router-dom";
import { DataTrustRow } from "../components/common/DataTrustRow";
import { CopyButton } from "../components/artifacts/CopyButton";

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
    aggregationPercentiles,
    activeFacetCount,
    handleClearFilters
  } = useTraceSearch();
  const navigate = useNavigate();
  const [compareSearchParams] = useSearchParams();
  const searchUrl = typeof window !== "undefined" ? window.location.href : "";
  const [compareTarget, setCompareTarget] = useState<"left" | "right">("right");

  const { health } = useOtelHealth();
  const { isHealthy, lastError: healthError } = health;
  const isHealthLoading = false; // Context doesn't expose loading state

  const handleLoadMore = () => {
    loadTraces(true);
  };

  const handleCompareSelection = (traceId: string) => {
    const params = new URLSearchParams(compareSearchParams);
    params.set(compareTarget, traceId);
    navigate(`/admin/traces/compare?${params.toString()}`);
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

          <div className="trace-search__hero-actions">
            <CopyButton text={searchUrl} label="Copy search link" />
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

        <DataTrustRow
          scopeLabel={
            facetSource === "server"
              ? facetMeta?.isSampled || facetMeta?.isTruncated
                ? "Dataset sample (server)"
                : "Dataset-wide (server)"
              : `Loaded subset (${facetSampleCount} traces)`
          }
          totalCount={facetTotalCount}
          filteredCount={filteredTraces.length}
          isSampled={facetMeta?.isSampled ?? null}
          sampleRate={facetMeta?.sampleRate ?? null}
          isTruncated={facetMeta?.isTruncated ?? null}
          asOf={aggregationAsOf}
          windowStart={aggregationWindow?.start ?? null}
          windowEnd={aggregationWindow?.end ?? null}
        />

        <div className="trace-search__compare-target">
          <span>Compare target</span>
          <div className="trace-search__compare-buttons">
            <button
              type="button"
              className={compareTarget === "left" ? "active" : ""}
              onClick={() => setCompareTarget("left")}
            >
              Left
            </button>
            <button
              type="button"
              className={compareTarget === "right" ? "active" : ""}
              onClick={() => setCompareTarget("right")}
            >
              Right
            </button>
          </div>
        </div>

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
          percentiles={aggregationPercentiles ?? undefined}
          range={{
            min: facets.durationMinMs ?? null,
            max: facets.durationMaxMs ?? null
          }}
          onRangeChange={(next) => setFacets({ ...facets, durationMinMs: next.min, durationMaxMs: next.max })}
        />

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
           compareTarget={compareTarget}
           onSelectForCompare={handleCompareSelection}
        />
    </div>
  );
}
