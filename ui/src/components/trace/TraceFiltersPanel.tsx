import React from "react";
import { TraceFilters, TIME_RANGES, getRangeValues } from "../../hooks/useTraceSearch";

interface Props {
  filters: TraceFilters;
  onFiltersChange: (f: TraceFilters) => void;
  onSearch: () => void;
  isLoading: boolean;
}

export function TraceFiltersPanel({ filters, onFiltersChange, onSearch, isLoading }: Props) {
  const handleRangeChange = (range: string) => {
    const rangeVals = getRangeValues(range);
    if (rangeVals) {
      onFiltersChange({
        ...filters,
        range,
        startTimeGte: rangeVals.start_gte,
        startTimeLte: rangeVals.start_lte,
      });
    } else {
       // Custom
       onFiltersChange({ ...filters, range: undefined });
    }
  };

  return (
    <div className="panel">
      <form
        onSubmit={(e) => {
          e.preventDefault();
          onSearch();
        }}
      >
        <div className="filter-bar" style={{ marginBottom: 0 }}>
          {/* Service */}
          <div className="filter-select">
            <span className="filter-select__label">Service</span>
            <input
              type="text"
              className="filter-select__dropdown"
              placeholder="All Services"
              value={filters.service}
              onChange={(e) => onFiltersChange({ ...filters, service: e.target.value })}
              style={{ cursor: "text", minWidth: "160px" }}
            />
          </div>

          {/* Trace ID */}
          <div className="filter-select">
            <span className="filter-select__label">Trace ID</span>
            <input
              type="text"
              className="filter-select__dropdown"
              placeholder="e.g. 523a..."
              value={filters.traceId}
              onChange={(e) => onFiltersChange({ ...filters, traceId: e.target.value })}
              style={{ cursor: "text", minWidth: "220px", fontFamily: "monospace" }}
            />
          </div>

          {/* Time Range */}
          <div className="filter-select">
            <span className="filter-select__label">Time Range</span>
            <select
              className="filter-select__dropdown"
              value={filters.range || "custom"}
              onChange={(e) => handleRangeChange(e.target.value)}
            >
              <option value="custom">Custom</option>
              {TIME_RANGES.map((r) => (
                <option key={r.value} value={r.value}>
                  Last {r.label}
                </option>
              ))}
            </select>
          </div>

          {!filters.range && (
            <>
              <div className="filter-select">
                <span className="filter-select__label">From</span>
                <input
                  type="datetime-local"
                  className="filter-select__dropdown"
                  value={filters.startTimeGte}
                  onChange={(e) => onFiltersChange({ ...filters, startTimeGte: e.target.value })}
                />
              </div>
              <div className="filter-select">
                <span className="filter-select__label">To</span>
                <input
                  type="datetime-local"
                  className="filter-select__dropdown"
                  value={filters.startTimeLte}
                  onChange={(e) => onFiltersChange({ ...filters, startTimeLte: e.target.value })}
                />
              </div>
            </>
          )}

          <div style={{ marginLeft: "auto", display: "flex", alignItems: "flex-end" }}>
            <button
              type="submit"
              disabled={isLoading}
              style={{
                background: "var(--accent)",
                color: "#fff",
                border: "none",
                padding: "10px 24px",
                borderRadius: "8px",
                fontWeight: 600,
                cursor: isLoading ? "wait" : "pointer",
              }}
            >
              {isLoading ? "Searching..." : "Search"}
            </button>
          </div>
        </div>
      </form>
    </div>
  );
}
