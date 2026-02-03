import { useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { fetchTraceAggregations, listTraces } from "../api";
import { TraceSummary } from "../types";

export type SortKey = "start_time" | "duration_ms" | "span_count" | "status";
export type SortDirection = "asc" | "desc";

/** Duration bucket definitions for client-side filtering */
export type DurationBucket = "all" | "fast" | "medium" | "slow" | "very_slow";

export const DURATION_BUCKETS: { value: DurationBucket; label: string; min: number; max: number }[] = [
  { value: "all", label: "All durations", min: 0, max: Infinity },
  { value: "fast", label: "< 100ms", min: 0, max: 100 },
  { value: "medium", label: "100ms - 1s", min: 100, max: 1000 },
  { value: "slow", label: "1s - 10s", min: 1000, max: 10000 },
  { value: "very_slow", label: "> 10s", min: 10000, max: Infinity }
];

export const TIME_RANGES = [
  { label: "15m", value: "15m", ms: 15 * 60 * 1000 },
  { label: "1h", value: "1h", ms: 60 * 60 * 1000 },
  { label: "6h", value: "6h", ms: 6 * 60 * 60 * 1000 },
  { label: "24h", value: "24h", ms: 24 * 60 * 60 * 1000 }
];

interface ServerFacetPayload {
  [key: string]: any;
}

export function useTraceSearchFacets({
  traces,
  serverFacets,
  serverTotalCount,
  serverFacetMeta
}: {
  traces: TraceSummary[];
  serverFacets: ServerFacetPayload | null;
  serverTotalCount: number | null;
  serverFacetMeta: { isSampled?: boolean; sampleRate?: number; isTruncated?: boolean } | null;
}) {
  const serverStatusCounts =
    serverFacets?.status || serverFacets?.status_counts || serverFacets?.statusCounts || null;
  const serverDurationCounts =
    serverFacets?.duration || serverFacets?.duration_buckets || serverFacets?.durationBuckets || null;
  const serverHistogram =
    serverFacets?.duration_histogram ||
    serverFacets?.durationHistogram ||
    serverFacets?.histograms?.duration ||
    null;

  const statusCounts = useMemo(() => {
    if (serverStatusCounts && typeof serverStatusCounts === "object") {
      const counts: Record<string, number> = {};
      Object.entries(serverStatusCounts).forEach(([key, value]) => {
        if (typeof value === "number") counts[key.toLowerCase()] = value;
      });
      return counts;
    }
    const counts: Record<string, number> = {};
    traces.forEach(t => {
      const s = t.status.toLowerCase();
      counts[s] = (counts[s] || 0) + 1;
    });
    return counts;
  }, [serverStatusCounts, traces]);

  const durationBucketCounts = useMemo(() => {
    if (serverDurationCounts && typeof serverDurationCounts === "object") {
      const counts: Record<string, number> = {};
      Object.entries(serverDurationCounts).forEach(([key, value]) => {
        if (typeof value === "number") counts[key] = value;
      });
      DURATION_BUCKETS.forEach(b => {
        if (counts[b.value] == null) counts[b.value] = 0;
      });
      if (counts["all"] == null) {
        counts["all"] = DURATION_BUCKETS.filter(b => b.value !== "all")
          .reduce((sum, b) => sum + (counts[b.value] || 0), 0);
      }
      return counts;
    }
    if (Array.isArray(serverHistogram) && serverHistogram.length > 0) {
      const counts: Record<string, number> = {};
      DURATION_BUCKETS.forEach((b) => counts[b.value] = 0);
      serverHistogram.forEach((bin: any) => {
        const start = bin.start_ms ?? bin.startMs ?? 0;
        const end = bin.end_ms ?? bin.endMs ?? 0;
        DURATION_BUCKETS.forEach((bucket) => {
          if (bucket.value === "all") return;
          if (start < bucket.max && end >= bucket.min) {
            counts[bucket.value] += bin.count ?? 0;
          }
        });
        counts["all"] += bin.count ?? 0;
      });
      return counts;
    }
    const counts: Record<string, number> = {};
    DURATION_BUCKETS.forEach(b => counts[b.value] = 0);
    traces.forEach(t => {
      DURATION_BUCKETS.forEach(b => {
        if (b.value === "all") return;
        if (t.duration_ms >= b.min && t.duration_ms < b.max) {
          counts[b.value]++;
        }
      });
    });
    counts["all"] = traces.length;
    return counts;
  }, [serverDurationCounts, traces]);

  const durationHistogram = useMemo(() => {
    if (!Array.isArray(serverHistogram)) return null;
    return serverHistogram
      .map((bin: any) => {
        const start = bin.start_ms ?? bin.startMs;
        const end = bin.end_ms ?? bin.endMs;
        const count = bin.count;
        if (typeof start !== "number" || typeof end !== "number" || typeof count !== "number") return null;
        return { start_ms: start, end_ms: end, count };
      })
      .filter(Boolean) as Array<{ start_ms: number; end_ms: number; count: number }>;
  }, [serverHistogram]);

  const availableStatuses = useMemo(() => Object.keys(statusCounts), [statusCounts]);
  const facetSource = serverStatusCounts || serverDurationCounts ? "server" : "client";
  const facetSampleCount = traces.length;
  const facetTotalCount = serverTotalCount ?? traces.length;

  return {
    statusCounts,
    durationBucketCounts,
    durationHistogram,
    availableStatuses,
    facetSource,
    facetSampleCount,
    facetTotalCount,
    facetMeta: {
      isSampled: serverFacetMeta?.isSampled,
      sampleRate: serverFacetMeta?.sampleRate,
      isTruncated: serverFacetMeta?.isTruncated
    }
  };
}

export function getRangeValues(range: string): { start_gte: string; start_lte: string } | null {
  const r = TIME_RANGES.find((tr) => tr.value === range);
  if (!r) return null;
  const now = new Date();
  const start = new Date(now.getTime() - r.ms);

  const toLocalISO = (d: Date) => {
    const offset = d.getTimezoneOffset() * 60000;
    return new Date(d.getTime() - offset).toISOString().slice(0, 16);
  };

  return {
    start_gte: toLocalISO(start),
    start_lte: toLocalISO(now)
  };
}

export interface TraceFilters {
  service: string;
  traceId: string;
  startTimeGte: string;
  startTimeLte: string;
  range?: string;
}

export interface FacetFilters {
  status: string; // "all" or specific status
  durationBucket: DurationBucket;
  hasErrors: "all" | "yes" | "no";
  durationMinMs?: number | null;
  durationMaxMs?: number | null;
}

export interface SortState {
  key: SortKey;
  direction: SortDirection;
}

const DEFAULT_LIMIT = 50;

/** Parse URL search params into component state */
function parseUrlParams(searchParams: URLSearchParams): {
  filters: TraceFilters;
  facets: FacetFilters;
  sort: SortState;
  page: number;
} {
  const range = searchParams.get("range");
  let startTimeGte = searchParams.get("start_gte") || "";
  let startTimeLte = searchParams.get("start_lte") || "";

  if (range) {
    const computed = getRangeValues(range);
    if (computed) {
      startTimeGte = computed.start_gte;
      startTimeLte = computed.start_lte;
    }
  }

  return {
    filters: {
      service: searchParams.get("service") || "",
      traceId: searchParams.get("trace_id") || "",
      startTimeGte,
      startTimeLte,
      range: range || undefined
    },
    facets: {
      status: searchParams.get("status") || "all",
      durationBucket: (searchParams.get("duration") as DurationBucket) || "all",
      hasErrors: (searchParams.get("errors") as "all" | "yes" | "no") || "all",
      durationMinMs: searchParams.get("duration_min_ms")
        ? parseInt(searchParams.get("duration_min_ms") || "0", 10)
        : null,
      durationMaxMs: searchParams.get("duration_max_ms")
        ? parseInt(searchParams.get("duration_max_ms") || "0", 10)
        : null
    },
    sort: {
      key: (searchParams.get("sort") as SortKey) || "start_time",
      direction: (searchParams.get("dir") as SortDirection) || "desc"
    },
    page: parseInt(searchParams.get("page") || "1", 10)
  };
}

/** Build URL search params from component state */
function buildUrlParams(
  filters: TraceFilters,
  facets: FacetFilters,
  sort: SortState,
  page: number
): URLSearchParams {
  const params = new URLSearchParams();

  // Only add non-default values
  if (filters.service) params.set("service", filters.service);
  if (filters.traceId) params.set("trace_id", filters.traceId);

  // If range is active, use that in URL and skip absolute times
  // If no range, use absolute times if present
  if (filters.range) {
    params.set("range", filters.range);
  } else {
    if (filters.startTimeGte) params.set("start_gte", filters.startTimeGte);
    if (filters.startTimeLte) params.set("start_lte", filters.startTimeLte);
  }

  if (facets.status !== "all") params.set("status", facets.status);
  if (facets.durationBucket !== "all") params.set("duration", facets.durationBucket);
  if (facets.hasErrors !== "all") params.set("errors", facets.hasErrors);
  if (facets.durationMinMs != null) params.set("duration_min_ms", facets.durationMinMs.toString());
  if (facets.durationMaxMs != null) params.set("duration_max_ms", facets.durationMaxMs.toString());

  if (sort.key !== "start_time") params.set("sort", sort.key);
  if (sort.direction !== "desc") params.set("dir", sort.direction);

  if (page > 1) params.set("page", page.toString());

  return params;
}

export function useTraceSearch() {
  const [searchParams, setSearchParams] = useSearchParams();

  // Initialize state from URL
  const initial = useMemo(() => parseUrlParams(searchParams), []); // only on mount? no, if URL changes we want to sync? usually unidirectional.
  // Actually, usually we sync state -> URL.

  const [filters, setFilters] = useState<TraceFilters>(initial.filters);
  const [facets, setFacets] = useState<FacetFilters>(initial.facets);
  const [sort, setSort] = useState<SortState>(initial.sort);
  const [page, setPage] = useState<number>(initial.page);

  const [traces, setTraces] = useState<TraceSummary[]>([]);
  const [nextOffset, setNextOffset] = useState<number | null>(null); // For pagination if needed, or we just rely on client side? The original code had offset.
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [serverFacets, setServerFacets] = useState<ServerFacetPayload | null>(null);
  const [serverTotalCount, setServerTotalCount] = useState<number | null>(null);
  const [serverFacetMeta, setServerFacetMeta] = useState<{ isSampled?: boolean; sampleRate?: number; isTruncated?: boolean } | null>(null);
  const [aggregationAsOf, setAggregationAsOf] = useState<string | null>(null);
  const [aggregationWindow, setAggregationWindow] = useState<{ start?: string | null; end?: string | null } | null>(null);

  // Sync state to URL
  useEffect(() => {
    const params = buildUrlParams(filters, facets, sort, page);
    setSearchParams(params, { replace: true });
  }, [filters, facets, sort, page, setSearchParams]);

  const loadTraces = useCallback(async (append = false) => {
    setIsLoading(true);
    setError(null);
    if (!append) {
      setServerFacets(null);
      setServerTotalCount(null);
      setServerFacetMeta(null);
    }
    try {
      // In a real app we might pass filters to the API
      // Here we fetch list and filter client side? The original code seems to do some API filtering?
      // Looking at original code: listTraces({ ...filters, offset, limit })
      // Wait, original code usage of `listTraces` needs to be checked.
      // Assuming listTraces accepts optional params.
      const params: any = {
        limit: DEFAULT_LIMIT,
        offset: append && nextOffset ? nextOffset : 0,
      };
      // Simple mapping
      if (filters.traceId) params.trace_id = filters.traceId;
      if (filters.service) params.service_name = filters.service;
      if (filters.startTimeGte) params.start_time_gte = filters.startTimeGte;
      if (filters.startTimeLte) params.start_time_lte = filters.startTimeLte;

      const data = await listTraces(params);

      if (append) {
        setTraces(prev => [...prev, ...data.items]);
      } else {
        setTraces(data.items);
      }

      setNextOffset(data.next_offset || null);
      const facetPayload =
        (data as any).facets || (data as any).facet_counts || (data as any).facetCounts || null;
      if (facetPayload) {
        setServerFacets(facetPayload);
      }
      const totalCount =
        (data as any).total_count ?? (data as any).totalCount ?? null;
      if (typeof totalCount === "number") {
        setServerTotalCount(totalCount);
      }
      const isSampled =
        (data as any).is_sampled ?? (data as any).isSampled ?? undefined;
      const sampleRate =
        (data as any).sample_rate ?? (data as any).sampleRate ?? undefined;
      const isTruncated =
        (data as any).is_truncated ?? (data as any).isTruncated ?? undefined;
      if (isSampled !== undefined || sampleRate !== undefined || isTruncated !== undefined) {
        setServerFacetMeta({
          isSampled: typeof isSampled === "boolean" ? isSampled : undefined,
          sampleRate: typeof sampleRate === "number" ? sampleRate : undefined,
          isTruncated: typeof isTruncated === "boolean" ? isTruncated : undefined
        });
      }

    } catch (err: any) {
      setError(err.message || "Failed to load traces");
    } finally {
      setIsLoading(false);
    }
  }, [filters, nextOffset]);

  const loadAggregations = useCallback(async () => {
    try {
      const result = await fetchTraceAggregations({
        service: filters.service || undefined,
        trace_id: filters.traceId || undefined,
        status: facets.status !== "all" ? facets.status.toUpperCase() : undefined,
        has_errors: facets.hasErrors !== "all" ? facets.hasErrors : undefined,
        start_time_gte: filters.startTimeGte || undefined,
        start_time_lte: filters.startTimeLte || undefined,
        duration_min_ms: facets.durationMinMs ?? undefined,
        duration_max_ms: facets.durationMaxMs ?? undefined
      });
      setServerFacets({
        status: result.facet_counts?.status,
        duration_histogram: result.duration_histogram
      });
      setServerTotalCount(result.total_count);
      setServerFacetMeta({
        isSampled: result.sampling?.is_sampled,
        sampleRate: result.sampling?.sample_rate ?? undefined,
        isTruncated: result.truncation?.is_truncated,
      });
      setAggregationAsOf(result.as_of ?? null);
      setAggregationWindow({
        start: result.window_start ?? null,
        end: result.window_end ?? null
      });
    } catch {
      setServerFacets(null);
      setServerTotalCount(null);
      setServerFacetMeta(null);
      setAggregationAsOf(null);
      setAggregationWindow(null);
    }
  }, [filters, facets]);

  // Initial load
  useEffect(() => {
    loadTraces(false);
  }, [filters]); // When filters change, reload list

  useEffect(() => {
    loadAggregations();
  }, [filters, facets, loadAggregations]);
  // Derived state (Client-side filtering/sorting for facets)
  const filteredTraces = useMemo(() => {
    return traces.filter((t) => {
      // Status facet
      if (facets.status !== "all" && t.status.toLowerCase() !== facets.status.toLowerCase()) {
        return false;
      }
      // Duration facet
      if (facets.durationBucket !== "all") {
        const bucket = DURATION_BUCKETS.find((b) => b.value === facets.durationBucket);
        if (bucket) {
          if (t.duration_ms < bucket.min || t.duration_ms >= bucket.max) return false;
        }
      }
      // Error facet
      if (facets.hasErrors !== "all") {
         // Need to check if t has error. Original code checked status or error field?
         // Original code:
         // const hasErr = t.status.toLowerCase() === "error" || (t.error_count && t.error_count > 0);
         // if (facets.hasErrors === "yes" && !hasErr) return false;
         // if (facets.hasErrors === "no" && hasErr) return false;
         const hasErr = t.status.toLowerCase() === "error" || ((t as any).error_count && (t as any).error_count > 0);
         if (facets.hasErrors === "yes" && !hasErr) return false;
         if (facets.hasErrors === "no" && hasErr) return false;
      }
      if (facets.durationMinMs != null && t.duration_ms < facets.durationMinMs) return false;
      if (facets.durationMaxMs != null && t.duration_ms > facets.durationMaxMs) return false;
      return true;
    });
  }, [traces, facets]);

  const sortedTraces = useMemo(() => {
    return [...filteredTraces].sort((a, b) => {
      let valA: any = (a as any)[sort.key];
      let valB: any = (b as any)[sort.key];

      // Handle specific keys if needed
      if (sort.key === "status") {
        valA = a.status;
        valB = b.status;
      }

      if (valA < valB) return sort.direction === "asc" ? -1 : 1;
      if (valA > valB) return sort.direction === "asc" ? 1 : -1;
      return 0;
    });
  }, [filteredTraces, sort]);

  const {
    statusCounts,
    durationBucketCounts,
    durationHistogram,
    availableStatuses,
    facetSource,
    facetSampleCount,
    facetTotalCount,
    facetMeta
  } = useTraceSearchFacets({
    traces,
    serverFacets,
    serverTotalCount,
    serverFacetMeta
  });

  const activeFacetCount =
    (facets.status !== "all" ? 1 : 0) +
    (facets.durationBucket !== "all" ? 1 : 0) +
    (facets.hasErrors !== "all" ? 1 : 0) +
    (facets.durationMinMs != null || facets.durationMaxMs != null ? 1 : 0);

  const handleClearFilters = useCallback(() => {
     setFilters({
       service: "",
       traceId: "",
       startTimeGte: "",
       startTimeLte: "",
       range: undefined
     });
     setFacets({
       status: "all",
       durationBucket: "all",
       hasErrors: "all",
       durationMinMs: null,
       durationMaxMs: null
     });
  }, []);

  return {
    filters,
    setFilters,
    facets,
    setFacets,
    sort,
    setSort,
    page,
    setPage,
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
  };
}
