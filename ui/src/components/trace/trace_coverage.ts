export interface SpanCoverageSummary {
  loadedCount: number;
  totalCount: number | null;
  totalKnown: boolean;
  coveragePct: number | null;
  reachedMaxLimit: boolean;
}

export function computeSpanCoverage(
  loadedCount: number,
  totalCount: number | null | undefined,
  maxLimit: number
): SpanCoverageSummary {
  const safeLoaded = Math.max(0, loadedCount);
  const normalizedTotal = totalCount != null ? totalCount : null;
  const totalKnown = normalizedTotal != null && normalizedTotal > 0;
  const coveragePct = totalKnown
    ? Math.min(100, Math.round((safeLoaded / normalizedTotal) * 100))
    : null;
  const reachedMaxLimit = safeLoaded >= maxLimit;

  return {
    loadedCount: safeLoaded,
    totalCount: normalizedTotal,
    totalKnown,
    coveragePct,
    reachedMaxLimit
  };
}
