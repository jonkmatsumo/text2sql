export type NumericDomain = [number, number];

export function getNumericExtent(
  values: Array<number | null | undefined>
): NumericDomain | null {
  const filtered = values.filter(
    (value): value is number => typeof value === "number" && !Number.isNaN(value)
  );

  if (!filtered.length) return null;
  return [Math.min(...filtered), Math.max(...filtered)];
}

export function linearScale(domain: NumericDomain, range: NumericDomain) {
  const [d0, d1] = domain;
  const [r0, r1] = range;
  const span = d1 - d0;

  if (span === 0) {
    const mid = (r0 + r1) / 2;
    return () => mid;
  }

  return (value: number) => r0 + ((value - d0) / span) * (r1 - r0);
}

export function toTimestamp(value: string | number | Date): number {
  if (value instanceof Date) return value.getTime();
  if (typeof value === "number") return value;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
}

export function getTimeExtent(
  values: Array<string | number | Date | null | undefined>
): NumericDomain | null {
  const timestamps = values
    .filter((value): value is string | number | Date => value != null)
    .map((value) => toTimestamp(value))
    .filter((value) => !Number.isNaN(value));

  if (!timestamps.length) return null;
  return [Math.min(...timestamps), Math.max(...timestamps)];
}

export function timeScale(domain: NumericDomain, range: NumericDomain) {
  const baseScale = linearScale(domain, range);
  return (value: string | number | Date) => baseScale(toTimestamp(value));
}

export interface BandScale {
  (value: string): number;
  bandwidth: number;
  domain: string[];
}

export function bandScale(
  domain: string[],
  range: NumericDomain,
  padding = 0.1
): BandScale {
  const [r0, r1] = range;
  const n = domain.length;
  const span = r1 - r0;
  const step = n > 0 ? span / n : 0;
  const bandwidth = step * (1 - padding);
  const offset = (step - bandwidth) / 2;

  const scale = ((value: string) => {
    const index = domain.indexOf(value);
    if (index === -1) return r0;
    return r0 + index * step + offset;
  }) as BandScale;

  scale.bandwidth = bandwidth;
  scale.domain = domain;

  return scale;
}
