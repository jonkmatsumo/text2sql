type TimestampInput = number | string | Date | null | undefined;

interface FormatTimestampOptions {
  inputInSeconds?: boolean;
  style?: "time" | "datetime";
  fallback?: string;
}

interface NormalizedDecisionEvent {
  event: any;
  timestampMs: number | null;
  key: string;
}

function toTimestampMs(
  value: TimestampInput,
  inputInSeconds: boolean = false
): number | null {
  if (value == null) return null;

  if (value instanceof Date) {
    const dateMs = value.getTime();
    return Number.isFinite(dateMs) ? dateMs : null;
  }

  if (typeof value === "number" && Number.isFinite(value)) {
    if (inputInSeconds) return value * 1000;
    return value > 1e12 ? value : value * 1000;
  }

  if (typeof value === "string" && value.trim()) {
    const asNumber = Number(value);
    if (Number.isFinite(asNumber)) {
      if (inputInSeconds) return asNumber * 1000;
      return asNumber > 1e12 ? asNumber : asNumber * 1000;
    }
    const parsed = Date.parse(value);
    if (Number.isFinite(parsed)) return parsed;
  }

  return null;
}

export function formatTimestamp(
  value: TimestampInput,
  options: FormatTimestampOptions = {}
): string {
  const fallback = options.fallback ?? "—";
  const ms = toTimestampMs(value, options.inputInSeconds);
  if (ms == null) return fallback;

  const date = new Date(ms);
  if (options.style === "time") {
    return date.toLocaleTimeString();
  }
  return date.toLocaleString();
}

export function toPrettyJson(value: unknown): string {
  if (value === undefined) return "";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

export async function copyTextToClipboard(text: string): Promise<boolean> {
  if (typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
    return false;
  }
  try {
    await navigator.clipboard.writeText(text);
    return true;
  } catch {
    return false;
  }
}

export async function copyPrettyJson(value: unknown): Promise<boolean> {
  return copyTextToClipboard(toPrettyJson(value));
}

export function normalizeDecisionEvents(events: any[]): NormalizedDecisionEvent[] {
  const sorted = events
    .map((event, originalIndex) => ({
      event,
      originalIndex,
      timestampMs: toTimestampMs(event?.timestamp),
    }))
    .sort((left, right) => {
      if (left.timestampMs != null && right.timestampMs != null && left.timestampMs !== right.timestampMs) {
        return left.timestampMs - right.timestampMs;
      }
      if (left.timestampMs != null && right.timestampMs == null) return -1;
      if (left.timestampMs == null && right.timestampMs != null) return 1;
      return left.originalIndex - right.originalIndex;
    });

  const eventFingerprintCounts = new Map<string, number>();
  return sorted.map((item, index) => {
    if (typeof item.event?.id === "string" && item.event.id.trim()) {
      return { event: item.event, timestampMs: item.timestampMs, key: item.event.id };
    }
    if (typeof item.event?.event_id === "string" && item.event.event_id.trim()) {
      return { event: item.event, timestampMs: item.timestampMs, key: item.event.event_id };
    }

    const fingerprint = [
      String(item.event?.timestamp ?? ""),
      String(item.event?.node ?? ""),
      String(item.event?.decision ?? ""),
      String(item.event?.reason ?? ""),
      String(item.event?.error_category ?? ""),
      String(item.event?.retry_count ?? ""),
    ].join("|");
    const seen = eventFingerprintCounts.get(fingerprint) ?? 0;
    eventFingerprintCounts.set(fingerprint, seen + 1);
    return {
      event: item.event,
      timestampMs: item.timestampMs,
      key: `${fingerprint}|${seen}|${index}`,
    };
  });
}
