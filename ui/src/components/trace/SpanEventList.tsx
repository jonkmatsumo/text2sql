import React from "react";

interface SpanEventListProps {
  events: any[];
  spanStartTime?: string;
}

function formatDurationMs(value: number) {
  if (Number.isNaN(value)) return "—";
  if (Math.abs(value) < 1000) return `${value.toFixed(1)} ms`;
  return `${(value / 1000).toFixed(2)} s`;
}

function formatAttributeValue(value: any) {
  if (value == null) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function getEventTimestampMs(event: any) {
  const rawTs = event?.time_unix_nano || event?.timeUnixNano;
  if (!rawTs) return null;
  const parsed = Number(rawTs);
  if (Number.isNaN(parsed)) return null;
  return parsed / 1_000_000;
}

export const SpanEventList: React.FC<SpanEventListProps> = ({ events, spanStartTime }) => {
  const spanStartMs = spanStartTime ? new Date(spanStartTime).getTime() : null;

  return (
    <div className="trace-event-list">
      {events.map((event, index) => {
        const timestampMs = getEventTimestampMs(event);
        const relativeMs = timestampMs != null && spanStartMs != null ? timestampMs - spanStartMs : null;
        const absoluteTime = timestampMs != null ? new Date(timestampMs).toLocaleString() : null;
        const attributes = event?.attributes && typeof event.attributes === "object" ? event.attributes : {};
        const entries = Object.entries(attributes);
        const previewEntries = entries.slice(0, 3);

        return (
          <div className="trace-event-row" key={`${event?.name || "event"}-${index}`}>
            <div className="trace-event-row__header">
              <div className="trace-event-row__title">
                <strong>{event?.name || "event"}</strong>
              </div>
              <div className="trace-event-row__time">
                <span>{relativeMs != null ? `+${formatDurationMs(relativeMs)}` : "—"}</span>
                {absoluteTime && <span className="trace-event-row__time-absolute">{absoluteTime}</span>}
              </div>
            </div>

            <div className="trace-event-row__attrs">
              {previewEntries.length > 0 ? (
                previewEntries.map(([key, value]) => (
                  <div key={key} className="trace-event-row__attr">
                    <span>{key}</span>
                    <strong>{formatAttributeValue(value)}</strong>
                  </div>
                ))
              ) : (
                <div className="trace-event-row__attr trace-event-row__attr--empty">No attributes</div>
              )}
            </div>

            {entries.length > previewEntries.length && (
              <details className="trace-event-row__details">
                <summary>View all attributes</summary>
                <pre>{JSON.stringify(attributes, null, 2)}</pre>
              </details>
            )}
          </div>
        );
      })}

      <details className="trace-event-list__raw">
        <summary>View Raw JSON</summary>
        <pre>{JSON.stringify(events, null, 2)}</pre>
      </details>
    </div>
  );
};
