import React from "react";

interface SpanLinksListProps {
  links: any[];
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

function getLinkId(link: any) {
  const traceId = link?.trace_id || link?.traceId;
  const spanId = link?.span_id || link?.spanId;
  if (traceId && spanId) return `${traceId.slice(0, 8)}… / ${spanId.slice(0, 8)}…`;
  if (traceId) return `${traceId.slice(0, 8)}…`;
  if (spanId) return `${spanId.slice(0, 8)}…`;
  return "—";
}

export const SpanLinksList: React.FC<SpanLinksListProps> = ({ links }) => {
  return (
    <div className="trace-links-list">
      {links.map((link, index) => {
        const attributes = link?.attributes && typeof link.attributes === "object" ? link.attributes : {};
        const entries = Object.entries(attributes);
        const previewEntries = entries.slice(0, 3);

        return (
          <div className="trace-links-row" key={`link-${index}`}>
            <div className="trace-links-row__header">
              <strong>Link {index + 1}</strong>
              <span className="trace-links-row__id">{getLinkId(link)}</span>
            </div>
            <div className="trace-links-row__attrs">
              {previewEntries.length > 0 ? (
                previewEntries.map(([key, value]) => (
                  <div key={key} className="trace-links-row__attr">
                    <span>{key}</span>
                    <strong>{formatAttributeValue(value)}</strong>
                  </div>
                ))
              ) : (
                <div className="trace-links-row__attr trace-links-row__attr--empty">No attributes</div>
              )}
            </div>
            {entries.length > previewEntries.length && (
              <details className="trace-links-row__details">
                <summary>View all attributes</summary>
                <pre>{JSON.stringify(attributes, null, 2)}</pre>
              </details>
            )}
          </div>
        );
      })}

      <details className="trace-links-list__raw">
        <summary>View Raw JSON</summary>
        <pre>{JSON.stringify(links, null, 2)}</pre>
      </details>
    </div>
  );
};
