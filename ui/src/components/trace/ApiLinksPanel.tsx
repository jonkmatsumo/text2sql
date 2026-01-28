import React, { useState } from "react";
import { otelWorkerBaseUrl, grafanaBaseUrl, buildGrafanaTraceUrl } from "../../config";

interface ApiLinksPanelProps {
  traceId: string;
}

interface LinkItem {
  label: string;
  url: string;
  description: string;
}

export default function ApiLinksPanel({ traceId }: ApiLinksPanelProps) {
  const [copied, setCopied] = useState<string | null>(null);

  const links: LinkItem[] = [
    {
      label: "Trace JSON",
      url: `${otelWorkerBaseUrl}/api/v1/traces/${traceId}?include=attributes`,
      description: "Full trace detail with attributes"
    },
    {
      label: "Spans JSON",
      url: `${otelWorkerBaseUrl}/api/v1/traces/${traceId}/spans?include=attributes`,
      description: "All spans for this trace"
    }
  ];

  // Add raw blob link if available (some traces have raw export)
  links.push({
    label: "Raw Export",
    url: `${otelWorkerBaseUrl}/api/v1/traces/${traceId}/raw`,
    description: "Raw OTLP export (if available)"
  });

  const grafanaUrl = buildGrafanaTraceUrl(traceId);
  if (grafanaUrl) {
    links.push({
      label: "Grafana Tempo",
      url: grafanaUrl,
      description: "View in Grafana Tempo"
    });
  }

  const handleCopy = async (url: string, label: string) => {
    try {
      await navigator.clipboard.writeText(url);
      setCopied(label);
      setTimeout(() => setCopied(null), 2000);
    } catch {
      // Fallback for older browsers
      const textArea = document.createElement("textarea");
      textArea.value = url;
      document.body.appendChild(textArea);
      textArea.select();
      document.execCommand("copy");
      document.body.removeChild(textArea);
      setCopied(label);
      setTimeout(() => setCopied(null), 2000);
    }
  };

  return (
    <div className="trace-panel">
      <h3>API Links</h3>
      <div style={{ display: "grid", gap: "12px" }}>
        {links.map((link) => (
          <div
            key={link.label}
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "10px 12px",
              backgroundColor: "var(--surface-muted)",
              borderRadius: "8px"
            }}
          >
            <div style={{ flex: 1, minWidth: 0 }}>
              <div style={{ fontWeight: 500, marginBottom: "2px" }}>{link.label}</div>
              <div
                style={{
                  fontSize: "0.8rem",
                  color: "var(--muted)",
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap"
                }}
                title={link.url}
              >
                {link.description}
              </div>
            </div>
            <div style={{ display: "flex", gap: "8px", marginLeft: "12px" }}>
              <button
                onClick={() => handleCopy(link.url, link.label)}
                style={{
                  padding: "6px 10px",
                  borderRadius: "6px",
                  border: "1px solid var(--border)",
                  backgroundColor: "transparent",
                  color: "var(--ink)",
                  fontSize: "0.8rem",
                  cursor: "pointer",
                  minWidth: "60px"
                }}
              >
                {copied === link.label ? "Copied!" : "Copy"}
              </button>
              <a
                href={link.url}
                target="_blank"
                rel="noreferrer"
                style={{
                  padding: "6px 10px",
                  borderRadius: "6px",
                  border: "1px solid var(--border)",
                  backgroundColor: "transparent",
                  color: "var(--accent)",
                  fontSize: "0.8rem",
                  textDecoration: "none",
                  display: "flex",
                  alignItems: "center",
                  gap: "4px"
                }}
              >
                Open
                <svg
                  width="12"
                  height="12"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                >
                  <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6" />
                  <polyline points="15 3 21 3 21 9" />
                  <line x1="10" y1="14" x2="21" y2="3" />
                </svg>
              </a>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
