import React, { useState } from "react";
import { CopyButton } from "./CopyButton";
import { ArtifactKeyValue } from "./ArtifactKeyValue";
import { fetchBlobContent } from "../../api";

interface ArtifactPanelProps {
  title: string;
  content: any;
  payloadType?: string;
  hash?: string;
  size?: number;
  blobUrl?: string;
  isRedacted?: boolean;
  onLoadFullPayload?: (content: any) => void;
}

function formatSize(bytes?: number) {
  if (!bytes) return "0 B";
  const k = 1024;
  const sizes = ["B", "KB", "MB", "GB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + " " + sizes[i];
}

export const ArtifactPanel: React.FC<ArtifactPanelProps> = ({
  title,
  content: initialContent,
  payloadType,
  hash,
  size,
  blobUrl,
  isRedacted,
  onLoadFullPayload
}) => {
  const [content, setContent] = useState(initialContent);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleLoadFullPayload = async () => {
    if (!blobUrl) return;

    setIsLoading(true);
    setError(null);
    try {
      // TODO: If blob_url is not directly fetchable due to CORS,
      // we need a backend proxy endpoint like /api/v1/proxy/blob?url=...
      const fullContent = await fetchBlobContent(blobUrl);
      setContent(fullContent);
      if (onLoadFullPayload) {
        onLoadFullPayload(fullContent);
      }
    } catch (err) {
      setError("Failed to load full payload. It might be blocked by CORS or the link has expired.");
      console.error(err);
    } finally {
      setIsLoading(false);
    }
  };

  const isLarge = size && size > 256 * 1024;
  const isJson = typeof content === "object" && content !== null;
  const contentString = isJson ? JSON.stringify(content, null, 2) : String(content || "");

  return (
    <div className="artifact-panel">
      <div className="artifact-panel__header">
        <div className="artifact-panel__title-wrap">
          <h4 className="artifact-panel__title">{title}</h4>
          <div className="artifact-panel__badges">
            {isLarge && <span className="artifact-badge artifact-badge--warning">Large Payload</span>}
            {isRedacted && <span className="artifact-badge artifact-badge--danger">Redacted</span>}
            {blobUrl && <span className="artifact-badge artifact-badge--info">Blob-backed</span>}
          </div>
        </div>
        <CopyButton text={contentString} />
      </div>

      <div className="artifact-panel__metadata">
        <ArtifactKeyValue label="Type" value={payloadType} />
        <ArtifactKeyValue label="Size" value={size ? formatSize(size) : null} />
        <ArtifactKeyValue label="Hash" value={hash ? hash.slice(0, 8) : null} />
      </div>

      <div className="artifact-panel__content">
        {content ? (
          <pre className="artifact-panel__pre">
            <code>{contentString}</code>
          </pre>
        ) : (
          <div className="artifact-panel__empty">
            {blobUrl ? "Payload is stored in a separate blob." : "No content available"}
          </div>
        )}

        {blobUrl && !content && !isLoading && (
          <div className="artifact-panel__blob-action">
            <button
              type="button"
              className="btn-load-blob"
              onClick={handleLoadFullPayload}
            >
              Load Full Payload
            </button>
          </div>
        )}

        {isLoading && (
          <div className="artifact-panel__loading">Loading full payload...</div>
        )}

        {error && (
          <div className="artifact-panel__error">
            {error}
            <div style={{ marginTop: "8px", fontSize: "0.75rem", color: "var(--muted)" }}>
              If this persists, please contact admin to ensure the telemetry proxy is configured.
            </div>
          </div>
        )}
      </div>
    </div>
  );
};
