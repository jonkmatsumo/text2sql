import React from "react";
import { CopyButton } from "./CopyButton";
import { ArtifactKeyValue } from "./ArtifactKeyValue";

interface ArtifactPanelProps {
  title: string;
  content: any;
  payloadType?: string;
  hash?: string;
  size?: number;
  blobUrl?: string;
  isRedacted?: boolean;
  onLoadFullPayload?: () => void;
  isLoadingFullPayload?: boolean;
  fullPayloadError?: string;
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
  content,
  payloadType,
  hash,
  size,
  blobUrl,
  isRedacted,
  onLoadFullPayload,
  isLoadingFullPayload,
  fullPayloadError
}) => {
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
          <div className="artifact-panel__empty">No content available</div>
        )}

        {blobUrl && !content && !isLoadingFullPayload && (
          <div className="artifact-panel__blob-action">
            <button
              type="button"
              className="btn-load-blob"
              onClick={onLoadFullPayload}
            >
              Load Full Payload
            </button>
          </div>
        )}

        {isLoadingFullPayload && (
          <div className="artifact-panel__loading">Loading full payload...</div>
        )}

        {fullPayloadError && (
          <div className="artifact-panel__error">{fullPayloadError}</div>
        )}
      </div>
    </div>
  );
};
