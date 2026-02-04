import React, { useMemo, useState } from "react";
import { ArtifactPanel } from "../../artifacts/ArtifactPanel";
import { VerboseArtifact } from "./verbose_step_builder";

const MAX_VISIBLE_CHARS = 4000;

function formatContent(content: unknown) {
  if (typeof content === "string") return content;
  if (content == null) return "";
  return JSON.stringify(content, null, 2);
}

export function VerboseArtifactPanel({ artifact }: { artifact: VerboseArtifact }) {
  const [expanded, setExpanded] = useState(false);
  const contentString = useMemo(() => formatContent(artifact.content), [artifact.content]);
  const isCapped = contentString.length > MAX_VISIBLE_CHARS;
  const visibleContent =
    expanded || !isCapped
      ? artifact.content
      : `${contentString.slice(0, MAX_VISIBLE_CHARS)}...`;

  return (
    <div style={{ marginBottom: "16px" }}>
      <ArtifactPanel
        title={artifact.title}
        content={visibleContent}
        payloadType={artifact.payloadType}
        size={artifact.size}
        hash={artifact.hash}
        isRedacted={artifact.isRedacted}
        isTruncated={artifact.isTruncated || isCapped}
      />
      {isCapped && !expanded && (
        <div style={{ marginTop: "-6px", marginBottom: "8px" }}>
          <button
            type="button"
            className="btn-load-blob"
            onClick={() => setExpanded(true)}
          >
            Load more
          </button>
        </div>
      )}
    </div>
  );
}
