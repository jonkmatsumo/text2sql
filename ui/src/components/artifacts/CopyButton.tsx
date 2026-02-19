import React, { useState } from "react";
import { copyTextToClipboard } from "../../utils/observability";

interface CopyButtonProps {
  text: string;
  label?: string;
  ariaLabel?: string;
}

const SR_ONLY_STYLE: React.CSSProperties = {
  border: 0,
  clip: "rect(0 0 0 0)",
  clipPath: "inset(50%)",
  height: "1px",
  margin: "-1px",
  overflow: "hidden",
  padding: 0,
  position: "absolute",
  whiteSpace: "nowrap",
  width: "1px",
};

export const CopyButton: React.FC<CopyButtonProps> = ({ text, label, ariaLabel }) => {
  const [copied, setCopied] = useState(false);
  const [announcement, setAnnouncement] = useState("");

  const handleCopy = async () => {
    const success = await copyTextToClipboard(text);
    if (success) {
      setCopied(true);
      setAnnouncement("Copied to clipboard");
      setTimeout(() => {
        setCopied(false);
        setAnnouncement("");
      }, 2000);
    } else {
      console.error("Clipboard copy failed");
      setAnnouncement("Could not copy to clipboard");
      setTimeout(() => setAnnouncement(""), 2000);
    }
  };

  return (
    <>
      <button
        type="button"
        className="copy-button"
        onClick={handleCopy}
        title="Copy to clipboard"
        aria-label={ariaLabel || label || "Copy to clipboard"}
      >
        {copied ? "✓ Copied" : label || "Copy"}
      </button>
      <span aria-live="polite" style={SR_ONLY_STYLE}>{announcement}</span>
    </>
  );
};
