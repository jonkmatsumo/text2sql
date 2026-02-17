import React, { useState } from "react";
import { copyTextToClipboard } from "../../utils/observability";

interface CopyButtonProps {
  text: string;
  label?: string;
}

export const CopyButton: React.FC<CopyButtonProps> = ({ text, label }) => {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    const copied = await copyTextToClipboard(text);
    if (copied) {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    } else {
      console.error("Failed to copy!");
    }
  };

  return (
    <button
      type="button"
      className="copy-button"
      onClick={handleCopy}
      title="Copy to clipboard"
    >
      {copied ? "✓ Copied" : label || "Copy"}
    </button>
  );
};
