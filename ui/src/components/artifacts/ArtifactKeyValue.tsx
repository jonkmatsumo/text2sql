import React from "react";

interface ArtifactKeyValueProps {
  label: string;
  value: string | number | boolean | null | undefined;
}

export const ArtifactKeyValue: React.FC<ArtifactKeyValueProps> = ({ label, value }) => {
  if (value == null) return null;

  return (
    <div className="artifact-kv">
      <span className="artifact-kv__label">{label}</span>
      <span className="artifact-kv__value">{String(value)}</span>
    </div>
  );
};
