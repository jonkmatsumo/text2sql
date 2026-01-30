import React from "react";

interface LoadingStateProps {
  message?: string;
}

export function LoadingState({ message = "Loading..." }: LoadingStateProps) {
  return (
    <div className="loading" style={{ padding: "48px 0" }}>
      {message}
    </div>
  );
}
