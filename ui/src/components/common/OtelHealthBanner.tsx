import React from "react";
import { useOtelHealth } from "../../hooks/useOtelHealth";

/**
 * Global banner that appears when the OTEL worker is unreachable.
 * Shows error details and a retry button.
 */
export default function OtelHealthBanner() {
  const { health, checkHealth } = useOtelHealth();
  const [isDismissed, setIsDismissed] = React.useState(() => {
    return sessionStorage.getItem("otel_banner_dismissed") === "true";
  });

  // Reset dismissal when health recovers, so future failures will be shown
  React.useEffect(() => {
    if (health.isHealthy) {
      setIsDismissed(false);
      sessionStorage.removeItem("otel_banner_dismissed");
    }
  }, [health.isHealthy]);

  const handleDismiss = () => {
    setIsDismissed(true);
    sessionStorage.setItem("otel_banner_dismissed", "true");
  };

  if (health.isHealthy || isDismissed) {
    return null;
  }

  return (
    <div
      role="alert"
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        zIndex: 1000,
        backgroundColor: "#fef2f2",
        borderBottom: "1px solid #fecaca",
        padding: "12px 20px",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        gap: "16px",
        boxShadow: "0 2px 4px rgba(0,0,0,0.05)"
      }}
    >
      <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
        <svg
          width="20"
          height="20"
          viewBox="0 0 24 24"
          fill="none"
          stroke="#dc2626"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
        >
          <circle cx="12" cy="12" r="10" />
          <line x1="12" y1="8" x2="12" y2="12" />
          <line x1="12" y1="16" x2="12.01" y2="16" />
        </svg>
        <span style={{ color: "#991b1b", fontWeight: 500 }}>
          OTEL Worker Unreachable
        </span>
        {health.lastError && (
          <span style={{ color: "#b91c1c", fontSize: "0.9rem" }}>
            ({health.lastError})
          </span>
        )}
      </div>
      <div style={{ display: "flex", gap: "8px" }}>
        <button
          type="button"
          onClick={() => checkHealth()}
          style={{
            padding: "6px 12px",
            borderRadius: "6px",
            border: "1px solid #dc2626",
            backgroundColor: "#fff",
            color: "#dc2626",
            fontWeight: 500,
            fontSize: "0.85rem",
            cursor: "pointer"
          }}
        >
          Retry
        </button>
        <button
          type="button"
          onClick={handleDismiss}
          style={{
            padding: "6px 12px",
            borderRadius: "6px",
            border: "none",
            backgroundColor: "transparent",
            color: "#991b1b",
            fontSize: "1.2rem",
            lineHeight: 1,
            cursor: "pointer",
            display: "flex",
            alignItems: "center"
          }}
          aria-label="Dismiss"
        >
          ✕
        </button>
      </div>
    </div>
  );
}
