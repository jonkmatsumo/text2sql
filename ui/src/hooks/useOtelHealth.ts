import { useContext } from "react";
import { OtelHealthContext, OtelHealthContextValue } from "../context/OtelHealthContext";

export function useOtelHealth(): OtelHealthContextValue {
  const context = useContext(OtelHealthContext);
  if (!context) {
    throw new Error("useOtelHealth must be used within an OtelHealthProvider");
  }
  return context;
}
