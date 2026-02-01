import { useEffect, useState } from "react";
import { fetchAvailableModels, LlmModelOption } from "../api";

const modelCache = new Map<string, LlmModelOption[]>();

interface UseAvailableModelsResult {
  models: LlmModelOption[];
  isLoading: boolean;
  error: string | null;
}

export function useAvailableModels(
  provider: string,
  fallback: LlmModelOption[] = []
): UseAvailableModelsResult {
  const [models, setModels] = useState<LlmModelOption[]>(
    () => modelCache.get(provider) ?? fallback
  );
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (!provider) {
      setModels(fallback);
      return;
    }

    const cached = modelCache.get(provider);
    if (cached) {
      setModels(cached);
      setError(null);
      return;
    }

    setIsLoading(true);
    setError(null);
    Promise.resolve(fetchAvailableModels(provider) ?? [])
      .then((next) => {
        if (cancelled) return;
        if (next.length > 0) {
          modelCache.set(provider, next);
          setModels(next);
        } else {
          setModels(fallback);
        }
      })
      .catch((err: any) => {
        if (cancelled) return;
        setModels(fallback);
        setError(err?.message || "Failed to load models");
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [provider]);

  return { models, isLoading, error };
}

export function resetAvailableModelsCache(): void {
  modelCache.clear();
}
