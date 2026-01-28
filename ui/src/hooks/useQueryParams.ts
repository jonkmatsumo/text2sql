import { useCallback, useMemo } from "react";
import { useSearchParams } from "react-router-dom";

export function useQueryParams<T extends Record<string, string>>(
  defaults: T
): [T, (params: Partial<T>) => void] {
  const [searchParams, setSearchParams] = useSearchParams();

  const params = useMemo(() => {
    const result = { ...defaults };
    for (const key of Object.keys(defaults)) {
      const value = searchParams.get(key);
      if (value !== null) {
        (result as Record<string, string>)[key] = value;
      }
    }
    return result;
  }, [searchParams, defaults]);

  const setParams = useCallback(
    (newParams: Partial<T>) => {
      setSearchParams((prev) => {
        const next = new URLSearchParams(prev);
        for (const [key, value] of Object.entries(newParams)) {
          if (value === undefined || value === null || value === defaults[key]) {
            next.delete(key);
          } else {
            next.set(key, value);
          }
        }
        return next;
      });
    },
    [setSearchParams, defaults]
  );

  return [params, setParams];
}
