export const VERBOSE_MODE_KEY = "VERBOSE_MODE";

export function getVerboseModeFromSearch(search: string): boolean {
  const params = new URLSearchParams(search);
  const value = params.get("verbose");
  if (!value) return false;
  return value === "1" || value.toLowerCase() === "true";
}

export function loadVerboseMode(search: string): boolean {
  if (getVerboseModeFromSearch(search)) return true;
  try {
    const stored = window.localStorage?.getItem(VERBOSE_MODE_KEY);
    return stored === "1" || stored === "true";
  } catch {
    return false;
  }
}

export function saveVerboseMode(enabled: boolean) {
  try {
    window.localStorage?.setItem(VERBOSE_MODE_KEY, enabled ? "true" : "false");
  } catch {
    // ignore storage failures
  }
}
