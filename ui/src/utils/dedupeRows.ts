/**
 * Deterministic row hash for deduplication across pagination pages.
 * Uses sorted column keys + JSON-stringified values.
 */
function hashRow(row: Record<string, unknown>): string {
  const keys = Object.keys(row).sort();
  return keys.map((k) => `${k}:${JSON.stringify(row[k])}`).join("|");
}

/**
 * Deduplicate rows that appear in both existing and new arrays.
 * Returns only the rows from `newRows` that are not already in `existingRows`.
 */
export function dedupeRows(
  existingRows: Record<string, unknown>[],
  newRows: Record<string, unknown>[]
): Record<string, unknown>[] {
  const seen = new Set(existingRows.map(hashRow));
  return newRows.filter((row) => !seen.has(hashRow(row)));
}
