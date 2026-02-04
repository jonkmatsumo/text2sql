const SUPPORTED_TIME_FORMATS = new Set(["%m/%d %H:%M", "%H:%M"]);

function pad(value: number) {
  return value.toString().padStart(2, "0");
}

export function formatNumber(value: number, precision = 2) {
  if (!Number.isFinite(value)) return "\u2014";
  return value.toFixed(precision);
}

export function formatTime(value: string | number | Date, format = "%m/%d %H:%M") {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return "\u2014";

  if (format === "%H:%M") {
    return `${pad(date.getHours())}:${pad(date.getMinutes())}`;
  }

  if (format === "%m/%d %H:%M") {
    return `${pad(date.getMonth() + 1)}/${pad(date.getDate())} ${pad(
      date.getHours()
    )}:${pad(date.getMinutes())}`;
  }

  if (!SUPPORTED_TIME_FORMATS.has(format)) {
    return date.toLocaleString();
  }

  return date.toLocaleString();
}
