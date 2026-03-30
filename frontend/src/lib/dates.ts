export function safeToISODate(val: any): string | null {
  if (!val) return null;
  if (val instanceof Date) {
    if (isNaN(val.getTime())) return null;
    return val.toISOString().slice(0, 10);
  }
  const s = String(val);
  return s.slice(0, 10);
}

export function safeToISOTimestamp(val: any): string | null {
  if (!val) return null;
  if (val instanceof Date) {
    if (isNaN(val.getTime())) return null;
    return val.toISOString().slice(0, 19).replace("T", " ");
  }
  const s = String(val);
  const normalized = s.replace(/\s([+-]\d{4})$/, (_, tz) => tz.slice(0, 3) + ":" + tz.slice(3));
  const d = new Date(normalized.replace(" ", "T"));
  if (isNaN(d.getTime())) {
    return s.slice(0, 19);
  }
  return d.toISOString().slice(0, 19).replace("T", " ");
}
