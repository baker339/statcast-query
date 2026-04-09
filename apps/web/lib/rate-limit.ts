type Entry = { count: number; windowStart: number };

const WINDOW_MS = 60_000;
const MAX_PER_WINDOW = 20;

const buckets = new Map<string, Entry>();

export function rateLimit(key: string): { ok: true } | { ok: false; retryAfterSec: number } {
  const now = Date.now();
  let e = buckets.get(key);
  if (!e || now - e.windowStart >= WINDOW_MS) {
    e = { count: 1, windowStart: now };
    buckets.set(key, e);
    return { ok: true };
  }
  e.count += 1;
  if (e.count > MAX_PER_WINDOW) {
    const retryAfterSec = Math.ceil((WINDOW_MS - (now - e.windowStart)) / 1000);
    return { ok: false, retryAfterSec: Math.max(1, retryAfterSec) };
  }
  return { ok: true };
}
