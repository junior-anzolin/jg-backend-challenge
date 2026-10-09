export function calculateExponentialBackoffDelay(
  attempt: number,
  baseDelayMs = 1_000,
  maxDelayMs = 5 * 60 * 1_000,
): number {
  if (!Number.isInteger(attempt) || attempt < 1) {
    throw new RangeError("Attempt must be a positive integer");
  }

  return Math.min(baseDelayMs * 2 ** (attempt - 1), maxDelayMs);
}
