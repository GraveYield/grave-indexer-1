// SPDX-License-Identifier: Apache-2.0
//
// Retry-aware RPC transport primitives, provider-aware rate governance and
// bounded, order-preserving concurrency (Option 2 — Phase 1 pipeline
// hardening).
//
// Design rules (non-negotiable):
//
//   * BOUNDED — every retry loop has a finite attempt count, a finite
//     backoff ceiling and an overall wall-clock deadline. There is no
//     unbounded retry anywhere in this module.
//   * FAIL-EXPLICIT — exhaustion and deadline expiry surface as TYPED
//     errors (`RpcRetriesExhaustedError`, `RpcDeadlineExceededError`).
//     A failure is never converted into a successful result and never
//     into evidence of any kind; callers decide what an unresolved
//     outcome means (in the pipeline: unresolved, blocking).
//   * RETRY-AFTER HONORED — HTTP 429 responses carry `Retry-After`
//     (integer seconds or HTTP-date); when present and parseable it
//     takes precedence over the computed exponential backoff (capped at
//     the backoff ceiling so a hostile header cannot stall the pipeline).
//   * JITTERED BACKOFF — exponential backoff with proportional jitter to
//     avoid synchronized retry storms across concurrent workers.
//   * RATE GOVERNANCE — a provider-aware global token-bucket governor
//     bounds request rate and in-flight request count for the whole
//     process (defaults are stricter for public/open endpoints).
//   * DETERMINISTIC PARALLELISM — `mapWithConcurrency` returns results
//     in INPUT order regardless of completion order, so callers can do
//     all accounting (health records, failure ledgers, maps) in a single
//     deterministic post-pass and byte-stable artifacts are preserved.

/** Bounded retry policy for one RPC operation. */
export interface RpcRetryPolicy {
  /** Maximum attempts AFTER the initial call (0 = no retries). */
  maxRetries: number;
  /** Base delay for the first retry (exponential growth from here). */
  baseDelayMs: number;
  /** Hard ceiling for any single computed backoff delay. */
  maxDelayMs: number;
  /** Proportional jitter (0..1): each delay is multiplied by
   *  (1 - jitter + 2·jitter·rand). */
  jitterRatio: number;
  /** Overall wall-clock budget for the whole operation including all
   *  retries. Expiry surfaces as `RpcDeadlineExceededError`. */
  overallDeadlineMs: number;
}

/** Conservative default policy used by pipeline stages (bounded). */
export const DEFAULT_RPC_RETRY_POLICY: RpcRetryPolicy = {
  maxRetries: 3,
  baseDelayMs: 250,
  maxDelayMs: 8_000,
  jitterRatio: 0.25,
  overallDeadlineMs: 45_000,
};

/** A retryable HTTP transport failure with the exact response status. */
export class RpcHttpError extends Error {
  override readonly name = "RpcHttpError";
  readonly status: number;
  /** Server-advised delay (ms) from `Retry-After`, when parseable. */
  readonly retryAfterMs: number | null;

  constructor(status: number, message: string, retryAfterMs: number | null = null) {
    super(message);
    this.status = status;
    this.retryAfterMs = retryAfterMs;
  }
}

/** A JSON-RPC-level error returned in a 200 response. */
export class RpcRpcError extends Error {
  override readonly name = "RpcRpcError";
  readonly code: number;

  constructor(code: number, message: string) {
    super(message);
    this.code = code;
  }
}

/** All retry attempts consumed without a successful result. */
export class RpcRetriesExhaustedError extends Error {
  override readonly name = "RpcRetriesExhaustedError";
  readonly attempts: number;
  readonly label: string;

  constructor(label: string, attempts: number, lastError: unknown) {
    super(
      `RPC retries exhausted after ${attempts} attempt(s) [${label}]: ` +
        `${String((lastError as Error)?.message ?? lastError).slice(0, 200)}`,
    );
    this.attempts = attempts;
    this.label = label;
  }
}

/** The overall deadline expired before a successful result. */
export class RpcDeadlineExceededError extends Error {
  override readonly name = "RpcDeadlineExceededError";
  readonly label: string;

  constructor(label: string, deadlineMs: number) {
    super(`RPC deadline exceeded after ${deadlineMs}ms [${label}]`);
    this.label = label;
  }
}

/** Solana JSON-RPC rate-limit error code (server-side throttling). */
const RPC_RATE_LIMIT_CODE = -32005;

function isRetryable(err: unknown): boolean {
  if (err instanceof RpcHttpError) {
    return err.status === 429 || err.status >= 500;
  }
  if (err instanceof RpcRpcError) {
    return err.code === RPC_RATE_LIMIT_CODE;
  }
  // Anything else thrown by the transport (fetch TypeError/DNS reset,
  // JSON parse failure on a truncated body, …) is a transport failure —
  // retryable within the bounded budget.
  return true;
}

/** Parse `Retry-After` (integer seconds or HTTP-date) into milliseconds. */
export function parseRetryAfterMs(value: string | null): number | null {
  if (!value) return null;
  const trimmed = value.trim();
  if (/^\d+$/.test(trimmed)) {
    const seconds = Number.parseInt(trimmed, 10);
    return Number.isFinite(seconds) && seconds >= 0 ? seconds * 1_000 : null;
  }
  const date = Date.parse(trimmed);
  if (Number.isNaN(date)) return null;
  const delta = date - Date.now();
  return delta > 0 ? delta : 0;
}

function backoffDelayMs(policy: RpcRetryPolicy, attempt: number): number {
  // attempt is 1-based for the first retry: base * 2^(attempt-1), capped.
  const exponential = Math.min(policy.baseDelayMs * 2 ** (attempt - 1), policy.maxDelayMs);
  const jitter = 1 - policy.jitterRatio + 2 * policy.jitterRatio * Math.random();
  return Math.max(0, Math.floor(exponential * jitter));
}

/** Test/diagnostic accessor for the deterministic backoff schedule. */
export const backoffDelayMsForTest = (policy: RpcRetryPolicy, attempt: number): number =>
  policy.jitterRatio === 0
    ? backoffDelayMs(policy, attempt)
    : (() => {
        throw new Error("backoffDelayMsForTest requires jitterRatio 0 for determinism");
      })();

export interface WithRpcRetryOptions {
  /** Diagnostic label used in typed failure messages. */
  label?: string;
  /** Overrides `policy.overallDeadlineMs` for this call. */
  deadlineMs?: number;
  /** Injectable sleep for deterministic tests. */
  sleepImpl?: (ms: number) => Promise<void>;
  /** Injectable clock for deterministic tests. */
  nowImpl?: () => number;
}

/**
 * Run `op` under the bounded retry policy. Retries ONLY retryable
 * failures (HTTP 429/5xx, JSON-RPC -32005, transport errors) honoring
 * `Retry-After` where provided. Exhaustion and deadline expiry surface
 * as typed errors — a failed operation never yields a fabricated result.
 */
export async function withRpcRetry<T>(
  op: (attempt: number) => Promise<T>,
  policy: RpcRetryPolicy,
  opts: WithRpcRetryOptions = {},
): Promise<T> {
  const label = opts.label ?? "rpc";
  const deadlineMs = opts.deadlineMs ?? policy.overallDeadlineMs;
  const sleep = opts.sleepImpl ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const now = opts.nowImpl ?? Date.now;
  const startedAt = now();
  const attemptsAllowed = policy.maxRetries + 1;
  let attemptsMade = 0;

  let lastError: unknown = null;
  for (let attempt = 1; attempt <= attemptsAllowed; attempt++) {
    if (now() - startedAt >= deadlineMs) {
      throw new RpcDeadlineExceededError(label, deadlineMs);
    }
    try {
      attemptsMade = attempt;
      return await op(attempt);
    } catch (err) {
      lastError = err;
      if (!isRetryable(err) || attempt === attemptsAllowed) break;
      if (now() - startedAt >= deadlineMs) break;
      const retryAfter =
        err instanceof RpcHttpError && err.retryAfterMs !== null ? err.retryAfterMs : null;
      const delay = retryAfter !== null ? Math.min(retryAfter, policy.maxDelayMs) : backoffDelayMs(policy, attempt);
      await sleep(delay);
    }
  }
  if (now() - startedAt >= deadlineMs) {
    throw new RpcDeadlineExceededError(label, deadlineMs);
  }
  throw new RpcRetriesExhaustedError(label, attemptsMade, lastError);
}

// ─── Provider-aware rate governor ────────────────────────────────────────────

export interface RateGovernorOptions {
  /** Sustained request rate (requests/second). 0 = unlimited. */
  requestsPerSecond: number;
  /** Maximum concurrently in-flight governed operations. 0 = unlimited. */
  maxConcurrent: number;
  /** Injectable clock for deterministic tests. */
  nowImpl?: () => number;
  /** Injectable sleep for deterministic tests. */
  sleepImpl?: (ms: number) => Promise<void>;
}

/**
 * Global (per-process) rate governor: a token bucket plus an in-flight
 * concurrency cap. Provider-aware defaults are chosen by the caller
 * (e.g. stricter for public/open endpoints); the governor itself is
 * policy-agnostic and shared so concurrent pipeline stages cannot
 * collectively exceed the negotiated budget.
 */
export class RateGovernor {
  private readonly rps: number;
  private readonly maxConcurrent: number;
  private readonly nowImpl: () => number;
  private readonly sleepImpl: (ms: number) => Promise<void>;

  private tokens: number;
  private lastRefillAt: number;
  private inFlight = 0;
  private readonly waiters: Array<() => void> = [];

  constructor(opts: RateGovernorOptions) {
    this.rps = Math.max(0, opts.requestsPerSecond);
    this.maxConcurrent = Math.max(0, opts.maxConcurrent);
    this.nowImpl = opts.nowImpl ?? Date.now;
    this.sleepImpl = opts.sleepImpl ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
    this.lastRefillAt = this.nowImpl();
    this.tokens = this.rps > 0 ? this.rps : Number.POSITIVE_INFINITY;
  }

  /**
   * Acquire one governed slot. Resolves with a release function that
   * MUST be called when the operation completes (use try/finally).
   */
  async acquire(): Promise<() => void> {
    // Concurrency cap first: wait for an in-flight slot.
    while (this.maxConcurrent > 0 && this.inFlight >= this.maxConcurrent) {
      await new Promise<void>((resolve) => this.waiters.push(resolve));
    }
    // Token bucket: wait for a token when rate-limited.
    if (this.rps > 0) {
      for (;;) {
        const now = this.nowImpl();
        const elapsed = now - this.lastRefillAt;
        const refill = (elapsed / 1_000) * this.rps;
        if (refill > 0) {
          this.tokens = Math.min(this.rps, this.tokens + refill);
          this.lastRefillAt = now;
        }
        if (this.tokens >= 1) {
          this.tokens -= 1;
          break;
        }
        const deficitMs = ((1 - this.tokens) / this.rps) * 1_000;
        await this.sleepImpl(Math.max(1, Math.ceil(deficitMs)));
      }
    }
    this.inFlight += 1;
    let released = false;
    return () => {
      if (released) return;
      released = true;
      this.inFlight -= 1;
      const next = this.waiters.shift();
      if (next) next();
    };
  }

  /** Current in-flight count (observability). */
  get inflight(): number {
    return this.inFlight;
  }
}

function endpointKind(rpcUrl: string): "public" | "private" {
  try {
    const url = new URL(rpcUrl);
    const host = url.hostname;
    // The canonical public endpoints (and localhost dev gates) get the
    // conservative default; dedicated provider endpoints are operator-
    // provisioned and can carry a higher negotiated budget.
    return /(^|\.)solana\.com$/.test(host) || host === "localhost" || host === "127.0.0.1"
      ? "public"
      : "private";
  } catch {
    return "public";
  }
}

/** Provider-aware default budgets for the global governor. */
export function defaultGovernorOptions(rpcUrl: string): { requestsPerSecond: number; maxConcurrent: number } {
  return endpointKind(rpcUrl) === "public"
    ? { requestsPerSecond: 4, maxConcurrent: 4 }
    : { requestsPerSecond: 0, maxConcurrent: 0 };
}

/** The process-wide shared governor (created once per endpoint kind). */
let sharedGovernor: RateGovernor | null = null;
let sharedGovernorKey: string | null = null;

export function getSharedGovernor(rpcUrl: string, overrides?: Partial<RateGovernorOptions>): RateGovernor {
  const key = endpointKind(rpcUrl);
  if (sharedGovernor === null || sharedGovernorKey !== key) {
    sharedGovernor = new RateGovernor({ ...defaultGovernorOptions(rpcUrl), ...overrides });
    sharedGovernorKey = key;
  }
  return sharedGovernor;
}

/** Test seam: reset the shared governor (tests must isolate state). */
export function resetSharedGovernor(): void {
  sharedGovernor = null;
  sharedGovernorKey = null;
}

// ─── Bounded, order-preserving concurrency ───────────────────────────────────

/**
 * Map `items` through `fn` with at most `concurrency` in-flight
 * invocations. Results are ALWAYS in input order regardless of
 * completion order, so callers can perform accounting deterministically
 * in a follow-up pass. Per-item failures are captured explicitly — the
 * aggregate promise never rejects.
 */
export type SettledResult<R> = { ok: true; value: R } | { ok: false; error: unknown };

export async function mapWithConcurrency<T, R>(
  items: readonly T[],
  concurrency: number,
  fn: (item: T, index: number) => Promise<R>,
): Promise<SettledResult<R>[]> {
  const results: SettledResult<R>[] = new Array(items.length);
  const cap = Math.max(1, Math.floor(concurrency));
  let nextIndex = 0;
  const workers: Promise<void>[] = [];
  const runWorker = async (): Promise<void> => {
    for (;;) {
      const index = nextIndex++;
      if (index >= items.length) return;
      try {
        const value = await fn(items[index] as T, index);
        results[index] = { ok: true, value };
      } catch (err) {
        results[index] = { ok: false, error: err };
      }
    }
  };
  for (let i = 0; i < Math.min(cap, items.length); i++) {
    workers.push(runWorker());
  }
  await Promise.all(workers);
  return results;
}
