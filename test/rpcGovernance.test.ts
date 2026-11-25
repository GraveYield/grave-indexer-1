// SPDX-License-Identifier: Apache-2.0
//
// Option 2 Phase 1 — bounded retry transport, provider-aware rate
// governor and order-preserving bounded concurrency. All timing is
// injected (sleepImpl/nowImpl) so the tests are deterministic and fast.
//
// Pinned behaviors:
//   * retries are BOUNDED (attempt cap, backoff ceiling, wall-clock
//     deadline) and exhaustion is a TYPED error — never a fabricated
//     result;
//   * HTTP 429/5xx, JSON-RPC -32005 and transport failures retry;
//     other HTTP statuses do not;
//   * Retry-After (seconds or HTTP-date) takes precedence over the
//     computed backoff, capped at the backoff ceiling;
//   * the governor enforces both a token-bucket rate and an in-flight
//     concurrency cap;
//   * mapWithConcurrency returns results in INPUT order regardless of
//     completion order, with an enforced concurrency bound.

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import {
  RateGovernor,
  RpcDeadlineExceededError,
  RpcHttpError,
  RpcRetriesExhaustedError,
  backoffDelayMsForTest,
  defaultGovernorOptions,
  mapWithConcurrency,
  parseRetryAfterMs,
  withRpcRetry,
  type RpcRetryPolicy,
} from "../src/rpcGovernance.js";

const noSleep = async (): Promise<void> => {};

function policy(overrides: Partial<RpcRetryPolicy> = {}): RpcRetryPolicy {
  return {
    maxRetries: 3,
    baseDelayMs: 100,
    maxDelayMs: 1_000,
    jitterRatio: 0,
    overallDeadlineMs: 60_000,
    ...overrides,
  };
}

describe("withRpcRetry — bounded, typed, Retry-After aware", () => {
  test("resolves on first success without retrying", async () => {
    let calls = 0;
    const result = await withRpcRetry(
      async () => {
        calls++;
        return "ok";
      },
      policy(),
      { sleepImpl: noSleep },
    );
    assert.equal(result, "ok");
    assert.equal(calls, 1);
  });

  test("retries HTTP 429 and honors Retry-After over computed backoff", async () => {
    const delays: number[] = [];
    let calls = 0;
    const result = await withRpcRetry(
      async () => {
        calls++;
        if (calls < 3) {
          throw new RpcHttpError(429, "rate limited", 5_000);
        }
        return "ok";
      },
      policy({ maxDelayMs: 2_000 }), // Retry-After 5000 must be CAPPED at 2000
      { sleepImpl: async (ms) => delays.push(ms) },
    );
    assert.equal(result, "ok");
    assert.equal(calls, 3);
    assert.deepEqual(delays, [2_000, 2_000]); // capped Retry-After, both retries
  });

  test("retries 5xx with exponential backoff (jitter 0 → deterministic)", async () => {
    const delays: number[] = [];
    let calls = 0;
    await withRpcRetry(
      async () => {
        calls++;
        if (calls < 4) throw new RpcHttpError(503, "overloaded");
        return "ok";
      },
      policy(),
      { sleepImpl: async (ms) => delays.push(ms) },
    );
    assert.equal(calls, 4);
    assert.deepEqual(delays, [100, 200, 400]);
  });

  test("exhaustion is a typed error carrying the ACTUAL attempt count", async () => {
    let calls = 0;
    await assert.rejects(
      withRpcRetry(
        async () => {
          calls++;
          throw new RpcHttpError(500, "always failing");
        },
        policy({ maxRetries: 2 }),
        { sleepImpl: noSleep },
      ),
      (err: unknown) => {
        assert.ok(err instanceof RpcRetriesExhaustedError);
        assert.equal(err.attempts, 3);
        return true;
      },
    );
    assert.equal(calls, 3);
  });

  test("non-retryable HTTP status fails immediately with the real attempt count", async () => {
    let calls = 0;
    await assert.rejects(
      withRpcRetry(
        async () => {
          calls++;
          throw new RpcHttpError(400, "bad request");
        },
        policy({ maxRetries: 5 }),
        { sleepImpl: noSleep },
      ),
      (err: unknown) => {
        assert.ok(err instanceof RpcRetriesExhaustedError);
        assert.equal(err.attempts, 1); // single attempt, never retried
        return true;
      },
    );
    assert.equal(calls, 1);
  });

  test("transport failures retry; deadline expiry is a typed error", async () => {
    let clock = 0;
    let calls = 0;
    await assert.rejects(
      withRpcRetry(
        async () => {
          calls++;
          clock += 30_000; // each attempt burns half the budget
          throw new TypeError("fetch failed");
        },
        policy({ overallDeadlineMs: 60_000 }),
        {
          sleepImpl: noSleep,
          nowImpl: () => clock,
        },
      ),
      (err: unknown) => {
        assert.ok(err instanceof RpcDeadlineExceededError);
        return true;
      },
    );
    // Attempt 1 (t=0→30s) retry → attempt 2 (t=30s→60s) → deadline check
    // fails before attempt 3.
    assert.equal(calls, 2);
  });
});

describe("parseRetryAfterMs", () => {
  test("integer seconds", () => {
    assert.equal(parseRetryAfterMs("30"), 30_000);
    assert.equal(parseRetryAfterMs("0"), 0);
  });
  test("HTTP-date in the future", () => {
    const future = new Date(Date.now() + 10_000).toUTCString();
    const ms = parseRetryAfterMs(future);
    assert.ok(ms !== null && ms > 0 && ms <= 10_000);
  });
  test("garbage → null", () => {
    assert.equal(parseRetryAfterMs("soon-ish"), null);
    assert.equal(parseRetryAfterMs(null), null);
  });
});

describe("backoffDelayMs — deterministic with zero jitter, capped", () => {
  test("exponential growth and ceiling", () => {
    const p = policy({ baseDelayMs: 100, maxDelayMs: 500, jitterRatio: 0 });
    assert.equal(backoffDelayMsForTest(p, 1), 100);
    assert.equal(backoffDelayMsForTest(p, 2), 200);
    assert.equal(backoffDelayMsForTest(p, 3), 400);
    assert.equal(backoffDelayMsForTest(p, 4), 500); // capped
    assert.equal(backoffDelayMsForTest(p, 9), 500);
  });
});

describe("RateGovernor — token bucket + concurrency cap", () => {
  test("token bucket blocks when the budget is exhausted, releases with time", async () => {
    // Fully injected clock: sleepImpl advances the fake clock instantly,
    // so the test is deterministic and consumes no real time.
    let nowMs = 0;
    const sleeps: number[] = [];
    const gov = new RateGovernor({
      requestsPerSecond: 2,
      maxConcurrent: 0,
      nowImpl: () => nowMs,
      sleepImpl: async (ms) => {
        sleeps.push(ms);
        nowMs += ms;
      },
    });
    const r1 = await gov.acquire();
    const r2 = await gov.acquire();
    assert.equal(gov.inflight, 2);
    // Budget (2 tokens) exhausted: the third acquire must WAIT for a
    // token — at 2 rps the deficit is ~500 ms — and then succeed.
    const r3 = await gov.acquire();
    assert.deepEqual(sleeps, [500]);
    assert.equal(typeof r3, "function");
    assert.equal(gov.inflight, 3);
    r1();
    r2();
    r3();
    assert.equal(gov.inflight, 0);
  });

  test("concurrency cap makes excess acquires wait for a release", async () => {
    const gov = new RateGovernor({ requestsPerSecond: 0, maxConcurrent: 2 });
    const a = await gov.acquire();
    const b = await gov.acquire();
    let thirdResolved = false;
    const third = gov.acquire().then((rel) => {
      thirdResolved = true;
      return rel;
    });
    await new Promise((r) => setTimeout(r, 10));
    assert.equal(thirdResolved, false);
    a();
    const c = await third;
    assert.ok(c);
    b();
    c();
  });
});

describe("mapWithConcurrency — bounded, order-preserving", () => {
  test("results are in input order regardless of completion order", async () => {
    // Deliberately reverse completion order.
    const items = [50, 40, 30, 10, 0];
    const settled = await mapWithConcurrency(items, 5, async (delay, i) => {
      await new Promise((r) => setTimeout(r, delay));
      return `#${i}`;
    });
    assert.deepEqual(
      settled.map((s) => (s.ok ? s.value : "REJECTED")),
      ["#0", "#1", "#2", "#3", "#4"],
    );
  });

  test("concurrency bound is enforced (never more than N in flight)", async () => {
    let inflight = 0;
    let peak = 0;
    const items = Array.from({ length: 20 }, (_, i) => i);
    await mapWithConcurrency(items, 3, async () => {
      inflight++;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 2));
      inflight--;
      return null;
    });
    assert.ok(peak <= 3, `peak inflight ${peak} exceeded bound 3`);
  });

  test("per-item failures are captured explicitly, order preserved", async () => {
    const settled = await mapWithConcurrency([0, 1, 2], 3, async (i) => {
      if (i === 1) throw new Error("boom-1");
      return i * 10;
    });
    assert.equal(settled[0].ok, true);
    assert.equal(settled[0].ok && settled[0].value, 0);
    assert.equal(settled[1].ok, false);
    assert.match(String(!settled[1].ok && settled[1].error), /boom-1/);
    assert.equal(settled[2].ok, true);
    assert.equal(settled[2].ok && settled[2].value, 20);
  });
});

describe("defaultGovernorOptions — provider-aware", () => {
  test("public endpoints get the conservative budget; others are unlimited by default", () => {
    assert.deepEqual(defaultGovernorOptions("https://api.mainnet-beta.solana.com"), {
      requestsPerSecond: 4,
      maxConcurrent: 4,
    });
    assert.deepEqual(defaultGovernorOptions("https://api.devnet.solana.com"), {
      requestsPerSecond: 4,
      maxConcurrent: 4,
    });
    assert.deepEqual(defaultGovernorOptions("https://my-helius-endpoint.helius-rpc.com"), {
      requestsPerSecond: 0,
      maxConcurrent: 0,
    });
  });
});
