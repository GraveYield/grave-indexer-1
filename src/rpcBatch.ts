// SPDX-License-Identifier: Apache-2.0
//
// Validated JSON-RPC batch transport (Option 2 — Phase 1 pipeline
// hardening, item D).
//
// Solana RPC providers accept a JSON-ARRAY request body where each
// element is an independent JSON-RPC call, and answer with a response
// array. Batching shrinks the number of logical RPC round-trips per
// pool, but a batched answer is NOT trustworthy without validation:
// providers may drop items under load, return items out of order, or
// interleave error objects. This module treats EVERY batch response as
// untrusted until proven otherwise:
//
//   * RESPONSE COUNT / MAPPING — every request id must appear EXACTLY
//     once in the response array. Missing, duplicated or stray ids are
//     validation failures recorded per item (and a stray id fails the
//     whole batch: the mapping itself is corrupt, so no item can be
//     trusted).
//   * PER-ITEM ERRORS — a JSON-RPC error object for one item marks that
//     item UNRESOLVED (kind "item-error"), never a negative result.
//   * MISSING ITEMS — a dropped item is UNRESOLVED (kind "missing"),
//     never a negative result.
//   * MALFORMED RESULTS — a non-object element, or an element with
//     neither `result` nor `error`, is UNRESOLVED (kind "malformed").
//   * ORDERING — results are returned in REQUEST order, keyed by the
//     request id, never by response order.
//
// Every unresolved outcome carries an explicit reason so callers can
// record it (the pipeline treats unresolved facts as per-pool failures —
// the artifact then fails enrichment completeness — never as evidence).
//
// Batch limits are NOT assumed: the caller opts in via configuration
// and the batch size stays fixed and small (see the enrichment stage).

import {
  RateGovernor,
  RpcHttpError,
  RpcRetriesExhaustedError,
  parseRetryAfterMs,
  withRpcRetry,
  type RpcRetryPolicy,
} from "./rpcGovernance.js";

/** One JSON-RPC request inside a batch. */
export interface BatchRequest {
  method: string;
  params: unknown[];
}

/** Validated per-item outcome, in request order. */
type BatchUnresolvedKind =
  | "item-error"
  | "missing"
  | "duplicate"
  | "malformed"
  | "mapping"
  | "batch";

export type BatchItemResult =
  | { ok: true; result: unknown }
  | { ok: false; unresolved: true; kind: BatchUnresolvedKind; reason: string };

export interface RpcBatchOptions {
  fetchImpl?: typeof fetch;
  retry?: RpcRetryPolicy;
  governor?: RateGovernor;
  /** Diagnostic label for typed transport errors. */
  label?: string;
}

interface RawBatchResponseItem {
  jsonrpc?: unknown;
  id?: unknown;
  result?: unknown;
  error?: { code?: unknown; message?: unknown } | null;
}

/**
 * Send one JSON-RPC batch (a JSON array body) and return validated
 * per-item results in REQUEST order. Transport-level failures (after
 * bounded retries) surface as ALL items unresolved with kind "batch" —
 * the caller cannot miss the failure, but also cannot misread it as a
 * negative result for any individual fact.
 */
export async function rpcBatch(
  rpcEndpoint: string,
  requests: readonly BatchRequest[],
  opts: RpcBatchOptions = {},
): Promise<BatchItemResult[]> {
  const allUnresolved = (kind: BatchUnresolvedKind, reason: string): BatchItemResult[] =>
    requests.map(() => ({ ok: false, unresolved: true, kind, reason } as BatchItemResult));

  if (requests.length === 0) return [];

  const label = opts.label ?? "rpc-batch";
  const retry = opts.retry ?? { maxRetries: 0, baseDelayMs: 0, maxDelayMs: 0, jitterRatio: 0, overallDeadlineMs: 30_000 };

  const body = JSON.stringify(
    requests.map((r, i) => ({ jsonrpc: "2.0", id: i, method: r.method, params: r.params })),
  );

  let json: unknown;
  try {
    json = await withRpcRetry(
      async () => {
        const acquire = opts.governor ? await opts.governor.acquire() : null;
        try {
          const res = await (opts.fetchImpl ?? fetch)(rpcEndpoint, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body,
          });
          if (!res.ok) {
            throw new RpcHttpError(
              res.status,
              `batch RPC HTTP ${res.status} from ${rpcEndpoint}`,
              parseRetryAfterMs(res.headers.get("retry-after")),
            );
          }
          return await res.json();
        } finally {
          acquire?.();
        }
      },
      retry,
      { label },
    );
  } catch (err) {
    const reason =
      err instanceof RpcRetriesExhaustedError
        ? `batch transport failed after bounded retries — ${err.message}`
        : `batch transport failed — ${String((err as Error)?.message ?? err).slice(0, 200)}`;
    return allUnresolved("batch", reason);
  }

  // The batch response MUST be an array mirroring the request ids.
  if (!Array.isArray(json)) {
    return allUnresolved("batch", "batch response is not an array");
  }

  const byId = new Map<number, RawBatchResponseItem>();
  const strayIds: unknown[] = [];
  const duplicateIds: number[] = [];
  for (const item of json as unknown[]) {
    if (typeof item !== "object" || item === null) {
      strayIds.push("non-object element");
      continue;
    }
    const rec = item as RawBatchResponseItem;
    if (typeof rec.id !== "number" || !Number.isSafeInteger(rec.id)) {
      strayIds.push(rec.id ?? null);
      continue;
    }
    // A response id OUTSIDE the requested range corrupts the mapping.
    if (rec.id < 0 || rec.id >= requests.length) {
      strayIds.push(rec.id);
      continue;
    }
    if (byId.has(rec.id)) {
      duplicateIds.push(rec.id);
      continue;
    }
    byId.set(rec.id, rec);
  }

  const results: BatchItemResult[] = [];
  for (let i = 0; i < requests.length; i++) {
    // Stray ids corrupt the mapping — fail the whole batch closed.
    if (strayIds.length > 0) {
      results.push({
        ok: false,
        unresolved: true,
        kind: "mapping",
        reason: `batch mapping corrupt: ${strayIds.length} stray response id(s) [${strayIds.slice(0, 5).map(String).join(", ")}]`,
      });
      continue;
    }
    const item = byId.get(i);
    if (item === undefined) {
      results.push({ ok: false, unresolved: true, kind: "missing", reason: `batch response missing item id ${i}` });
      continue;
    }
    if (duplicateIds.includes(i)) {
      results.push({ ok: false, unresolved: true, kind: "duplicate", reason: `batch response contains duplicate id ${i}` });
      continue;
    }
    if (item.error !== undefined && item.error !== null) {
      const code = typeof item.error.code === "number" ? item.error.code : "unknown";
      const message = typeof item.error.message === "string" ? item.error.message : "unknown error";
      results.push({
        ok: false,
        unresolved: true,
        kind: "item-error",
        reason: `batch item ${i} (${requests[i]?.method ?? "?"}) RPC error ${code}: ${message.slice(0, 160)}`,
      });
      continue;
    }
    if (!("result" in item)) {
      results.push({
        ok: false,
        unresolved: true,
        kind: "malformed",
        reason: `batch item ${i} has neither result nor error`,
      });
      continue;
    }
    results.push({ ok: true, result: item.result });
  }
  return results;
}
