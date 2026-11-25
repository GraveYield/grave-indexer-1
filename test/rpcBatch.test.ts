// SPDX-License-Identifier: Apache-2.0
//
// Option 2 Phase 1 (item D) — validated JSON-RPC batch transport.
//
// Pinned behaviors (fail-closed per Part 2 §1-D):
//   * results are keyed by REQUEST id and returned in REQUEST order,
//     never by response order;
//   * a per-item JSON-RPC error is UNRESOLVED ("item-error") — never a
//     negative result;
//   * a missing item is UNRESOLVED ("missing") — never a negative
//     result;
//   * a duplicate response id is UNRESOLVED ("duplicate");
//   * a stray response id corrupts the mapping — the WHOLE batch fails
//     closed ("mapping");
//   * malformed elements are UNRESOLVED ("malformed");
//   * transport failure after bounded retries leaves every item
//     UNRESOLVED ("batch") with the exhaustion reason;
//   * HTTP 429/5xx transport failures retry within the bounded budget.

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { rpcBatch, type BatchRequest } from "../src/rpcBatch.js";
import { type RpcRetryPolicy } from "../src/rpcGovernance.js";

const noSleep = async (): Promise<void> => {};

const retry: RpcRetryPolicy = {
  maxRetries: 2,
  baseDelayMs: 1,
  maxDelayMs: 4,
  jitterRatio: 0,
  overallDeadlineMs: 5_000,
};

function jsonResponse(payload: unknown, status = 200): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "content-type": "application/json" },
  });
}

const requests: BatchRequest[] = [
  { method: "getTokenAccountBalance", params: ["A"] },
  { method: "getTokenAccountBalance", params: ["B"] },
  { method: "getTokenSupply", params: ["C"] },
  { method: "getTokenSupply", params: ["D"] },
];

function okResponses(values: unknown[]): unknown[] {
  return values.map((v, i) => ({ jsonrpc: "2.0", id: i, result: v }));
}

describe("rpcBatch — validated batch semantics", () => {
  test("happy path: results mapped by id, returned in REQUEST order", async () => {
    // Respond OUT of order to prove id-keyed mapping.
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
        { jsonrpc: "2.0", id: 1, result: "bal-memecoin" },
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.equal(results.length, 4);
    assert.deepEqual(
      results.map((r) => (r.ok ? r.result : `UNRESOLVED:${r.kind}`)),
      ["bal-wsol", "bal-memecoin", "supply-memecoin", "supply-lp"],
    );
  });

  test("per-item JSON-RPC error → unresolved 'item-error', never negative", async () => {
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 1, error: { code: -32004, message: "account not found" } },
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.equal(results[0].ok, true);
    assert.equal(results[1].ok, false);
    assert.ok(!results[1].ok && results[1].kind === "item-error");
    assert.match(!results[1].ok ? results[1].reason : "", /account not found/);
  });

  test("missing response item → unresolved 'missing' (never a negative result)", async () => {
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
        // id 1 dropped by the provider under load
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.ok(!results[1].ok && results[1].kind === "missing");
    assert.match(!results[1].ok ? results[1].reason : "", /missing item id 1/);
  });

  test("duplicate response id → unresolved 'duplicate'", async () => {
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 1, result: "first" },
        { jsonrpc: "2.0", id: 1, result: "second" },
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.ok(!results[1].ok && results[1].kind === "duplicate");
  });

  test("stray response id corrupts the mapping → WHOLE batch fails closed", async () => {
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 1, result: "bal-memecoin" },
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
        { jsonrpc: "2.0", id: 99, result: "who-am-i" },
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.equal(results.length, 4);
    for (const r of results) {
      assert.ok(!r.ok && r.kind === "mapping");
    }
  });

  test("element with neither result nor error → unresolved 'malformed'", async () => {
    const fetchImpl = (async () =>
      jsonResponse([
        { jsonrpc: "2.0", id: 0, result: "bal-wsol" },
        { jsonrpc: "2.0", id: 1 },
        { jsonrpc: "2.0", id: 2, result: "supply-memecoin" },
        { jsonrpc: "2.0", id: 3, result: "supply-lp" },
      ])) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.ok(!results[1].ok && results[1].kind === "malformed");
  });

  test("non-array response → every item unresolved 'batch'", async () => {
    const fetchImpl = (async () => jsonResponse({ error: { code: -32600 } })) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl });
    assert.equal(results.length, 4);
    for (const r of results) assert.ok(!r.ok && r.kind === "batch");
  });

  test("HTTP 500s retry within the bounded budget; exhaustion leaves all items unresolved", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      return jsonResponse({ boom: true }, 500);
    }) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl, retry });
    assert.equal(calls, 3); // 1 + maxRetries 2 — bounded
    for (const r of results) {
      assert.ok(!r.ok && r.kind === "batch");
      assert.match(!r.ok ? r.reason : "", /retries exhausted/);
    }
  });

  test("HTTP 429 with Retry-After retries and then succeeds", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      if (calls === 1) {
        return new Response(JSON.stringify({ tooMany: true }), {
          status: 429,
          headers: { "content-type": "application/json", "retry-after": "1" },
        });
      }
      return jsonResponse(okResponses(["a", "b", "c", "d"]));
    }) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", requests, { fetchImpl, retry });
    assert.equal(calls, 2);
    for (const r of results) assert.ok(r.ok);
  });

  test("empty request list → empty result, no fetch", async () => {
    let calls = 0;
    const fetchImpl = (async () => {
      calls++;
      return jsonResponse([]);
    }) as unknown as typeof fetch;
    const results = await rpcBatch("http://rpc.invalid", [], { fetchImpl });
    assert.deepEqual(results, []);
    assert.equal(calls, 0);
  });
});
