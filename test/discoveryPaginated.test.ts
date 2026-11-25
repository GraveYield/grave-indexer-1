// SPDX-License-Identifier: Apache-2.0
//
// Option 2 Phase 3 — paginated discovery: capability probe, page
// ledger, completeness semantics and deterministic output.
//
// Pinned behaviors (fail-closed per Part 2 §2 / Part 3 Phase 3):
//   * the capability probe is the ONLY way the paginated path opens;
//     public-endpoint -32601 evidence keeps the gate closed;
//   * a page ledger records every received page (fingerprint, context
//     slot, retries, end condition) — receiving proof, NOT completeness
//     proof;
//   * with the provider contract undocumented (default), completeness
//     stays INCOMPLETE even when every page was received;
//   * page-budget exhaustion, page-request failure and cross-page
//     duplicates are counted and reported — never silent;
//   * output is deterministic (ascending base58 order, shared
//     provenance shape with the classic path).

import { test, describe } from "node:test";
import assert from "node:assert/strict";

import { PublicKey } from "@solana/web3.js";
import { RAYDIUM_V4_PROGRAM_ID } from "@graveyield/sdk";

import {
  discoverRaydiumV4PoolsPaginated,
  makeHeliusGpaV2Provider,
  probeGetProgramAccountsV2,
  type PaginatedDiscoveryProvider,
  type RawProgramAccount,
} from "../src/discoveryPaginated.js";
import { buildAmmInfoSlice, synth } from "./fixtures.js";

const PROGRAM_ID = RAYDIUM_V4_PROGRAM_ID;
const CLUSTER = "mainnet-beta" as const;

function account(pubkey: PublicKey): RawProgramAccount {
  return {
    pubkey: pubkey.toBase58(),
    account: {
      data: [Buffer.from(buildAmmInfoSlice()).toString("base64"), "base64"],
      owner: PROGRAM_ID.toBase58(),
    },
  };
}

/** Deterministic mock provider: scripted pages + call log. */
function mockProvider(pages: Array<{ accounts: RawProgramAccount[]; endFlag: boolean | null; contextSlot?: number; fail?: boolean }>): PaginatedDiscoveryProvider & { pagesRequested: number[] } {
  let call = 0;
  return {
    name: "mock-paged",
    pagesRequested: [],
    async fetchPage(page) {
      this.pagesRequested.push(page);
      const script = pages[call++] ?? { accounts: [], endFlag: true };
      if (script.fail) throw new Error("mock page failure after bounded retries");
      return {
        accounts: script.accounts,
        contextSlot: script.contextSlot ?? null,
        providerEndFlag: script.endFlag,
        retriesUsed: 0,
      };
    },
  };
}

const fakeConnection = { getSlot: async () => 5_000_000n };

describe("capability probe — getProgramAccountsV2", () => {
  test("public-endpoint -32601 evidence keeps the gate CLOSED", async () => {
    const fetchImpl = (async () =>
      new Response(
        JSON.stringify({ jsonrpc: "2.0", id: 1, error: { code: -32601, message: "Method not found" } }),
        { status: 200, headers: { "content-type": "application/json" } },
      )) as unknown as typeof fetch;
    const probe = await probeGetProgramAccountsV2("https://api.devnet.solana.com", PROGRAM_ID, { fetchImpl });
    assert.equal(probe.supported, false);
    assert.match(probe.evidence, /-32601/);
  });

  test("a clean page-1 answer opens the gate (probe only — contract still unverified)", async () => {
    const fetchImpl = (async () =>
      new Response(JSON.stringify({ jsonrpc: "2.0", id: 1, result: [] }), {
        status: 200,
        headers: { "content-type": "application/json" },
      })) as unknown as typeof fetch;
    const probe = await probeGetProgramAccountsV2("https://provider.invalid", PROGRAM_ID, { fetchImpl });
    assert.equal(probe.supported, true);
  });

  test("transport failure fails the probe closed", async () => {
    const fetchImpl = (async () => {
      throw new TypeError("network unreachable");
    }) as unknown as typeof fetch;
    const probe = await probeGetProgramAccountsV2("https://provider.invalid", PROGRAM_ID, { fetchImpl });
    assert.equal(probe.supported, false);
    assert.match(probe.evidence, /fail closed/);
  });
});

describe("discoverRaydiumV4PoolsPaginated — ledger + completeness", () => {
  test("happy path: pages received, ledger recorded, deterministic order", async () => {
    const p1 = [account(synth(0x61)), account(synth(0x63))];
    const p2 = [account(synth(0x62))];
    const provider = mockProvider([
      { accounts: p1, endFlag: null, contextSlot: 4_999_999 },
      { accounts: p2, endFlag: null, contextSlot: 5_000_000 },
      { accounts: [], endFlag: true },
    ]);
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 10,
      requireDocumentedContract: false, // owner-verified scenario
    });
    assert.equal(run.completeness.kind, "complete");
    assert.deepEqual(
      run.records.map((r) => r.poolAddress.toBase58()),
      [synth(0x61), synth(0x62), synth(0x63)]
        .map((p) => p.toBase58())
        .sort((a, b) => a.localeCompare(b)), // ascending base58 order (same comparator as the classic path)
    );
    assert.equal(run.stats.rawAccounts, 3);
    assert.equal(run.stats.pagesReceived, 3);
    assert.equal(run.stats.pagesRequested, 3);
    assert.equal(run.ledger.length, 3);
    assert.equal(run.ledger[0].contextSlot, 4_999_999);
    assert.equal(run.ledger[2].endCondition, "empty-page");
    assert.equal(run.stats.crossPageDuplicates, 0);
    // Bounded-observation provenance is recorded, never a snapshot claim.
    assert.match(run.stats.observation.boundedWindowDescription, /NOT an atomic snapshot/);
    assert.match(run.stats.observation.boundedWindowDescription, /bounded observation window/);
    assert.deepEqual(run.stats.observation.pageContextSlots, [4_999_999, 5_000_000, null]);
  });

  test("DEFAULT (contract undocumented): completeness stays INCOMPLETE even when all pages were received", async () => {
    const provider = mockProvider([
      { accounts: [account(synth(0x61))], endFlag: null },
      { accounts: [], endFlag: true },
    ]);
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 10,
      // requireDocumentedContract defaults to true
    });
    assert.equal(run.completeness.kind, "incomplete");
    assert.ok(run.completeness.kind === "incomplete" && run.completeness.reason === "provider-contract-unverified");
    assert.match(run.completeness.kind === "incomplete" ? run.completeness.detail : "", /no-gap\/no-duplicate/);
  });

  test("cross-page duplicates are COUNTED, kept-first — never silent", async () => {
    const dupe = synth(0x61);
    const provider = mockProvider([
      { accounts: [account(dupe), account(synth(0x62))], endFlag: null },
      { accounts: [account(dupe), account(synth(0x63))], endFlag: null },
      { accounts: [], endFlag: true },
    ]);
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 10,
      requireDocumentedContract: false,
    });
    assert.equal(run.stats.crossPageDuplicates, 1);
    assert.equal(run.stats.rawAccounts, 4); // every returned account counted
    assert.equal(run.stats.iteratedAccounts, 3); // first occurrences parsed
    assert.equal(run.records.length, 3);
  });

  test("page-request failure → incomplete 'page-request-failed' with ledger error entry", async () => {
    const provider = mockProvider([
      { accounts: [account(synth(0x61))], endFlag: null },
      { fail: true },
    ]);
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 10,
    });
    assert.equal(run.completeness.kind, "incomplete");
    assert.ok(run.completeness.kind === "incomplete" && run.completeness.reason === "page-request-failed");
    assert.equal(run.ledger[1].error !== undefined, true);
    assert.match(run.ledger[1].error ?? "", /mock page failure/);
    assert.equal(run.stats.pagesReceived, 1);
  });

  test("page-budget exhaustion → incomplete 'page-budget-exhausted'", async () => {
    const provider = mockProvider([
      { accounts: [account(synth(0x61))], endFlag: null },
      { accounts: [account(synth(0x62))], endFlag: null },
      { accounts: [account(synth(0x63))], endFlag: null },
    ]);
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 3,
      requireDocumentedContract: false,
    });
    assert.equal(run.completeness.kind, "incomplete");
    assert.ok(run.completeness.kind === "incomplete" && run.completeness.reason === "page-budget-exhausted");
    assert.equal(run.stats.pagesRequested, 3);
  });
});

describe("Helius gPA V2 adapter — wire contract", () => {
  test("sends page-based requests and parses the {context, value} wrapper", async () => {
    const bodies: string[] = [];
    const fetchImpl = (async (_url: string | URL, init?: { body?: string }) => {
      bodies.push(String(init?.body));
      const page = (JSON.parse(String(init?.body)) as { params: [string, { page: number }] }).params[1].page;
      if (page === 1) {
        return new Response(
          JSON.stringify({
            jsonrpc: "2.0",
            id: 1,
            result: { context: { slot: 4_999_999 }, value: [account(synth(0x61))] },
          }),
          { status: 200, headers: { "content-type": "application/json" } },
        );
      }
      return new Response(JSON.stringify({ jsonrpc: "2.0", id: 1, result: { context: { slot: 5_000_000 }, value: [] } }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }) as unknown as typeof fetch;
    const provider = makeHeliusGpaV2Provider({
      rpcUrl: "https://provider.invalid",
      programId: PROGRAM_ID,
      filters: [{ dataSize: 752 }],
      dataSlice: { offset: 336, length: 160 },
    });
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://provider.invalid",
      provider,
      maxPages: 5,
      fetchImpl,
      requireDocumentedContract: false,
    });
    // Page parameter present in every request body.
    assert.deepEqual(
      bodies.map((b) => (JSON.parse(b) as { params: [string, { page: number }] }).params[1].page),
      [1, 2],
    );
    assert.equal(run.stats.pagesReceived, 2);
    assert.equal(run.stats.rawAccounts, 1);
    assert.equal(run.stats.observation.responseContextSlot, null);
    assert.deepEqual(run.stats.observation.pageContextSlots, [4_999_999, 5_000_000]);
    assert.equal(run.completeness.kind, "complete");
  });

  test("RPC-level error (-32601) on a page → page-request-failed (fail closed)", async () => {
    const fetchImpl = (async () =>
      new Response(
        JSON.stringify({ jsonrpc: "2.0", id: 1, error: { code: -32601, message: "Method not found" } }),
        { status: 200, headers: { "content-type": "application/json" } },
      )) as unknown as typeof fetch;
    const provider = makeHeliusGpaV2Provider({
      rpcUrl: "https://api.mainnet-beta.solana.com",
      programId: PROGRAM_ID,
      filters: [{ dataSize: 752 }],
    });
    const run = await discoverRaydiumV4PoolsPaginated(fakeConnection, {
      cluster: CLUSTER,
      rpcUrl: "https://api.mainnet-beta.solana.com",
      provider,
      maxPages: 3,
      fetchImpl,
    });
    assert.equal(run.completeness.kind, "incomplete");
    assert.ok(run.completeness.kind === "incomplete" && run.completeness.reason === "page-request-failed");
    assert.match(run.completeness.kind === "incomplete" ? run.completeness.detail : "", /-32601/);
  });
});
