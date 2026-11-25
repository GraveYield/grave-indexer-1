// SPDX-License-Identifier: Apache-2.0
//
// Paginated discovery over a provider-extended `getProgramAccountsV2`
// (Option 2 — Phase 3), with an EXPLICIT capability gate, a per-page
// ledger, and fail-closed completeness semantics.
//
// WHY THIS EXISTS: the classic single-response `getProgramAccounts` is
// the Option 2 baseline's primary bottleneck — one unpaginated response
// for the memcmp-filtered Raydium V4 corpus (~561k accounts) was
// estimated at 180–240 MB and web3.js 3.x cannot even decode it. True
// pagination shrinks every response, but ONLY if the provider's
// pagination contract is actually trustworthy.
//
// WHAT THIS MODULE DOES NOT CLAIM:
//
//   * A page ledger proves which pages the CLIENT received. It does
//     NOT prove that the provider returned every matching account.
//     Provider completeness semantics (no-gap/no-duplicate behavior,
//     end-of-enumeration conditions, behavior during concurrent account
//     changes, context-slot semantics, retention/limits) must be
//     verified and documented per provider BEFORE any paginated run is
//     treated as complete.
//   * The Helius `getProgramAccountsV2` adapter below implements the
//     page-based contract as publicly documented, but it is UNVERIFIED
//     against a live provider in this repository (no provider
//     credentials in CI/sandbox). It therefore sits behind an explicit
//     capability gate, and by default every paginated run reports
//     completeness "incomplete / provider-contract-unverified" even
//     when all pages were received and validated.
//
// CAPABILITY GATE (fail-closed; probe evidence captured 2026-11-23):
//   * `getProgramAccountsV2` on https://api.devnet.solana.com →
//     JSON-RPC error -32601 "Method not found".
//   * `getProgramAccountsV2` on https://api.mainnet-beta.solana.com →
//     JSON-RPC error -32601 "Method not found".
// A probe that fails with -32601 (or anything unexpected) means the
// gate STAYS CLOSED. There is NO silent fallback to classic
// `getProgramAccounts` for corpus-scale runs: the classic path remains
// available only as an EXPLICITLY CONFIGURED bounded fallback whose
// completeness is capped by the caller.
//
// COMPLETENESS: a paginated run is reported `complete` only when every
// page of the enumerated sequence was received and validated within the
// page budget, end-of-enumeration was signaled, AND the provider's
// completeness contract is documented/verified (requireDocumentedContract=false,
// an owner-only switch). Page-budget exhaustion, page-request failure,
// mapping anomalies or an undocumented end condition yield `incomplete`
// with an explicit reason. Records are deduplicated deterministically
// (first occurrence wins; cross-page duplicates are COUNTED in the run
// stats — never silently dropped) and emitted in ascending base58
// pool-address order (byte-stable with the classic path's ordering
// contract).

import { createHash } from "node:crypto";
import { PublicKey } from "@solana/web3.js";
import {
  RAYDIUM_V4_PROGRAM_ID,
  RAYDIUM_V4_AMM_INFO_SIZE,
  parseV4AmmInfo,
  identifyBaseToken,
  type V4PoolAccount,
} from "@graveyield/sdk";
import { accountFingerprint, endpointKindFor, type Cluster, type EndpointKind } from "./provenance.js";
import {
  RpcHttpError,
  RpcRpcError,
  parseRetryAfterMs,
  withRpcRetry,
  type RateGovernor,
  type RpcRetryPolicy,
} from "./rpcGovernance.js";
import { parseV4AmmInfoSlice, SLICE_LENGTH, type DiscoveredPoolRecord } from "./discovery.js";

/** Raw account shape shared with the classic discovery transport. */
export interface RawProgramAccount {
  pubkey: string;
  account: { data: [string, string]; owner?: string };
}

/** Why a paginated run did not (or cannot) prove complete enumeration. */
export type PaginatedIncompleteReason =
  | "provider-contract-unverified"
  | "page-budget-exhausted"
  | "page-request-failed";

export interface PageLedgerEntry {
  /** 1-based page index requested. */
  page: number;
  /** Number of accounts returned on this page. */
  accountCount: number;
  /** sha256 over the page's ordered pubkey list (receiving proof). */
  pageFingerprint: string;
  /** Response `context.slot` when the provider supplied one. */
  contextSlot: number | null;
  /** Wall-clock receive time. */
  receivedAtUnixMs: number;
  /** Bounded retries consumed for this page. */
  retriesUsed: number;
  /** How the end of enumeration was signaled (or not). */
  endCondition: "empty-page" | "provider-flag" | null;
  /** Set when the page itself failed (run is incomplete). */
  error?: string;
}

export type DiscoveryCompleteness =
  | { kind: "complete" }
  | { kind: "incomplete"; reason: PaginatedIncompleteReason; detail: string };

export interface PaginatedDiscoveryRun {
  records: DiscoveredPoolRecord[];
  /** Same core shape as the classic run's stats, plus pagination facts. */
  stats: {
    rawAccounts: number;
    parsedPools: number;
    malformedSkipped: number;
    unsupportedPairs: number;
    duplicatesInBatch: number;
    /** Cross-page duplicates — counted, kept-first, never silent. */
    crossPageDuplicates: number;
    slotLowerBound: number;
    slotUpperBound: number;
    durationMs: number;
    iteratedAccounts: number;
    truncated: boolean;
    pagesReceived: number;
    pagesRequested: number;
    /** Bounded-observation provenance — NEVER a point-in-time claim. */
    observation: {
      providerIdentity: string;
      commitment: string;
      /** No single-response context slot (per-page slots instead). */
      responseContextSlot: number | null;
      /** Context slots observed per received page (bounded window). */
      pageContextSlots: Array<number | null>;
      boundedWindowDescription: string;
    };
  };
  ledger: PageLedgerEntry[];
  completeness: DiscoveryCompleteness;
}

/** One provider-specific paging adapter. The pipeline is provider-
 *  agnostic; only this adapter knows the wire contract. */
export interface PaginatedDiscoveryProvider {
  readonly name: string;
  /** Fetch one page (1-based). Returns the accounts plus whatever
   *  end-of-enumeration signal the provider gives (null when the end
   *  condition is undocumented — the run then reports the limitation). */
  fetchPage(
    page: number,
    ctx: { fetchImpl: typeof fetch; retry: RpcRetryPolicy; governor?: RateGovernor },
  ): Promise<{
    accounts: RawProgramAccount[];
    contextSlot: number | null;
    providerEndFlag: boolean | null;
    retriesUsed: number;
  }>;
}

export interface PaginatedDiscoveryOptions {
  cluster: Cluster;
  rpcUrl: string;
  provider: PaginatedDiscoveryProvider;
  /** Hard cap on pages per run (budget exhaustion → incomplete). */
  maxPages: number;
  /** Cap on parsed pools (same meaning as the classic path). */
  maxPools?: number;
  mode?: "full" | "identity-slice";
  rpcFilters?: unknown[];
  fetchImpl?: typeof fetch;
  retry?: RpcRetryPolicy;
  governor?: RateGovernor;
  /** Owner-only switch (default TRUE, fail-closed): while the provider
   *  pagination contract is not documented and verified, completeness
   *  stays "incomplete/provider-contract-unverified" even when every
   *  page was received. Setting this false is an owner decision that
   *  must be backed by documented provider guarantees (Part 2 §2). */
  requireDocumentedContract?: boolean;
}

// ─── Helius getProgramAccountsV2 adapter ─────────────────────────────────────

/**
 * Adapter for the provider-extended paginated enumeration.
 *
 * CONTRACT STATUS: UNVERIFIED. The paging contract implemented here
 * (`page` parameter, 1-based; empty page = end of enumeration; optional
 * `context.slot` in the `{ context, value }` wrapper) follows the
 * provider's published documentation as of the implementation date, but
 * has NOT been validated against a live provider inside this repository
 * (no provider credentials in CI). Cursor progression semantics,
 * no-gap/no-duplicate behavior, behavior during concurrent account
 * changes, retention and provider limits remain owner verification
 * items (Part 2 §2).
 */
export function makeHeliusGpaV2Provider(opts: {
  rpcUrl: string;
  programId: PublicKey;
  filters: unknown[];
  dataSlice?: { offset: number; length: number };
}): PaginatedDiscoveryProvider {
  return {
    name: "getProgramAccountsV2",
    async fetchPage(page, ctx) {
      const body = {
        jsonrpc: "2.0",
        id: 1,
        method: "getProgramAccountsV2",
        params: [
          opts.programId.toBase58(),
          {
            encoding: "base64",
            filters: opts.filters,
            page,
            ...(opts.dataSlice !== undefined ? { dataSlice: opts.dataSlice } : {}),
          },
        ],
      };
      let retriesUsed = 0;
      const json = await withRpcRetry(
        async (attempt) => {
          retriesUsed = attempt - 1;
          const acquire = ctx.governor ? await ctx.governor.acquire() : null;
          try {
            const res = await ctx.fetchImpl(opts.rpcUrl, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify(body),
            });
            if (!res.ok) {
              throw new RpcHttpError(
                res.status,
                `getProgramAccountsV2 HTTP ${res.status} from ${endpointKindFor(opts.rpcUrl)} endpoint`,
                parseRetryAfterMs(res.headers.get("retry-after")),
              );
            }
            return (await res.json()) as {
              error?: { code?: unknown; message?: unknown } | null;
              result?:
                | RawProgramAccount[]
                | { context?: { slot?: unknown }; value?: RawProgramAccount[] };
            };
          } finally {
            acquire?.();
          }
        },
        ctx.retry,
        { label: `getProgramAccountsV2 page ${page}` },
      );
      if (json.error !== undefined && json.error !== null) {
        const code = typeof json.error.code === "number" ? json.error.code : 0;
        const message = typeof json.error.message === "string" ? json.error.message : "unknown";
        throw new RpcRpcError(code, `getProgramAccountsV2 RPC error ${code}: ${message}`);
      }
      const result = json.result;
      const accounts = Array.isArray(result)
        ? result
        : Array.isArray(result?.value)
          ? (result as { value: RawProgramAccount[] }).value
          : [];
      const contextSlotRaw = Array.isArray(result) ? undefined : result?.context?.slot;
      const contextSlot =
        typeof contextSlotRaw === "number" && Number.isSafeInteger(contextSlotRaw)
          ? contextSlotRaw
          : null;
      // Page-based paging: an empty page signals the end of enumeration.
      // This end condition relies on the (unverified) provider contract
      // and is recorded as such in the ledger.
      const providerEndFlag = accounts.length === 0 ? true : null;
      return { accounts, contextSlot, providerEndFlag, retriesUsed };
    },
  };
}

// ─── Capability probe ────────────────────────────────────────────────────────

export type CapabilityProbeResult =
  | { supported: true; evidence: string }
  | { supported: false; evidence: string };

/**
 * Single bounded probe for `getProgramAccountsV2` support. Verified
 * evidence (2026-11-23): the public devnet and mainnet endpoints both
 * answer JSON-RPC error -32601 "Method not found" → the gate stays
 * closed. Anything other than a definitive method-found answer is
 * reported as unsupported-with-evidence (fail closed).
 */
export async function probeGetProgramAccountsV2(
  rpcUrl: string,
  programId: PublicKey,
  opts: { fetchImpl?: typeof fetch; governor?: RateGovernor } = {},
): Promise<CapabilityProbeResult> {
  const doFetch = opts.fetchImpl ?? fetch;
  const body = {
    jsonrpc: "2.0",
    id: 1,
    method: "getProgramAccountsV2",
    params: [
      programId.toBase58(),
      { encoding: "base64", page: 1, filters: [{ dataSize: RAYDIUM_V4_AMM_INFO_SIZE }] },
    ],
  };
  try {
    const acquire = opts.governor ? await opts.governor.acquire() : null;
    let res: Response;
    try {
      res = await doFetch(rpcUrl, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      });
    } finally {
      acquire?.();
    }
    const json = (await res.json()) as { error?: { code?: unknown; message?: unknown } | null };
    if (json?.error !== undefined && json?.error !== null) {
      const code = typeof json.error.code === "number" ? json.error.code : 0;
      const message = typeof json.error.message === "string" ? json.error.message : "unknown";
      return { supported: false, evidence: `HTTP ${res.status}, JSON-RPC error ${code}: ${message}` };
    }
    return { supported: true, evidence: `HTTP ${res.status}, no JSON-RPC error on page-1 probe` };
  } catch (err) {
    return {
      supported: false,
      evidence: `probe transport failure (fail closed): ${String((err as Error)?.message ?? err).slice(0, 200)}`,
    };
  }
}

// ─── Paginated discovery run ─────────────────────────────────────────────────

type ParsedAttempt =
  | { kind: "malformed"; record: null }
  | { kind: "parsed" | "unsupported"; record: DiscoveredPoolRecord };

/** Parse a pool record from a raw account payload (classic-path logic). */
function parseRawAccount(
  raw: RawProgramAccount,
  mode: "full" | "identity-slice",
  cluster: Cluster,
  endpointKind: EndpointKind,
  slotLowerBound: number,
  fetchedAtUnixMs: number,
): ParsedAttempt {
  let poolAddress: PublicKey;
  try {
    poolAddress = new PublicKey(raw.pubkey);
  } catch {
    return { kind: "malformed", record: null };
  }
  const data = new Uint8Array(Buffer.from(raw.account.data[0], "base64"));
  const expectedLen = mode === "identity-slice" ? SLICE_LENGTH : RAYDIUM_V4_AMM_INFO_SIZE;
  if (data.length !== expectedLen) return { kind: "malformed", record: null };
  let parsed: V4PoolAccount;
  try {
    parsed =
      mode === "identity-slice"
        ? parseV4AmmInfoSlice(poolAddress, data)
        : parseV4AmmInfo(poolAddress, data);
  } catch {
    return { kind: "malformed", record: null };
  }
  let wsolSide: DiscoveredPoolRecord["wsolSide"] = null;
  let unsupported = false;
  try {
    wsolSide = identifyBaseToken(parsed);
  } catch {
    unsupported = true; // 7019-guard failure: keep the record, exclude from the funnel
  }
  return {
    kind: unsupported ? "unsupported" : "parsed",
    record: {
      poolAddress,
      ammProgramId: RAYDIUM_V4_PROGRAM_ID,
      pool: {
        coinVault: parsed.coinVault,
        pcVault: parsed.pcVault,
        baseMint: parsed.baseMint,
        quoteMint: parsed.quoteMint,
        lpMint: parsed.lpMint,
      },
      wsolSide,
      raw: {
        mode,
        dataLen: data.length,
        fingerprint: accountFingerprint(poolAddress, data),
        owner: RAYDIUM_V4_PROGRAM_ID.toBase58(),
      },
      provenance: {
        source: "raydium-v4-gpa",
        cluster,
        endpointKind,
        slotLowerBound,
        slotUpperBound: 0, // patched after the run (see below)
        fetchedAtUnixMs,
      },
    },
  };
}

function pageFingerprint(accounts: RawProgramAccount[]): string {
  const h = createHash("sha256");
  for (const a of accounts) h.update(a.pubkey);
  return h.digest("hex");
}

const DEFAULT_PAGED_RETRY: RpcRetryPolicy = {
  maxRetries: 3,
  baseDelayMs: 250,
  maxDelayMs: 8_000,
  jitterRatio: 0.25,
  overallDeadlineMs: 45_000,
};

/**
 * Run the paginated discovery. NEVER throws for provider-side page
 * failures — incompleteness is reported in `completeness` and the
 * ledger; only a truly exceptional local error escapes.
 */
export async function discoverRaydiumV4PoolsPaginated(
  connection: { getSlot: () => Promise<number | bigint> },
  opts: PaginatedDiscoveryOptions,
): Promise<PaginatedDiscoveryRun> {
  const startedAt = Date.now();
  const endpointKind = endpointKindFor(opts.rpcUrl);
  const mode = opts.mode ?? "identity-slice";
  const maxPools = opts.maxPools ?? Number.POSITIVE_INFINITY;
  const fetchImpl = opts.fetchImpl ?? fetch;
  const retry = opts.retry ?? DEFAULT_PAGED_RETRY;
  const requireDocumentedContract = opts.requireDocumentedContract ?? true;

  const slotLowerBound = Number(await connection.getSlot());

  const ledger: PageLedgerEntry[] = [];
  const records = new Map<string, DiscoveredPoolRecord>();
  const seenPubkeys = new Set<string>();
  const pageContextSlots: Array<number | null> = [];
  let rawAccounts = 0;
  let iteratedAccounts = 0;
  let malformedSkipped = 0;
  let unsupportedPairs = 0;
  let crossPageDuplicates = 0;
  let parsedPools = 0;
  let truncatedByMaxPools = false;
  let pagesRequested = 0;
  let pagesReceived = 0;

  let completeness: DiscoveryCompleteness = {
    kind: "incomplete",
    reason: "provider-contract-unverified",
    detail:
      "provider completeness guarantees are not documented/verified in this repository " +
      "(cursor progression, no-gap/no-duplicate behavior, account-change behavior, retention, limits)",
  };

  for (let page = 1; page <= opts.maxPages; page++) {
    pagesRequested = page;
    let pageResult: Awaited<ReturnType<PaginatedDiscoveryProvider["fetchPage"]>>;
    try {
      pageResult = await opts.provider.fetchPage(page, {
        fetchImpl,
        retry,
        ...(opts.governor !== undefined ? { governor: opts.governor } : {}),
      });
    } catch (err) {
      ledger.push({
        page,
        accountCount: 0,
        pageFingerprint: "",
        contextSlot: null,
        receivedAtUnixMs: Date.now(),
        retriesUsed: 0,
        endCondition: null,
        error: String((err as Error)?.message ?? err).slice(0, 240),
      });
      completeness = {
        kind: "incomplete",
        reason: "page-request-failed",
        detail: `page ${page} failed after bounded retries: ${String((err as Error)?.message ?? err).slice(0, 160)}`,
      };
      break;
    }
    pagesReceived += 1;
    pageContextSlots.push(pageResult.contextSlot);
    for (const raw of pageResult.accounts) {
      rawAccounts += 1;
      if (seenPubkeys.has(raw.pubkey)) {
        crossPageDuplicates += 1;
        continue; // counted, kept-first — never silently dropped
      }
      seenPubkeys.add(raw.pubkey);
      iteratedAccounts += 1;
      const attempt = parseRawAccount(raw, mode, opts.cluster, endpointKind, slotLowerBound, startedAt);
      if (attempt.kind === "malformed") {
        malformedSkipped += 1;
        continue;
      }
      if (attempt.kind === "unsupported") unsupportedPairs += 1;
      records.set(attempt.record.poolAddress.toBase58(), attempt.record);
      parsedPools += 1;
      if (parsedPools >= maxPools) {
        truncatedByMaxPools = true;
        break;
      }
    }
    ledger.push({
      page,
      accountCount: pageResult.accounts.length,
      pageFingerprint: pageFingerprint(pageResult.accounts),
      contextSlot: pageResult.contextSlot,
      receivedAtUnixMs: Date.now(),
      retriesUsed: pageResult.retriesUsed,
      endCondition:
        pageResult.providerEndFlag === true
          ? "empty-page"
          : pageResult.providerEndFlag === null
            ? null
            : "provider-flag",
    });
    if (truncatedByMaxPools) break;
    if (pageResult.providerEndFlag === true) {
      completeness = requireDocumentedContract
        ? {
            kind: "incomplete",
            reason: "provider-contract-unverified",
            detail:
              "end-of-enumeration observed (empty page) but the provider's no-gap/no-duplicate " +
              "guarantees are not documented/verified in this repository",
          }
        : { kind: "complete" };
      break;
    }
  }

  if (
    completeness.kind === "incomplete" &&
    completeness.reason === "provider-contract-unverified" &&
    pagesRequested === opts.maxPages &&
    pagesReceived === pagesRequested
  ) {
    // Ran through the whole page budget without an end signal.
    completeness = {
      kind: "incomplete",
      reason: "page-budget-exhausted",
      detail: `page budget (${opts.maxPages}) exhausted before end-of-enumeration`,
    };
  }

  const slotUpperBound = Number(await connection.getSlot());
  for (const rec of records.values()) {
    rec.provenance.slotUpperBound = slotUpperBound;
  }

  const ordered = [...records.values()].sort((a, b) =>
    a.poolAddress.toBase58().localeCompare(b.poolAddress.toBase58()),
  );

  return {
    records: ordered,
    stats: {
      rawAccounts,
      parsedPools,
      malformedSkipped,
      unsupportedPairs,
      duplicatesInBatch: 0,
      crossPageDuplicates,
      slotLowerBound,
      slotUpperBound,
      durationMs: Date.now() - startedAt,
      iteratedAccounts,
      truncated: truncatedByMaxPools,
      pagesReceived,
      pagesRequested,
      observation: {
        providerIdentity: `${opts.provider.name} @ ${endpointKind} endpoint`,
        commitment: "finalized-or-provider-default (per-page context slots recorded — NOT a point-in-time snapshot)",
        responseContextSlot: null,
        pageContextSlots,
        boundedWindowDescription:
          `bounded observation window: slot ${slotLowerBound} observed immediately BEFORE the first page and ` +
          `slot ${slotUpperBound} immediately AFTER the last page; per-page provider context slots are recorded ` +
          "in the ledger. A lower/upper finalized slot bracket — even within 400 slots — is NOT an atomic " +
          "snapshot: accounts may change during the window and pages may observe different context slots. " +
          "This run claims a bounded observation window, never a point-in-time snapshot.",
      },
    },
    ledger,
    completeness,
  };
}
