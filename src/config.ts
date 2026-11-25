// SPDX-License-Identifier: Apache-2.0
//
// Configuration — env-driven with safe defaults (org convention inherited
// from @graveyield/indexer). Two-cluster support is the point of Phase 2:
//
//   * DISCOVERY cluster (default mainnet-beta) — where Raydium V4 pools
//     actually live; all discovery/derivation RPC is read-only there.
//   * VERIFICATION cluster (default devnet) — where the deployed
//     GraveScanner consumes attestations; evidence issuance (issued_slot +
//     slot_hash) MUST happen against this cluster, because the on-chain
//     freshness check resolves issued_slot in THIS cluster's SlotHashes.
//
// Secret handling:
//
//   * The activity oracle key is loaded from a FILE (`ORACLE_KEY_FILE`,
//     recommended: solana-keygen JSON, mode 0600) or the `ACTIVITY_ORACLE_KEY`
//     env var (base58 32-byte seed). Key material is parsed into memory and
//     NEVER logged, echoed, serialized, or committed. See docs/ORACLE_PIPELINE.md
//     for the custody/rotation runbook.

import { PublicKey } from "@solana/web3.js";
import bs58 from "bs58";

/** NS-3 GraveScanner program ID (assigned, DR-002; deployed and accepted 2026-11-12 — see graveyield-protocol docs/DEVNET.md). */
export const DEVNET_SCANNER_PROGRAM_ID = new PublicKey(
  "EFJpKtWx5gDnc4Hh6GEEgPLUrPh377E4Nfic8jsJAQJL",
);

/** NS-3 GraveVault program ID (assigned, DR-002; deployed and accepted 2026-11-12 — see graveyield-protocol docs/DEVNET.md). */
export const DEVNET_VAULT_PROGRAM_ID = new PublicKey(
  "7VxUGJNgi1bZ8x4rTtLBNdgLs9sCLmUTpVE3on1nshya",
);

/**
 * NS-3 ProtocolConfig authority — the dedicated GOVERNANCE authority,
 * initial and permanent config authority for BOTH programs (DR-002
 * arrangement). The deployer `cfJKAz…4831` pays fees and holds the
 * upgrade authority but is NOT the config authority. Public key only.
 */
export const NS3_GOVERNANCE_AUTHORITY = "BMnrpx9mSZDzCmM8aJipnzgfLzfxa5tfFgWi3ZvBUwf5";

/** NS-3 dedicated GraveScanner activity (C1) oracle signer — public key only. */
export const NS3_ACTIVITY_ORACLE = "2ZygwHNXUBm3ESuvzxqL8ywdqKo8pkkcS6XfXQoZqf8o";

/** NS-3 dedicated GraveScanner launch-price (C2) oracle signer — public key only. */
export const NS3_LAUNCH_PRICE_ORACLE = "97JVKVd6E8Tmax8AciUNA4U92hwEQxN9MpeAessQBcee";

/**
 * SlotHashes holds ~512 entries; the on-chain freshness check fails once
 * issued_slot ages out. Evidence issuance must complete within this
 * window. Pipeline policy keeps a safety margin under the window.
 */
export const SLOTHASHES_WINDOW_SLOTS = 512;

/** Discovery snapshots older than this (slots) are stale and must be
 *  re-fetched before any evidence is issued from them. Default 400 ≈
 *  ~17 min at 2.5 s/slot — comfortably inside the SlotHashes window. */
export const DEFAULT_DISCOVERY_STALENESS_SLOTS = 400;

/** Verifier refuses to rate a bundle whose issued_slot is further back
 *  than this (it could never be accepted on chain). Default 450 < 512. */
export const DEFAULT_EVIDENCE_FRESHNESS_SLOTS = 450;

// ─── Option 2 governance configuration (Phase 1/3) ──────────────────────────
//
// Every knob is env-driven with a default that PRESERVES the previous
// pipeline behavior. The new capabilities (retry transport, bounded
// enrichment concurrency, validated RPC batching, paginated discovery)
// activate only when the operator opts in — except the bounded retry
// policy and enrichment concurrency, which ship with conservative
// defaults that keep artifacts byte-stable for identical inputs.

/** Retry/governance/batching/pagination configuration. */
export interface RpcGovernanceConfig {
  /** Bounded retry policy applied to pipeline RPC operations. */
  rpcRetry: {
    maxRetries: number;
    baseDelayMs: number;
    maxDelayMs: number;
    /** Overall wall-clock deadline per operation (ms). */
    overallDeadlineMs: number;
  };
  /** Enrichment worker concurrency (per-pool parallelism). The input
   *  order is preserved by the concurrency primitive, so artifacts stay
   *  deterministic. Default 4 (was: sequential). */
  enrichmentConcurrency: number;
  /** Explicit rate budget for the shared governor (requests/second).
   *  0 (default) = provider-aware defaults (public endpoints: 4 rps). */
  rateLimitPerSecond: number;
  /** Validated JSON-RPC batching for enrichment facts (default OFF —
   *  per-call web3.js path preserved until the owner opts in). */
  enrichmentBatchEnabled: boolean;
  /** Paginated discovery mode (default OFF = exact classic behavior):
   *  "off"    — classic unpaginated getProgramAccounts (current path);
   *  "probe"  — probe getProgramAccountsV2; if unsupported, fail closed
   *             with an explicit discovery-completeness failure (and,
   *             only when discoveryClassicFallback is "bounded", run the
   *             capped classic fallback which CANNOT report completeness
   *             above the classic-fallback account bound);
   *  "require" — paginated enumeration required; capability failure is
   *             a hard error. */
  discoveryPagination: "off" | "probe" | "require";
  /** Page budget for paginated discovery (exhaustion → incomplete). */
  discoveryMaxPages: number;
  /** Explicit bounded classic fallback when the capability gate is
   *  closed ("none" = fail closed, no fallback). */
  discoveryClassicFallback: "none" | "bounded";
  /** A bounded classic-fallback run may only be treated as complete
   *  when the corpus is smaller than this (a fully-iterated classic
   *  response at corpus scale cannot rule out provider-side response
   *  limits — fail closed above this bound). */
  classicFallbackCompleteMaxAccounts: number;
}

/** Load the Option 2 governance configuration from env. */
export function loadRpcGovernanceConfig(): RpcGovernanceConfig {
  const paginationEnv = (process.env.DISCOVERY_PAGINATION ?? "off").toLowerCase();
  const discoveryPagination =
    paginationEnv === "probe" ? "probe" : paginationEnv === "require" ? "require" : "off";
  const fallbackEnv = (process.env.DISCOVERY_CLASSIC_FALLBACK ?? "none").toLowerCase();
  const discoveryClassicFallback = fallbackEnv === "bounded" ? "bounded" : "none";
  return {
    rpcRetry: {
      maxRetries: intEnv("RPC_MAX_RETRIES", 3),
      baseDelayMs: intEnv("RPC_BASE_DELAY_MS", 250),
      maxDelayMs: intEnv("RPC_MAX_DELAY_MS", 8_000),
      overallDeadlineMs: intEnv("RPC_DEADLINE_MS", 45_000),
    },
    enrichmentConcurrency: intEnv("ENRICHMENT_CONCURRENCY", 4),
    rateLimitPerSecond: intEnv("RPC_RATE_LIMIT_PER_SEC", 0),
    enrichmentBatchEnabled: (process.env.RPC_BATCH_ENABLED ?? "false") === "true",
    discoveryPagination,
    discoveryMaxPages: intEnv("DISCOVERY_MAX_PAGES", 100),
    discoveryClassicFallback,
    classicFallbackCompleteMaxAccounts: intEnv("CLASSIC_FALLBACK_COMPLETE_MAX_ACCOUNTS", 10_000),
  };
}

/** Build the bounded retry policy object from the governance config. */
export function retryPolicyFromConfig(cfg: RpcGovernanceConfig): {
  maxRetries: number;
  baseDelayMs: number;
  maxDelayMs: number;
  jitterRatio: number;
  overallDeadlineMs: number;
} {
  return {
    maxRetries: cfg.rpcRetry.maxRetries,
    baseDelayMs: cfg.rpcRetry.baseDelayMs,
    maxDelayMs: cfg.rpcRetry.maxDelayMs,
    jitterRatio: 0.25,
    overallDeadlineMs: cfg.rpcRetry.overallDeadlineMs,
  };
}

/** Pipeline configuration, loaded from env with safe defaults. */
export interface PipelineConfig {
  /** Discovery RPC endpoint (read-only usage enforced by convention). */
  discoveryRpcUrl: string;
  discoveryCluster: "devnet" | "mainnet-beta";
  /** Verification / evidence-issuance RPC endpoint (default devnet). */
  verificationRpcUrl: string;
  /** GraveScanner program id on the verification cluster. */
  scannerProgramId: PublicKey;
  /** GraveVault program id on the verification cluster. */
  vaultProgramId: PublicKey;
  /** Max pools enumerated per discovery scan (RPC politeness). */
  maxPoolsPerScan: number;
  /** Signature-scan limit for last-swap derivations. */
  signatureScanLimit: number;
  /** Discovery staleness policy (slots). */
  discoveryStalenessSlots: number;
  /** Evidence freshness policy (slots). */
  evidenceFreshnessSlots: number;
  /** Directory for ledgers, journals and evidence bundles. */
  dataDir: string;
  /** Path to the oracle key file (never serialized anywhere). */
  oracleKeyFile: string | null;
  /** Base58 32-byte oracle seed from env (discouraged; file preferred). */
  oracleKeyEnv: string | null;
  /** When true, verify-path expects the canonical case to fail with
   *  AttestationOracleMismatch (signer ≠ configured oracle). Set
   *  ORACLE_IS_REGISTERED=true once a dedicated oracle key has been
   *  registered on chain via update_protocol_config. */
  oracleIsRegistered: boolean;
}

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const parsed = Number.parseInt(raw, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/** Load the pipeline configuration from the environment. */
export function loadPipelineConfig(): PipelineConfig {
  const discoveryRpcUrl = process.env.DISCOVERY_RPC_URL ?? "https://api.mainnet-beta.solana.com";
  const discoveryCluster =
    (process.env.DISCOVERY_CLUSTER ?? "mainnet-beta") === "devnet" ? "devnet" : "mainnet-beta";
  const verificationRpcUrl = process.env.DEVNET_RPC_URL ?? "https://api.devnet.solana.com";
  const scannerProgramId = process.env.SCANNER_PROGRAM_ID
    ? new PublicKey(process.env.SCANNER_PROGRAM_ID)
    : DEVNET_SCANNER_PROGRAM_ID;
  const vaultProgramId = process.env.VAULT_PROGRAM_ID
    ? new PublicKey(process.env.VAULT_PROGRAM_ID)
    : DEVNET_VAULT_PROGRAM_ID;

  const oracleKeyFile = process.env.ORACLE_KEY_FILE ?? null;
  const oracleKeyEnv = process.env.ACTIVITY_ORACLE_KEY ?? null;

  return {
    discoveryRpcUrl,
    discoveryCluster,
    verificationRpcUrl,
    scannerProgramId,
    vaultProgramId,
    maxPoolsPerScan: intEnv("MAX_POOLS_PER_SCAN", 500),
    signatureScanLimit: intEnv("SIGNATURE_SCAN_LIMIT", 1000),
    discoveryStalenessSlots: intEnv("DISCOVERY_STALENESS_SLOTS", DEFAULT_DISCOVERY_STALENESS_SLOTS),
    evidenceFreshnessSlots: intEnv("EVIDENCE_FRESHNESS_SLOTS", DEFAULT_EVIDENCE_FRESHNESS_SLOTS),
    dataDir: process.env.DATA_DIR ?? "./data",
    oracleKeyFile,
    oracleKeyEnv,
    oracleIsRegistered: (process.env.ORACLE_IS_REGISTERED ?? "false") === "true",
  };
}

/**
 * Parse an Ed25519 seed (32 bytes) from a key-file payload. Accepts:
 *   - solana-keygen JSON arrays of 64 bytes (`[se crate…, pubkey]`)
 *   - solana-keygen JSON arrays of 32 bytes (seed-only files)
 *   - JSON objects `{ "seed_base58": "…" }`
 * Returns ONLY the 32-byte seed. Never log the input.
 */
export function parseOracleSeedFromFilePayload(payload: string): Uint8Array {
  const trimmed = payload.trim();
  if (trimmed.startsWith("[")) {
    const arr = JSON.parse(trimmed) as unknown;
    if (!Array.isArray(arr) || (arr.length !== 32 && arr.length !== 64)) {
      throw new Error(
        `oracle key file must be a JSON array of 32 (seed) or 64 (secret) bytes, got ${Array.isArray(arr) ? arr.length : "non-array"}`,
      );
    }
    const bytes = Uint8Array.from(arr as number[]);
    return bytes.length === 64 ? bytes.slice(0, 32) : bytes;
  }
  const obj = JSON.parse(trimmed) as { seed_base58?: string };
  if (typeof obj?.seed_base58 === "string") {
    const decoded = bs58.decode(obj.seed_base58);
    if (decoded.length !== 32) {
      throw new Error(`seed_base58 must decode to 32 bytes, got ${decoded.length}`);
    }
    return new Uint8Array(decoded);
  }
  throw new Error("oracle key file payload not recognized (expected JSON array or {seed_base58})");
}

/** Parse a base58 32-byte seed from the env-var form. Never log the input. */
export function parseOracleSeedFromEnv(value: string): Uint8Array {
  const decoded = bs58.decode(value.trim());
  if (decoded.length !== 32) {
    throw new Error(`ACTIVITY_ORACLE_KEY must decode to 32 bytes (Ed25519 seed), got ${decoded.length}`);
  }
  return new Uint8Array(decoded);
}
