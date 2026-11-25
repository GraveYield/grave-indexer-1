// SPDX-License-Identifier: Apache-2.0
//
// Option 2 Phase 1 — enrichment-stage hardening, integration level:
//   * VAULT PASSTHROUGH (item A): the discovery record's parsed AmmInfo
//     identity is passed into every activity derivation (the indexer
//     must forward it to the SDK as authoritative vaults);
//   * BOUNDED CONCURRENCY (item C): pools are enriched concurrently
//     while per-pool accounting, failure records and the enrichment
//     map stay deterministic;
//   * VALIDATED BATCHING (item D, opt-in): with RPC_BATCH_ENABLED=true
//     the four token facts travel in ONE validated JSON-RPC batch and
//     an unresolved batch item is an explicit per-pool enrich failure —
//     never a negative fact.

import { test, describe } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { PublicKey, type AccountInfo, type Connection } from "@solana/web3.js";
import { AccountDisc, RAYDIUM_V4_PROGRAM_ID, decodeScannerProtocolConfig } from "@graveyield/sdk";

import { cmdConsoleSnapshot } from "../src/cli.js";
import { ActivityIndexerLive } from "../src/activityLive.js";
import { parseV4AmmInfoSlice } from "../src/discovery.js";
import { DEVNET_SCANNER_PROGRAM_ID, DEVNET_VAULT_PROGRAM_ID, type PipelineConfig } from "../src/config.js";
import { buildAmmInfoSlice, synth } from "./fixtures.js";

// ─── Seams (mirrors consoleSnapshotCompleteness.test.ts) ─────────────────────

function protocolConfigBytes(): Uint8Array {
  const out = new Uint8Array(8 + 32 + 32 + 8 + 8 + 2 + 8 + 8 + 8 + 8 + 1 + 32 + 32 + 1);
  const view = new DataView(out.buffer);
  out.set(AccountDisc.ProtocolConfig, 0);
  let at = 8;
  const put32 = (pk: PublicKey): void => {
    out.set(pk.toBytes(), at);
    at += 32;
  };
  const put64 = (v: bigint): void => {
    view.setBigUint64(at, v, true);
    at += 8;
  };
  put32(synth(0xa1));
  put32(synth(0xa2));
  view.setBigInt64(at, 0n, true); at += 8;
  put64(7_776_000n); // inactivitySeconds — 90 d (spec default)
  view.setUint16(at, 9_900, true); at += 2; // priceCollapseBps
  put64(500_000_000n); // minTvlLamports — 0.5 SOL (spec default)
  put64(1_209_600n); // anchorStalenessSeconds — 14 d
  put64(1_000n); // lpBurnDustThreshold
  view.setBigInt64(at, 3_600n, true); at += 8; // certTtlSeconds
  out[at] = 0; at += 1;
  put32(synth(0xa3));
  put32(synth(0xa4));
  out[at] = 0; at += 1;
  return out;
}

const CONFIG_PDA = (
  await PublicKey.findProgramAddress([Buffer.from("protocol_config")], DEVNET_SCANNER_PROGRAM_ID)
)[0]!;

class VerificationRpc {
  asConnection(): Connection {
    return this as unknown as Connection;
  }
  async getAccountInfo(address: PublicKey): Promise<AccountInfo<Uint8Array> | null> {
    if (address.equals(CONFIG_PDA)) {
      return {
        lamports: 1_000_000,
        data: Buffer.from(protocolConfigBytes()),
        owner: DEVNET_SCANNER_PROGRAM_ID,
        executable: false,
        rentEpoch: 0n,
      } as unknown as AccountInfo<Uint8Array>;
    }
    return null;
  }
}

class DiscoveryRpc {
  async getSlot(_c?: unknown): Promise<bigint> {
    return 5_000_000n;
  }
  async getTokenAccountBalance(_vault: PublicKey): Promise<{ value: { amount: string; decimals: number } }> {
    return { value: { amount: "600000000", decimals: 9 } };
  }
  async getTokenSupply(_mint: PublicKey): Promise<{ value: { amount: string; decimals: number } }> {
    return { value: { amount: "1000000", decimals: 6 } };
  }
  async getSignaturesForAddress(_a: PublicKey, _o?: { limit?: number }): Promise<never[]> {
    return [];
  }
  async getTransaction(_s: string): Promise<null> {
    return null;
  }
  asConnection(): Connection {
    return this as unknown as Connection;
  }
}

function gpaFetch(n: number): { fetch: typeof fetch } {
  const fetchImpl = (async (_url: string | URL, init?: { body?: string }) => {
    const body = JSON.parse(String(init?.body)) as { method: string };
    assert.equal(body.method, "getProgramAccounts");
    const pools = Array.from({ length: n }, (_, i) => synth(0x60 + i));
    return new Response(
      JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        result: pools.map((p) => ({
          pubkey: p.toBase58(),
          account: { data: [Buffer.from(buildAmmInfoSlice()).toString("base64"), "base64"], owner: RAYDIUM_V4_PROGRAM_ID.toBase58() },
        })),
      }),
      { status: 200, headers: { "content-type": "application/json" } },
    );
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl };
}

/** Capturing indexer: records the EXACT opts each derivation receives. */
class CapturingIndexer extends ActivityIndexerLive {
  readonly captured: Array<{ pool: string; coinVault?: PublicKey; pcVault?: PublicKey }> = [];
  override async indexActivity(
    connection: Connection,
    poolAddress: PublicKey,
    opts?: { scanLimit?: number; forceRefresh?: boolean; coinVault?: PublicKey; pcVault?: PublicKey },
  ) {
    this.captured.push({
      pool: poolAddress.toBase58(),
      coinVault: opts?.coinVault,
      pcVault: opts?.pcVault,
    });
    return super.indexActivity(connection, poolAddress, { scanLimit: opts?.scanLimit });
  }
}

function snapshotConfig(dataDir: string): PipelineConfig {
  return {
    discoveryRpcUrl: "http://discovery.invalid",
    discoveryCluster: "mainnet-beta",
    verificationRpcUrl: "http://verification.invalid",
    scannerProgramId: DEVNET_SCANNER_PROGRAM_ID,
    vaultProgramId: DEVNET_VAULT_PROGRAM_ID,
    maxPoolsPerScan: 10,
    signatureScanLimit: 50,
    discoveryStalenessSlots: 400,
    evidenceFreshnessSlots: 450,
    dataDir,
    oracleKeyFile: null,
    oracleKeyEnv: null,
    oracleIsRegistered: false,
  };
}

function withTempDataDir(fn: (dataDir: string) => Promise<void>): Promise<void> {
  const dataDir = mkdtempSync(join(tmpdir(), "grave-indexer-option2-"));
  return fn(dataDir).finally(() => rmSync(dataDir, { recursive: true, force: true }));
}

function withEnv<T>(env: Record<string, string>, fn: () => Promise<T>): Promise<T> {
  const saved: Record<string, string | undefined> = {};
  for (const [k, v] of Object.entries(env)) {
    saved[k] = process.env[k];
    process.env[k] = v;
  }
  return fn().finally(() => {
    for (const [k, v] of Object.entries(saved)) {
      if (v === undefined) delete process.env[k];
      else process.env[k] = v;
    }
  });
}

// The authoritative vault pair encoded in the synthetic identity slice.
const expectedVaults = parseV4AmmInfoSlice(new PublicKey(synth(0x99).toBase58()), buildAmmInfoSlice());

describe("Option 2 enrichment hardening — cmdConsoleSnapshot", () => {
  test("vault passthrough: every activity derivation receives the discovery record's authoritative vault pair", async () => {
    await withTempDataDir(async (dataDir) => {
      const { fetch: discoveryFetchImpl } = gpaFetch(3);
      const indexer = new CapturingIndexer({
        cluster: "mainnet-beta",
        rpcUrl: "http://discovery.invalid",
      });
      await cmdConsoleSnapshot(snapshotConfig(dataDir), {
        maxPools: 10,
        enrich: 3,
        maxAgeHours: 24,
        discoveryConnection: new DiscoveryRpc().asConnection(),
        verificationConnection: new VerificationRpc().asConnection(),
        discoveryFetchImpl,
        activityIndexer: indexer,
      });
      assert.equal(indexer.captured.length, 3);
      for (const capture of indexer.captured) {
        assert.ok(capture.coinVault instanceof PublicKey, "coinVault must be a PublicKey");
        assert.ok(capture.pcVault instanceof PublicKey, "pcVault must be a PublicKey");
        assert.ok(capture.coinVault?.equals(expectedVaults.coinVault));
        assert.ok(capture.pcVault?.equals(expectedVaults.pcVault));
      }
    });
  });

  test("concurrent enrichment keeps per-pool accounting deterministic (same enriched set + failure records)", async () => {
    await withTempDataDir(async (dataDir) => {
      const { fetch: discoveryFetchImpl } = gpaFetch(5);
      const code = await cmdConsoleSnapshot(snapshotConfig(dataDir), {
        maxPools: 10,
        enrich: 5,
        maxAgeHours: 24,
        discoveryConnection: new DiscoveryRpc().asConnection(),
        verificationConnection: new VerificationRpc().asConnection(),
        discoveryFetchImpl,
      });
      const snapshot = JSON.parse(
        readFileSync(join(dataDir, "console-snapshot.json"), "utf8"),
      ) as { counts: { enriched: number }; pools: Array<{ pool: string }> };
      assert.equal(code, 0);
      assert.equal(snapshot.counts.enriched, 5);
      // Deterministic ascending pool order preserved under concurrency.
      const addresses = snapshot.pools.map((p) => p.pool);
      assert.deepEqual(addresses, [...addresses].sort());
    });
  });

  test("validated batch path (opt-in): facts arrive through ONE validated batch per pool", async () => {
    await withEnv({ RPC_BATCH_ENABLED: "true", ENRICHMENT_CONCURRENCY: "2" }, async () => {
      await withTempDataDir(async (dataDir) => {
        const { fetch: discoveryFetchImpl } = gpaFetch(2);
        const batchCalls: string[][] = [];
        const batchFetchImpl = (async (_url: string | URL, init?: { body?: string }) => {
          const body = JSON.parse(String(init?.body)) as Array<{ id: number; method: string; params: unknown[] }>;
          assert.ok(Array.isArray(body), "batch request body must be a JSON array");
          batchCalls.push(body.map((r) => r.method));
          return new Response(
            JSON.stringify(
              body.map((r) => ({
                jsonrpc: "2.0",
                id: r.id,
                result:
                  r.method === "getTokenAccountBalance"
                    ? { value: { amount: "600000000", decimals: 9, uiAmount: 0.6, uiAmountString: "0.6" } }
                    : { value: { amount: "1000000", decimals: 6, uiAmount: 1, uiAmountString: "1" } },
              })),
            ),
            { status: 200, headers: { "content-type": "application/json" } },
          );
        }) as unknown as typeof fetch;
        const code = await cmdConsoleSnapshot(snapshotConfig(dataDir), {
          maxPools: 10,
          enrich: 2,
          maxAgeHours: 24,
          discoveryConnection: new DiscoveryRpc().asConnection(),
          verificationConnection: new VerificationRpc().asConnection(),
          discoveryFetchImpl,
          batchFetchImpl,
        });
        assert.equal(code, 0);
        assert.equal(batchCalls.length, 2);
        for (const methods of batchCalls) {
          assert.deepEqual(methods, [
            "getTokenAccountBalance",
            "getTokenAccountBalance",
            "getTokenSupply",
            "getTokenSupply",
          ]);
        }
        const snapshot = JSON.parse(
          readFileSync(join(dataDir, "console-snapshot.json"), "utf8"),
        ) as { counts: { enriched: number } };
        assert.equal(snapshot.counts.enriched, 2);
      });
    });
  });

  test("validated batch path: an unresolved batch item is an explicit per-pool enrich failure (never a negative fact)", async () => {
    await withEnv({ RPC_BATCH_ENABLED: "true" }, async () => {
      await withTempDataDir(async (dataDir) => {
        const { fetch: discoveryFetchImpl } = gpaFetch(1);
        const batchFetchImpl = (async (_url: string | URL, init?: { body?: string }) => {
          const body = JSON.parse(String(init?.body)) as Array<{ id: number; method: string }>;
          // Item id 1 (memecoin vault balance) is dropped by the provider.
          return new Response(
            JSON.stringify(
              body
                .filter((r) => r.id !== 1)
                .map((r) => ({
                  jsonrpc: "2.0",
                  id: r.id,
                  result: { value: { amount: "1", decimals: 0, uiAmount: 1, uiAmountString: "1" } },
                })),
            ),
            { status: 200, headers: { "content-type": "application/json" } },
          );
        }) as unknown as typeof fetch;
        const code = await cmdConsoleSnapshot(snapshotConfig(dataDir), {
          maxPools: 10,
          enrich: 1,
          maxAgeHours: 24,
          discoveryConnection: new DiscoveryRpc().asConnection(),
          verificationConnection: new VerificationRpc().asConnection(),
          discoveryFetchImpl,
          batchFetchImpl,
        });
        // Fail-closed: the pool produced no facts → artifact incomplete.
        assert.equal(code, 1);
        const snapshot = JSON.parse(
          readFileSync(join(dataDir, "console-snapshot.json"), "utf8"),
        ) as { failures: Array<{ stage: string; reason: string }> };
        const enrichFailure = snapshot.failures.find((f) => f.stage === "enrich");
        assert.ok(enrichFailure, "an explicit enrich-stage failure must be recorded");
        assert.match(enrichFailure.reason, /unresolved/);
        assert.match(enrichFailure.reason, /missing item id 1/);
        const completeness = snapshot.failures.find((f) => f.stage === "enrichment-completeness");
        assert.ok(completeness, "enrichment completeness must be marked incomplete");
      });
    });
  });
});
