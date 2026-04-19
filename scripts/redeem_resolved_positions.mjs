#!/usr/bin/env node

import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { RelayClient, RelayerTxType } from "../frontend/node_modules/@polymarket/builder-relayer-client/dist/index.js";
import { createWalletClient, encodeFunctionData, http, zeroHash } from "../frontend/node_modules/viem/_esm/index.js";
import { privateKeyToAccount } from "../frontend/node_modules/viem/_esm/accounts/index.js";
import { polygon } from "../frontend/node_modules/viem/_esm/chains/index.js";

const CTF_ADDRESS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045";
const USDC_E_ADDRESS = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174";
const RELAYER_URL = "https://relayer-v2.polymarket.com";
const CHAIN_ID = 137;

const ctfRedeemAbi = [
  {
    constant: false,
    inputs: [
      { name: "collateralToken", type: "address" },
      { name: "parentCollectionId", type: "bytes32" },
      { name: "conditionId", type: "bytes32" },
      { name: "indexSets", type: "uint256[]" },
    ],
    name: "redeemPositions",
    outputs: [],
    payable: false,
    stateMutability: "nonpayable",
    type: "function",
  },
];

function loadEnv(envPath) {
  const values = {};
  const content = readFileSync(envPath, "utf8");
  for (const line of content.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const equalsIndex = line.indexOf("=");
    if (equalsIndex === -1) {
      continue;
    }
    const key = line.slice(0, equalsIndex).trim();
    const value = line.slice(equalsIndex + 1).trim();
    values[key] = value;
  }
  return values;
}

function getArgValue(flag, fallback) {
  const index = process.argv.indexOf(flag);
  if (index === -1 || index + 1 >= process.argv.length) {
    return fallback;
  }
  return process.argv[index + 1];
}

function hasFlag(flag) {
  return process.argv.includes(flag);
}

function buildRedeemTransaction(conditionId) {
  return {
    to: CTF_ADDRESS,
    data: encodeFunctionData({
      abi: ctfRedeemAbi,
      functionName: "redeemPositions",
      args: [USDC_E_ADDRESS, zeroHash, conditionId, [1n, 2n]],
    }),
    value: "0",
  };
}

function loadRedeemTargets(repoRoot, envFile) {
  const output = execFileSync(
    resolve(repoRoot, ".venv/bin/python"),
    [resolve(repoRoot, "scripts/classify_open_positions.py"), "--env-file", envFile, "--json"],
    {
      cwd: repoRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"],
    },
  );
  const payload = JSON.parse(output);
  const unique = new Map();
  for (const row of payload.positions || []) {
    if (row.status !== "resolved") {
      continue;
    }
    if (!row.redeemable) {
      continue;
    }
    if (!unique.has(row.condition_id)) {
      unique.set(row.condition_id, {
        conditionId: row.condition_id,
        title: row.title,
        slug: row.slug,
        winningOutcome: row.winning_outcome,
        token: row.token,
      });
    }
  }
  return [...unique.values()];
}

async function main() {
  const repoRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));
  const envFile = resolve(repoRoot, getArgValue("--env-file", ".env"));
  const env = loadEnv(envFile);
  const dryRun = hasFlag("--dry-run");
  const targets = loadRedeemTargets(repoRoot, envFile);
  const account = privateKeyToAccount(env.POLYMARKET_PRIVATE_KEY);

  console.log(`Signer: ${account.address}`);
  console.log(`Proxy: ${env.POLYMARKET_PROXY_ADDRESS}`);
  console.log(`Resolved redeem targets: ${targets.length}`);
  for (const target of targets) {
    console.log(`- ${target.slug} | winner=${target.winningOutcome} | condition=${target.conditionId}`);
  }

  if (targets.length === 0) {
    console.log("Nothing to redeem.");
    return;
  }
  if (dryRun) {
    console.log("Dry run only. No transactions submitted.");
    return;
  }

  const wallet = createWalletClient({
    account,
    chain: polygon,
    transport: http(env.POLYGON_RPC_URL || "https://polygon-rpc.com"),
  });
  const relayClient = new RelayClient({
    host: RELAYER_URL,
    chain: CHAIN_ID,
    signer: wallet,
    relayerApiKey: env.POLYMARKET_RELAYER_API_KEY,
    relayerApiKeyAddress: env.POLYMARKET_RELAYER_API_KEY_ADDRESS,
    txType: RelayerTxType.PROXY,
  });

  const successes = [];
  const failures = [];

  for (const target of targets) {
    const tx = buildRedeemTransaction(target.conditionId);
    const metadata = `Redeem ${target.slug}`;
    process.stdout.write(`Submitting ${target.slug}... `);
    try {
      const response = await relayClient.execute([tx], metadata);
      const result = await response.wait();
      if (!result?.transactionHash) {
        throw new Error("relayer returned no mined transaction hash");
      }
      console.log(`ok ${result.transactionHash}`);
      successes.push({
        ...target,
        transactionHash: result.transactionHash,
        state: result.state,
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      console.log(`failed ${message}`);
      failures.push({
        ...target,
        error: message,
      });
    }
  }

  console.log("");
  console.log(`Redeemed successfully: ${successes.length}`);
  console.log(`Failed: ${failures.length}`);
  if (failures.length > 0) {
    for (const failure of failures) {
      console.log(`FAIL ${failure.slug} | ${failure.conditionId} | ${failure.error}`);
    }
    process.exitCode = 1;
  }
}

await main();
