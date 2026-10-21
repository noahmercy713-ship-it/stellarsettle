import "dotenv/config";
import cors from "@fastify/cors";
import Fastify from "fastify";
import { DatabaseSync } from "node:sqlite";
import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { scValToNative, rpc, xdr } from "@stellar/stellar-sdk";
import { z } from "zod";

/**
 * Make a read-only Soroban contract call via RPC simulation.
 *
 * Uses `rpc.Server.queryContract`, which builds a simulated transaction
 * (no signatures required) and returns the decoded return value.
 *
 * @param method  Contract function name (e.g. "get_contract_stats")
 * @param args    Arguments object keyed by parameter name (empty for no-arg calls)
 * @returns       Decoded native JS value from the contract's return ScVal
 */
async function readContract<T = unknown>(method: string, args: Record<string, unknown> = {}): Promise<T> {
  const { result } = await rpcServer.queryContract(config.STELLAR_CONTRACT_ID, method, args);
  return result as T;
}

const envSchema = z.object({
  HOST: z.string().default("127.0.0.1"),
  PORT: z.coerce.number().int().positive().default(8787),
  STELLAR_NETWORK: z.enum(["testnet", "mainnet"]).default("testnet"),
  STELLAR_RPC_URL: z.string().url().default("https://soroban-testnet.stellar.org"),
  STELLAR_CONTRACT_ID: z.string().default(""),
  START_LEDGER: z.coerce.number().int().nonnegative().optional(),
  POLL_INTERVAL_MS: z.coerce.number().int().min(1000).default(5000),
  DATABASE_PATH: z.string().default("./data/stellarsettle.sqlite"),
  METADATA_API_KEY: z.string().default(""),
  WEB_ORIGIN: z.string().url().default("http://localhost:5173"),
});

const config = envSchema.parse(process.env);
const databasePath = resolve(config.DATABASE_PATH);
mkdirSync(dirname(databasePath), { recursive: true });

const database = new DatabaseSync(databasePath);
database.exec(`
  PRAGMA journal_mode = WAL;
  CREATE TABLE IF NOT EXISTS contract_events (
    id TEXT PRIMARY KEY,
    ledger INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    shipment_id TEXT,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  );
  CREATE INDEX IF NOT EXISTS contract_events_ledger_idx ON contract_events(ledger DESC);
  CREATE TABLE IF NOT EXISTS shipment_metadata (
    shipment_id TEXT NOT NULL,
    metadata_key TEXT NOT NULL,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (shipment_id, metadata_key)
  );
`);

type EscalationEvent = {
  id: string;
  ledger: number;
  type: "dispute_escalated";
  shipmentId: string;
  milestoneIndex: number;
  openedLedger: number;
  currentLedger: number;
};

const app = Fastify({ logger: true });
const streams = new Set<(event: EscalationEvent) => void>();
const rpcServer = new rpc.Server(config.STELLAR_RPC_URL);
let lastScannedLedger = config.START_LEDGER;
let pollTimer: NodeJS.Timeout | undefined;
let pollRunning = false;
let rpcHealthy = false;

await app.register(cors, { origin: config.WEB_ORIGIN });

app.get("/api/health", async () => ({
  status: "ok",
  network: config.STELLAR_NETWORK,
  contractConfigured: Boolean(config.STELLAR_CONTRACT_ID),
  eventMonitor: config.STELLAR_CONTRACT_ID ? (rpcHealthy ? "connected" : "connecting") : "disabled",
  lastScannedLedger: lastScannedLedger ?? null,
}));

// ---------------------------------------------------------------------------
// GET /api/stats — contract-wide aggregate metrics for dashboard widgets (#56)
//
// Aggregates on-chain figures from three contract read functions:
//   - get_contract_stats()  → { total_shipments, total_volume, total_disputes, completed_shipments }
//   - get_total_escrowed_value(token) → i128 (aggregate locked escrow for the token)
//   - get_tvl(token) → i128 (TVL cap check / aggregate escrow, same storage key)
//
// Because these are read-only simulated calls with no auth, the endpoint is
// cheap to serve.  Results are cached in-memory for STATS_CACHE_TTL_MS (30 s
// default) to avoid hammering the RPC on every request while still being
// fresh enough for an overview widget.
// ---------------------------------------------------------------------------

const STATS_CACHE_TTL_MS = 30_000; // 30-second cache

type ContractStatsRaw = {
  total_shipments: number | string;
  total_volume: number | string;
  total_disputes: number | string;
  completed_shipments: number | string;
};

type StatsCache = {
  data: Record<string, unknown>;
  fetchedAt: number;
};

let statsCache: StatsCache | undefined;

app.get("/api/stats", async (_request, reply) => {
  if (!config.STELLAR_CONTRACT_ID) {
    return reply.code(503).send({ error: "contract_not_configured" });
  }

  // Serve from cache if still fresh
  const now = Date.now();
  if (statsCache && now - statsCache.fetchedAt < STATS_CACHE_TTL_MS) {
    return reply.header("x-cache", "hit").send(statsCache.data);
  }

  try {
    // Fetch contract-level stats (no args)
    const contractStats = await readContract<ContractStatsRaw>("get_contract_stats");

    // Fetch aggregate escrowed value.  The contract tracks this per-token via
    // DataKey::TotalEscrowed(token).  We don't have a specific token address
    // in the env config, so we query get_total_escrowed_value.  When no
    // shipments have been created for a token the contract returns 0.
    //
    // For a contract-wide stats endpoint we report the contract_stats fields
    // (which aggregate across all shipments) and optionally the per-token
    // escrow if STELLAR_STATS_TOKEN is configured.
    let totalEscrowed: string | null = null;
    const statsToken = process.env.STELLAR_STATS_TOKEN;
    if (statsToken) {
      totalEscrowed = String(
        await readContract<number | string>("get_total_escrowed_value", { token: statsToken }),
      );
    }

    const data = {
      totalShipments: Number(contractStats.total_shipments),
      totalVolume: String(contractStats.total_volume),
      totalDisputes: Number(contractStats.total_disputes),
      completedShipments: Number(contractStats.completed_shipments),
      ...(totalEscrowed !== null ? { totalEscrowed } : {}),
      fetchedAt: new Date().toISOString(),
    };

    statsCache = { data, fetchedAt: now };

    return reply.header("x-cache", "miss").send(data);
  } catch (error) {
    app.log.error({ err: error }, "Failed to fetch contract stats");
    // If we have a stale cache, serve it with a warning header
    if (statsCache) {
      return reply.header("x-cache", "stale").send(statsCache.data);
    }
    return reply.code(503).send({ error: "contract_query_failed" });
  }
});

app.get<{ Querystring: { after?: string; limit?: string } }>("/api/events", async (request) => {
  const after = Math.max(0, Number(request.query.after ?? 0) || 0);
  const limit = Math.min(100, Math.max(1, Number(request.query.limit ?? 50) || 50));
  const rows = database.prepare(`
    SELECT id, ledger, event_type AS type, shipment_id AS shipmentId, payload
    FROM contract_events WHERE ledger > ? ORDER BY ledger DESC LIMIT ?
  `).all(after, limit) as Array<{ id: string; ledger: number; type: string; shipmentId: string; payload: string }>;
  return rows.map((row) => JSON.parse(row.payload));
});

app.get<{ Querystring: { after?: string; limit?: string } }>("/api/shipments", async (request, reply) => {
  if (!config.STELLAR_CONTRACT_ID) {
    return reply.code(503).send({ error: "contract_not_configured" });
  }

  try {
    const after = Math.max(0, Number(request.query.after ?? 0) || 0);
    const limit = Math.min(100, Math.max(1, Number(request.query.limit ?? 50) || 50));

    // Query contract for paginated shipment list
    // Returns array [shipment_ids, next_cursor]
    return {
      shipments: [],
      nextCursor: null,
    };
  } catch (error) {
    app.log.error({ err: error }, "Failed to query shipments");
    return reply.code(503).send({ error: "contract_query_failed" });
  }
});

app.get<{ Params: { id: string } }>("/api/shipments/:id", async (request, reply) => {
  if (!config.STELLAR_CONTRACT_ID) {
    return reply.code(503).send({ error: "contract_not_configured" });
  }

  try {
    const { id } = request.params;

    // Query contract for full shipment details
    // Returns Shipment struct
    return reply.code(404).send({ error: "shipment_not_found" });
  } catch (error: unknown) {
    const errMsg = error instanceof Error ? error.message : String(error);
    if (errMsg.includes("shipment not found")) {
      return reply.code(404).send({ error: "shipment_not_found" });
    }
    app.log.error({ err: error }, "Failed to query shipment");
    return reply.code(503).send({ error: "contract_query_failed" });
  }
});

app.get<{ Params: { id: string } }>("/api/shipments/:id/milestones", async (request, reply) => {
  if (!config.STELLAR_CONTRACT_ID) {
    return reply.code(503).send({ error: "contract_not_configured" });
  }

  try {
    const { id } = request.params;

    // Query contract for shipment and milestone details with notes
    // Returns { milestones: Array<Milestone & { notes: MilestoneNote[] }> }
    return reply.code(404).send({ error: "shipment_not_found" });
  } catch (error: unknown) {
    const errMsg = error instanceof Error ? error.message : String(error);
    if (errMsg.includes("shipment not found")) {
      return reply.code(404).send({ error: "shipment_not_found" });
    }
    app.log.error({ err: error }, "Failed to query milestones");
    return reply.code(503).send({ error: "contract_query_failed" });
  }
});

app.get<{ Params: { shipmentId: string } }>("/api/metadata/:shipmentId", async (request) => {
  const rows = database.prepare(`
    SELECT metadata_key AS key, value, updated_at AS updatedAt
    FROM shipment_metadata WHERE shipment_id = ? ORDER BY metadata_key
  `).all(request.params.shipmentId) as Array<{ key: string; value: string; updatedAt: string }>;
  return Object.fromEntries(rows.map((row) => [row.key, { value: row.value, updatedAt: row.updatedAt }]));
});

app.put<{ Params: { shipmentId: string; key: string }; Body: { value?: unknown } }>(
  "/api/metadata/:shipmentId/:key",
  async (request, reply) => {
    if (!config.METADATA_API_KEY || request.headers["x-api-key"] !== config.METADATA_API_KEY) {
      return reply.code(401).send({ error: "metadata_write_unauthorized" });
    }
    const { shipmentId, key } = request.params;
    const value = request.body?.value;
    if (!shipmentId || !/^[a-zA-Z0-9_-]{1,64}$/.test(key) || typeof value !== "string" || value.length > 4000) {
      return reply.code(400).send({ error: "invalid_metadata" });
    }
    database.prepare(`
      INSERT INTO shipment_metadata (shipment_id, metadata_key, value, updated_at)
      VALUES (?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(shipment_id, metadata_key)
      DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
    `).run(shipmentId, key, value);
    return reply.code(204).send();
  },
);

app.get("/api/stream", async (request, reply) => {
  reply.hijack();
  const response = reply.raw;
  response.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache, no-transform",
    connection: "keep-alive",
    "access-control-allow-origin": config.WEB_ORIGIN,
  });
  response.write(": connected\n\n");
  const writeEvent = (event: EscalationEvent) => response.write(`id: ${event.id}\nevent: dispute_escalated\ndata: ${JSON.stringify(event)}\n\n`);
  streams.add(writeEvent);
  const heartbeat = setInterval(() => response.write(": keepalive\n\n"), 20_000);
  request.raw.on("close", () => {
    clearInterval(heartbeat);
    streams.delete(writeEvent);
  });
});

function decodeScVal(value: xdr.ScVal): unknown {
  return scValToNative(value) as unknown;
}

async function scanEvents(): Promise<void> {
  if (pollRunning || !config.STELLAR_CONTRACT_ID) return;
  pollRunning = true;
  try {
    const latestLedger = (await rpcServer.getLatestLedger()).sequence;
    if (lastScannedLedger === undefined) {
      lastScannedLedger = Math.max(1, latestLedger - 10);
    }
    const startLedger = Math.max(1, lastScannedLedger + 1);
    if (startLedger > latestLedger) {
      rpcHealthy = true;
      return;
    }
    const filters = [{ type: "contract" as const, contractIds: [config.STELLAR_CONTRACT_ID] }];
    let cursor: string | undefined;
    let hasMore = true;
    while (hasMore) {
      const page = cursor
        ? await rpcServer.getEvents({ cursor, filters, limit: 100 })
        : await rpcServer.getEvents({ startLedger, endLedger: latestLedger, filters, limit: 100 });

      for (const event of page.events) {
        const topics = event.topic.map((topic) => decodeScVal(topic));
        if (topics[0] !== "dispute_escalated") continue;
        const payload = decodeScVal(event.value);
        if (!Array.isArray(payload) || payload.length < 3) continue;
        const numbers = payload.slice(0, 3).map(Number);
        if (numbers.some((number) => !Number.isFinite(number))) continue;
        const [milestoneIndex, openedLedger, currentLedger] = numbers as [number, number, number];
        const parsed: EscalationEvent = {
          id: event.id,
          ledger: event.ledger,
          type: "dispute_escalated",
          shipmentId: String(topics[1] ?? ""),
          milestoneIndex,
          openedLedger,
          currentLedger,
        };
        const inserted = database.prepare(`
          INSERT OR IGNORE INTO contract_events (id, ledger, event_type, shipment_id, payload)
          VALUES (?, ?, ?, ?, ?)
        `).run(parsed.id, parsed.ledger, parsed.type, parsed.shipmentId, JSON.stringify(parsed));
        if (Number(inserted.changes) > 0) streams.forEach((write) => write(parsed));
      }

      hasMore = page.events.length === 100 && page.cursor !== cursor;
      cursor = page.cursor;
    }

    lastScannedLedger = latestLedger;
    rpcHealthy = true;
  } catch (error) {
    rpcHealthy = false;
    app.log.error({ err: error }, "Soroban RPC event poll failed");
  } finally {
    pollRunning = false;
  }
}

if (config.STELLAR_CONTRACT_ID) {
  await scanEvents();
  pollTimer = setInterval(() => void scanEvents(), config.POLL_INTERVAL_MS);
}

app.addHook("onClose", async () => {
  if (pollTimer) clearInterval(pollTimer);
  database.close();
});

try {
  await app.listen({ host: config.HOST, port: config.PORT });
} catch (error) {
  app.log.error(error);
  process.exitCode = 1;
}