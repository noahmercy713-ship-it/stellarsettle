# StellarSettle Backend

Node.js API and Soroban RPC event monitor for shipment escalation alerts and off-chain shipment metadata.

## Local setup

```sh
cp .env.example .env
npm install
npm run dev
```

Set `STELLAR_CONTRACT_ID` to enable the event monitor. By default, it scans recent contract events on Testnet and persists `dispute_escalated` alerts and metadata in a local SQLite database. The API exposes `GET /api/health`, `GET /api/events`, `GET /api/stream` (SSE), shipment endpoints, and metadata read/write endpoints. Metadata writes are disabled until a non-empty `METADATA_API_KEY` is configured; keep that key on the server and never expose it in frontend variables.

The event monitor starts from the latest ledger minus ten when `START_LEDGER` is unset. Configure a ledger explicitly when recovering an older range. This service does not sign or submit contract transactions; the connected wallet remains responsible for user-authorized actions.

```sh
npm run typecheck
npm run build
npm start
```

## API Endpoints

### Health & Events

- **GET /api/health** — Check service status, network, and event monitor state
- **GET /api/events** — Retrieve paginated `dispute_escalated` events (query params: `after` ledger, `limit` max 100)
- **GET /api/stream** — Server-Sent Events stream of new disputes (SSE)

### Stats

- **GET /api/stats** — Contract-wide aggregate metrics for dashboard/landing widgets (#56)
  - Returns: `{ totalShipments, totalVolume, totalDisputes, completedShipments, totalEscrowed?, fetchedAt }`
  - `totalVolume` is returned as a string to preserve i128 precision (stroops)
  - `totalEscrowed` is included only when `STELLAR_STATS_TOKEN` env var is set to a token SAC address
  - Results are cached in-memory for 30 seconds (`x-cache` header: `hit` / `miss` / `stale`)
  - Returns 503 if `STELLAR_CONTRACT_ID` is not configured or the contract call fails
  - Description: Aggregates on-chain figures from the contract's `get_contract_stats` and `get_total_escrowed_value` read functions

### Shipments

- **GET /api/shipments** — List shipment IDs with pagination (query params: `after` cursor, `limit` max 100)
  - Returns: `{ shipments: string[], nextCursor?: number }`
  - Description: Proxies contract's `list_shipments` function with pagination support matching `/api/events` style
  
- **GET /api/shipments/:id** — Retrieve full shipment details
  - Response: Full `Shipment` struct (all 51+ fields)
  - Returns 404 if shipment ID not found
  - Description: Reads full shipment state via Soroban RPC
  
- **GET /api/shipments/:id/milestones** — List all milestones for a shipment with notes
  - Response: `{ milestones: Array<Milestone & { notes: MilestoneNote[] }> }`
  - Returns 404 if shipment ID not found
  - Description: Returns milestone list with status, proof hash, and associated notes for each milestone

### Metadata

- **GET /api/metadata/:shipmentId** — Retrieve all metadata for a shipment
  - Returns: `{ [key: string]: { value: string, updatedAt: string } }`

- **PUT /api/metadata/:shipmentId/:key** — Store off-chain metadata (requires `X-API-Key` header)
  - Body: `{ value: string }`
  - Returns 401 if `METADATA_API_KEY` not configured or mismatched
  - Returns 400 if value exceeds 4000 characters