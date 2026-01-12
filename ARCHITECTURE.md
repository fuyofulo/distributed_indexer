# Distributed Indexer Architecture

## Overview
The system is split into two layers:

1) **Ingestor**
   - Connects to Yellowstone gRPC.
   - Subscribes to multiple named **transaction** filters (e.g., token, defi, custom).
   - Builds a JSON payload for each update.
   - Routes that payload to a Kafka topic based on the filter name.

2) **Indexers**
   - Separate processes (can run on different machines).
   - Each indexer uses its own Kafka consumer group ID.
   - Subscribes to one or more topics (e.g., ingest.token, ingest.defi).
   - Parses and writes to its database.

## Routing Model
We use Yellowstone filter names as routing labels.

Example filters (names are up to you):
- token
- defi
- custom

Each filter name maps to a Kafka topic:
- ingest.token
- ingest.defi
- ingest.custom

If a Yellowstone update matches a filter named `token`, the ingestor publishes to
`ingest.token`. This is why the filter names matter.

Fallback routing:
- If an update has no matching filter name, the ingestor tries to match by
  program_id (from the payload).
- If that also fails, it publishes to `ingest.raw`.

## Why Filter Names Matter
Yellowstone includes the filter name(s) that matched in each update:
- `filters: ["token"]`
- `filters: ["defi"]`

The ingestor uses this list to pick the Kafka topic:
- First matching filter name wins.
- If none match, it falls back to `ingest.raw`.

## How Yellowstone Filters Work
Yellowstone uses the *keys* of your subscription filters as labels in the update.

Example subscription:
- `transactions["token"]` with account_include = Tokenkeg + Tokenz
- `transactions["defi"]` with account_include = Defi programs

Then each update includes:
- `filters: ["token"]` if it matched the token owners
- `filters: ["defi"]` if it matched the defi owners

This lets the router route without parsing the raw payload. The router only needs
the `filters` array (and can fall back to program_id if needed).

In the current setup, the ingestor subscribes to **transaction filters**. This is
required for PumpSwap parsing because swap instructions live in transactions.

## Differentiating Token vs DeFi Programs
Yellowstone does not infer program semantics. It only matches what you told it to
match.

If you subscribe to:
- Token program owners => you get *all* token account updates across *all* apps
  (including DeFi programs).
- DeFi program owners => you get changes to *DeFi-owned* accounts, not token accounts.

To tag Pumpfun-specific activity:
- Add a filter for the Pumpfun program ID (transactions or accounts).
- Route those updates to `ingest.defi` (or `ingest.pumpfun`).
- Token transfers caused by Pumpfun will still appear in `ingest.token` because
  they touch token accounts. If you need to know “token transfers caused by
  Pumpfun,” the indexer has to correlate by transaction signature or parse
  instructions.

If a single update matches multiple filters (for example, PumpSwap transactions
also include token program instructions), the ingestor publishes to **all**
matching topics so both indexers can see the event.

## Consumer Groups (Indexers)
Each indexer runs in its own Kafka consumer group.

Example:
- Indexer 1 (token + defi): group.id = `indexer.token_defi`
- Indexer 2 (defi only): group.id = `indexer.defi`
- Indexer 3 (custom only): group.id = `indexer.custom`

Each group gets its own offsets, so they are isolated from each other. If you run
multiple copies of the same indexer with the same group.id, Kafka will load balance
partitions across them (scale-out).

## Indexer Access Control
Indexers authenticate directly with Kafka. Access control is enforced using Kafka ACLs.

The indexer supports SASL/SCRAM or SASL/PLAIN via environment variables:
- KAFKA_SECURITY_PROTOCOL (e.g., SASL_SSL, SASL_PLAINTEXT)
- KAFKA_SASL_MECHANISM (e.g., SCRAM-SHA-512)
- KAFKA_USERNAME / KAFKA_PASSWORD
- KAFKA_SSL_CA_LOCATION (optional)

Without Kafka security enabled, the indexer connects in PLAINTEXT mode.

## Updating Filters and New Programs
To add a new program to the pipeline:

1) Update `YELLOWSTONE_FILTERS` with the new filter name and program IDs.
2) Restart the ingestor (for now) so it rebuilds the subscription request.
3) The ingestor will publish to `ingest.<filter_name>`.
4) Start a new indexer that consumes that topic.

Future improvement: support live subscription updates without restart.

## Provider Filter Limits
Some Yellowstone endpoints only allow a single filter per subscription. When this
limit is set, the ingestor merges all filter owners into one combined filter for
the subscription request while still routing by program_id.

Set `YELLOWSTONE_MAX_FILTERS=1` to enable this mode.

## Environment Variables
See `ingestor/.env.example` for a full list and format.

## Next Steps
1) Add the first real indexer(s) that consume specific topics.
2) Add transaction filters if you need program-level activity instead of only
   account updates.
3) Add Kafka ACLs/SASL for indexer authorization.
4) Add metrics and a dead-letter topic for malformed payloads.
