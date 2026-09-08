# us-rail-performance-archiver

A polling archiver for U.S. transit-agency [GTFS-Realtime](https://gtfs.org/realtime/) feeds. It snapshots raw protobuf/JSON payloads from ~23 agencies on a per-feed cadence, rolls them up into queryable Parquet, and (optionally) ships everything to S3 — cold raw bundles to Deep Archive, hot Parquet to standard storage. A separate analysis layer turns the curated data into TransitMatters-style headway / dwell / arrival metrics.

**Status:** alpha / single-host (shardable). Built primarily as an OOP / data-engineering exercise. Schemas and config layout may still change.

---

## Why it exists

Most agencies publish GTFS-RT as a live snapshot — there's no historical archive you can query. This project closes that gap:

- **Land everything first, transform later.** Every poll is durably written to disk before any decoding happens, so a schema change or a parsing bug can't lose history. Reprocess from the raw payloads any time.
- **Per-agency quirks are a strategy-pattern problem.** MTA's NYCT extensions, MARTA's bespoke JSON API, and the standard GTFS-RT protobuf all flow through the same archiver via pluggable `Parser` + `Decoder` classes registered by name in [config/feeds.yaml](config/feeds.yaml).
- **The hot path is lean.** An async event loop polls every feed on its own interval through a shared concurrency cap, conditional GET + content dedup skip unchanged payloads, and per-agency rate limiting keeps us inside published quotas. Parquet rollup and S3 shipping are separate processes you run on their own cadence.
- **Don't store the same bytes twice.** A feed that republishes an identical payload (or answers `304 Not Modified`) is recorded in metadata but writes no new raw frame, and the batching writer is content-addressed — duplicate payloads within a window collapse to one frame.

---

## Two-tier architecture

```
                  ┌──────────────────────┐
                  │   GTFS-RT endpoints  │
                  └──────────┬───────────┘
                             │ async HTTP poll (per-feed interval)
                             │ conditional GET (ETag / If-Modified-Since)
                             ▼
            ┌──────────────────────────────────────┐
            │               main.py                 │  ← FeedArchiver
            │  Scheduler (min-heap + jitter)        │
            │  Semaphore concurrency cap            │
            │  per-agency TokenBucket rate limit    │
            │  FeedHealth (backoff + quarantine)    │
            │  heartbeat file → autoheal            │
            └──────────────────┬─────────────────────┘
                               │ BatchingWriter: framed, content-addressed
                               │ window files + jsonl metadata
                               ▼
        archive/<feed>/raw/year=…/month=…/day=…/window=<unix>.bin
        archive/<feed>/metadata/year=…/month=…/day=…/data.jsonl

                               │ (separately, on a cron)
                               ▼
                  ┌────────────────────────────┐
                  │     pipeline/rollup.py     │  ← Rollup (ProcessPool)
                  │  (curated cold)            │
                  └──────────┬─────────────────┘
                             │ unframe → parse → decode → typed rows → parquet
                             ▼
              curated/<kind>/feed=…/year=…/month=…/day=…/data.parquet

                             │ (optional, on a cron)
                             ▼
                  ┌────────────────────────────┐
                  │      pipeline/ship.py      │  ← Shipper (ThreadPool)
                  └──────────┬─────────────────┘
                             │
                  ┌──────────┴─────────────────┐
                  ▼                             ▼
        s3://cold-bucket/         s3://hot-bucket/
        <feed>/year=…/…tar.gz     <kind>/feed=…/…parquet
        (DEEP_ARCHIVE)            (standard)
```

The landing zone is append-only and complete per poll. The curated zone is reproducible from the landing zone, so schema changes are safe — re-run `pipeline/rollup.py --force`.

---

## Quickstart

### Requirements

- Python 3.13 (managed via [`uv`](https://docs.astral.sh/uv/) — `.python-version` pins it)
- Optional: Docker / Docker Compose, a Datadog agent, an AWS account

### Local dev

```bash
uv sync
cp .env.example .env   # fill in any API keys you need (see "Auth" below)
uv run main.py --frequency 60 --polls 10
```

That schedules every feed in `config/feeds.yaml`, polling each on its own `poll_interval_seconds` (falling back to `--frequency`), runs ten loop iterations, and writes to `./archive/`. Drop `--polls` to run forever.

### Docker Compose

```bash
docker compose up -d
```

The dev compose file (`docker-compose.yml`) brings up a Datadog agent, the poller (`app`), and a once-a-day rollup+ship loop (`batch`). It mounts `./config` read-only and `./archive` / `./data/curated` read-write and loads `.env`. For production deployment see [Deployment](#deployment).

---

## CLI reference

Three entrypoints, each loads `config/feeds.yaml` and `.env`.

### `main.py` — archive feeds

```
uv run main.py [-n POLLS] [-f FREQUENCY] [-w WORKERS] [--shard-index I] [--shard-count N] [-v]
```

| flag | default | meaning |
|---|---|---|
| `-n / --polls` | infinite | number of loop iterations before exiting |
| `-f / --frequency` | `60` | default seconds between polls (per-feed `poll_interval_seconds` overrides) |
| `-w / --workers` | `10` | max concurrent in-flight polls (semaphore cap) |
| `--shard-index` | `0` | this worker's shard index, in `[0, shard-count)` |
| `--shard-count` | `1` | total number of shards (`1` = no sharding) |
| `-v / --verbose` | off | enable DEBUG logging |

The loop is a single asyncio event loop driven by a min-heap `Scheduler`: each feed comes due on its own interval (with ±10 % jitter and a startup spread so feeds don't herd). When a feed is due it's polled concurrently behind the worker semaphore; if no slot is free the cycle is shed (`poll.skipped`), and if the agency's token bucket is empty it's skipped (`poll.rate_limited`). A transport error or HTTP ≥ 400 feeds `FeedHealth`, which backs the feed off exponentially and quarantines it after repeated failures. A `SIGTERM`/`SIGINT` drains in-flight polls, flushes buffered frames, and closes clients cleanly.

### `pipeline/rollup.py` — landing → curated Parquet

```
uv run pipeline/rollup.py [--feed NAME] [--day YYYY-MM-DD] [-f] [-v]
```

| flag | meaning |
|---|---|
| `--feed` | restrict to one feed name (e.g. `bart-trips`) |
| `--day` | restrict to one UTC day |
| `-f / --force` | re-roll even if the output parquet already exists |
| `-v / --verbose` | DEBUG logging |

Discovers any day-partition in `archive/<feed>/metadata/...` older than *today UTC* and emits one Parquet per output kind (metadata, vehicles, trip_updates, alerts, marta_predictions). Today's partition is intentionally skipped — it's still being written to. Work is fanned across a `ProcessPoolExecutor` sized by `ROLLUP_WORKERS` (default = CPU count); the unframing loop reads both legacy single-payload `.bin` files and the newer framed `window=…` files transparently.

### `pipeline/ship.py` — curated + landing → S3

```
uv run pipeline/ship.py [--feed NAME] [--day YYYY-MM-DD] [--force] [--hot-only] [-v]
```

Requires `s3.enabled: true` in `feeds.yaml` and AWS creds in the environment. For every day-partition older than today:

- **Cold tier:** tars `raw/ + metadata/` for the day and uploads to `s3://<cold_bucket>/<cold_prefix><feed>/year=…/month=…/day=…tar.gz` with `StorageClass=DEEP_ARCHIVE`.
- **Hot tier:** uploads each curated Parquet under `s3://<hot_bucket>/<hot_prefix><kind>/feed=…/year=…/month=…/day=…/data.parquet`.

Existing keys are skipped unless `--force`. `--hot-only` ships just the curated parquets (use after re-rolling, so re-shipping doesn't re-charge the Deep Archive tarballs against the early-deletion minimum). Uploads fan across a thread pool sharing one boto3 client.

### `dashboard/api` — metrics/alerts HTTP API

```
uv run fastapi dev dashboard/api/main.py
```

Serves on `http://localhost:8000`. FastAPI app reading gold-layer Parquet and alert snapshots straight from the hot S3 bucket (`dashboard/api/services/data.py`, `dashboard/api/services/alerts.py`) — no local state. Agencies come from `config/feeds.yaml`. `DASHBOARD_API_CORS_ORIGINS` (comma-separated, default `http://localhost:3000`) controls which browser origins may call it.

---

## Reliability & efficiency

Everything below lives on the hot path and is exercised by [tests/](tests/).

| Concern | Mechanism | Where |
|---|---|---|
| **Don't re-download unchanged data** | Conditional GET — sends `If-None-Match` / `If-Modified-Since` from the last poll's `ETag` / `Last-Modified`; a `304` records `NotModifiedResponse` and stores no frame | [archiver.py](archiver/archiver.py), [poll_state.py](archiver/poll_state.py) |
| **Don't re-store identical bytes** | Content dedup — SHA-256 of the body is compared to the last stored digest; a match records `DuplicateResponse` with no new frame. The poll state is persisted per-feed to `poll_state/` so dedup survives restarts | [archiver.py](archiver/archiver.py), [poll_state.py](archiver/poll_state.py) |
| **Even, herd-free scheduling** | Min-heap `Scheduler` with per-reschedule ±jitter and a one-interval startup spread | [scheduler.py](archiver/scheduler.py) |
| **Bounded concurrency** | `asyncio.Semaphore(workers)` admission gate; over-cap cycles are shed, not queued unboundedly | [main.py](main.py) |
| **Stay within agency quotas** | Per-agency continuous `TokenBucket` consulted as a non-blocking gate | [rate_limit.py](archiver/rate_limit.py) |
| **Survive flaky / dead feeds** | `FeedHealth` — exponential backoff per consecutive failure (capped), then quarantine to a long interval after N fails; one success resets it | [health.py](archiver/health.py) |
| **Detect a hung process** | Heartbeat metric + `poll_state/.heartbeat` file refreshed every tick; the container `HEALTHCHECK` stats it and an `autoheal` sidecar restarts on staleness | [main.py](main.py), [dockerfile](dockerfile) |
| **Crash-safe writes** | All raw/parquet writes go through `*.tmp` → atomic rename; raw payloads are named by their own SHA-256 digest, so integrity is just re-hashing the file. Framed multi-payload window files are a legacy, read-only format (still supported for old cold-archive tarballs) whose per-payload digests serve the same purpose | [writer.py](archiver/writer.py) |
| **Notice schema drift** | Decoders validate input keys; a feed dropping a required field records `DecodeFailureResponse` (raw bytes still kept) and emits `decoder.schema_drift` | [parser.py](archiver/parser.py), [decoder.py](archiver/decoder.py) |
| **Horizontal scale** | Stable `sha256(agency_id) % shard_count` assignment; run N workers with disjoint `--shard-index` | [shard.py](archiver/shard.py) |

---

## Configuration

### `config/feeds.yaml`

Top-level keys (see [archiver/config.py](archiver/config.py) for the full pydantic schema):

```yaml
writer:
  writer_type: content_addressed  # "local" (timestamp-named) | "content_addressed" (sha256-named)
  landing_dir: ./archive
  curated_dir: ./data/curated
  poll_state_dir: ./data/poll_state # conditional-GET / dedup state + .heartbeat
  ship_window_seconds: 3600   # how often raw payloads are tarred and shipped to S3; ignored by the local writer

telemetry:
  enabled: false              # set true to emit DogStatsD metrics
  service: rail-archiver
  env: dev
  agent_host: localhost
  statsd_port: 8125
  tags: {}

s3:
  enabled: false              # set true to use pipeline/ship.py
  region: us-east-1
  cold_bucket: my-cold-bucket
  hot_bucket:  my-hot-bucket
  cold_prefix: ""             # e.g. "raw/"
  hot_prefix:  ""             # e.g. "curated/"

agencies:
  - agency_id: BART
    name: Bay Area Rapid Transit
    region: San Francisco Bay Area
    timezone: America/Los_Angeles   # IANA tz; validated at load
    base_url: https://api.bart.gov/gtfsrt
    mdb_feed_id: mdb-53             # optional Mobility Database id (analysis GTFS lookup)
    auth:
      type: none                    # none | api_key | bearer | basic
    rate_limit:                     # optional; omit for unlimited
      requests: 60                  # tokens minted per window
      per_seconds: 3600             # window length → refill_rate = requests/per_seconds
      burst: 10                     # bucket capacity; defaults to `requests`
    feeds:
      - name: bart-trips            # globally unique across all agencies
        path: /tripupdate.aspx
        expected_format: protobuf   # protobuf | json | auto
        decoder: standard           # standard | mta_nyct | marta_json
        poll_interval_seconds: 15   # optional; falls back to --frequency
```

Validation enforces unique agency names, globally-unique feed names, and valid IANA timezones.

### Auth

Credentials never live in `feeds.yaml` — they're referenced by env-var name and read from the process environment (loaded from `.env` by `python-dotenv`).

| `auth.type` | YAML keys | env vars consumed |
|---|---|---|
| `none` | — | — |
| `api_key` | exactly one of `header:` or `param:`, plus `env:` | the named `env` |
| `bearer` | `env:` | the named `env` |
| `basic` | `username_env:`, `password_env:` | both |

A missing required env var raises at startup, not on first request.

### Supported agencies

23 agencies are active today:

> BART, MetroLink (St. Louis), CATS (Charlotte), GCRTA (Cleveland), Metra, Metro Transit (Twin Cities), MTA NYCT, Pittsburgh Regional Transit, RTD (Denver), SacRT, SEPTA (rail + bus/trolley), Bay Area 511 (SFMTA, VTA, Caltrain, ACE, Capitol Corridor, SMART), MTS (San Diego), Sound Transit, TriMet, UTA, Valley Metro, WMATA, Tri-Rail, MARTA, METRO Houston, and LACMTA.

Several more (HRT, CTA, DART, MBTA, Miami-Dade, Baltimore MTA, NCTD, Embark) are present but commented out — some endpoints are unreachable, others were never enabled. See notes in [feeds.yaml](config/feeds.yaml).

---

## On-disk layout

### Landing zone

```
archive/
└── <feed-name>/
    ├── raw/
    │   └── year=YYYY/month=M/day=D/
    │       └── window=<unix>.bin       # batch writer: many framed payloads per window
    │       └── <unix>.bin              # local writer: one payload per poll (legacy)
    └── metadata/
        └── year=YYYY/month=M/day=D/
            └── data.jsonl              # one row per poll (appended), incl. dedup/304 polls
```

- Partition keys come from the response's capture time (UTC), not wall-clock at write time.
- The **batching writer** buffers a window's distinct payloads in memory and, when the window rolls over, writes a single content-addressed file: a `\x89GRT` magic header followed by `[len][sha256][payload]` frames. The metadata jsonl is the index that maps each frame's digest back to its exact poll timestamp.
- A poll with no new body (transport error, `304`, or duplicate digest) writes only the metadata row.
- All writes are `*.tmp` → rename, so a crash mid-write can't leave a half-payload.

### Curated zone

```
curated/
├── metadata/feed=…/year=…/month=…/day=…/data.parquet
├── vehicles/feed=…/year=…/month=…/day=…/data.parquet
├── trip_updates/feed=…/year=…/month=…/day=…/data.parquet
├── alerts/feed=…/year=…/month=…/day=…/data.parquet
└── marta_predictions/feed=…/year=…/month=…/day=…/data.parquet
```

Partitions are hive-style so PyArrow / DuckDB / Spark / Athena read them as a partitioned dataset directly. `agency` is intentionally a *column* rather than an outer partition, since feed names are already globally unique. Decoder `TableSpec`s rename columns to LAMP-style dotted paths (e.g. `vehicle.vehicle.id`) and can declare schema-only `extra_columns` written all-null for downstream readers.

### S3 keys

```
s3://<cold_bucket>/<cold_prefix><feed>/year=Y/month=M/day=D.tar.gz
s3://<hot_bucket>/<hot_prefix><kind>/feed=…/year=…/month=…/day=…/data.parquet
```

---

## Storage cost model

Cost is dominated by S3 storage. Local disk is sunk if you own the box; egress only matters if you query.

> **Note:** the rate below was measured before conditional GET + content dedup landed. Both reduce raw landing volume (a feed that republishes unchanged data no longer stores a frame), so treat these figures as a conservative upper bound.

### Measured rate (sample: May 19–24, 2026, 6 full days at steady state)

| Bucket | Mean GB/day | Storage class |
|---|---|---|
| Cold tarball (raw `.bin` + metadata jsonl, gzipped) | 5.7 | `DEEP_ARCHIVE` |
| Hot parquet (curated) | 5.85 | `STANDARD` |

Cold is derived as `raw landing × tar.gz ratio`. Sampled compression ratios on real day-22 data: 0.226 (nyct-1234567s), 0.238 (metrostl-trips), 0.316 (metromn-trips), 0.071 (bart-alerts). Volume-weighted ≈ **0.25**; raw landing averaged 22.7 GB/day, so 22.7 × 0.25 ≈ 5.7 GB/day shipped cold.

### Formula

Let:
- `D_cold` = 5.7 GB/day shipped to cold
- `D_hot`  = 5.85 GB/day shipped to hot
- `p_cold` = $0.00099 / GB-month (Deep Archive, us-east-1)
- `p_hot`  = storage-class price per GB-month
- `n`      = month index (starting at 1)

Storage is cumulative (no lifecycle deletion assumed). Bill in month `n` uses the average GB-months stored across that month:

```
month_n_cost = D_cold · 30 · (n − 0.5) · p_cold
            + D_hot  · 30 · (n − 0.5) · p_hot
```

Cumulative cost through month N (arithmetic series):

```
total(N) = 30 · (N² / 2) · (D_cold · p_cold + D_hot · p_hot)
```

PUT requests and Deep Archive's 40 KB-per-object Standard overhead are <$0.20/month at ~88 feeds/day — drop them.

### Projection — month 60 (5 years), by hot tier

| Hot tier | `p_hot` ($/GB-mo) | Month-60 bill | 5-yr cumulative |
|---|---|---|---|
| Standard | 0.023 | $250/mo | $7,570 |
| Standard-IA | 0.0125 | $141/mo | $4,254 |
| Glacier Instant Retrieval | 0.004 | $52/mo | $1,568 |

Cold Deep Archive at year 5 is ~$10/mo — rounding error. The bill is the hot bucket.

### Projection — Glacier Instant Retrieval timeline

Same formula with `p_hot = $0.004/GB-mo`. Marginal is the bill for that specific month; cumulative is the total spent from month 1 through that month.

| End of… | Cold stored (GB) | Hot stored (GB) | Marginal $/mo | Cumulative $ |
|---|---|---|---|---|
| Month 1  | 171    | 176    | $0.44  | $0.44   |
| Month 6  | 1,026  | 1,053  | $4.79  | $15.68  |
| Month 12 | 2,053  | 2,106  | $10.02 | $62.73  |
| Month 24 | 4,106  | 4,212  | $20.47 | $251    |
| Month 60 | 10,264 | 10,530 | $51.84 | $1,568  |

For ~$1.5K over 5 years you keep every payload from every configured agency, queryable in milliseconds. The catch is the $0.01/GB retrieval fee and 90-day minimum — fine for the typical "load a partition into a notebook" workflow, painful only if you regularly scan years of data at once.

### Caveats and unmodeled items

- **Volume is steady-state.** Adding more agencies (or re-enabling the commented-out feeds) scales the rate linearly with that feed's poll volume.
- **Local disk is not auto-pruned.** At ~22.7 GB raw/day, 1 TB fills in ~44 days. The shipper does not delete after a successful upload — wire that up before this model holds.
- **Deep Archive has a 180-day minimum** (early-deletion fee). Use `pipeline/ship.py --hot-only` for parquet re-rolls so re-shipping curated outputs doesn't re-charge the tarballs.
- **Egress for queries** is $0.09/GB outbound; pulling a full year of hot parquet ≈ $190 one-time.
- **Glacier Instant Retrieval** reads in milliseconds with a $0.01/GB retrieval fee and a 90-day minimum — likely the right fit for parquet queried occasionally from notebooks. A lifecycle rule `STANDARD → GIR at 30d` keeps recent partitions cheap to read while collapsing long-term storage cost.

---

## Output schemas

### `metadata` (one row per poll)

| column | type | notes |
|---|---|---|
| `timestamp` | double | epoch seconds, UTC, when the poll was received |
| `content_type` | string | HTTP `Content-Type` header |
| `status_code` | int | HTTP status |
| `response_type` | string | `ProtobufResponse` / `JsonResponse` / `ErrorResponse` / `UnknownResponse` / `TransportErrorResponse` / `DecodeFailureResponse` / `DuplicateResponse` / `NotModifiedResponse` |
| `digest` | string | SHA-256 of the payload — the join key from a framed window's frames back to this poll |
| `vehicle_count` / `trip_update_count` / `alert_count` | int | populated only for `ProtobufResponse` |
| `error_body` | string | first 2000 chars of body, only on `ErrorResponse` |
| `error_type` / `error_message` | string | populated only on `TransportErrorResponse` |
| `drift_missing_required` / `drift_extras` | list[string] | populated only on `DecodeFailureResponse` |

### `vehicles` (decoder: `standard`, `mta_nyct`)

`feed_timestamp`, `vehicle_id`, `vehicle_label`, `trip_id`, `route_id`, `direction_id`, `start_date`, `start_time`, `schedule_relationship`, `latitude`, `longitude`, `bearing`, `speed`, `current_stop_sequence`, `stop_id`, `current_status`, `occupancy_status`, `occupancy_percentage`, `vehicle_timestamp`. See [VehicleRow](archiver/decoder.py#L83).

### `trip_updates` (decoder: `standard`, `mta_nyct`)

One row per `StopTimeUpdate` (not per trip). `feed_timestamp`, `trip_update_timestamp`, `trip_id`, `route_id`, `direction_id`, `start_date`, `start_time`, `schedule_relationship`, `vehicle_id`, `vehicle_label`, `stop_sequence`, `stop_id`, `arrival_delay`, `arrival_time`, `arrival_uncertainty`, `departure_delay`, `departure_time`, `departure_uncertainty`, `stop_time_schedule_relationship`. See [StopTimeUpdateRow](archiver/decoder.py#L106).

### `alerts` (decoder: `standard`, `mta_nyct`)

One row per `InformedEntity` (a single alert may produce many rows). `feed_timestamp`, `alert_id`, `cause`, `effect`, `severity_level`, `header_text`, `description_text`, `url`, `agency_id`, `route_id`, `route_type`, `direction_id`, `trip_id`, `stop_id`. See [AlertRow](archiver/decoder.py#L131).

### `marta_predictions` (decoder: `marta_json`)

MARTA exposes its own JSON prediction API (not GTFS-RT). See [MartaPredictionRow](archiver/decoder.py#L27).

### `metrics/stop_day` (gold, written by [pipeline/gold.py](pipeline/gold.py))

One row per (route, direction, stop, service_date). `route_id`, `direction_id` (nullable), `stop_id`, `service_date`, `visit_count`, `trip_count`, `distinct_vehicle_count`, `headway_p50_s`, `headway_p90_s`, `headway_mean_s`, `headway_cov`, `dwell_p50_s`, `dwell_p90_s`, `first_service_unix`, `last_service_unix`, `service_span_s`. Headway/dwell percentiles are null when there's too little data (e.g. a single visit yields no headway). See [STOP_DAY_SCHEMA](analysis/metrics.py).

### `metrics/route_day` (gold, written by [pipeline/gold.py](pipeline/gold.py))

One row per (route, direction, service_date). Same throughput / span columns as `stop_day` plus `distinct_stop_count`, minus `stop_id`. `headway_p50_s` is the median of the route's per-stop headway medians (pooling raw inter-arrivals across stops would mix unrelated cadences); dwell is pooled. See [ROUTE_DAY_SCHEMA](analysis/metrics.py).

### `metrics/events` (gold, written by [pipeline/gold.py](pipeline/gold.py))

One row per ARR/DEP — each stop visit explodes into an `ARR` at its arrival and a `DEP` at its departure. Every route shares one time-ordered parquet per (feed, day). `route_id`, `direction_id` (nullable), `stop_id`, `stop_sequence` (nullable), `trip_id`, `vehicle_id`, `event_type` (`ARR`/`DEP`), `event_unix`, `service_date`. Same universe as the day marts (null `route_id` dropped, null `direction_id` kept). This is the queryable, single-file-per-day counterpart to the fanned-out gobble `events/feed=…/{route}-{dir}-{stop}/…/events.csv` tree from [event_export.py](analysis/event_export.py). See [EVENTS_SCHEMA](analysis/metrics.py).

### `metrics/adherence` (gold OTP, written by [pipeline/gold.py](pipeline/gold.py))

The schedule-joined fact table: one row per stop visit that matched the static GTFS schedule. Each visit is joined to its scheduled stop by `(trip_id, stop_id)` (not by observed time — that would mis-attribute a neighbouring trip when a vehicle runs early/late), and `route_id`/`direction_id`/`stop_sequence` are taken from the **schedule** (authoritative), so OTP covers trip_id-only feeds (NYCT subway, TriMet) that omit them in realtime. Columns: `route_id`, `direction_id` (nullable), `stop_id`, `stop_sequence` (nullable), `trip_id`, `vehicle_id` (nullable), `route_mode` (nullable; `rapid`/`bus`/`cr`/`other`), `service_date`, `arrival_unix`, `scheduled_arrival_unix` (nullable), `arrival_delay_s` (nullable; +late/−early), `departure_unix`, `scheduled_departure_unix` (nullable), `departure_delay_s` (nullable), `status` (`early`/`on_time`/`late`), `on_time` (bool). Scheduled times are anchored to the trip's service day, so owl trips (e.g. scheduled `25:30:00`) match the prior day. See [ADHERENCE_SCHEMA](analysis/adherence.py).

### `metrics/stop_day_otp` (gold OTP, written by [pipeline/gold.py](pipeline/gold.py))

One row per (route, direction, stop, service_date), aggregating `adherence`. `route_id`, `direction_id` (nullable), `stop_id`, `service_date`, `matched_count`, `on_time_count`, `early_count`, `late_count`, `on_time_pct` (fraction over matched visits), `arr_delay_p50_s`, `arr_delay_p90_s`, `arr_delay_mean_s`, `dep_delay_p50_s`. See [STOP_DAY_OTP_SCHEMA](analysis/adherence.py).

### `metrics/route_day_otp` (gold OTP, written by [pipeline/gold.py](pipeline/gold.py))

One row per (route, direction, service_date). Same aggregate columns as `stop_day_otp` plus `distinct_stop_count`, minus `stop_id`; delay percentiles are pooled across the route's stops. See [ROUTE_DAY_OTP_SCHEMA](analysis/adherence.py).

### `metrics/routes` (gold, written by [pipeline/gold.py](pipeline/gold.py))

A small, rarely-changing routes reference table, rebuilt daily off the same GTFS resolver as OTP so `dashboard/api` can read it via the same day-partition path as every other mart instead of resolving GTFS itself. `route_id`, `route_short_name` (nullable), `route_long_name` (nullable), `mode` (`rapid`/`bus`/`cr`/`other`), `service_date`. See `ROUTES_SCHEMA` in [pipeline/gold.py](pipeline/gold.py).

### `metrics/gtfs_versions` (written by [pipeline/gtfs.py](pipeline/gtfs.py))

One row per (feed, service_date) — a day-partitioned pointer to the GTFS snapshot in effect that day. `service_date`, `version_slug`, `feed_version` (nullable), `feed_start_date`, `feed_end_date`. Pair this with the version-partitioned marts below to answer "what was the schedule on day D" without re-hitting the archived-feeds catalog. See [GTFS_VERSIONS_SCHEMA](pipeline/gtfs.py).

### `metrics/gtfs_stops`, `metrics/gtfs_calendar`, `metrics/gtfs_calendar_dates`, `metrics/gtfs_shapes` (written by [pipeline/gtfs.py](pipeline/gtfs.py))

Static GTFS, normalized and persisted once per `(feed, version_slug)` rather than per day — see [docs/design/static-gtfs-normalization.md](docs/design/static-gtfs-normalization.md) for why. Partition path: `metrics/<mart>/feed=…/version=…/data.parquet`.

- `gtfs_stops` — `stop_id`, `stop_code` (nullable), `stop_name` (nullable), `stop_lat` (nullable), `stop_lon` (nullable), `version_slug`.
- `gtfs_calendar` — typed passthrough of `calendar.txt`: `service_id`, `monday`…`sunday` (bool), `start_date`, `end_date`, `version_slug`.
- `gtfs_calendar_dates` — typed passthrough of `calendar_dates.txt`: `service_id`, `date`, `exception_type` (1=added, 2=removed), `version_slug`.
- `gtfs_shapes` — one row per shape point: `shape_id`, `shape_pt_sequence`, `shape_pt_lat`, `shape_pt_lon`, `shape_dist_traveled` (nullable), `version_slug`.

See [GTFS_STOPS_SCHEMA / GTFS_CALENDAR_SCHEMA / GTFS_CALENDAR_DATES_SCHEMA / GTFS_SHAPES_SCHEMA](pipeline/gtfs.py).

### `metrics/gtfs_route_patterns`, `metrics/gtfs_directions`, `metrics/gtfs_checkpoints` (written by [pipeline/gtfs.py](pipeline/gtfs.py))

MBTA GTFS extensions, version-partitioned the same way as `gtfs_stops` above; most non-MBTA feeds don't publish these files, so they degrade to an empty-but-present mart rather than being absent.

- `gtfs_route_patterns` — `route_pattern_id`, `route_id`, `direction_id`, `route_pattern_name` (nullable), `route_pattern_typicality` (nullable; 0=not defined, 1=typical, 2=deviation, 3=atypical, 4=diversion), `representative_trip_id` (nullable), `version_slug`.
- `gtfs_directions` — `route_id`, `direction_id`, `direction` (nullable), `direction_destination` (nullable), `version_slug`.
- `gtfs_checkpoints` — reference table only: `checkpoint_id`, `checkpoint_name` (nullable), `version_slug`. MBTA's `checkpoint_id` lives on `stop_times.txt` (per trip, not per stop), so this mart doesn't derive a stop→checkpoint mapping — `StaticGtfs.stop_times` exposes `checkpoint_id` directly for callers that want that join themselves.

See [GTFS_ROUTE_PATTERNS_SCHEMA / GTFS_DIRECTIONS_SCHEMA / GTFS_CHECKPOINTS_SCHEMA](pipeline/gtfs.py).

---

## Analysis layer

The [analysis/](analysis/) package turns curated parquet into the metrics a rider actually cares about — travel time, headway, dwell, on-time arrival — following [TransitMatters' gobble](https://github.com/transitmatters) conventions. It's the "write-it-for-me" half of the project (the archiver is the OOP exercise; analysis is applied).

| module | role |
|---|---|
| [analysis/vehicle_day.py](analysis/vehicle_day.py) | per-(feed, day) vehicle motion + dwell from the `vehicles` parquet |
| [analysis/trip_updates_day.py](analysis/trip_updates_day.py) | derive Visits from `trip_updates` for feeds whose vehicle positions lack `stop_id`/`current_status` (e.g. Metro Transit MN light rail) |
| [analysis/marta_day.py](analysis/marta_day.py) | recover arrival events from MARTA's prediction stream (no vehicle positions) |
| [analysis/alert_snapshot.py](analysis/alert_snapshot.py) | aggregate a day's GTFS-RT alerts into a last-write-wins snapshot keyed by alert v3 id |
| [analysis/alert_classifier.py](analysis/alert_classifier.py) | classify alerts (TransitMatters delays/process.py port) |
| [analysis/event_export.py](analysis/event_export.py) | emit gobble-style per-stop ARR/DEP `events.csv` |
| [analysis/metrics.py](analysis/metrics.py) | fold a `Visit` stream into the **schedule-free** gold marts (headway, dwell, throughput, regularity, service span) |
| [analysis/adherence.py](analysis/adherence.py) | fold a `Visit` stream + GTFS schedule into the gold **on-time-performance** marts (delay, on-time %, early/late) |
| [analysis/timeutil.py](analysis/timeutil.py) | dependency-light local-time helpers (`service_date`, `fmt_local`) shared by the pandas-free gold path |
| [analysis/static_gtfs.py](analysis/static_gtfs.py) | load a static GTFS zip for scheduled travel-time / headway lookups |
| [analysis/gtfs_fetcher.py](analysis/gtfs_fetcher.py) | resolve a service date to the right archived static GTFS zip |

Driver scripts in [scripts/](scripts/) wrap these for CLI use:

- [scripts/export_events.py](scripts/export_events.py) — gobble `events.csv` for one/all feeds and days (auto GTFS enrichment when an agency declares `mdb_feed_id`)
- [scripts/export_marta_events.py](scripts/export_marta_events.py) — ARR-only events from MARTA predictions
- [scripts/build_alert_snapshot.py](scripts/build_alert_snapshot.py) — daily alert snapshot JSON.gz for one feed
- [scripts/fetch_static_gtfs.py](scripts/fetch_static_gtfs.py) — resolve + cache a static GTFS zip for a service date
- [scripts/feed_quality.py](scripts/feed_quality.py) — per-feed field coverage in the silver `vehicles` parquet (route / direction / stop / current_status), to see which feeds can drive which gold analyses
- [scripts/sync_datadog.py](scripts/sync_datadog.py) — idempotently upsert the committed monitors/dashboard into Datadog

### Gold tier

[pipeline/gold.py](pipeline/gold.py) is the daily aggregation runner (alongside [pipeline/rollup.py](pipeline/rollup.py) and [pipeline/ship.py](pipeline/ship.py)). For each (feed, day) it derives Visits from the silver `vehicles` (or `trip_updates`, via `--source trip-updates`) parquet and writes two families of partitioned parquet marts off a single load.

**Schedule-free** (always, via [analysis/metrics.py](analysis/metrics.py)) — headway p50/p90, headway mean + coefficient of variation (regularity), dwell p50/p90, trip / visit / distinct-vehicle counts, and service span:

- `curated/metrics/stop_day/feed=…/year=…/month=…/day=…/data.parquet` — one row per (route, direction, stop, service_date)
- `curated/metrics/route_day/…` — one row per (route, direction, service_date)
- `curated/metrics/events/…` — one row per ARR/DEP, every route in one partition per day (the columnar counterpart to the gobble `events/*.csv` tree)

Visits without a `route_id` are dropped; a null `direction_id` is kept as its own bucket.

**On-time performance** (when the feed's agency declares an `mdb_feed_id`, via [analysis/adherence.py](analysis/adherence.py)) — joins each visit to the static GTFS schedule and writes `metrics/adherence` (trip-stop fact), `metrics/stop_day_otp`, and `metrics/route_day_otp`. The default on-time window is `[−60s, +300s]` (≤1 min early, ≤5 min late), configurable via `--early-threshold-seconds` / `--late-threshold-seconds`; the verdict is judged on arrival. `on_time_pct`'s denominator is matched trips.

OTP runs in the daily prod batch (`pandas` is a runtime dependency). It's **best-effort**: it self-skips when an agency has no `mdb_feed_id`, when the schedule snapshot can't be resolved, or — defensively — when pandas is unavailable; none of these block the schedule-free marts. Pass `--no-otp` to skip it explicitly. GTFS zips are fetched and cached on demand (the `static_gtfs/` cache is not baked into the image).

Partition keys (`feed`, `year`, `month`, `day`) live in the path only, matching the silver layout. [notebooks/gold_metrics.ipynb](notebooks/gold_metrics.ipynb) validates the marts against the raw visits.

```bash
uv run python pipeline/gold.py --feed wmata-vehicles --day 2026-05-20   # schedule-free + OTP (GTFS auto-resolved)
uv run python pipeline/gold.py --all-days --no-otp                      # every feed/day on disk, schedule-free only
```

### Static GTFS reference tier

[pipeline/gtfs.py](pipeline/gtfs.py) persists each agency's static GTFS schedule as normalized parquet: `metrics/gtfs_stops`, `metrics/gtfs_calendar`, `metrics/gtfs_calendar_dates`, `metrics/gtfs_shapes`, and the MBTA-extension marts `metrics/gtfs_route_patterns`, `metrics/gtfs_directions`, `metrics/gtfs_checkpoints`. Unlike every other mart these are **not** day-partitioned — a schedule snapshot is valid for weeks or months, so they're written once per `(feed, version_slug)` and paired with a tiny day-partitioned `metrics/gtfs_versions` pointer mart that resolves "the schedule on day D" to a version. See [docs/design/static-gtfs-normalization.md](docs/design/static-gtfs-normalization.md) for the full design and why it departs from the day-partitioned convention every other mart uses.

```bash
uv run python pipeline/gtfs.py --feed wmata-vehicles --day 2026-05-20   # resolves + persists that day's schedule snapshot if new
uv run python pipeline/gtfs.py --all-days                                # every feed/day on disk
```

---

## Adding a new feed

1. **Find the endpoint.** Check the agency's developer portal for a GTFS-RT URL.
2. **Add the agency to [config/feeds.yaml](config/feeds.yaml):**
   ```yaml
   - agency_id: NEW_AGENCY
     name: My Transit Agency
     region: Somewhere
     timezone: America/New_York
     base_url: https://example.com/gtfs-rt
     auth:
       type: api_key
       header: X-API-Key
       env: NEW_AGENCY_API_KEY
     rate_limit:                     # optional
       requests: 60
       per_seconds: 60
     feeds:
       - name: newagency-vehicles    # must be globally unique
         path: /VehiclePositions.pb
         expected_format: protobuf
         decoder: standard
         poll_interval_seconds: 30
   ```
3. **Add the secret to `.env`** (and rotate it if you've ever exposed it).
4. **Pick a decoder.**
   - Standard GTFS-RT protobuf: `decoder: standard`.
   - GTFS-RT with proprietary extensions: subclass `StandardDecoder` in [archiver/decoder.py](archiver/decoder.py) and `@Decoder.register("...")` it.
   - Non-GTFS-RT JSON: subclass `Decoder` directly (see `MartaJsonDecoder` for the pattern, including a `validate()` for schema-drift detection).
5. **Run it once with `-n 1 -v`** to confirm the response parses and metadata looks sane before letting it loop.

---

## Deployment

Production runs on a single EC2 box via Docker Compose (`compose.prod.yml`):

- **`app`** — the poller, pulled from `ghcr.io/ankoure/us-rail-performance-archiver`, labeled `autoheal=true`.
- **`autoheal`** — a sidecar that watches the container `HEALTHCHECK` (which stats `poll_state/.heartbeat`) and restarts the poller if the loop goes stale.
- **`batch`** — a daily loop that runs `pipeline/rollup.py --day yesterday && pipeline/gtfs.py --day yesterday && pipeline/gold.py --day yesterday && pipeline/ship.py --day yesterday && pipeline/cert_check.py` (its inherited healthcheck is disabled since it never writes the heartbeat).
- **`datadog-agent`** — receives DogStatsD metrics/spans.

CI deploys on every push to `main`: GitHub Actions builds the image, pushes to GHCR, assumes an AWS role via OIDC (no static keys), and triggers `docker compose up -d` on the instance over SSM. Full one-time setup (IAM, OIDC, EC2 bootstrap, secrets) is in [deploy/README.md](deploy/README.md).

To run more than one poller, shard by agency: give each worker a distinct `--shard-index` with a shared `--shard-count`.

---

## Module map

| path | role |
|---|---|
| [main.py](main.py), [pipeline/rollup.py](pipeline/rollup.py), [pipeline/ship.py](pipeline/ship.py) | CLI entrypoints — parse args, build, run |
| [archiver/loader.py](archiver/loader.py) | composition root — wires config → objects (clients, limiters, writer, store) |
| [archiver/config.py](archiver/config.py) | pydantic models for `feeds.yaml` |
| [archiver/archiver.py](archiver/archiver.py) | `FeedArchiver` — per-poll work (conditional GET, dedup, parse, write) |
| [archiver/scheduler.py](archiver/scheduler.py) | `Scheduler` — min-heap of due times with jitter + startup spread |
| [archiver/health.py](archiver/health.py) | `FeedHealth` — backoff + quarantine; `is_transient_failure` |
| [archiver/rate_limit.py](archiver/rate_limit.py) | `RateLimiter` Protocol + `TokenBucket` + `NullRateLimiter` |
| [archiver/poll_state.py](archiver/poll_state.py) | `PollStateStore` — persisted ETag / Last-Modified / digest per feed |
| [archiver/shard.py](archiver/shard.py) | deterministic `agency_id → shard` assignment |
| [archiver/parallel.py](archiver/parallel.py) | `run_parallel` — ProcessPool fan-out for rollup |
| [archiver/auth.py](archiver/auth.py) | `APIClient` (async httpx) + auth strategies (bearer / api-key header / api-key query / basic) |
| [archiver/feed.py](archiver/feed.py) | `Feed` dataclass bundling name + client + parser + decoder + interval |
| [archiver/parser.py](archiver/parser.py) | `Parser` registry (`protobuf`, `json`) + `parse_response` dispatch |
| [archiver/response.py](archiver/response.py) | response polymorphism (incl. `DuplicateResponse`, `NotModifiedResponse`, `DecodeFailureResponse`) |
| [archiver/writer.py](archiver/writer.py) | `LocalWriter` + `BatchingWriter` (framed, content-addressed windows) + frame reader/writer |
| [archiver/decoder.py](archiver/decoder.py) | `Decoder` registry + GTFS-RT entity decoders + row dataclasses + `TableSpec` |
| [archiver/rollup.py](archiver/rollup.py) | `Rollup` — landing → curated Parquet (format-agnostic unframing) |
| [archiver/shipper.py](archiver/shipper.py) | `Shipper` — curated + landing → S3 |
| [archiver/uploader.py](archiver/uploader.py) | `Uploader` Protocol + `S3Uploader` implementation |
| [archiver/telemetry.py](archiver/telemetry.py) | `Telemetry` Protocol + `NoOpTelemetry` default |
| [archiver/telemetry_datadog.py](archiver/telemetry_datadog.py) | DogStatsD implementation (lazy-imported) |
| [archiver/summary.py](archiver/summary.py) | `summarize_feed()` — entity counts for metadata rows |

The Parser, Decoder, RateLimiter, Uploader, and Telemetry classes are deliberately Protocols / registries so a new feed shape or backend can be added without touching the poll loop.

---

## Testing

```bash
uv run pytest
```

Tests live under [tests/](tests/) and exercise the config validator, parser dispatch, decoder output + schema-drift, writer + framing, rollup behavior, shipper key construction, rate limiting, poll-state dedup, sharding, feed health, the scheduler, the Datadog telemetry adapter, and the analysis modules. Fakes live in [tests/fakes/](tests/fakes/) — there is no live network or S3 dependency. Async tests run under `pytest-asyncio` (`asyncio_mode = auto`).

---

## Observability

If `telemetry.enabled: true`, the archiver, rollup, and shipper emit StatsD metrics + spans to the configured Datadog agent. Notable signals:

- `poller.heartbeat` gauge — liveness, refreshed every tick (also a local file)
- `feed.poll` span (per feed, per poll) — tagged with `feed`, `interval_class`
- `feed.not_modified` / `feed.duplicate` counters — conditional-GET and dedup hits
- `feed.quarantined` counter — a feed crossed into quarantine
- `poll.skipped` / `poll.rate_limited` counters — concurrency shed / rate-limited cycles
- `poll.error` counter — an archiver-side exception
- `decoder.schema_drift` counter — a feed dropped a required field
- `rollup.run` / `rollup.day` spans, `rollup.skipped` counter
- `ship.cold` / `ship.hot` spans, `ship.cold.bytes` / `ship.hot.bytes` histograms, `ship.*.skipped` counters

All metrics carry a `shard` tag. With telemetry disabled, `NoOpTelemetry` is a true no-op — no agent required.

### Monitors and dashboard

Datadog definitions live in the repo so they can be reproduced from a clone:

- [datadog/monitors/rail-archiver.json](datadog/monitors/rail-archiver.json) — metric alerts: feed-went-dark, slow-feed-went-dark, poller-heartbeat-absent, poll-error-rate-elevated, ingest-saturated (polls being shed), feed-quarantined, schema-drift, archiver-internal-error, batch-step-errored
- [datadog/dashboards/rail-archiver.json](datadog/dashboards/rail-archiver.json) — pipeline-health dashboard (templated on `env` and `feed`)

Sync them with [scripts/sync_datadog.py](scripts/sync_datadog.py) (idempotent upsert by name/title), or import via [`datadog-ci`](https://github.com/DataDog/datadog-ci) / the Datadog UI. The monitor queries are tagged `env:prod` — adjust if you run a different env.

---

## Roadmap / known gaps

- **MTA NYCT decoder is a stub.** `MTADecoder` extends `StandardDecoder` but doesn't yet read the NYCT protobuf extensions.
- **Feed health is in-memory.** Backoff/quarantine state resets on restart (poll-state dedup does persist).
- **Local disk is not auto-pruned.** The shipper uploads but never deletes — a retention/prune step is still needed before the cost model holds long-term.
- **Several agencies are disabled.** See commented blocks in [feeds.yaml](config/feeds.yaml); some endpoints are unreachable, others were never enabled.

---

## License

[MIT](LICENSE) © 2026 Andrew Kouré
