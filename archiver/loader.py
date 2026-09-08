# loader.py
import os
import yaml
import ssl
import certifi
import archiver.decoder  # noqa: F401 — populate Decoder._registry via import side effects
import archiver.parser  # noqa: F401 — populate Parser._registry via import side effects
from archiver.backfill import LandingBackfill
from archiver.landing_uploader import LandingUploader
from archiver.archiver import FeedArchiver
from archiver.auth import APIClient
from archiver.logger import logger
from archiver.config import (
    AgencyConfig,
    ArchiverConfig,
    APIKeyAuthConfig,
    BearerAuthConfig,
    BasicAuthConfig,
    NoAuthConfig,
    RateLimitConfig,
    S3Config,
    TelemetryConfig,
)
from archiver.rate_limit import NullRateLimiter, RateLimiter, TokenBucket
from archiver.decoder import AlertRow, Decoder
from archiver.feed import Feed
from archiver.parser import Parser
from archiver.poll_state import PollStateStore
from archiver.region import belongs_to_continent, TIMEZONE_TO_CONTINENT
from archiver.rollup import Rollup
from archiver.shard import belongs_to_shard
from archiver.shipper import Shipper
from archiver.source import LocalSource, S3Source, Source
from archiver.telemetry import NoOpTelemetry, Telemetry
from archiver.uploader import Uploader
from archiver.writer import (
    BaseWriter,
    ContentAddressedWriter,
    LocalWriter,
)

# Single source of truth for what a valid --continent value is: whatever
# boxes region.py actually assigns timezones to, so this can't drift out
# of sync with region.py if a box is ever added or renamed.
VALID_CONTINENTS = frozenset(TIMEZONE_TO_CONTINENT.values())


def _read_env(name: str) -> str:
    value = os.environ.get(name)
    if value is None:
        raise RuntimeError(f"Required env var '{name}' is not set")
    return value


def load_config(path: str) -> ArchiverConfig:
    with open(path, "r") as f:
        return ArchiverConfig.model_validate(yaml.safe_load(f))


def build_limiter(cfg: RateLimitConfig | None) -> RateLimiter:
    if cfg is None:
        return NullRateLimiter()
    capacity = cfg.burst if cfg.burst is not None else cfg.requests
    return TokenBucket(capacity=capacity, refill_rate=cfg.requests / cfg.per_seconds)


def _build_verify(tls_verify: bool, tls_extra_ca_cert: str | None):
    if tls_extra_ca_cert is None:
        return tls_verify  # False or True, same as today
    ctx = ssl.create_default_context()
    ctx.load_verify_locations(cafile=certifi.where())  # all the normal roots
    ctx.load_verify_locations(
        cafile=tls_extra_ca_cert
    )  # appends the missing intermediate
    return ctx


def build_client(agency: AgencyConfig) -> APIClient:
    base_url = str(agency.base_url)
    limiter = build_limiter(agency.rate_limit)
    headers = agency.default_headers
    verify = _build_verify(agency.tls_verify, agency.tls_extra_ca_cert)
    match agency.auth:
        case NoAuthConfig():
            return APIClient(
                base_url, limiter=limiter, default_headers=headers, verify=verify
            )
        case APIKeyAuthConfig() as a:
            if a.header is not None:
                return APIClient.with_api_key(
                    base_url,
                    key=_read_env(a.env),
                    header=a.header,
                    limiter=limiter,
                    default_headers=headers,
                    verify=verify,
                )
            else:
                return APIClient.with_api_key_query(
                    base_url,
                    key=_read_env(a.env),
                    param=a.param,
                    limiter=limiter,
                    default_headers=headers,
                    verify=verify,
                )
        case BearerAuthConfig() as a:
            return APIClient.with_bearer(
                base_url,
                token=_read_env(a.env),
                limiter=limiter,
                default_headers=headers,
                verify=verify,
            )
        case BasicAuthConfig() as a:
            return APIClient.with_basic(
                base_url,
                username=_read_env(a.username_env),
                password=_read_env(a.password_env),
                limiter=limiter,
                default_headers=headers,
                verify=verify,
            )


def build_feeds(
    config: ArchiverConfig,
    shard_index: int = 0,
    shard_count: int = 1,
    continent: str | None = None,
) -> list[Feed]:
    if continent is not None and continent not in VALID_CONTINENTS:
        raise ValueError(
            f"continent {continent!r} must be one of {sorted(VALID_CONTINENTS)}"
        )
    feeds: list[Feed] = []
    for agency in config.agencies:
        if not belongs_to_shard(
            agency.agency_id, shard_index, shard_count, pin=agency.shard_pin
        ):
            continue
        if continent is not None and not belongs_to_continent(
            agency.timezone, continent
        ):
            continue
        for feed_cfg in agency.feeds:
            feeds.append(
                Feed(
                    name=feed_cfg.name,
                    path=feed_cfg.path,
                    parser=Parser.from_name(feed_cfg.expected_format),
                    decoder=Decoder.from_name(feed_cfg.decoder),
                    poll_interval_seconds=feed_cfg.poll_interval_seconds,
                    agency_id=agency.agency_id,
                    method=feed_cfg.method,
                    body=feed_cfg.body,
                )
            )
    return feeds


def build_agency_clients(
    config: ArchiverConfig,
    shard_index: int = 0,
    shard_count: int = 1,
    continent: str | None = None,
) -> dict[str, APIClient]:
    """agency_id -> authenticated client, for the live poller ONLY.

    Every other consumer of build_feeds() (rollup, snapshot, shipper, dev
    scripts) reads landing/curated data and never sends a live HTTP request,
    so it has no business needing an agency's API key to be present. Keeping
    client construction here, isolated from build_feeds(), means a missing
    credential can only ever break polling for that one agency -- not crash
    every offline pipeline step for every agency, as it did when TFNSW/PTV
    were added without their keys wired into the rollup task's secrets.
    """
    if continent is not None and continent not in VALID_CONTINENTS:
        raise ValueError(
            f"continent {continent!r} must be one of {sorted(VALID_CONTINENTS)}"
        )
    clients: dict[str, APIClient] = {}
    for agency in config.agencies:
        if not belongs_to_shard(
            agency.agency_id, shard_index, shard_count, pin=agency.shard_pin
        ):
            continue
        if continent is not None and not belongs_to_continent(
            agency.timezone, continent
        ):
            continue
        try:
            clients[agency.agency_id] = build_client(agency)
        except RuntimeError as exc:
            # One agency's missing key (a secret provisioning gap, not a code
            # bug) shouldn't crash every other agency's polling on this box --
            # skip just this one. Its feeds still get scheduled (they're built
            # by build_feeds() independently of this function) and will fail
            # per-poll via archive_one's own error handling, which is visible
            # in telemetry/logs without taking the whole box down.
            logger.error(
                "skipping agency %s, feeds won't poll until fixed: %s",
                agency.agency_id,
                exc,
            )
    return clients


def agency_env_keys(config: ArchiverConfig, continent: str | None = None) -> list[str]:
    """Sorted env var names build_client() would read, optionally scoped to one
    continent -- the minimal secret set a poller box actually needs.

    Used by each box's bootstrap to trim the shared Secrets Manager blob down
    to just its own region's keys, so a region-local box never has another
    region's credentials sitting on disk (not just unused -- absent).
    """
    if continent is not None and continent not in VALID_CONTINENTS:
        raise ValueError(
            f"continent {continent!r} must be one of {sorted(VALID_CONTINENTS)}"
        )
    keys: set[str] = set()
    for agency in config.agencies:
        if continent is not None and not belongs_to_continent(
            agency.timezone, continent
        ):
            continue
        match agency.auth:
            case APIKeyAuthConfig() as a:
                keys.add(a.env)
            case BearerAuthConfig() as a:
                keys.add(a.env)
            case BasicAuthConfig() as a:
                keys.add(a.username_env)
                keys.add(a.password_env)
    return sorted(keys)


def build_telemetry(config: TelemetryConfig, shard_index: int = 0) -> Telemetry:
    if not config.enabled:
        return NoOpTelemetry()

    # Lazy import — datadog only loaded if actually enabled
    from datadog.dogstatsd.base import DogStatsd
    from archiver.telemetry_datadog import DatadogTelemetry

    # disable_buffering=False batches metrics into fewer UDP packets, preventing
    # the Datadog Agent's intake queue from saturating on burst poll cycles.
    client = DogStatsd(
        host=config.agent_host, port=config.statsd_port, disable_buffering=False
    )
    default_tags = {
        "service": config.service,
        "env": config.env,
        "shard": str(shard_index),
        **config.tags,
    }
    return DatadogTelemetry(client, default_tags=default_tags)


def build_source(config: ArchiverConfig) -> Source:
    w = config.writer
    match w.rollup_source:
        case "local":
            return LocalSource(w.landing_dir)
        case "s3":
            telemetry = build_telemetry(config.telemetry)
            return S3Source(
                build_uploader(config.s3, telemetry), w.landing_bucket, w.landing_prefix
            )

        case other:
            raise ValueError(f"Unsupported rollup_source: {other}")


def build_writer(config: ArchiverConfig) -> BaseWriter:
    writer = config.writer
    match writer.writer_type:
        case "local":
            return LocalWriter(writer.landing_dir)  # legacy per-poll; no sink
        case "content_addressed":
            return ContentAddressedWriter(writer.landing_dir)

        case other:
            raise ValueError(f"Unsupported writer_type: {other}")


def build_archiver(
    config: ArchiverConfig,
    shard_index: int,
    shard_count: int,
    continent: str | None = None,
) -> FeedArchiver:
    feeds = build_feeds(config, shard_index, shard_count, continent)
    clients = build_agency_clients(config, shard_index, shard_count, continent)
    writer = build_writer(config)
    telemetry = build_telemetry(config.telemetry, shard_index)
    store = PollStateStore(str(config.writer.poll_state_dir))
    return FeedArchiver(
        feeds=feeds, clients=clients, writer=writer, telemetry=telemetry, store=store
    )


def build_rollup(config: ArchiverConfig) -> Rollup:
    feeds = build_feeds(config)
    telemetry = build_telemetry(config.telemetry)
    return Rollup(
        feeds=feeds,
        source=build_source(config),
        curated_dir=config.writer.curated_dir,
        telemetry=telemetry,
    )


def build_uploader(config: S3Config, telemetry: Telemetry) -> Uploader:
    if not config.enabled:
        raise RuntimeError("s3 is not enabled in config")

    # Lazy import — boto3 only loaded if actually enabled
    import boto3
    from botocore.config import Config
    from archiver.uploader import S3Uploader, InstrumentedUploader

    # Shipper.run fans uploads across a ThreadPoolExecutor (default 8 workers) that
    # share this one client; the default urllib3 pool of 10 then thrashes ("Connection
    # pool is full, discarding connection"). Size the pool comfortably above the ship
    # worker count (plus headroom for multipart) so connections are reused, not dropped.
    client = boto3.client(
        "s3",
        region_name=config.region,
        config=Config(max_pool_connections=25),
    )
    return InstrumentedUploader(S3Uploader(client), telemetry)


def build_shipper(config: ArchiverConfig) -> Shipper:
    telemetry = build_telemetry(config.telemetry)
    uploader = build_uploader(config.s3, telemetry)
    return Shipper(
        source=build_source(config),  # local or S3 per writer.rollup_source
        curated_dir=config.writer.curated_dir,
        uploader=uploader,
        cold_bucket=config.s3.cold_bucket,
        hot_bucket=config.s3.hot_bucket,
        cold_prefix=config.s3.cold_prefix,
        hot_prefix=config.s3.hot_prefix,
        telemetry=telemetry,
        # Names only, straight from config -- NOT build_feeds(config), which
        # builds a live APIClient per agency (reading its API key env var) for
        # every configured feed. Shipping never makes an HTTP request against
        # an agency, so it shouldn't need that agency's secret to be present.
        feed_names=[feed.name for agency in config.agencies for feed in agency.feeds],
        feed_agency={
            feed.name: agency.agency_id
            for agency in config.agencies
            for feed in agency.feeds
        },
        # Same "names only" reasoning: Decoder.from_name only resolves the
        # registered class, same as build_feeds does internally, without
        # needing build_feeds' agency_id/shard filtering machinery.
        feed_alerts_capable=frozenset(
            feed.name
            for agency in config.agencies
            for feed in agency.feeds
            if AlertRow in Decoder.from_name(feed.decoder).produces
        ),
        # feed_name -> the curated "kind" subdirectories rollup.py must write
        # for it -- the same feed.decoder.produces -> TableSpec.name mapping
        # Rollup._expected_outputs (archiver/rollup.py) uses to decide its own
        # completeness, reused here so prune_s3 can ask the identical question.
        # "metadata" is always expected too (Rollup._METADATA_KIND) -- hardcoded
        # as a literal here rather than reaching into Rollup's internals, the
        # same tradeoff Shipper._snapshot_key already makes for "alerts".
        feed_expected_kinds={
            feed.name: {"metadata"}
            | {spec.name for spec in Decoder.from_name(feed.decoder).produces.values()}
            for agency in config.agencies
            for feed in agency.feeds
        },
        # Local landing; used by prune. S3 landing bucket/prefix used by prune_s3.
        landing_dir=config.writer.landing_dir,
        landing_bucket=config.writer.landing_bucket,
        landing_prefix=config.writer.landing_prefix,
    )


def build_landing_uploader(
    config: ArchiverConfig,
    shard_index: int = 0,
    shard_count: int = 1,
    continent: str | None = None,
) -> LandingUploader | None:
    if config.writer.landing_mode == "s3":
        telemetry = build_telemetry(config.telemetry)
        uploader = build_uploader(config.s3, telemetry)
        feed_names = {
            f.name for f in build_feeds(config, shard_index, shard_count, continent)
        }
        return LandingUploader(
            landing_dir=config.writer.landing_dir,
            uploader=uploader,
            bucket=config.writer.landing_bucket,
            prefix=config.writer.landing_prefix,
            telemetry=telemetry,
            ship_window_seconds=config.writer.ship_window_seconds,
            feed_names=feed_names,
        )
    else:
        return None


def build_landing_backfill(config: ArchiverConfig) -> LandingBackfill:
    # Operational tool, run by hand during the soak (poller is in `local` mode),
    # so it does not gate on landing_mode -- it always builds. build_uploader
    # raises if s3 isn't enabled.
    telemetry = build_telemetry(config.telemetry)
    uploader = build_uploader(config.s3, telemetry)
    return LandingBackfill(
        landing_dir=config.writer.landing_dir,
        uploader=uploader,
        bucket=config.writer.landing_bucket,
        prefix=config.writer.landing_prefix,
        telemetry=telemetry,
    )
