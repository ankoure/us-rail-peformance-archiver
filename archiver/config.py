from pathlib import Path
from typing import Literal, Annotated, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)


class HasName(Protocol):
    name: str


def names_must_be_unique(
    config_list: list[HasName], config_model_name: str
) -> list[HasName]:
    names = [config.name for config in config_list]
    if len(names) != len(set(names)):
        duplicates = {name for name in names if names.count(name) > 1}
        raise ValueError(
            f"{config_model_name} names must be unique, duplicates found: {duplicates}"
        )
    return config_list


class NoAuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["none"]


class APIKeyAuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["api_key"]
    header: str | None = None
    param: str | None = None
    env: str

    @model_validator(mode="after")
    def exactly_one_of_header_or_param(self) -> "APIKeyAuthConfig":
        if self.header is None and self.param is None:
            raise ValueError("Either header or param should be set")
        if self.header and self.param:
            raise ValueError("Header and param are both set")
        return self


class BearerAuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["bearer"]
    env: str


class BasicAuthConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["basic"]
    username_env: str
    password_env: str


AuthConfig = Annotated[
    NoAuthConfig | APIKeyAuthConfig | BearerAuthConfig | BasicAuthConfig,
    Field(discriminator="type"),
]


class FeedConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str
    path: str
    expected_format: Literal["protobuf", "json", "auto"] = "protobuf"
    decoder: Literal[
        "standard",
        "mta_nyct",
        "tfnsw",
        "marta_json",
        "mta_lirr_json",
        "mwrta_json",
        "routematch_json",
        "trillium_json",
        "swiv_json",
        "vta_json",
        "passio_json",
    ] = "standard"
    poll_interval_seconds: int | None = Field(default=None, gt=0)
    mdb_feed_id: str | None = None
    # Scopes pipeline/gtfs.py's static-GTFS processing to one GTFS agency_id
    # prefix (matched as f"{prefix}:" against routes.txt's agency_id column)
    # for a feed whose agency-level mdb_feed_id points at a zip covering
    # MULTIPLE operators -- e.g. GO_AHEAD's feeds each cover one operator
    # (GOA/RUT/VYB/VYX) within Entur's Norwegian national feed. None (the
    # default) means the feed's static schedule is single-operator and
    # StaticGtfs reads it whole, unfiltered, exactly as before this field
    # existed. See analysis/static_gtfs.py's StaticGtfs docstring for why this
    # matters: that national feed's shapes.txt is 38M rows / 2.57 GB, and
    # reading it whole OOMed a 20 GiB container (confirmed 2026-09-06).
    gtfs_agency_prefix: str | None = None
    method: Literal["GET", "POST"] = "GET"
    body: dict | None = None


class RateLimitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    requests: int  # e.g. 60
    per_seconds: float  # e.g. 3600  -> refill_rate = requests/per_seconds
    burst: int | None = None  # capacity; default to `requests` if omitted


class AgencyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    agency_id: str
    name: str
    region: str
    timezone: str
    base_url: HttpUrl
    auth: AuthConfig
    feeds: list[FeedConfig]
    default_headers: dict[str, str] = Field(default_factory=dict)
    rate_limit: RateLimitConfig | None = None  # None => unlimited (NullRateLimiter)
    mdb_feed_id: str | None = None
    tls_verify: bool = True
    tls_extra_ca_cert: str | None = None
    shard_pin: int | None = None  # TODO: docstring/comment on what this means

    @field_validator("feeds")
    @classmethod
    def validate_feeds(cls, v: list[FeedConfig]) -> list[FeedConfig]:
        return names_must_be_unique(v, "feed")

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, v: str) -> str:
        try:
            ZoneInfo(v)
        except ZoneInfoNotFoundError:
            raise ValueError(f"Unknown IANA timezone: {v!r}")
        return v


class WriterConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    writer_type: Literal["local", "batch"] = "batch"
    landing_bucket: str = ""
    landing_prefix: str = ""
    landing_mode: Literal["local", "s3"] = "local"
    rollup_source: Literal["local", "s3"] = "local"
    window_seconds: int = 300
    merge_to_hourly: bool = False
    landing_dir: Path
    curated_dir: Path
    poll_state_dir: Path = Path("./data/poll_state")


class TelemetryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    service: str = "rail-archiver"
    env: str = "dev"
    agent_host: str = "localhost"
    statsd_port: int = 8125
    tags: dict[str, str] = {}


class S3Config(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = False
    region: str = "us-east-1"
    cold_bucket: str | None = None
    hot_bucket: str | None = None
    cold_prefix: str = ""
    hot_prefix: str = ""

    @model_validator(mode="after")
    def buckets_required_when_enabled(self) -> "S3Config":
        if self.enabled and not (self.cold_bucket and self.hot_bucket):
            raise ValueError("cold_bucket and hot_bucket required when s3 enabled")
        return self


class ArchiverConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    writer: WriterConfig
    telemetry: TelemetryConfig = TelemetryConfig()
    s3: S3Config = S3Config()
    agencies: list[AgencyConfig]

    @field_validator("agencies")
    @classmethod
    def validate_agencies(cls, v: list[AgencyConfig]) -> list[AgencyConfig]:
        return names_must_be_unique(v, "agency")

    @model_validator(mode="after")
    def feed_names_globally_unique(self) -> "ArchiverConfig":
        all_feed_names = [
            feed.name for agency in self.agencies for feed in agency.feeds
        ]
        if len(all_feed_names) != len(set(all_feed_names)):
            duplicates = {n for n in all_feed_names if all_feed_names.count(n) > 1}
            raise ValueError(
                f"feed names must be globally unique, duplicates: {duplicates}"
            )
        return self
