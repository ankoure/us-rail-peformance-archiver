import time

from archiver.auth import APIClient
from archiver.poll_state import PollState, PollStateStore
from archiver.response import (
    DecodeFailureResponse,
    DuplicateResponse,
    JsonResponse,
    NotModifiedResponse,
    ProtobufResponse,
    TransportErrorResponse,
)
from archiver.parser import parse_response
from archiver.feed import Feed
from archiver.writer import LocalWriter
import httpx
from archiver.logger import logger
from archiver.telemetry import Telemetry, NoOpTelemetry

# Feeds polling less often than this are tagged interval_class:slow so the
# dark-feed monitor can give them a longer "gone dark" window (a 15-min window
# would false-positive on a feed that only polls every ~17-30 min, e.g. the
# rate-limited Bay Area 511 feeds).
SLOW_POLL_THRESHOLD_SECONDS = 600


class FeedArchiver:
    def __init__(
        self,
        feeds: list[Feed],
        clients: dict[str, APIClient],
        writer: LocalWriter,
        store: PollStateStore,
        telemetry: Telemetry | None = None,
    ):
        self.feeds = feeds
        self.clients = clients
        self.writer = writer
        self.telemetry = telemetry or NoOpTelemetry()
        self.store = store

    async def archive_one(self, feed: Feed):
        try:
            prior = self.store.get(feed.name)
            conditional_headers = {}
            if prior.etag:
                conditional_headers["If-None-Match"] = prior.etag
            if prior.last_modified:
                conditional_headers["If-Modified-Since"] = prior.last_modified

            interval_class = (
                "slow"
                if (feed.poll_interval_seconds or 0) >= SLOW_POLL_THRESHOLD_SECONDS
                else "fast"
            )
            try:
                with self.telemetry.span(
                    "feed.poll",
                    tags={"feed": feed.name, "interval_class": interval_class},
                ):
                    start = time.monotonic()
                    client = self.clients[feed.agency_id]
                    if feed.method == "POST":
                        http = await client.post(
                            feed.path, json=feed.body, headers=conditional_headers
                        )
                    else:
                        http = await client.get(feed.path, headers=conditional_headers)

                    if http.status_code == 304:
                        self.telemetry.incr(
                            "feed.not_modified", tags={"feed": feed.name}
                        )
                        response = NotModifiedResponse(http, prior.last_digest)
                    else:
                        response = parse_response(http, feed.parser, feed.decoder)
                        current_digest = response.content_digest()
                        if current_digest == prior.last_digest:
                            self.telemetry.incr(
                                "feed.duplicate", tags={"feed": feed.name}
                            )
                            response = DuplicateResponse(http)
                        else:
                            if isinstance(response, (ProtobufResponse, JsonResponse)):
                                self.store.set(
                                    feed.name,
                                    PollState(
                                        etag=http.headers.get("ETag"),
                                        last_modified=http.headers.get("Last-Modified"),
                                        last_digest=current_digest,
                                    ),
                                )
                    poll_duration = time.monotonic() - start

                if isinstance(response, DecodeFailureResponse):
                    self.telemetry.incr(
                        "decoder.schema_drift", tags={"feed": feed.name}
                    )

                if feed.poll_interval_seconds:
                    if poll_duration > feed.poll_interval_seconds:
                        logger.warning(
                            "Poll for %s took %.2fs, exceeds configured interval %ds",
                            feed.name,
                            poll_duration,
                            feed.poll_interval_seconds,
                        )
            except httpx.RequestError as e:
                response = TransportErrorResponse(
                    error_type=type(e).__name__, error_message=str(e)
                )
                logger.warning(
                    "Transport error on feed %s: %s: %s",
                    feed.name,
                    type(e).__name__,
                    e,
                )
            await self.writer.write(feed.name, response)
            # Returned so the loop can classify the outcome (success vs failure)
            # for backoff/quarantine. On the outer-except path below, returns None
            # implicitly => an archiver-side error, which the loop ignores for
            # feed health (it doesn't reflect on the feed).
            return response
        except Exception:
            self.telemetry.incr("poll.error", tags={"feed": feed.name})
            logger.exception("unexpected error polling %s", feed.name)
