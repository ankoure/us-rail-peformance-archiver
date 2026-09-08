import asyncio
import contextlib
import functools

from archiver.scheduler import Scheduler
from archiver.health import FeedHealth, is_transient_failure
from archiver.feed import Feed
from archiver.logger import logger, setup_logging
from dotenv import load_dotenv
from archiver.loader import (
    build_archiver,
    build_landing_uploader,
    load_config,
    VALID_CONTINENTS,
)
import argparse
import logging
import time
import signal

load_dotenv()

# Fraction of a feed's interval to jitter reschedules by (±), desyncing feeds
# that share an origin and softening the startup burst.
POLL_JITTER = 0.1


def parse_args():
    parser = argparse.ArgumentParser(description="Archive configured feeds")
    parser.add_argument(
        "-n",
        "--polls",
        type=int,
        default=None,
        help="Number of times to poll (omit for infinite)",
    )
    parser.add_argument(
        "-f",
        "--frequency",
        type=int,
        default=60,
        help="Seconds between polls (default: 60)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=10,
        help="Max concurrent in-flight polls (semaphore cap)",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="This worker's shard index, in [0, shard-count)",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Total number of shards (default: 1 = no sharding)",
    )
    parser.add_argument(
        "--continent",
        choices=sorted(VALID_CONTINENTS),
        default=None,
        help="Restrict this worker to agencies in one continent/box (default: unrestricted)",
    )

    parser.add_argument(
        "-c",
        "--config",
        default="config/feeds.yaml",
        help="Path to the feeds config YAML (default: config/feeds.yaml)",
    )

    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


async def run(args):
    config = load_config(args.config)
    archiver = build_archiver(
        config,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        continent=args.continent,
    )
    landing_uploader = build_landing_uploader(
        config,
        shard_index=args.shard_index,
        shard_count=args.shard_count,
        continent=args.continent,
    )  # None unless landing_mode == "s3"
    scheduler = Scheduler(
        archiver.feeds,
        default_interval=args.frequency,
        jitter=POLL_JITTER,
        seed_spread=1.0,  # spread first polls over a full interval → no startup herd
    )
    # Per-feed failure tracking → exponential backoff + dead-feed quarantine.
    health = FeedHealth()

    # Liveness signal: a metric for alerting (no data => process down/hung) and
    # a local file the container HEALTHCHECK can stat for freshness. Refreshed
    # every loop tick, after the sleep, so it reflects an actively-running loop.
    heartbeat_path = config.writer.poll_state_dir / ".heartbeat"
    heartbeat_path.parent.mkdir(parents=True, exist_ok=True)

    polls = 0
    stop = asyncio.Event()

    # Signals must wake an awaiting loop on the loop thread: add_signal_handler
    # schedules stop.set() on the loop, unlike signal.signal() which fires on an
    # arbitrary thread and can't safely touch loop state.
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM, stop.set)
    loop.add_signal_handler(signal.SIGINT, stop.set)

    async with contextlib.AsyncExitStack() as stack:
        # Enter every distinct agency client so their pools open now and close on
        # exit. One client per agency, shared across all of that agency's feeds.
        for client in archiver.clients.values():
            await stack.enter_async_context(client)
        if landing_uploader is not None:
            await stack.enter_async_context(landing_uploader)

        inflight: set[asyncio.Task] = set()
        sem = asyncio.Semaphore(args.workers)
        polling_now: set[str] = set()  # feed names with an in-flight archive_one() call

        def _on_done(feed: Feed, task: asyncio.Task) -> None:
            inflight.discard(task)
            polling_now.discard(feed.name)
            sem.release()

            response = task.result()  # archive_one is self-protecting; never raises
            if response is None:
                return  # archiver-side error (already logged) — don't blame the feed
            if is_transient_failure(response):
                if health.record_failure(feed.name):
                    archiver.telemetry.incr(
                        "feed.quarantined", tags={"feed": feed.name}
                    )
                    logger.warning(
                        "Feed %s quarantined after %d consecutive failures",
                        feed.name,
                        health.consecutive_failures(feed.name),
                    )
            else:
                if health.is_quarantined(feed.name):
                    logger.info("Feed %s recovered from quarantine", feed.name)
                health.record_success(feed.name)

        try:
            while not stop.is_set() and (args.polls is None or polls < args.polls):
                due_at, feed = scheduler.next_due()
                now = time.monotonic()

                delay = due_at - now
                if delay > 0:
                    with contextlib.suppress(TimeoutError):
                        await asyncio.wait_for(stop.wait(), timeout=delay)
                if stop.is_set():
                    break

                archiver.telemetry.gauge("poller.heartbeat", 1)
                heartbeat_path.touch()

                # Next interval reflects the feed's health: normal, backed-off, or
                # quarantined. Computed once and used for the reschedule on every
                # path — a skip doesn't change health, so a quarantined feed that
                # gets skipped stays quarantined.
                base_interval = feed.poll_interval_seconds or args.frequency
                interval = health.next_interval(feed.name, base_interval)
                if sem.locked():  # concurrency gate: no free slot → shed this cycle
                    archiver.telemetry.incr("poll.skipped", tags={"feed": feed.name})
                elif feed.name in polling_now:
                    archiver.telemetry.incr("poll.overrun", tags={"feed": feed.name})
                elif not archiver.clients[feed.agency_id].limiter.try_acquire():
                    archiver.telemetry.incr(
                        "poll.rate_limited", tags={"feed": feed.name}
                    )
                else:
                    await sem.acquire()  # can't block: we just checked locked()
                    polling_now.add(feed.name)
                    task = asyncio.create_task(archiver.archive_one(feed))
                    inflight.add(task)
                    task.add_done_callback(functools.partial(_on_done, feed))
                scheduler.mark_polled(feed, interval=interval)

                polls += 1
        finally:
            await asyncio.gather(*inflight)


def main(args):
    # Sync boundary: set up logging, then cross into the event loop. This is the
    # one place asyncio.run is called.
    setup_logging(level=logging.DEBUG if args.verbose else logging.INFO)
    asyncio.run(run(args))


if __name__ == "__main__":
    args = parse_args()
    main(args)
