import io
import tarfile
from datetime import date
from pathlib import Path
from typing import Iterator, Protocol

from archiver.logger import logger


class Source(Protocol):
    def discover(
        self, feed: str | None = None, day: date | None = None
    ) -> Iterator[tuple[str, date]]: ...
    def read_metadata(
        self, feed: str, day: date
    ) -> bytes: ...  # day's full jsonl; b"" if absent
    def iter_bins(
        self, feed: str, day: date
    ) -> Iterator[tuple[str, bytes]]: ...  # (name, bytes)
    def iter_metadata(
        self, feed: str, day: date
    ) -> Iterator[tuple[str, bytes]]: ...  # (name, bytes) per metadata file


class LocalSource:
    def __init__(self, landing_dir: str | Path):
        self.landing_dir = Path(landing_dir)

    def discover(
        self, feed: str | None = None, day: date | None = None
    ) -> Iterator[tuple[str, date]]:
        """Yield (feed_name, day) for every metadata partition"""
        for metadata_dir in self.landing_dir.glob("*/metadata"):
            feed_name = metadata_dir.parent.name
            if feed is not None and feed_name != feed:
                continue
            for jsonl_path in metadata_dir.rglob("data.jsonl"):
                partitions = {
                    part.split("=")[0]: int(part.split("=")[1])
                    for part in jsonl_path.parts
                    if "=" in part
                }
                partition_day = date(
                    partitions["year"], partitions["month"], partitions["day"]
                )
                if day is not None and partition_day != day:
                    continue
                yield feed_name, partition_day

    def read_metadata(self, feed: str, day: date) -> bytes:
        """Read the full metadata jsonl for the given feed and day, or b"" if absent."""
        path = (
            self.landing_dir
            / feed
            / "metadata"
            / f"year={day.year}"
            / f"month={day.month}"
            / f"day={day.day}"
            / "data.jsonl"
        )
        return path.read_bytes() if path.exists() else b""

    def iter_bins(self, feed: str, day: date) -> Iterator[tuple[str, bytes]]:
        """Yield (bin_name, bytes) for every bin for the given feed and day.

        No tar branch here on purpose: the window tars only ever exist inside
        LandingUploader's scratch directory, which is torn down on every exit
        path. What is on the local disk is always loose {digest}.bin files.
        """
        bin_files = (self.landing_dir / feed / "raw").glob(
            f"year={day.year}/month={day.month}/day={day.day}/*.bin"
        )
        for path in bin_files:
            yield (path.name, path.read_bytes())

    def iter_metadata(self, feed: str, day: date) -> Iterator[tuple[str, bytes]]:
        """Yield (name, bytes) for every metadata file (data.jsonl and/or
        window=*.jsonl) for the given feed and day — the per-file form the cold
        tarball needs, vs read_metadata's concatenated stream the rollup needs."""
        meta_files = (self.landing_dir / feed / "metadata").glob(
            f"year={day.year}/month={day.month}/day={day.day}/*.jsonl"
        )
        for path in meta_files:
            yield (path.name, path.read_bytes())


class S3Source:
    def __init__(self, uploader, bucket, prefix=""):
        self._uploader, self._bucket, self._prefix = uploader, bucket, prefix

    def _list_keys(self, bucket, prefix):
        return self._uploader.list_keys(bucket, prefix)

    def _day_prefix(self, feed, kind, day):
        return (
            self._prefix
            + f"{feed}/{kind}/year={day.year}/month={day.month}/day={day.day}/"
        )

    def discover(self, feed=None, day=None):
        # Optional optimization: when both are pinned, list only that day's
        # metadata subtree. The exact filters below still run, so correctness
        # never depends on this narrowing.
        if feed is not None and day is not None:
            prefix = self._day_prefix(feed, "metadata", day)
        else:
            prefix = self._prefix

        found = set()
        for key in self._list_keys(self._bucket, prefix):
            if "/metadata/" not in key:
                continue
            rel = key[len(self._prefix) :]
            f = rel.split("/")[0]
            kv = {}
            for seg in rel.split("/"):
                k, sep, v = seg.partition("=")
                if sep:
                    kv[k] = v
            try:
                d = date(int(kv["year"]), int(kv["month"]), int(kv["day"]))
            except (KeyError, ValueError):
                continue  # not a year=/month=/day= layout; skip
            found.add((f, d))  # many window=*.jsonl collapse to one (feed, day)

        if feed is not None:
            found = {(f, d) for (f, d) in found if f == feed}
        if day is not None:
            found = {(f, d) for (f, d) in found if d == day}
        return found

    def iter_bins(self, feed, day):
        """Yield (name, bytes) for every payload in the day's raw prefix.

        Two object shapes coexist and both come out of one call: legacy flat
        `.bin` objects from before the migration, yielded as-is, and `.tar`
        objects written by LandingUploader._ship_raw_window, unpacked into
        their `{digest}.bin` members. Callers see the same (name, bytes)
        contract either way — payloads.iter_payloads dispatches on the name.
        """
        prefix = self._day_prefix(feed, "raw", day)
        seen_members: set[str] = set()

        for key in self._list_keys(self._bucket, prefix):
            if key.endswith(".bin"):
                name = key.rsplit("/", 1)[-1]  # window=*.bin — keep the ext
                data = self._uploader.get_bytes(self._bucket, key)
                yield name, data

            elif key.endswith(".tar"):
                data = self._uploader.get_bytes(self._bucket, key)
                # mode="r:" is exact-match uncompressed, matching the
                # tarfile.open(staged, "w") that wrote these. "r"/"r:*" would
                # auto-detect and silently accept a gzip'd tar that has no
                # business being in this path; a loud ReadError is the point.
                try:
                    with tarfile.open(fileobj=io.BytesIO(data), mode="r:") as tar:
                        for member in tar:
                            if not member.isfile():
                                continue  # dir/link entries carry no payload

                            # arcname was path.name, so a member name should be
                            # a bare "{digest}.bin". Anything with a path in it
                            # didn't come from _ship_raw_window; nothing is
                            # written to disk here so it can't traverse, but it
                            # would break the digest join downstream.
                            name = member.name
                            if "/" in name or name in (".", ".."):
                                logger.warning(
                                    "Unexpected non-flat member %r in %s; skipping",
                                    name,
                                    key,
                                )
                                continue

                            # Retried ships produce a second tar (window_tar_key
                            # stamps int(time.time()), so the key differs) with
                            # overlapping members. Names are content digests, so
                            # a repeat is the same bytes by construction and
                            # dropping it is safe — yielding it twice would
                            # double-count the poll in the rollup.
                            if name in seen_members:
                                continue
                            seen_members.add(name)

                            extracted = tar.extractfile(member)
                            if extracted is None:
                                continue
                            yield name, extracted.read()

                except tarfile.TarError:
                    logger.exception(
                        "Unreadable raw tar %s; skipping object (its payloads are "
                        "lost for this run — the local {digest}.bin files were "
                        "deleted once the upload was confirmed)",
                        key,
                    )
                    continue

    def read_metadata(self, feed, day):
        prefix = self._day_prefix(feed, "metadata", day)
        keys = sorted(
            k for k in self._list_keys(self._bucket, prefix) if k.endswith(".jsonl")
        )
        chunks = [self._uploader.get_bytes(self._bucket, k) for k in keys]
        return b"".join(chunks)

    def iter_metadata(self, feed, day):
        prefix = self._day_prefix(feed, "metadata", day)
        for key in self._list_keys(self._bucket, prefix):
            if key.endswith(".jsonl"):
                name = key.rsplit("/", 1)[-1]  # window=*.jsonl — keep the ext
                yield name, self._uploader.get_bytes(self._bucket, key)
