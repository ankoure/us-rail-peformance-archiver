"""Single source of truth for landing-zone metadata-object identity, plus the
raw-payload tar key format.

Raw and metadata are no longer symmetric. `ContentAddressedWriter` writes each
poll's payload immediately to a content-hash-named `{digest}.bin` — there is
no `window=*.bin` file for a glob to find, so raw-payload *discovery* isn't
this module's job anymore: `LandingUploader`'s raw-tar shipping pass finds
which digests belong to a window by reading the local day's `data.jsonl`
directly (timestamp + digest per row), not by listing files here. This module
still owns two things: (1) metadata-object selection/keys (`WINDOW_OBJECT_GLOBS`,
`iter_window_objects`, `window_object_key`) — a window's metadata row-slice IS
still materialized to disk as a discoverable `window=*.jsonl` file, shipped by
the same generic scan `LandingUploader` and the one-shot `LandingBackfill`
(soak-phase parity) both use, so they can't drift apart — and (2) the raw tar's
S3 key shape (`window_tar_key`), computed directly rather than discovered,
since the caller already knows which window it just built a tar for.

Layout:
    {feed}/metadata/year=YYYY/month=MM/day=DD/window=*.jsonl   (WINDOW_OBJECT_GLOBS)
    {feed}/raw/year=YYYY/month=MM/day=DD/window=*--shipped=*.tar.gz   (window_tar_key)
The daily `data.jsonl` (writer.py `append_metadata`) sits in the same metadata
directory but is local-only — it is excluded for free because its name doesn't
match `window=*`. Each glob `*` matches exactly one path segment (pathlib never
crosses "/"), so the metadata pattern is pinned to that depth on purpose; a
layout change in writer.py's metadata materialization must be reflected here.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

WINDOW_OBJECT_GLOBS: tuple[str, ...] = (
    "*/metadata/year=*/month=*/day=*/window=*.jsonl",
)


def iter_window_objects(landing_dir: Path) -> Iterator[Path]:
    """Yield every shippable window object under ``landing_dir`` (unordered).

    Files only — a directory whose name happens to match the glob is skipped,
    and in-progress ``*.tmp`` writes never match the pinned suffix.
    """
    for pattern in WINDOW_OBJECT_GLOBS:
        for p in landing_dir.glob(pattern):
            if p.is_file():
                yield p


def window_object_key(landing_dir: Path, path: Path, prefix: str = "") -> str:
    """Map a local window-object path to its S3 key.

    ``prefix + <path relative to landing_dir>``, slash-joined — byte-identical
    to the old synchronous ``S3Sink`` keys so the rollup reads the same layout.
    Caller is responsible for ``prefix`` ending in "/" (validated where used).
    """
    return prefix + path.relative_to(landing_dir).as_posix()


def _day_parts(unix: int) -> tuple[str, str, str]:
    """UTC year=/month=/day= components for a unix timestamp.

    Shared by window_metadata_local_path and window_tar_key so the two
    can't drift. Mirrors the format BatchingWriter._window_key produced.
    """
    dt = datetime.fromtimestamp(unix, tz=timezone.utc)
    return f"year={dt.year}", f"month={dt.month}", f"day={dt.day}"


def window_metadata_local_path(landing_dir: Path, feed: str, window_unix: int) -> Path:
    """Local path where a window's metadata row-slice is materialized.

    Must match WINDOW_OBJECT_GLOBS' metadata pattern exactly — this file
    exists to be discovered by the unchanged _scan_once_window loop.
    """
    y, m, d = _day_parts(window_unix)
    return landing_dir / feed / "metadata" / y / m / d / f"window={window_unix}.jsonl"


def window_tar_key(prefix: str, feed: str, window_unix: int, shipped_unix: int) -> str:
    """S3 key for a window's raw-payload tar.

    The shipped= component makes the key unique per upload. Content that
    recurs after its first window's tar was shipped-and-deleted gets a
    fresh {digest}.bin, so a second shipment for the same calendar window
    is possible — a pure window=<start>.tar.gz key would silently clobber the
    first (the latent hazard in _merge_and_ship).

    `.tar.gz`, not `.tar`: gzip cuts cross-region transfer cost for the EU/AU
    pollers (raw protobuf + repeated JSON-ish structure compresses well) —
    see archiver/source.py's iter_bins for why this is a distinct extension
    from the older uncompressed `.tar` objects rather than a format flip on
    the same one.
    """
    y, m, d = _day_parts(window_unix)
    return (
        f"{prefix}{feed}/raw/{y}/{m}/{d}"
        f"/window={window_unix}--shipped={shipped_unix}.tar.gz"
    )
