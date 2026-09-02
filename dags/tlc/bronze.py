"""Bronze layer: land the source file byte-for-byte, atomically, once."""
from __future__ import annotations

import hashlib
import logging
import os
import urllib.request
from pathlib import Path

from . import config

log = logging.getLogger(__name__)


def source_url(year: int, month: int) -> str:
    return config.SOURCE_URL.format(year=year, month=month)


def source_exists(year: int, month: int) -> tuple[bool, int]:
    """HEAD the source. Returns (available, content_length)."""
    req = urllib.request.Request(source_url(year, month), method="HEAD")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status == 200, int(r.headers.get("Content-Length", 0))
    except Exception as exc:  # 403/404 both mean "not published yet"
        log.info("source not available for %04d-%02d: %s", year, month, exc)
        return False, 0


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ingest(year: int, month: int, force: bool = False) -> dict:
    """Download one month into bronze.

    Idempotent by construction: the download goes to a temp file in the same
    directory and is moved into place with os.replace, which is atomic on the
    same filesystem. A crash mid-download therefore cannot leave a truncated
    parquet behind for the next task to read. Re-running with an unchanged
    source is a no-op.
    """
    config.ensure_dirs()
    dest_dir = config.partition_path(config.BRONZE, year, month)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "trips.parquet"

    available, remote_size = source_exists(year, month)
    if not available:
        raise FileNotFoundError(f"TLC has not published {year:04d}-{month:02d} yet")

    if dest.exists() and not force:
        local_size = dest.stat().st_size
        if local_size == remote_size:
            log.info("bronze %04d-%02d already landed (%d bytes), skipping download",
                     year, month, local_size)
            return {
                "year": year, "month": month, "path": str(dest),
                "bytes": local_size, "sha256": _sha256(dest), "downloaded": False,
            }
        log.warning("bronze %04d-%02d size drift local=%d remote=%d, re-downloading",
                    year, month, local_size, remote_size)

    tmp = dest_dir / f".{dest.name}.tmp"
    try:
        with urllib.request.urlopen(source_url(year, month), timeout=300) as r, tmp.open("wb") as fh:
            while chunk := r.read(1 << 20):
                fh.write(chunk)
        if remote_size and tmp.stat().st_size != remote_size:
            raise IOError(f"short read: got {tmp.stat().st_size} of {remote_size} bytes")
        os.replace(tmp, dest)          # atomic publish
    finally:
        tmp.unlink(missing_ok=True)

    meta = {
        "year": year, "month": month, "path": str(dest),
        "bytes": dest.stat().st_size, "sha256": _sha256(dest), "downloaded": True,
    }
    log.info("bronze %04d-%02d landed: %d bytes sha256=%.12s",
             year, month, meta["bytes"], meta["sha256"])
    return meta
