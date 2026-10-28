"""Bronze ingestion: download source zip files and keep them byte-for-byte.

Every distinct version of a source file lands exactly once, read-only, under

    bronze/landing/<family>/<name>/<sha256[:12]>/<name>.zip

and is recorded as a "landed" event in the manifest. Before downloading we
compare the server's Last-Modified and size with earlier events, and after
downloading we compare checksums, so reruns and the weekly schedule neither
re-download unchanged files nor create duplicate versions.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
import tempfile
import urllib.request
from email.utils import formatdate
from pathlib import Path
from urllib.parse import unquote, urlparse

from maude.bronze.manifest import Manifest
from maude.config import Config
from maude.run_context import RunContext
from maude.sources import SourceFile

log = logging.getLogger(__name__)

USER_AGENT = "dat535-group17-maude-pipeline (+https://github.com/erikhafnor/group17)"
CHUNK_SIZE = 1 << 20
TIMEOUT_SECONDS = 120


def source_url(cfg: Config, src: SourceFile) -> str:
    base = cfg.classification_base_url if src.classification else cfg.source_base_url
    return f"{base}/{src.name}.zip"


def _local_path(url: str) -> Path:
    return Path(unquote(urlparse(url).path))


def _probe(url: str) -> dict:
    """Last-Modified and size of a source file, without downloading it."""
    if url.startswith("file://"):
        stat = _local_path(url).stat()
        return {"last_modified": formatdate(stat.st_mtime, usegmt=True), "bytes": stat.st_size}
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
        length = response.headers.get("Content-Length")
        return {
            "last_modified": response.headers.get("Last-Modified"),
            "bytes": int(length) if length else None,
        }


def _download(url: str, staging_dir: Path) -> tuple[Path, str, int]:
    """Stream a file to the staging area, returning its path, sha256 and size."""
    staging_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with (
        urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response,
        tempfile.NamedTemporaryFile(dir=staging_dir, suffix=".zip", delete=False) as out,
    ):
        while chunk := response.read(CHUNK_SIZE):
            digest.update(chunk)
            out.write(chunk)
            size += len(chunk)
    return Path(out.name), digest.hexdigest(), size


def ingest(cfg: Config, run: RunContext, sources: list[SourceFile]) -> list[dict]:
    manifest = Manifest(cfg.manifest_path)
    landed = manifest.landed()
    results = []

    for src in sources:
        url = source_url(cfg, src)
        earlier = [e for e in landed if e["url"] == url]
        remote = _probe(url)

        if remote["last_modified"] and any(
            e["last_modified"] == remote["last_modified"] and e["bytes"] == remote["bytes"]
            for e in earlier
        ):
            log.info("%-28s unchanged at source, not downloaded", src.name)
            results.append({"name": src.name, "status": "unchanged"})
            continue

        tmp, sha256, size = _download(url, cfg.staging_dir)
        if any(e["sha256"] == sha256 for e in earlier):
            tmp.unlink()
            log.info("%-28s already landed (same checksum), discarded", src.name)
            results.append({"name": src.name, "status": "duplicate"})
            continue

        dest = cfg.landing_dir / src.family / src.name / sha256[:12] / f"{src.name}.zip"
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(tmp, dest)
        dest.chmod(0o444)

        event = {
            "event": "landed",
            "file_id": f"{src.name}-{sha256[:12]}",
            "name": src.name,
            "family": src.family,
            "kind": src.kind,
            "fmt": src.fmt,
            "encoding": src.encoding,
            "columns": list(src.columns) if src.columns else None,
            "url": url,
            "path": str(dest.relative_to(cfg.data_root)),
            "sha256": sha256,
            "bytes": size,
            "last_modified": remote["last_modified"],
            "run_id": run.run_id,
            "at": run.started_iso,
        }
        manifest.append(event)
        landed.append(event)
        log.info("%-28s landed %s (%.1f MB)", src.name, event["file_id"], size / 1e6)
        results.append(
            {"name": src.name, "status": "landed", "file_id": event["file_id"], "bytes": size}
        )
    return results
