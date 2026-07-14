"""Offload trajectory media to S3 for dataset snapshots.

A dataset example pins a run's full trajectory (see the ClickHouse
``dataset_trajectory_spans`` table) so agentic eval can run offline forever.
Media in that trajectory (`_media_ref` parts — see the SDK's media_reference)
carries either a small inline ``data`` (base64) or a ``url``; neither survives
long-term (inline is capped/stripped, origin URLs rot). :func:`store_trajectory_media`
recovers the bytes once, uploads them to the private S3 bucket, and rewrites the
ref to carry a stable ``s3_key`` (dropping the volatile payload).
:func:`hydrate_trajectory_media` turns that key back into a fresh presigned URL
at read time so the evaluator's vision/OCR paths (which fetch ``source == 'url'``)
work unchanged.

All best-effort and fail-open: media that can't be recovered keeps its original
ref, and no failure here ever breaks the import / eval.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import urllib.request
from typing import Any, Callable, Dict, List

from shared import s3

logger = logging.getLogger(__name__)

# Cap a single fetched/stored media object so a hostile or huge URL can't blow
# up the import. 8 MiB matches the security worker's OCR fetch cap.
_MAX_MEDIA_BYTES = 8 * 1024 * 1024
_FETCH_TIMEOUT = 8

_MIME_EXT = {
    "image/png": ".png", "image/jpeg": ".jpg", "image/jpg": ".jpg",
    "image/gif": ".gif", "image/webp": ".webp",
    "audio/mpeg": ".mp3", "audio/wav": ".wav", "audio/mp3": ".mp3",
    "application/pdf": ".pdf",
}


def _ext_for(mime: str | None) -> str:
    if not mime:
        return ""
    return _MIME_EXT.get(str(mime).lower(), "")


def _each_media_ref(events: List[dict], fn: Callable[[Dict[str, Any]], None]) -> None:
    """Invoke ``fn`` on every inner ``_media_ref`` dict found anywhere in events."""
    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            ref = obj.get("_media_ref")
            if isinstance(ref, dict):
                fn(ref)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for v in obj:
                walk(v)
    for ev in events:
        walk(ev)


def _fetch_url(url: str) -> bytes | None:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "fluiq-dataset-media"})
        with urllib.request.urlopen(req, timeout=_FETCH_TIMEOUT) as resp:  # noqa: S310
            chunks: List[bytes] = []
            total = 0
            while True:
                chunk = resp.read(65536)
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_MEDIA_BYTES:
                    return None
                chunks.append(chunk)
            return b"".join(chunks)
    except Exception:
        return None


def _recover_bytes(ref: Dict[str, Any]) -> bytes | None:
    """Get the media bytes from an inline base64 ``data`` or a fetchable ``url``."""
    source = ref.get("source")
    if source == "base64" and ref.get("data"):
        try:
            return base64.b64decode(str(ref["data"]) + "===", validate=False)
        except Exception:
            return None
    if source == "url":
        url = ref.get("url")
        if isinstance(url, str) and url.startswith(("http://", "https://")):
            return _fetch_url(url)
    return None


async def store_trajectory_media(org_id: Any, events: List[dict]) -> List[dict]:
    """Upload every recoverable media ref to S3 and rewrite it to carry s3_key.

    Mutates ``events`` in place (and returns it). Best-effort: a ref whose bytes
    can't be recovered, or an upload that fails, is left untouched.
    """
    refs: List[Dict[str, Any]] = []
    _each_media_ref(events, refs.append)
    for ref in refs:
        if ref.get("s3_key"):
            continue  # already stored
        try:
            data = await asyncio.to_thread(_recover_bytes, ref)
            if not data:
                continue
            sha = ref.get("sha256") or hashlib.sha256(data).hexdigest()[:16]
            mime = ref.get("mime") or "application/octet-stream"
            key = f"datasets/media/{org_id}/{sha}{_ext_for(mime)}"
            await asyncio.to_thread(s3.put_object, key, data, str(mime))
            ref["s3_key"] = key
            ref.pop("data", None)  # payload now lives in S3, not the snapshot
        except Exception as exc:  # noqa: BLE001 — media offload must never break import
            logger.warning("[DATASET-MEDIA] offload failed sha=%s: %s", ref.get("sha256"), exc)
    return events


def hydrate_trajectory_media(events: List[dict]) -> List[dict]:
    """Turn stored ``s3_key`` refs back into fresh presigned URLs (read time).

    Sets ``url`` + ``source='url'`` so the evaluator's existing url-based media
    extraction (vision judging, OCR injection) works against the S3 copy.
    """
    def hydrate(ref: Dict[str, Any]) -> None:
        key = ref.get("s3_key")
        if not key:
            return
        try:
            ref["url"] = s3.presigned_url(str(key), content_type=ref.get("mime"))
            ref["source"] = "url"
        except Exception:
            pass
    _each_media_ref(events, hydrate)
    return events
