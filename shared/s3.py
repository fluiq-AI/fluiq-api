"""S3 helpers for blog media.

Objects live in a private bucket (``config.S3_BLOG_MEDIA_BUCKET``); only the
object key is persisted in Postgres. Reads are served via short-lived presigned
GET URLs so the bucket never needs public access. Credentials are resolved by
boto3's default chain — in production that's the ECS task role (no static keys).

boto3 is synchronous; ``put_object`` does network I/O and must be called via a
threadpool from async routes. ``presigned_url`` is a local signing operation
(no network) and is cheap to call inline.
"""
from __future__ import annotations

import logging
from functools import lru_cache
from typing import Optional

import boto3
from botocore.config import Config

import config

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _client():
    # SigV4 is required for presigned URLs to work across all regions.
    return boto3.client(
        "s3",
        region_name=config.AWS_REGION,
        config=Config(signature_version="s3v4"),
    )


def _require_bucket() -> str:
    bucket = config.S3_BLOG_MEDIA_BUCKET
    if not bucket:
        raise RuntimeError("S3_BLOG_MEDIA_BUCKET is not configured")
    return bucket


def put_object(key: str, body: bytes, content_type: str) -> None:
    """Upload bytes to the blog-media bucket. Blocking — run in a threadpool."""
    _client().put_object(
        Bucket=_require_bucket(),
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def presigned_url(key: str, content_type: Optional[str] = None, ttl: Optional[int] = None) -> str:
    """Return a short-lived presigned GET URL for an object key."""
    params = {"Bucket": _require_bucket(), "Key": key}
    if content_type:
        # Make S3 serve the right Content-Type for inline <img> display.
        params["ResponseContentType"] = content_type
    return _client().generate_presigned_url(
        "get_object",
        Params=params,
        ExpiresIn=ttl or config.S3_PRESIGN_TTL,
    )


def delete_object(key: str) -> None:
    """Best-effort delete of an object key. Never raises."""
    try:
        _client().delete_object(Bucket=_require_bucket(), Key=key)
    except Exception as exc:  # noqa: BLE001 — orphan cleanup must not fail callers
        logger.warning("[S3] failed to delete %s: %s", key, exc)
