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
    # Pin to the regional S3 endpoint (+ virtual addressing) so presigned URLs
    # are signed for `bucket.s3.<region>.amazonaws.com`. Without this, boto3 may
    # emit the global `s3.amazonaws.com` host, which S3 answers with a
    # TemporaryRedirect for non-us-east-1 buckets — breaking the SigV4 signature.
    #
    # An S3_ENDPOINT_URL override (e.g. MinIO) uses path-style addressing, since
    # S3-compatible servers usually don't support virtual-host buckets.
    endpoint = config.S3_ENDPOINT_URL or f"https://s3.{config.AWS_REGION}.amazonaws.com"
    style = "path" if config.S3_ENDPOINT_URL else "virtual"
    return boto3.client(
        "s3",
        region_name=config.AWS_REGION,
        endpoint_url=endpoint,
        config=Config(signature_version="s3v4", s3={"addressing_style": style}),
    )


@lru_cache(maxsize=1)
def _presign_client():
    # Presigned URLs the BROWSER uses must be signed for a host the browser can
    # reach. Locally that's the published MinIO endpoint (localhost), which
    # differs from the internal one the API uses. In prod both are unset → the
    # regional AWS endpoint, identical to _client().
    override = config.S3_PUBLIC_ENDPOINT_URL or config.S3_ENDPOINT_URL
    endpoint = override or f"https://s3.{config.AWS_REGION}.amazonaws.com"
    style = "path" if override else "virtual"
    return boto3.client(
        "s3",
        region_name=config.AWS_REGION,
        endpoint_url=endpoint,
        config=Config(signature_version="s3v4", s3={"addressing_style": style}),
    )


def _require_bucket(bucket: Optional[str] = None) -> str:
    b = bucket or config.S3_BLOG_MEDIA_BUCKET
    if not b:
        raise RuntimeError("S3 bucket is not configured")
    return b


def presigned_put_url(key: str, bucket: Optional[str] = None, ttl: Optional[int] = None) -> str:
    """Return a short-lived presigned PUT URL so a client can upload directly to S3.

    Content-Type is intentionally NOT part of the signed params, so the browser
    can PUT with whatever type it likes without breaking the signature.
    """
    return _presign_client().generate_presigned_url(
        "put_object",
        Params={"Bucket": _require_bucket(bucket), "Key": key},
        ExpiresIn=ttl or config.S3_PRESIGN_TTL,
    )


def get_object_bytes(key: str, bucket: Optional[str] = None) -> bytes:
    """Fetch an object's bytes. Blocking — run in a threadpool from async routes."""
    resp = _client().get_object(Bucket=_require_bucket(bucket), Key=key)
    return resp["Body"].read()


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


def object_size(key: str, bucket: Optional[str] = None) -> int:
    """Object size in bytes via HEAD (so a huge upload can be rejected before read)."""
    resp = _client().head_object(Bucket=_require_bucket(bucket), Key=key)
    return int(resp.get("ContentLength", 0))


def delete_object(key: str, bucket: Optional[str] = None) -> None:
    """Best-effort delete of an object key. Never raises."""
    try:
        _client().delete_object(Bucket=_require_bucket(bucket), Key=key)
    except Exception as exc:  # noqa: BLE001 — orphan cleanup must not fail callers
        logger.warning("[S3] failed to delete %s: %s", key, exc)
