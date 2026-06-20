"""fluiq-api — Blog routes (public reads + admin-authored content).

Public:
  GET    /api/v1/blog/posts                 list published posts (paginated)
  GET    /api/v1/blog/posts/{slug}          fetch a single published post
  GET    /api/v1/blog/slugs                 published slugs (frontend prerender)
  GET    /api/v1/blog/media/{media_id}      serve an uploaded image

Admin (require_admin):
  GET    /api/v1/blog/admin/posts           list all posts (incl. drafts)
  POST   /api/v1/blog/admin/posts           create
  GET    /api/v1/blog/admin/posts/{id}      fetch one (incl. draft)
  PATCH  /api/v1/blog/admin/posts/{id}      update
  DELETE /api/v1/blog/admin/posts/{id}      delete
  POST   /api/v1/blog/admin/posts/{id}/publish   {publish: bool}
  POST   /api/v1/blog/admin/media           upload image (multipart)
"""
from __future__ import annotations

import logging
import re
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import (
    APIRouter, Depends, File, HTTPException, Query, UploadFile, status,
)
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, field_validator

import config
from db_queues.postgresql import blog as blog_db
from routes.admin import require_admin
from shared import s3

logger = logging.getLogger(__name__)

blog_router = APIRouter(prefix="/blog")

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-]{0,126}$")
_MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB
_ALLOWED_IMAGE_TYPES = {"image/png", "image/jpeg", "image/webp", "image/gif", "image/svg+xml"}


# ── Models ────────────────────────────────────────────────────────────────────

class PostCreate(BaseModel):
    slug: str
    title: str
    excerpt: str = ""
    body_html: str = ""
    cover_image_url: Optional[str] = None
    author: str = "Fluiq"
    tags: List[str] = []
    status: str = "draft"
    seo_title: Optional[str] = None
    seo_description: Optional[str] = None

    @field_validator("slug")
    @classmethod
    def _slug(cls, v: str) -> str:
        v = v.strip().lower()
        if not _SLUG_RE.match(v):
            raise ValueError(
                "slug must be 1–127 lowercase alphanumeric characters or hyphens, "
                "and cannot start or end with a hyphen"
            )
        return v

    @field_validator("title")
    @classmethod
    def _title(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("title is required")
        return v.strip()

    @field_validator("status")
    @classmethod
    def _status(cls, v: str) -> str:
        if v not in ("draft", "published"):
            raise ValueError("status must be 'draft' or 'published'")
        return v


class PostUpdate(BaseModel):
    slug: Optional[str] = None
    title: Optional[str] = None
    excerpt: Optional[str] = None
    body_html: Optional[str] = None
    cover_image_url: Optional[str] = None
    author: Optional[str] = None
    tags: Optional[List[str]] = None
    status: Optional[str] = None
    seo_title: Optional[str] = None
    seo_description: Optional[str] = None

    @field_validator("slug")
    @classmethod
    def _slug(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        v = v.strip().lower()
        if not _SLUG_RE.match(v):
            raise ValueError("invalid slug")
        return v

    @field_validator("status")
    @classmethod
    def _status(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and v not in ("draft", "published"):
            raise ValueError("status must be 'draft' or 'published'")
        return v


class PublishRequest(BaseModel):
    publish: bool = True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _reading_minutes(body_html: str) -> int:
    text = re.sub(r"<[^>]+>", " ", body_html or "")
    words = len(text.split())
    return max(1, round(words / 200))


async def _trigger_rebuild() -> None:
    """Kick the Render deploy hook so the static site re-prerenders. Fails open."""
    url = config.RENDER_DEPLOY_HOOK_URL
    if not url:
        return
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(url)
        logger.info("[BLOG] triggered frontend rebuild")
    except Exception as exc:  # noqa: BLE001 — never fail a publish on this
        logger.warning("[BLOG] rebuild trigger failed: %s", exc)


def _serialize_summary(row: dict) -> dict:
    return {
        "post_id":         str(row["post_id"]),
        "slug":            row["slug"],
        "title":           row["title"],
        "excerpt":         row.get("excerpt", ""),
        "cover_image_url": row.get("cover_image_url"),
        "author":          row.get("author", "Fluiq"),
        "tags":            row.get("tags") or [],
        "status":          row.get("status"),
        "reading_minutes": row.get("reading_minutes", 1),
        "published_at":    _iso(row.get("published_at")),
        "created_at":      _iso(row.get("created_at")),
        "updated_at":      _iso(row.get("updated_at")),
    }


def _serialize_full(row: dict) -> dict:
    return {
        **_serialize_summary(row),
        "body_html":       row.get("body_html", ""),
        "seo_title":       row.get("seo_title"),
        "seo_description": row.get("seo_description"),
    }


def _iso(value: Any) -> Optional[str]:
    return value.isoformat() if hasattr(value, "isoformat") else value


# ── Public ────────────────────────────────────────────────────────────────────

@blog_router.get("/posts")
async def list_posts(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    tag: Optional[str] = Query(None),
):
    rows, total = await blog_db.list_published(limit=limit, offset=offset, tag=tag)
    return {"posts": [_serialize_summary(r) for r in rows], "total": total}


@blog_router.get("/slugs")
async def published_slugs():
    return {"slugs": await blog_db.list_published_slugs()}


@blog_router.get("/posts/{slug}")
async def get_post(slug: str):
    row = await blog_db.get_published_by_slug(slug)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post not found")
    return _serialize_full(row)


@blog_router.get("/media/{media_id}")
async def get_media(media_id: uuid.UUID):
    """Redirect to a short-lived presigned S3 URL for the object.

    The stable URL stored in post content is this endpoint; the presigned URL it
    redirects to expires, so it's never baked into ``body_html``. A 307 keeps the
    method and lets <img> tags follow the redirect transparently.
    """
    row = await blog_db.get_media(media_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Media not found")
    url = s3.presigned_url(row["s3_key"], content_type=row["content_type"])
    return RedirectResponse(url, status_code=status.HTTP_307_TEMPORARY_REDIRECT)


# ── Admin ─────────────────────────────────────────────────────────────────────

@blog_router.get("/admin/posts")
async def admin_list_posts(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query(""),
    post_status: Optional[str] = Query(None, alias="status"),
    _session: dict = Depends(require_admin),
):
    rows, total = await blog_db.list_all(page=page, limit=limit, search=search, status=post_status)
    return {
        "posts": [_serialize_summary(r) for r in rows],
        "total": total, "page": page, "limit": limit,
    }


@blog_router.post("/admin/posts", status_code=status.HTTP_201_CREATED)
async def admin_create_post(payload: PostCreate, _session: dict = Depends(require_admin)):
    try:
        row = await blog_db.create_post(
            slug=payload.slug,
            title=payload.title,
            excerpt=payload.excerpt,
            body_html=payload.body_html,
            cover_image_url=payload.cover_image_url,
            author=payload.author,
            tags=payload.tags,
            status=payload.status,
            seo_title=payload.seo_title,
            seo_description=payload.seo_description,
            reading_minutes=_reading_minutes(payload.body_html),
        )
    except Exception as exc:  # noqa: BLE001
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"A post with slug '{payload.slug}' already exists.",
            )
        raise
    if row["status"] == "published":
        await _trigger_rebuild()
    return _serialize_full(row)


@blog_router.get("/admin/posts/{post_id}")
async def admin_get_post(post_id: uuid.UUID, _session: dict = Depends(require_admin)):
    row = await blog_db.get_by_id(post_id)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post not found")
    return _serialize_full(row)


@blog_router.patch("/admin/posts/{post_id}")
async def admin_update_post(
    post_id: uuid.UUID,
    payload: PostUpdate,
    _session: dict = Depends(require_admin),
):
    fields: Dict[str, Any] = payload.model_dump(exclude_unset=True)
    if "body_html" in fields:
        fields["reading_minutes"] = _reading_minutes(fields["body_html"] or "")
    try:
        row = await blog_db.update_post(post_id, **fields)
    except Exception as exc:  # noqa: BLE001
        if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Slug already in use.")
        raise
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post not found")
    # Any edit to a live post should re-prerender the static site.
    if row["status"] == "published":
        await _trigger_rebuild()
    return _serialize_full(row)


@blog_router.post("/admin/posts/{post_id}/publish")
async def admin_publish_post(
    post_id: uuid.UUID,
    payload: PublishRequest,
    _session: dict = Depends(require_admin),
):
    new_status = "published" if payload.publish else "draft"
    row = await blog_db.update_post(post_id, status=new_status)
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post not found")
    await _trigger_rebuild()  # publish OR unpublish both change the static output
    return _serialize_full(row)


@blog_router.delete("/admin/posts/{post_id}", status_code=status.HTTP_204_NO_CONTENT)
async def admin_delete_post(post_id: uuid.UUID, _session: dict = Depends(require_admin)):
    existing = await blog_db.get_by_id(post_id)
    deleted = await blog_db.delete_post(post_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Post not found")
    if existing and existing["status"] == "published":
        await _trigger_rebuild()


@blog_router.post("/admin/media", status_code=status.HTTP_201_CREATED)
async def admin_upload_media(
    file: UploadFile = File(...),
    _session: dict = Depends(require_admin),
):
    content_type = file.content_type or "application/octet-stream"
    if content_type not in _ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported type '{content_type}'. Allowed: {', '.join(sorted(_ALLOWED_IMAGE_TYPES))}",
        )
    data = await file.read()
    if len(data) > _MAX_IMAGE_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Image exceeds the 5 MB limit.",
        )
    filename = file.filename or "upload"
    # Opaque, collision-free key; preserve the extension for nicer downloads.
    ext = ""
    if "." in filename:
        ext = "." + re.sub(r"[^a-zA-Z0-9]", "", filename.rsplit(".", 1)[-1])[:8].lower()
    s3_key = f"blog/{uuid.uuid4().hex}{ext}"
    await run_in_threadpool(s3.put_object, s3_key, data, content_type)
    media_id = await blog_db.insert_media(filename, content_type, s3_key, len(data))
    return {"media_id": str(media_id), "url": f"/api/v1/blog/media/{media_id}"}


__all__ = ["blog_router"]
