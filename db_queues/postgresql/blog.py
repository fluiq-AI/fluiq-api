"""PostgreSQL CRUD helpers for the blog (posts + media).

Posts are platform-wide marketing content (not org-scoped). Writes are gated to
Admin users at the route layer; reads of *published* posts are public.
"""
from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Tuple

from . import postgres_client

_POST_COLUMNS = """
    post_id, slug, title, excerpt, body_html, cover_image_url, author, tags,
    status, seo_title, seo_description, reading_minutes,
    published_at, created_at, updated_at
"""


# ── Public reads ──────────────────────────────────────────────────────────────

async def list_published(
    limit: int = 20,
    offset: int = 0,
    tag: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Return (rows, total) of published posts, newest first. Excludes body_html."""
    where = "status = 'published'"
    args: List[Any] = []
    if tag:
        args.append(tag)
        where += f" AND ${len(args)} = ANY(tags)"

    async with postgres_client.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM blog_posts WHERE {where}", *args)
        args.extend([limit, offset])
        rows = await conn.fetch(
            f"""
            SELECT post_id, slug, title, excerpt, cover_image_url, author, tags,
                   reading_minutes, published_at
            FROM blog_posts
            WHERE {where}
            ORDER BY published_at DESC NULLS LAST, created_at DESC
            LIMIT ${len(args) - 1} OFFSET ${len(args)}
            """,
            *args,
        )
        return [dict(r) for r in rows], int(total or 0)


async def get_published_by_slug(slug: str) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_POST_COLUMNS} FROM blog_posts WHERE slug = $1 AND status = 'published'",
            slug,
        )
        return dict(row) if row else None


async def list_published_slugs() -> List[str]:
    """All published slugs — consumed by the frontend build to prerender posts."""
    async with postgres_client.acquire() as conn:
        rows = await conn.fetch(
            "SELECT slug FROM blog_posts WHERE status = 'published' "
            "ORDER BY published_at DESC NULLS LAST"
        )
        return [r["slug"] for r in rows]


# ── Admin reads ───────────────────────────────────────────────────────────────

async def list_all(
    page: int = 1,
    limit: int = 50,
    search: str = "",
    status: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], int]:
    where = "TRUE"
    args: List[Any] = []
    if search:
        args.append(f"%{search.lower()}%")
        where += f" AND (LOWER(title) LIKE ${len(args)} OR LOWER(slug) LIKE ${len(args)})"
    if status:
        args.append(status)
        where += f" AND status = ${len(args)}"

    async with postgres_client.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*) FROM blog_posts WHERE {where}", *args)
        offset = (page - 1) * limit
        args.extend([limit, offset])
        rows = await conn.fetch(
            f"""
            SELECT post_id, slug, title, excerpt, cover_image_url, author, tags,
                   status, reading_minutes, published_at, created_at, updated_at
            FROM blog_posts
            WHERE {where}
            ORDER BY created_at DESC
            LIMIT ${len(args) - 1} OFFSET ${len(args)}
            """,
            *args,
        )
        return [dict(r) for r in rows], int(total or 0)


async def get_by_id(post_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"SELECT {_POST_COLUMNS} FROM blog_posts WHERE post_id = $1", post_id
        )
        return dict(row) if row else None


# ── Admin writes ──────────────────────────────────────────────────────────────

async def create_post(
    *,
    slug: str,
    title: str,
    excerpt: str,
    body_html: str,
    cover_image_url: Optional[str],
    author: str,
    tags: List[str],
    status: str,
    seo_title: Optional[str],
    seo_description: Optional[str],
    reading_minutes: int,
) -> Dict[str, Any]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"""
            INSERT INTO blog_posts
                (slug, title, excerpt, body_html, cover_image_url, author, tags,
                 status, seo_title, seo_description, reading_minutes, published_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                    CASE WHEN $8 = 'published' THEN NOW() ELSE NULL END)
            RETURNING {_POST_COLUMNS}
            """,
            slug, title, excerpt, body_html, cover_image_url, author, tags,
            status, seo_title, seo_description, reading_minutes,
        )
        return dict(row)


async def update_post(post_id: uuid.UUID, **fields: Any) -> Optional[Dict[str, Any]]:
    """Patch the given columns. ``status`` transitions manage ``published_at``."""
    allowed = {
        "slug", "title", "excerpt", "body_html", "cover_image_url", "author",
        "tags", "status", "seo_title", "seo_description", "reading_minutes",
    }
    sets: List[str] = []
    args: List[Any] = []
    for key, value in fields.items():
        if key not in allowed or value is None:
            continue
        args.append(value)
        sets.append(f"{key} = ${len(args)}")

    # Stamp published_at the first time a post becomes published.
    if fields.get("status") == "published":
        sets.append("published_at = COALESCE(published_at, NOW())")
    elif fields.get("status") == "draft":
        sets.append("published_at = NULL")

    if not sets:
        return await get_by_id(post_id)

    sets.append("updated_at = NOW()")
    args.append(post_id)
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE blog_posts SET {', '.join(sets)} "
            f"WHERE post_id = ${len(args)} RETURNING {_POST_COLUMNS}",
            *args,
        )
        return dict(row) if row else None


async def delete_post(post_id: uuid.UUID) -> bool:
    async with postgres_client.acquire() as conn:
        result = await conn.execute("DELETE FROM blog_posts WHERE post_id = $1", post_id)
        return result.split()[-1] != "0"


# ── Media ─────────────────────────────────────────────────────────────────────

async def insert_media(filename: str, content_type: str, data: bytes) -> uuid.UUID:
    async with postgres_client.acquire() as conn:
        media_id = await conn.fetchval(
            """
            INSERT INTO blog_media (filename, content_type, data, byte_size)
            VALUES ($1, $2, $3, $4)
            RETURNING media_id
            """,
            filename, content_type, data, len(data),
        )
        return media_id


async def get_media(media_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT content_type, data FROM blog_media WHERE media_id = $1", media_id
        )
        return dict(row) if row else None
