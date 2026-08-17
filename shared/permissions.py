"""Who may do what inside an organization.

Roles live on ``organization_members``. Until now there were three and they
differed only in who could manage members, so the checks that existed were
scattered and mostly implicit.

``annotator`` changes that. Subject-matter experts — clinicians, lawyers,
domain reviewers — are frequently contractors rather than staff, and the only
thing they need is to read traces and record verdicts. Giving them ``member`` to
let them annotate would also hand them API keys, provider credentials, billing,
and the ability to delete a dataset. Teams facing that choice tend to pick
"don't invite them", and then do the review badly in-house.

So the rule is stated once, here, rather than re-derived at each route.
"""
from __future__ import annotations

import uuid
from typing import Iterable, Optional, Set

from fastapi import HTTPException, status

from db_queues.postgresql import postgres_client

#: Every role, most privileged first.
ROLES = ("owner", "admin", "member", "annotator")

#: Roles that may read and write ordinary product data.
FULL_ACCESS: Set[str] = {"owner", "admin", "member"}

#: Roles that may change org-level settings and membership.
ADMIN_ACCESS: Set[str] = {"owner", "admin"}

#: What an annotator may reach. Everything else is refused — an allowlist, not a
#: denylist, because a new route should be invisible to a contractor until
#: someone decides otherwise rather than exposed until someone notices.
ANNOTATOR_PATHS = (
    "/api/v1/review",              # the queue and the 2x2
    "/api/v1/rubric",              # the questions they answer
    "/api/v1/traces",              # reading a trace, and reviewing/annotating it
    "/api/v1/auth",                # their own session
)


async def role_of(user_id: str, org_id: uuid.UUID) -> Optional[str]:
    """This user's role in this org, or None when they are not a member."""
    async with postgres_client.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT role FROM organization_members WHERE org_id = $1 AND user_id = $2::uuid",
            org_id, str(user_id),
        )
    return row["role"] if row else None


async def require_role(session: dict, allowed: Iterable[str]) -> uuid.UUID:
    """Assert the caller holds one of ``allowed``; return the org id.

    Fails closed. A membership row that cannot be read is treated as no
    membership, because the alternative — assuming a role on a database blip —
    grants access on exactly the failure you would least like it to.
    """
    org_id = uuid.UUID(session["org_id"])
    user_id = session.get("sub")
    if not user_id:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not signed in")

    allowed = set(allowed)
    try:
        role = await role_of(str(user_id), org_id)
    except Exception:  # noqa: BLE001
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE,
            "Could not verify your permissions; please retry.",
        ) from None

    # A legacy deployment may predate organization_members for some users. The
    # backfill in schema.sql covers owners; anyone else missing a row is treated
    # as a plain member, which is what they effectively were before roles
    # existed — but never as an admin.
    effective = role or "member"
    if effective not in allowed:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"This action needs one of: {', '.join(sorted(allowed))}. "
            f"Your role: {effective}.",
        )
    return org_id


def annotator_may_reach(path: str) -> bool:
    """Whether an annotator is allowed at this path."""
    return any(path.startswith(prefix) for prefix in ANNOTATOR_PATHS)


__all__ = [
    "ADMIN_ACCESS",
    "ANNOTATOR_PATHS",
    "FULL_ACCESS",
    "ROLES",
    "annotator_may_reach",
    "require_role",
    "role_of",
]
