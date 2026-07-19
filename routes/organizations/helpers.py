import uuid
from typing import List
from fastapi import HTTPException, status
from db_queues.postgresql.organizations import (
    ROLE_RANK,
    get_membership_role,
)
from db_queues.postgresql.auth import get_organization, get_user_by_id
from routes.auth.helper import (
    _create_access_token,
    _create_refresh_token 
)

from shared.model import LoginResponse, UserPublic

_MAX_NAME = 80

def _uid(session: dict) -> uuid.UUID:
    return uuid.UUID(session["sub"])


async def _require_role(user_id: uuid.UUID, org_id: uuid.UUID, min_role: str | List[str]) -> str:
    """Return the caller's role in ``org_id`` or raise 403/404."""
    role = await get_membership_role(org_id, user_id)
    if role is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Organization not found")
    if(isinstance(min_role, str)):
        if ROLE_RANK[role] < ROLE_RANK[min_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires the {min_role} role.",
            )
    if(isinstance(min_role, List)):
        max_role = "member"
        for r in min_role:
            if ROLE_RANK[r] > ROLE_RANK[max_role]:
                max_role = r
        if ROLE_RANK[role] < ROLE_RANK[max_role]:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires the {min_role} role.",
            )
    return role


def _clean_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="Organization name is required.")
    if len(name) > _MAX_NAME:
        raise HTTPException(status_code=400, detail=f"Name must be ≤ {_MAX_NAME} characters.")
    return name


async def _build_session(user_id: uuid.UUID, org_id: uuid.UUID) -> LoginResponse:
    """Assemble a fresh login-shaped session for ``user_id`` scoped to ``org_id``."""
    user = await get_user_by_id(user_id)
    organization = await get_organization(org_id)
    if user is None or organization is None:
        raise HTTPException(status_code=404, detail="Account or organization not found")
    access_token, expires_in = _create_access_token(user_id=str(user_id), org_id=str(org_id))
    refresh_token, refresh_expires_in = _create_refresh_token(
        user_id=str(user_id), org_id=str(org_id)
    )
    return LoginResponse(
        user=UserPublic(**user.model_dump(exclude={"hashed_password"})),
        organization=organization,
        access_token=access_token,
        expires_in=expires_in,
        refresh_token=refresh_token,
        refresh_expires_in=refresh_expires_in,
    )