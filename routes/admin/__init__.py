import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from db_queues.postgresql.auth import (
    admin_list_organizations,
    admin_list_users,
    admin_update_user_type,
    get_platform_stats,
    get_user_by_id,
)
from routes.auth.helper import get_current_session

admin_router = APIRouter()

# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

async def require_admin(session: dict = Depends(get_current_session)) -> dict:
    user = await get_user_by_id(uuid.UUID(session["sub"]))
    if user is None or user.user_type != "Admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin access required",
        )
    return session

# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------

class PlatformStatsResponse(BaseModel):
    total_users: int
    total_orgs: int
    users_by_type: dict[str, int]


class UserAdminView(BaseModel):
    user_id: uuid.UUID
    email: str
    name: str
    user_type: str
    org_id: uuid.UUID
    org_name: Optional[str]
    created_at: datetime


class UserListResponse(BaseModel):
    users: list[UserAdminView]
    total: int
    page: int
    limit: int


class OrgAdminView(BaseModel):
    org_id: uuid.UUID
    org_name: str
    owner_email: Optional[str]
    owner_name: Optional[str]
    owner_type: Optional[str]
    api_key_usage: int
    api_key_limit: int
    created_at: datetime


class OrgListResponse(BaseModel):
    organizations: list[OrgAdminView]
    total: int
    page: int
    limit: int


class UpdateUserTypeRequest(BaseModel):
    user_type: str


class UpdateUserTypeResponse(BaseModel):
    ok: bool


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

_BILLABLE_TYPES = {"Free", "Team", "Growth", "Enterprise"}


@admin_router.get("/stats", response_model=PlatformStatsResponse)
async def platform_stats(
    _session: dict = Depends(require_admin),
) -> PlatformStatsResponse:
    data = await get_platform_stats()
    return PlatformStatsResponse(**data)


@admin_router.get("/users", response_model=UserListResponse)
async def list_users(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    search: str = Query(""),
    user_type: Optional[str] = Query(None),
    _session: dict = Depends(require_admin),
) -> UserListResponse:
    rows, total = await admin_list_users(
        page=page,
        limit=limit,
        search=search,
        user_type_filter=user_type,
    )
    return UserListResponse(
        users=[UserAdminView(**r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


@admin_router.patch("/users/{user_id}", response_model=UpdateUserTypeResponse)
async def update_user(
    user_id: uuid.UUID,
    body: UpdateUserTypeRequest,
    _session: dict = Depends(require_admin),
) -> UpdateUserTypeResponse:
    if body.user_type not in _BILLABLE_TYPES:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"user_type must be one of: {', '.join(sorted(_BILLABLE_TYPES))}",
        )
    ok = await admin_update_user_type(user_id, body.user_type)
    if not ok:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return UpdateUserTypeResponse(ok=True)


@admin_router.get("/organizations", response_model=OrgListResponse)
async def list_organizations(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=200),
    _session: dict = Depends(require_admin),
) -> OrgListResponse:
    rows, total = await admin_list_organizations(page=page, limit=limit)
    return OrgListResponse(
        organizations=[OrgAdminView(**r) for r in rows],
        total=total,
        page=page,
        limit=limit,
    )


__all__ = ["admin_router"]
