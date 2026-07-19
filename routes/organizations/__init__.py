"""Multi-user organization endpoints: membership, invitations, and org switching.

Mounted at ``/api/v1/organizations``. Auth is the dashboard Bearer access token
(``get_current_session``). Role gating is always evaluated against the *path*
org's membership (``get_membership_role``), so a user can manage any org they
belong to, not only their currently active one.

Switching orgs simply re-mints the JWT with a new ``org_id`` after verifying
membership — every downstream data route already trusts ``session['org_id']``.
"""
import logging
import uuid
from urllib.parse import quote

import config
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status

from db_queues.postgresql.auth import get_organization, get_user_by_id
from db_queues.postgresql.organizations import (
    INVITE_EXPIRE_HOURS,
    accept_invitation,
    create_invitation,
    create_organization,
    delete_organization,
    get_invitation_by_token,
    get_membership_role,
    list_members,
    list_pending_invitations,
    list_user_organizations,
    remove_member,
    rename_organization,
    revoke_invitation,
    set_member_role,
    set_user_current_org,
    user_org_count,
)
from routes.auth.helper import (
    get_current_session,
)
from shared.email import email_service
from shared.model import LoginResponse

from .helpers import (
    _uid, _require_role, _clean_name,
    _build_session,
)

from .model import (
    CreateOrgRequest,
    InvitationsResponse,
    InvitationView,
    InvitePreview,
    InviteRequest,
    MembersResponse,
    OkResponse,
    OrgListResponse,
    OrgMembershipView,
    OrgMemberView,
    RenameOrgRequest,
    SetRoleRequest,
    SwitchOrgRequest,
)

logger = logging.getLogger(__name__)

organizations_router = APIRouter()

_VALID_ROLES = {"owner", "admin", "member"}
_INVITE_ROLES = {"admin", "member"}


# ── Org list / create / switch ─────────────────────────────────────────────────

@organizations_router.get("", response_model=OrgListResponse)
async def my_organizations(session: dict = Depends(get_current_session)) -> OrgListResponse:
    user_id = _uid(session)
    current = uuid.UUID(session["org_id"])
    rows = await list_user_organizations(user_id)
    return OrgListResponse(
        organizations=[
            OrgMembershipView(
                org_id=r["org_id"],
                name=r["name"],
                role=r["role"],
                plan=r.get("plan"),
                member_count=int(r["member_count"]),
                is_current=r["org_id"] == current,
                created_at=r["created_at"],
            )
            for r in rows
        ]
    )


@organizations_router.post("", response_model=OrgMembershipView, status_code=201)
async def create_org(
    body: CreateOrgRequest, session: dict = Depends(get_current_session)
) -> OrgMembershipView:
    user_id = _uid(session)
    name = _clean_name(body.name)
    org = await create_organization(user_id, name)
    return OrgMembershipView(
        org_id=org.org_id,
        name=org.name,
        role="owner",
        plan=org.plan_tier,
        member_count=1,
        is_current=False,
        created_at=org.created_at,
    )


@organizations_router.post("/switch", response_model=LoginResponse)
async def switch_org(
    body: SwitchOrgRequest, session: dict = Depends(get_current_session)
) -> LoginResponse:
    user_id = _uid(session)
    role = await get_membership_role(body.org_id, user_id)
    if role is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You are not a member of that organization.",
        )
    await set_user_current_org(user_id, body.org_id)
    return await _build_session(user_id, body.org_id)


@organizations_router.patch("/{org_id}", response_model=OkResponse)
async def rename_org(
    org_id: uuid.UUID, body: RenameOrgRequest, session: dict = Depends(get_current_session)
) -> OkResponse:
    user_id = _uid(session)
    await _require_role(user_id, org_id, "admin")
    name = _clean_name(body.name)
    updated = await rename_organization(org_id, name)
    if updated is None:
        raise HTTPException(status_code=404, detail="Organization not found")
    return OkResponse()


@organizations_router.delete("/{org_id}", response_model=OkResponse)
async def delete_org(
    org_id: uuid.UUID, session: dict = Depends(get_current_session)
) -> OkResponse:
    user_id = _uid(session)
    await _require_role(user_id, org_id, "owner")
    if await user_org_count(user_id) <= 1:
        raise HTTPException(
            status_code=400,
            detail="You can't delete your only organization.",
        )
    await delete_organization(org_id)
    return OkResponse()


# ── Members ────────────────────────────────────────────────────────────────────

@organizations_router.get("/{org_id}/members", response_model=MembersResponse)
async def get_members(
    org_id: uuid.UUID, session: dict = Depends(get_current_session)
) -> MembersResponse:
    user_id = _uid(session)
    your_role = await _require_role(user_id, org_id, "member")
    rows = await list_members(org_id)
    return MembersResponse(
        members=[
            OrgMemberView(
                user_id=r["user_id"],
                name=r["name"],
                email=r["email"],
                role=r["role"],
                created_at=r["created_at"],
                is_you=r["user_id"] == user_id,
            )
            for r in rows
        ],
        your_role=your_role,
    )


@organizations_router.patch("/{org_id}/members/{member_id}", response_model=OkResponse)
async def update_member_role(
    org_id: uuid.UUID,
    member_id: uuid.UUID,
    body: SetRoleRequest,
    session: dict = Depends(get_current_session),
) -> OkResponse:
    user_id = _uid(session)
    if body.role not in _VALID_ROLES:
        raise HTTPException(status_code=400, detail="Invalid role.")
    your_role = await _require_role(user_id, org_id, "admin")
    # Only an owner may grant or move ownership.
    if body.role == "owner" and your_role != "owner":
        raise HTTPException(
            status_code=403, detail="Only an owner can transfer ownership."
        )
    ok = await set_member_role(org_id, member_id, body.role)
    if not ok:
        raise HTTPException(status_code=404, detail="That member was not found.")
    return OkResponse()


@organizations_router.delete("/{org_id}/members/{member_id}", response_model=OkResponse)
async def remove_org_member(
    org_id: uuid.UUID,
    member_id: uuid.UUID,
    session: dict = Depends(get_current_session),
) -> OkResponse:
    user_id = _uid(session)
    # Members may remove themselves (leave); removing anyone else requires admin+.
    if member_id != user_id:
        await _require_role(user_id, org_id, "admin")
    else:
        await _require_role(user_id, org_id, "member")
    ok, err = await remove_member(org_id, member_id)
    if not ok:
        raise HTTPException(status_code=400, detail=err)
    return OkResponse()


# ── Invitations ────────────────────────────────────────────────────────────────

@organizations_router.get("/{org_id}/invitations", response_model=InvitationsResponse)
async def get_invitations(
    org_id: uuid.UUID, session: dict = Depends(get_current_session)
) -> InvitationsResponse:
    user_id = _uid(session)
    await _require_role(user_id, org_id, "admin")
    rows = await list_pending_invitations(org_id)
    return InvitationsResponse(invitations=[InvitationView(**r) for r in rows])


@organizations_router.post("/{org_id}/invitations", response_model=OkResponse)
async def invite_member(
    org_id: uuid.UUID,
    body: InviteRequest,
    background_tasks: BackgroundTasks,
    session: dict = Depends(get_current_session),
) -> OkResponse:
    user_id = _uid(session)
    await _require_role(user_id, org_id, ["admin","owner"])
    role = body.role if body.role in _INVITE_ROLES else "member"
    email = (body.email or "").strip()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required.")

    token, err = await create_invitation(org_id, email, role, user_id)
    if err:
        raise HTTPException(status_code=400, detail=err)

    inviter = await get_user_by_id(user_id)
    organization = await get_organization(org_id)
    accept_url = f"{config.FRONTEND_BASE_URL}/invite?token={quote(token)}"
    background_tasks.add_task(
        email_service.send_org_invitation_email,
        to=email,
        org_name=organization.name if organization else "your team",
        inviter_name=inviter.name if inviter else "A teammate",
        role=role,
        accept_url=accept_url,
        expires_in_hours=INVITE_EXPIRE_HOURS,
    )
    return OkResponse()


@organizations_router.delete(
    "/{org_id}/invitations/{invite_id}", response_model=OkResponse
)
async def cancel_invitation(
    org_id: uuid.UUID,
    invite_id: uuid.UUID,
    session: dict = Depends(get_current_session),
) -> OkResponse:
    user_id = _uid(session)
    await _require_role(user_id, org_id, "admin")
    ok = await revoke_invitation(org_id, invite_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Invitation not found.")
    return OkResponse()


# ── Invitation accept (token-based) ────────────────────────────────────────────

@organizations_router.get("/invitations/{token}", response_model=InvitePreview)
async def preview_invitation(token: str) -> InvitePreview:
    """Public preview for the accept page. Never requires auth."""
    inv = await get_invitation_by_token(token)
    if inv is None:
        return InvitePreview(valid=False, reason="This invitation link is invalid.")
    if inv["status"] != "pending":
        return InvitePreview(valid=False, reason="This invitation has already been used or revoked.")
    from datetime import datetime, timezone
    if inv["expires_at"] <= datetime.now(timezone.utc):
        return InvitePreview(valid=False, reason="This invitation has expired.")
    return InvitePreview(
        valid=True,
        org_name=inv["org_name"],
        email=inv["email"],
        role=inv["role"],
        inviter_name=inv.get("inviter_name"),
    )


@organizations_router.post("/invitations/{token}/accept", response_model=LoginResponse)
async def accept_invite(
    token: str, session: dict = Depends(get_current_session)
) -> LoginResponse:
    user_id = _uid(session)
    user = await get_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="Account not found")
    org_id, err = await accept_invitation(token, user_id, user.email)
    if err:
        raise HTTPException(status_code=400, detail=err)
    # Land the user directly inside the org they just joined.
    await set_user_current_org(user_id, org_id)
    return await _build_session(user_id, org_id)


__all__ = ["organizations_router"]
