import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CreateOrgRequest(BaseModel):
    name: str


class SwitchOrgRequest(BaseModel):
    org_id: uuid.UUID


class RenameOrgRequest(BaseModel):
    name: str


class SetRoleRequest(BaseModel):
    role: str  # 'owner' | 'admin' | 'member'


class InviteRequest(BaseModel):
    email: str
    role: str = "member"  # 'admin' | 'member'


class OrgMembershipView(BaseModel):
    org_id: uuid.UUID
    name: str
    role: str
    plan: Optional[str]
    member_count: int
    is_current: bool
    created_at: datetime


class OrgListResponse(BaseModel):
    organizations: list[OrgMembershipView]


class OrgMemberView(BaseModel):
    user_id: uuid.UUID
    name: str
    email: str
    role: str
    created_at: datetime
    is_you: bool


class MembersResponse(BaseModel):
    members: list[OrgMemberView]
    your_role: str


class InvitationView(BaseModel):
    invite_id: uuid.UUID
    email: str
    role: str
    created_at: datetime
    expires_at: datetime


class InvitationsResponse(BaseModel):
    invitations: list[InvitationView]


class InvitePreview(BaseModel):
    valid: bool
    org_name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    inviter_name: Optional[str] = None
    reason: Optional[str] = None


class OkResponse(BaseModel):
    ok: bool = True
