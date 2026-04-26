from datetime import datetime
from typing import Literal, Optional
from uuid import UUID

from pydantic import BaseModel, Field

UserType = Literal["Free", "Team", "Enterprise"]


class UserModel(BaseModel):
    user_id: UUID
    email: str
    name: str
    hashed_password: str
    user_type: UserType = "Free"
    org_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None


class OrganizationModel(BaseModel):
    org_id: UUID
    name: str
    user_id: UUID
    team_ids: list[UUID] = Field(default_factory=list)
    api_keys: list[UUID] = Field(default_factory=list)
    api_key_limit: int = 1
    api_key_usage: int = 0
    created_at: datetime
    updated_at: Optional[datetime] = None


class UserPublic(BaseModel):
    user_id: UUID
    email: str
    name: str
    user_type: UserType
    org_id: UUID
    created_at: datetime
    updated_at: Optional[datetime] = None


class RegisterResponse(BaseModel):
    user: UserPublic
    organization: OrganizationModel
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str
    refresh_expires_in: int


class LoginResponse(BaseModel):
    user: UserPublic
    organization: OrganizationModel
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str
    refresh_expires_in: int


class RefreshResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    refresh_token: str
    refresh_expires_in: int


class LogoutResponse(BaseModel):
    ok: bool = True
