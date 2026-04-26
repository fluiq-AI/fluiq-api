from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, status

from db_queues.postgresql.auth import (
    email_exists,
    get_user_by_email,
    is_refresh_token_revoked,
    register_user,
    revoke_refresh_token,
)
from shared.model import (
    LoginResponse,
    LogoutResponse,
    RefreshResponse,
    RegisterResponse,
    UserPublic,
)

from .helper import (
    _create_access_token,
    _create_refresh_token,
    _decode_refresh_token,
    _hash_password,
    _normalize_email,
    _validate_password,
    _verify_password,
)
from .model import LoginPayload, LogoutPayload, RefreshPayload, RegisterPayload

auth_router = APIRouter()

@auth_router.post(
    "/register",
    status_code=status.HTTP_201_CREATED,
    response_model=RegisterResponse,
)
async def register(payload: RegisterPayload) -> RegisterResponse:
    email = _normalize_email(payload.email)
    _validate_password(payload.password)

    if await email_exists(email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )

    result = await register_user(
        name=payload.name,
        email=email,
        hashed_password=_hash_password(payload.password),
        user_type="Free",
    )
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered",
        )
    user, organization = result
    access_token, expires_in = _create_access_token(
        user_id=str(user.user_id), org_id=str(user.org_id)
    )
    refresh_token, refresh_expires_in = _create_refresh_token(
        user_id=str(user.user_id), org_id=str(user.org_id)
    )
    return RegisterResponse(
        user=UserPublic(**user.model_dump(exclude={"hashed_password"})),
        organization=organization,
        access_token=access_token,
        expires_in=expires_in,
        refresh_token=refresh_token,
        refresh_expires_in=refresh_expires_in,
    )


@auth_router.post(
    "/login",
    status_code=status.HTTP_200_OK,
    response_model=LoginResponse,
)
async def login(payload: LoginPayload) -> LoginResponse:
    email = _normalize_email(payload.email)

    result = await get_user_by_email(email)
    if result is None or not _verify_password(
        payload.password, result[0].hashed_password
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password",
        )
    user, organization = result
    access_token, expires_in = _create_access_token(
        user_id=str(user.user_id), org_id=str(user.org_id)
    )
    refresh_token, refresh_expires_in = _create_refresh_token(
        user_id=str(user.user_id), org_id=str(user.org_id)
    )
    return LoginResponse(
        user=UserPublic(**user.model_dump(exclude={"hashed_password"})),
        organization=organization,
        access_token=access_token,
        expires_in=expires_in,
        refresh_token=refresh_token,
        refresh_expires_in=refresh_expires_in,
    )


@auth_router.post(
    "/refresh",
    status_code=status.HTTP_200_OK,
    response_model=RefreshResponse,
)
async def refresh(payload: RefreshPayload) -> RefreshResponse:
    claims = _decode_refresh_token(payload.refresh_token)
    jti = claims.get("jti")
    if jti is None or await is_refresh_token_revoked(jti):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    expires_at = datetime.fromtimestamp(claims["exp"], tz=timezone.utc)
    await revoke_refresh_token(jti=jti, expires_at=expires_at)
    access_token, expires_in = _create_access_token(
        user_id=claims["sub"], org_id=claims["org_id"]
    )
    refresh_token, refresh_expires_in = _create_refresh_token(
        user_id=claims["sub"], org_id=claims["org_id"]
    )
    return RefreshResponse(
        access_token=access_token,
        expires_in=expires_in,
        refresh_token=refresh_token,
        refresh_expires_in=refresh_expires_in,
    )


@auth_router.post(
    "/logout",
    status_code=status.HTTP_200_OK,
    response_model=LogoutResponse,
)
async def logout(payload: LogoutPayload) -> LogoutResponse:
    claims = _decode_refresh_token(payload.refresh_token)
    jti = claims.get("jti")
    if jti is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    expires_at = datetime.fromtimestamp(claims["exp"], tz=timezone.utc)
    await revoke_refresh_token(jti=jti, expires_at=expires_at)
    return LogoutResponse(ok=True)
