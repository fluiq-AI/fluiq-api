import logging
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, BackgroundTasks, HTTPException, status

import config
from db_queues.postgresql.auth import (
    consume_password_reset,
    create_password_reset,
    email_exists,
    fetch_active_password_reset,
    get_user_by_email,
    is_refresh_token_revoked,
    register_user,
    revoke_refresh_token,
    update_user_password,
)
from shared.email import email_service
from shared.model import (
    ForgotPasswordResponse,
    LoginResponse,
    LogoutResponse,
    RefreshResponse,
    RegisterResponse,
    ResetPasswordResponse,
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
from .model import (
    ForgotPasswordPayload,
    LoginPayload,
    LogoutPayload,
    RefreshPayload,
    RegisterPayload,
    ResetPasswordPayload,
)

logger = logging.getLogger(__name__)

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


def _generate_otp(length: int) -> str:
    upper = 10 ** length
    return f"{secrets.randbelow(upper):0{length}d}"


@auth_router.post(
    "/forgot-password",
    status_code=status.HTTP_200_OK,
    response_model=ForgotPasswordResponse,
)
async def forgot_password(
    payload: ForgotPasswordPayload, background_tasks: BackgroundTasks
) -> ForgotPasswordResponse:
    """Issue a one-time reset code for the email if it exists.

    The response is identical whether or not the email is registered to
    avoid leaking account existence. Email delivery is dispatched in a
    background task so the response is fast and resilient to SMTP delays.
    """
    email = _normalize_email(payload.email)
    
    logger.info("[auth] forgot-password attempt for: %s", email)

    result = await get_user_by_email(email)
    if result is not None:
        logger.info("[auth] user found, queueing email")
        user, _ = result
        otp = _generate_otp(config.PASSWORD_RESET_OTP_LENGTH)
        otp_hash = _hash_password(otp)
        expires_at = datetime.now(timezone.utc) + timedelta(
            minutes=config.PASSWORD_RESET_EXPIRE_MINUTES
        )
        await create_password_reset(
            user_id=user.user_id, otp_hash=otp_hash, expires_at=expires_at
        )
        reset_url = (
            f"{config.FRONTEND_BASE_URL}/reset-password"
            f"?email={email}&otp={otp}"
        )
        background_tasks.add_task(
            email_service.send_password_reset_email,
            to=email,
            name=user.name,
            otp=otp,
            reset_url=reset_url,
            expires_in_minutes=config.PASSWORD_RESET_EXPIRE_MINUTES,
        )
    else:
        logger.warning("[auth] no user found for email: %s", email) 
        logger.info("[auth] forgot-password requested for unknown email")
    return ForgotPasswordResponse(ok=True)


@auth_router.post(
    "/reset-password",
    status_code=status.HTTP_200_OK,
    response_model=ResetPasswordResponse,
)
async def reset_password(payload: ResetPasswordPayload) -> ResetPasswordResponse:
    email = _normalize_email(payload.email)
    _validate_password(payload.new_password)

    result = await get_user_by_email(email)
    invalid = HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Invalid or expired reset code",
    )
    if result is None:
        raise invalid
    user, _ = result

    active = await fetch_active_password_reset(user.user_id)
    if active is None:
        raise invalid
    token_id, otp_hash, _expires_at = active
    if not _verify_password(payload.otp, otp_hash):
        raise invalid
    if not await consume_password_reset(token_id):
        raise invalid

    await update_user_password(
        user_id=user.user_id, hashed_password=_hash_password(payload.new_password)
    )
    return ResetPasswordResponse(ok=True)
