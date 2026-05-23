import json
import uuid
import httpx
import base64
import config
import secrets
import logging
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, urlencode
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status, Cookie, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from typing import Optional


from db_queues.postgresql.auth import (
    consume_password_reset,
    create_password_reset,
    delete_user_account,
    email_exists,
    fetch_active_password_reset,
    get_user_by_email,
    is_refresh_token_revoked,
    register_user,
    revoke_refresh_token,
    store_deletion_feedback,
    update_user_password,
    find_or_create_oauth_user,
)
from db_queues.clickhouse import clickhouse_client
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
    get_current_session,
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
            f"?email={quote(email)}&otp={otp}"
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

# ── ACCOUNT DELETION ─────────────────────────────────────────────────────────

class DeleteAccountPayload(BaseModel):
    reason: Optional[str] = None


class DeleteAccountResponse(BaseModel):
    ok: bool = True


@auth_router.delete(
    "/delete-account",
    status_code=status.HTTP_200_OK,
    response_model=DeleteAccountResponse,
)
async def delete_account(
    payload: DeleteAccountPayload,
    session: dict = Depends(get_current_session),
) -> DeleteAccountResponse:
    from db_queues.postgresql.auth import get_user_by_id

    user_id = uuid.UUID(session["sub"])
    org_id = uuid.UUID(session["org_id"])

    user = await get_user_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")

    await store_deletion_feedback(user_id=user_id, email=user.email, reason=payload.reason)
    await clickhouse_client.delete_org_data(org_id)
    await delete_user_account(user_id=user_id, org_id=org_id)

    logger.info("[auth] account deleted for user %s org %s", user_id, org_id)
    return DeleteAccountResponse(ok=True)


# ── OAUTH ────────────────────────────────────────────────────────────────────

GOOGLE_AUTH_URL   = "https://accounts.google.com/o/oauth2/v2/auth"
GOOGLE_TOKEN_URL  = "https://oauth2.googleapis.com/token"
GOOGLE_USER_URL   = "https://www.googleapis.com/oauth2/v3/userinfo"

GITHUB_AUTH_URL   = "https://github.com/login/oauth/authorize"
GITHUB_TOKEN_URL  = "https://github.com/login/oauth/access_token"
GITHUB_USER_URL   = "https://api.github.com/user"
GITHUB_EMAILS_URL = "https://api.github.com/user/emails"

COOKIE_NAME = "oauth_state"
COOKIE_MAX_AGE = 600 

def _build_session_redirect(user, organization, user_id: str, org_id: str) -> RedirectResponse:
    """
    Build a redirect to the frontend /auth/callback with the full session
    encoded as base64 JSON so the frontend can hydrate the Redux store.
    """
    access_token, expires_in = _create_access_token(user_id=user_id, org_id=org_id)
    refresh_token, refresh_expires_in = _create_refresh_token(user_id=user_id, org_id=org_id)

    session = {
        "user": user.model_dump(mode="json"),
        "organization": organization.model_dump(mode="json"),
        "access_token": access_token,
        "token_type": "bearer",
        "expires_in": expires_in,
        "refresh_token": refresh_token,
        "refresh_expires_in": refresh_expires_in,
    }

    encoded = base64.urlsafe_b64encode(json.dumps(session).encode()).decode()
    url = f"{config.FRONTEND_BASE_URL}/auth/callback?session={encoded}"
    return RedirectResponse(url=url, status_code=302)


def _error_redirect(message: str) -> RedirectResponse:
    from urllib.parse import quote
    url = f"{config.FRONTEND_BASE_URL}/login?error={quote(message)}"
    return RedirectResponse(url=url, status_code=302)


@auth_router.get("/oauth/google")
async def google_login():
    if not config.GOOGLE_CLIENT_ID:
        raise HTTPException(status_code=501, detail="Google OAuth not configured")

    state = secrets.token_urlsafe(32)
    callback_url = f"{config.API_BASE_URL}/auth/oauth/google/callback"

    params = {
        "client_id": config.GOOGLE_CLIENT_ID,
        "redirect_uri": callback_url,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "offline",
        "prompt": "select_account",
    }

    response = RedirectResponse(
        url=f"{GOOGLE_AUTH_URL}?{urlencode(params)}",
        status_code=302,
    )
    response.set_cookie(
        key=COOKIE_NAME,
        value=state,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@auth_router.get("/oauth/google/callback")
async def google_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    oauth_state: str | None = Cookie(default=None),
):
    if error or not code:
        return _error_redirect("Google sign-in was cancelled")

    if not state or state != oauth_state:
        return _error_redirect("Invalid OAuth state — please try again")

    callback_url = f"{config.API_BASE_URL}/auth/oauth/google/callback"

    async with httpx.AsyncClient() as client:
        # Exchange code for tokens
        token_resp = await client.post(GOOGLE_TOKEN_URL, data={
            "code": code,
            "client_id": config.GOOGLE_CLIENT_ID,
            "client_secret": config.GOOGLE_CLIENT_SECRET,
            "redirect_uri": callback_url,
            "grant_type": "authorization_code",
        })
        if token_resp.status_code != 200:
            logger.error("[oauth/google] token exchange failed: %s", token_resp.text)
            return _error_redirect("Google authentication failed")

        google_access_token = token_resp.json().get("access_token")

        # Fetch user info
        user_resp = await client.get(
            GOOGLE_USER_URL,
            headers={"Authorization": f"Bearer {google_access_token}"},
        )
        if user_resp.status_code != 200:
            return _error_redirect("Could not fetch Google profile")

        profile = user_resp.json()

    email = profile.get("email")
    name = profile.get("name") or profile.get("email", "").split("@")[0]

    if not email:
        return _error_redirect("Google account has no email address")

    result = await find_or_create_oauth_user(name=name, email=email)
    if result is None:
        return _error_redirect("Could not create account")

    user, organization = result
    resp = _build_session_redirect(
        user=user,
        organization=organization,
        user_id=str(user.user_id),
        org_id=str(user.org_id),
    )
    resp.delete_cookie(COOKIE_NAME)
    return resp


# ── GitHub ────────────────────────────────────────────────────────────────────

@auth_router.get("/oauth/github")
async def github_login():
    if not config.GITHUB_CLIENT_ID:
        raise HTTPException(status_code=501, detail="GitHub OAuth not configured")

    state = secrets.token_urlsafe(32)
    callback_url = f"{config.API_BASE_URL}/auth/oauth/github/callback"

    params = {
        "client_id": config.GITHUB_CLIENT_ID,
        "redirect_uri": callback_url,
        "scope": "read:user user:email",
        "state": state,
    }

    response = RedirectResponse(
        url=f"{GITHUB_AUTH_URL}?{urlencode(params)}",
        status_code=302,
    )
    response.set_cookie(
        key=COOKIE_NAME,
        value=state,
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
    )
    return response


@auth_router.get("/oauth/github/callback")
async def github_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    oauth_state: str | None = Cookie(default=None),
):
    if error or not code:
        return _error_redirect("GitHub sign-in was cancelled")

    if not state or state != oauth_state:
        return _error_redirect("Invalid OAuth state — please try again")

    callback_url = f"{config.API_BASE_URL}/auth/oauth/github/callback"

    async with httpx.AsyncClient() as client:
        # Exchange code for access token
        token_resp = await client.post(
            GITHUB_TOKEN_URL,
            data={
                "client_id": config.GITHUB_CLIENT_ID,
                "client_secret": config.GITHUB_CLIENT_SECRET,
                "code": code,
                "redirect_uri": callback_url,
            },
            headers={"Accept": "application/json"},
        )
        if token_resp.status_code != 200:
            logger.error("[oauth/github] token exchange failed: %s", token_resp.text)
            return _error_redirect("GitHub authentication failed")

        github_access_token = token_resp.json().get("access_token")
        auth_header = {"Authorization": f"Bearer {github_access_token}"}

        # Fetch profile
        user_resp = await client.get(GITHUB_USER_URL, headers=auth_header)
        if user_resp.status_code != 200:
            return _error_redirect("Could not fetch GitHub profile")

        profile = user_resp.json()
        email = profile.get("email")

        # GitHub may not expose email in profile — fetch from emails endpoint
        if not email:
            emails_resp = await client.get(GITHUB_EMAILS_URL, headers=auth_header)
            if emails_resp.status_code == 200:
                emails = emails_resp.json()
                primary = next(
                    (e["email"] for e in emails if e.get("primary") and e.get("verified")),
                    None,
                )
                email = primary

    if not email:
        return _error_redirect(
            "Your GitHub account has no verified email. "
            "Please add a public email in GitHub settings."
        )

    name = profile.get("name") or profile.get("login") or email.split("@")[0]

    result = await find_or_create_oauth_user(name=name, email=email)
    if result is None:
        return _error_redirect("Could not create account")

    user, organization = result
    resp = _build_session_redirect(
        user=user,
        organization=organization,
        user_id=str(user.user_id),
        org_id=str(user.org_id),
    )
    resp.delete_cookie(COOKIE_NAME)
    return resp