import os
import re
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt
import config
from email_validator import EmailNotValidError, validate_email
from fastapi import Depends, Header, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from typing import Optional

PASSWORD_MIN_LENGTH = 8
_PASSWORD_LETTER_RE = re.compile(r"[A-Za-z]")
_PASSWORD_DIGIT_RE = re.compile(r"[0-9]")


def _validate_password(password: str) -> None:
    if (
        len(password) < PASSWORD_MIN_LENGTH
        or not _PASSWORD_LETTER_RE.search(password)
        or not _PASSWORD_DIGIT_RE.search(password)
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                "Password must be at least 8 characters and contain "
                "at least one letter and one digit"
            ),
        )


def _normalize_email(email: str) -> str:
    try:
        result = validate_email(email, check_deliverability=False)
    except EmailNotValidError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid email: {exc}",
        )
    return result.normalized


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, hashed_password: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed_password.encode("utf-8"))
    except ValueError:
        return False


def _create_access_token(user_id: str, org_id: str) -> tuple[str, int]:
    """Encode a short-lived access JWT. Returns (token, expires_in_seconds)."""
    expires_in = config.JWT_EXPIRE_MINUTES * 60
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "org_id": str(org_id),
        "type": "access",
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
    }
    token = jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALGORITHM)
    return token, expires_in


def _create_refresh_token(user_id: str, org_id: str) -> tuple[str, int]:
    """Encode a long-lived refresh JWT. Returns (token, expires_in_seconds)."""
    expires_in = config.JWT_REFRESH_EXPIRE_DAYS * 86400
    now = datetime.now(timezone.utc)
    payload = {
        "sub": str(user_id),
        "org_id": str(org_id),
        "type": "refresh",
        "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(seconds=expires_in)).timestamp()),
    }
    token = jwt.encode(payload, config.JWT_SECRET, algorithm=config.JWT_ALGORITHM)
    return token, expires_in


def _decode_refresh_token(token: str) -> dict:
    """Validate a refresh JWT and return its claims. Raises 401 on failure."""
    try:
        payload = jwt.decode(token, config.JWT_SECRET, algorithms=[config.JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token expired",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    if payload.get("type") != "refresh":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid refresh token",
        )
    return payload


_bearer_scheme = HTTPBearer(auto_error=True)


def _decode_access_token(token: str) -> dict:
    """Validate an access JWT and return its claims. Raises 401 on failure."""
    try:
        payload = jwt.decode(token, config.JWT_SECRET, algorithms=[config.JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Access token expired",
        )
    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        )
    if payload.get("type") != "access":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid access token",
        )
    return payload


def get_current_session(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer_scheme),
) -> dict:
    """FastAPI dependency: decode Bearer access token; return JWT claims."""
    return _decode_access_token(credentials.credentials)


# ── SDK API-key auth ───────────────────────────────────────────────────────────

# auto_error=False so a missing header doesn't 403 before we can fall back to the
# legacy x-api-key header / request body and raise our own 401.
_api_key_bearer = HTTPBearer(auto_error=False)


def extract_api_key(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_api_key_bearer),
    x_api_key: Optional[str] = Header(default=None, alias="x-api-key"),
) -> Optional[str]:
    """FastAPI dependency: pull the SDK API key out of the request.

    The current SDK sends it as an ``Authorization: Bearer <key>`` header.
    Falls back to the legacy ``x-api-key`` header for older SDK builds. Returns
    ``None`` when neither is present so body-based routes can fall back to a
    payload field before raising 401.
    """
    if credentials and credentials.credentials:
        return credentials.credentials
    if x_api_key:
        return x_api_key
    return None
