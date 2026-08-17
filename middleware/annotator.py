"""Confine annotators to the review surface.

An annotator is usually a contractor — a clinician, a lawyer, a domain expert —
invited to record verdicts and nothing else. Enforcing that per route would mean
remembering to add a check to every route ever written, and the first one anyone
forgets is the one that exposes provider keys.

So it is enforced here, as an allowlist: an annotator reaches the review
surface, and everything else is 403 by default. A route added tomorrow is closed
to contractors until someone decides otherwise, which is the right direction for
that mistake to fail in.

Only session (JWT) requests are considered. API-key traffic is machine traffic
and carries no role.
"""
from __future__ import annotations

import logging

import jwt
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

import config
from shared.permissions import annotator_may_reach

logger = logging.getLogger(__name__)

#: Methods an annotator may use outside the allowlist — none. Reads elsewhere
#: are refused too: a contractor who can list every dataset can copy it.
SAFE_METHODS: tuple[str, ...] = ()


class AnnotatorScopeMiddleware(BaseHTTPMiddleware):
    """403 an annotator anywhere outside the review surface."""

    async def dispatch(self, request: Request, call_next) -> Response:
        role = _role_from_token(request)
        if role != "annotator":
            return await call_next(request)

        path = request.url.path
        if annotator_may_reach(path) or request.method in SAFE_METHODS:
            return await call_next(request)

        logger.info("[ANNOTATOR] blocked %s %s", request.method, path)
        return JSONResponse(
            status_code=403,
            content={
                "detail": (
                    "Your account is a reviewer account. It can open the Review "
                    "queue and record verdicts, but not reach the rest of the "
                    "workspace. Ask an admin if you need broader access."
                )
            },
        )


def _role_from_token(request: Request) -> str | None:
    """The role claim on the caller's JWT, or None.

    Best-effort and deliberately silent: this middleware only *restricts*, so a
    token it cannot read means the request proceeds to the route's own auth,
    which will reject it properly. Failing loudly here would turn a malformed
    header into a 500 on a path that would have 401'd anyway.
    """
    auth = request.headers.get("authorization") or ""
    if not auth.lower().startswith("bearer "):
        return None
    token = auth.split(" ", 1)[1].strip()
    try:
        claims = jwt.decode(
            token,
            config.JWT_SECRET,
            algorithms=[config.JWT_ALGORITHM],
            options={"verify_exp": True},
        )
    except Exception:  # noqa: BLE001
        return None
    role = claims.get("role")
    return str(role) if role else None


__all__ = ["AnnotatorScopeMiddleware"]
