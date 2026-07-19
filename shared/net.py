"""SSRF-safe URL validation for outbound, user-supplied webhook URLs.

Guardrail ``alert_webhook`` and org Slack webhooks are strings the customer
controls, POSTed server-side from inside the VPC. Without validation they can
point at loopback, link-local (``169.254.169.254`` metadata), or private-range
hosts and turn the API into an SSRF pivot. ``is_safe_public_url`` resolves the
hostname and rejects the URL if *any* resolved address is non-public.

Note: this validates at call time. It reduces — but does not fully eliminate —
DNS-rebinding risk (the socket layer re-resolves on connect). Callers should
also disable redirect following. Pinning the validated IP is a future hardening.
"""
from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse


def _ip_is_public(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    # An IPv4-mapped IPv6 answer (::ffff:169.254.169.254) can smuggle a
    # private/loopback v4 past the v6 flag checks — unwrap and judge the v4.
    mapped = getattr(addr, "ipv4_mapped", None)
    if mapped is not None:
        addr = mapped
    return not (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    )


def is_safe_public_url_sync(url: str, *, require_https: bool = False) -> bool:
    """True only if the URL is http(s), has a hostname, and every resolved IP
    is a public address. Resolution failures return False (reject-by-default)."""
    if not url or not isinstance(url, str):
        return False
    try:
        parsed = urlparse(url.strip())
    except Exception:
        return False
    allowed_schemes = ("https",) if require_https else ("http", "https")
    if parsed.scheme not in allowed_schemes or not parsed.hostname:
        return False
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parsed.hostname, port, proto=socket.IPPROTO_TCP)
    except Exception:
        return False
    if not infos:
        return False
    return all(_ip_is_public(info[4][0]) for info in infos)


async def is_safe_public_url(url: str, *, require_https: bool = False) -> bool:
    """Async wrapper — runs the blocking DNS lookup in a thread."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(
        None, lambda: is_safe_public_url_sync(url, require_https=require_https)
    )
