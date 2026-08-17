"""Test bootstrap.

``config`` reads a number of settings with no default and coerces several to
int, so importing any route module outside a configured environment raises
before a single test runs. Filling in placeholders here lets tests import real
modules and exercise real logic; nothing in the test suite connects to any of
these services.

A developer's ``.env.development`` wins when present, so running the suite
locally exercises the same values the local API does.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path


API_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(API_ROOT))

try:  # optional: dotenv is not required to run the suite
    from dotenv import load_dotenv

    load_dotenv(API_ROOT / ".env.development")
except Exception:  # noqa: BLE001
    pass

_PLACEHOLDERS = {
    "JWT_SECRET":                    "test-secret",
    "JWT_ALGORITHM":                 "HS256",
    "JWT_EXPIRE_MINUTES":            "60",
    "JWT_REFRESH_EXPIRE_DAYS":       "7",
    "PASSWORD_RESET_OTP_LENGTH":     "6",
    "PASSWORD_RESET_EXPIRE_MINUTES": "15",
    "POSTGRES_DSN":                  "postgresql://test/test",
    "POSTGRES_POOL_MIN":             "1",
    "POSTGRES_POOL_MAX":             "2",
    "CLICKHOUSE_HOST":               "localhost",
    "CLICKHOUSE_PORT":               "8123",
}

for name, value in _PLACEHOLDERS.items():
    os.environ.setdefault(name, value)
