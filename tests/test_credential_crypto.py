"""Envelope encryption for BYOK provider credentials (pure, no AWS).

Exercises shared/crypto.py through the `local` backend. The KMS backend differs
only in how the data key is wrapped — the cipher, the org binding, and the
tamper checks below are the same code path in production.

Run:  python fluiq-api/tests/test_credential_crypto.py
"""
import base64
import os
import secrets
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

# config.py reads several vars with int()/float() and raises on import if unset.
for _k, _v in [
    ("JWT_EXPIRE_MINUTES", "60"),
    ("JWT_REFRESH_EXPIRE_DAYS", "30"),
    ("POSTGRES_DSN", "postgresql://localhost/fluiq"),
    ("POSTGRES_POOL_MIN", "1"),
    ("POSTGRES_POOL_MAX", "5"),
    ("PASSWORD_RESET_OTP_LENGTH", "6"),
    ("PASSWORD_RESET_EXPIRE_MINUTES", "15"),
    ("CLICKHOUSE_PORT", "8123"),
    ("KAFKA_SECURITY_CHECK_TIMEOUT", "30"),
]:
    os.environ.setdefault(_k, _v)
# Select the AWS-free backend before config is imported.
os.environ["CREDENTIAL_ENCRYPTION_BACKEND"] = "local"
os.environ["CREDENTIAL_ENCRYPTION_LOCAL_KEY"] = base64.b64encode(secrets.token_bytes(32)).decode()

from shared import crypto  # noqa: E402

ORG_A = "11111111-1111-1111-1111-111111111111"
ORG_B = "22222222-2222-2222-2222-222222222222"
KEY = "sk-proj-EXAMPLEnotarealkey0123456789abcdefXYZ"


def test_roundtrip():
    sealed = crypto.seal(KEY, org_id=ORG_A)
    assert crypto.unseal(sealed, org_id=ORG_A) == KEY


def test_plaintext_never_appears_in_the_sealed_blob():
    sealed = crypto.seal(KEY, org_id=ORG_A)
    blob = sealed.ciphertext + sealed.nonce + sealed.wrapped_dek
    assert KEY.encode() not in blob
    # Also guard the tail, which is the part we do surface as `last4`.
    assert KEY[-8:].encode() not in blob


def test_ciphertext_is_bound_to_one_org():
    """A row lifted into another org's context must fail, not decrypt."""
    from cryptography.exceptions import InvalidTag

    sealed = crypto.seal(KEY, org_id=ORG_A)
    try:
        crypto.unseal(sealed, org_id=ORG_B)
    except InvalidTag:
        return
    raise AssertionError("org B decrypted org A's credential — AAD binding is not enforced")


def test_tampering_with_ciphertext_is_detected():
    from cryptography.exceptions import InvalidTag

    sealed = crypto.seal(KEY, org_id=ORG_A)
    flipped = bytearray(sealed.ciphertext)
    flipped[0] ^= 0x01
    tampered = crypto.SealedSecret(
        ciphertext=bytes(flipped),
        nonce=sealed.nonce,
        wrapped_dek=sealed.wrapped_dek,
        key_version=sealed.key_version,
    )
    try:
        crypto.unseal(tampered, org_id=ORG_A)
    except InvalidTag:
        return
    raise AssertionError("a flipped ciphertext bit decrypted — GCM tag not being checked")


def test_each_seal_uses_a_fresh_dek_and_nonce():
    """No nonce/key reuse across credentials, even for identical plaintext."""
    a = crypto.seal(KEY, org_id=ORG_A)
    b = crypto.seal(KEY, org_id=ORG_A)
    assert a.nonce != b.nonce
    assert a.wrapped_dek != b.wrapped_dek
    assert a.ciphertext != b.ciphertext
    assert crypto.unseal(a, org_id=ORG_A) == crypto.unseal(b, org_id=ORG_A) == KEY


def test_fingerprint_is_stable_and_not_reversible():
    assert crypto.fingerprint(KEY) == crypto.fingerprint(KEY)
    assert crypto.fingerprint(KEY) != crypto.fingerprint(KEY + "x")
    fp = crypto.fingerprint(KEY)
    assert len(fp) == 16
    assert KEY[-8:] not in fp


def test_last4_is_the_only_thing_we_expose():
    assert crypto.last4(KEY) == KEY[-4:]
    assert len(crypto.last4(KEY)) == 4


def test_refuses_empty_secret():
    try:
        crypto.seal("", org_id=ORG_A)
    except ValueError:
        return
    raise AssertionError("sealed an empty secret")


def test_dek_cache_does_not_cross_credentials():
    """Two credentials must not resolve to each other via the DEK cache."""
    crypto.clear_dek_cache()
    a = crypto.seal("key-alpha", org_id=ORG_A)
    b = crypto.seal("key-beta", org_id=ORG_A)
    assert crypto.unseal(a, org_id=ORG_A) == "key-alpha"
    assert crypto.unseal(b, org_id=ORG_A) == "key-beta"
    # Re-read through the warm cache.
    assert crypto.unseal(a, org_id=ORG_A) == "key-alpha"


def test_is_configured_reflects_backend_state():
    assert crypto.is_configured() is True
    saved = os.environ.pop("CREDENTIAL_ENCRYPTION_LOCAL_KEY")
    try:
        crypto._local_key.cache_clear()
        import config
        config.CREDENTIAL_ENCRYPTION_LOCAL_KEY = None
        assert crypto.is_configured() is False, (
            "is_configured() must report False with no key, so routes disable "
            "BYOK instead of failing mid-write"
        )
    finally:
        os.environ["CREDENTIAL_ENCRYPTION_LOCAL_KEY"] = saved
        import config
        config.CREDENTIAL_ENCRYPTION_LOCAL_KEY = saved
        crypto._local_key.cache_clear()


if __name__ == "__main__":
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"PASS  {name}")
        except AssertionError as exc:
            failures += 1
            print(f"FAIL  {name}\n      {exc}")
        except Exception as exc:  # noqa: BLE001 - surface wiring breakage
            failures += 1
            print(f"ERROR {name}\n      {type(exc).__name__}: {exc}")
    print("\n" + ("all green" if not failures else f"{failures} failing"))
    sys.exit(1 if failures else 0)
