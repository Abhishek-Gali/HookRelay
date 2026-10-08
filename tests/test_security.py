from app.security import (
    calculate_signature,
    verify_signature,
    verify_stripe_signature,
    verify_secret_token,
)


def test_valid_signature_accepted():
    secret = "my_secure_secret"
    body = b'{"action": "opened", "issue": {"id": 123}}'
    valid_sig = calculate_signature(secret, body)

    assert valid_sig.startswith("sha256=")
    assert verify_signature(secret, body, valid_sig) is True


def test_invalid_signature_rejected():
    secret = "my_secure_secret"
    body = b'{"action": "opened"}'
    wrong_sig = "sha256=1111111111111111111111111111111111111111111111111111111111111111"

    assert verify_signature(secret, body, wrong_sig) is False


def test_tampered_payload_rejected():
    secret = "my_secure_secret"
    original_body = b'{"action": "opened", "amount": 100}'
    sig = calculate_signature(secret, original_body)

    # Attacker alters body by 1 character
    tampered_body = b'{"action": "opened", "amount": 101}'
    assert verify_signature(secret, tampered_body, sig) is False


def test_missing_or_empty_headers_rejected():
    secret = "my_secure_secret"
    body = b'{"event": "push"}'

    assert verify_signature(secret, body, None) is False
    assert verify_signature(secret, body, "") is False
    assert verify_signature("", body, "sha256=somehash") is False


def test_malformed_header_prefix_rejected():
    secret = "my_secure_secret"
    body = b'{"event": "push"}'
    sig = calculate_signature(secret, body)
    # Strip 'sha256=' prefix
    raw_hash_only = sig.replace("sha256=", "")

    assert verify_signature(secret, body, raw_hash_only) is False
    assert verify_signature(secret, body, f"sha1={raw_hash_only}") is False


def test_stripe_signature_verification_and_timestamp_window():
    import hashlib
    import hmac

    secret = "whsec_test_stripe_secret_key"
    body = b'{"id": "evt_12345", "type": "payment_intent.succeeded"}'
    ts = 1700000000
    signed_payload = f"{ts}.".encode("utf-8") + body
    v1_sig = hmac.new(secret.encode("utf-8"), signed_payload, hashlib.sha256).hexdigest()
    header = f"t={ts},v1={v1_sig}"

    assert verify_stripe_signature(secret, body, header, tolerance_seconds=300, now_ts=ts + 10) is True
    # Outside replay tolerance window (>300s)
    assert verify_stripe_signature(secret, body, header, tolerance_seconds=300, now_ts=ts + 600) is False
    # Tampered body
    assert verify_stripe_signature(secret, b'{"id": "evt_999"}', header, tolerance_seconds=300, now_ts=ts) is False


def test_gitlab_and_custom_secret_token_verification():
    secret = "gl_secret_token_98765"
    assert verify_secret_token(secret, "gl_secret_token_98765") is True
    assert verify_secret_token(secret, "wrong_token") is False
    assert verify_secret_token(secret, None) is False

