import pytest
from app.security import calculate_signature, verify_signature


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
