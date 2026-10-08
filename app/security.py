import hashlib
import hmac


def calculate_signature(secret: str, raw_body: bytes) -> str:
    """
    Computes HMAC-SHA256 of raw bytes using the provided secret key.
    Prefixed with 'sha256=' exactly as GitHub generates in X-Hub-Signature-256.
    """
    digest = hmac.new(
        key=secret.encode("utf-8"),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest()
    return f"sha256={digest}"


def verify_signature(secret: str, raw_body: bytes, signature_header: str | None) -> bool:
    """
    Timing-attack-safe verification of GitHub webhook HMAC signature.

    Key principles:
    1. Must verify against RAW request bytes (never re-serialized JSON).
    2. Header must exist and start with 'sha256='.
    3. Use hmac.compare_digest for constant-time comparison to prevent timing side channels.
    """
    if not signature_header or not secret:
        return False

    if not signature_header.startswith("sha256="):
        return False

    expected_signature = calculate_signature(secret, raw_body)
    return hmac.compare_digest(expected_signature, signature_header)
