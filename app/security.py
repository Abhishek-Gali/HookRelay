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


def verify_stripe_signature(
    secret: str,
    raw_body: bytes,
    stripe_header: str | None,
    tolerance_seconds: int = 300,
    now_ts: int | None = None,
) -> bool:
    """
    Verifies a Stripe-Signature header ('t=<timestamp>,v1=<hex>') over raw bytes
    using HMAC-SHA256('<timestamp>.<raw_body>') and constant-time comparison.
    """
    if not stripe_header or not secret:
        return False

    import time

    timestamp_str: str | None = None
    signatures: list[str] = []
    for item in stripe_header.split(","):
        parts = item.strip().split("=", 1)
        if len(parts) != 2:
            continue
        key, val = parts[0].strip(), parts[1].strip()
        if key == "t":
            timestamp_str = val
        elif key == "v1":
            signatures.append(val)

    if not timestamp_str or not signatures:
        return False

    try:
        ts = int(timestamp_str)
    except ValueError:
        return False

    current_time = now_ts if now_ts is not None else int(time.time())
    if tolerance_seconds > 0 and abs(current_time - ts) > tolerance_seconds:
        return False

    signed_payload = timestamp_str.encode("utf-8") + b"." + raw_body
    expected_sig = hmac.new(
        key=secret.encode("utf-8"),
        msg=signed_payload,
        digestmod=hashlib.sha256,
    ).hexdigest()

    return any(hmac.compare_digest(expected_sig, candidate) for candidate in signatures)


def verify_secret_token(secret: str, provided_token: str | None) -> bool:
    """
    Constant-time verification for header token schemes such as GitLab's X-Gitlab-Token.
    """
    if not provided_token or not secret:
        return False
    return hmac.compare_digest(secret.encode("utf-8"), provided_token.encode("utf-8"))

