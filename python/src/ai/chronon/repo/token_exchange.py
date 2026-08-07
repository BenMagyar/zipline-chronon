"""Resolve a ZIPLINE_TOKEN environment variable to a usable JWT.

Supports two token types (auto-detected):
- **JWT** (from ``zipline auth get-access-token``): used directly, no exchange needed.
- **Session token** (from a service principal or CLI login): exchanged for a
  short-lived JWT via ``POST {auth_url}/api/auth/cli-token``.  Exchanged JWTs are
  cached and automatically refreshed when within 60 s of expiry.

The exchange sends the opaque session token in the request body rather than the
``Authorization`` header. Perimeter API gateways commonly reject any
``Authorization`` header that isn't a JWT, which would block a bearer-carried
opaque token before it reaches the origin; a header-less request is not blocked.

The endpoint lives under ``/api/auth/`` deliberately. Gateways don't only filter
on headers — they also allowlist paths and methods, and ``/api/auth/**`` is the
prefix the device flow already crosses. A path outside it is refused before it
reaches the origin (our own canary answers ``405 allow: GET`` for POST
everywhere else), so the exchange has to share the device flow's namespace.

Frontends that predate the endpoint are handled by falling back to the legacy
``GET /api/auth/token`` bearer exchange.
"""

import base64
import json
import threading
import time

import requests

from ai.chronon.logger import get_logger

LOG = get_logger()


def is_jwt(token: str) -> bool:
    """Return True if *token* looks like a JWT (three base64url segments)."""
    parts = token.split(".")
    if len(parts) != 3:
        return False
    for part in parts:
        # Add base64url padding and attempt decode
        padded = part + "=" * (4 - len(part) % 4)
        try:
            base64.urlsafe_b64decode(padded)
        except Exception:
            return False
    return True


def decode_jwt_claims(jwt_token: str) -> dict:
    """Decode a JWT payload (claims) without verifying the signature."""
    payload_b64 = jwt_token.split(".")[1]
    payload_b64 += "=" * (4 - len(payload_b64) % 4)
    return json.loads(base64.urlsafe_b64decode(payload_b64))


def _decode_jwt_exp(jwt_token: str) -> float:
    """Decode the ``exp`` claim from a JWT without signature verification."""
    return float(decode_jwt_claims(jwt_token)["exp"])


# Statuses that mean "the exchange endpoint isn't there" rather than "your token
# was rejected". A frontend that predates the endpoint answers 404; a perimeter
# gateway that allowlists paths/methods invents its own refusal — canary returns
# 405 with ``allow: GET``, others use 403 or 501. 401 is deliberately absent: it
# is a real verdict on the token, and retrying it as a bearer would only send the
# opaque token back through the gateway that already refuses it.
_ENDPOINT_ABSENT_STATUSES = frozenset({403, 404, 405, 501})


def _request_exchange(session_token: str, base_url: str) -> requests.Response:
    """Call the token-exchange endpoint, carrying the opaque token in the body.

    Sends ``POST {base_url}/api/auth/cli-token`` with the session token in the
    JSON body (no ``Authorization`` header) so JWT-only perimeter gateways don't
    reject it. Falls back to the legacy ``GET /api/auth/token`` bearer exchange
    when the endpoint isn't reachable (see ``_ENDPOINT_ABSENT_STATUSES``).

    Redirects are not followed: a gateway that bounces unauthenticated requests
    to a login page would otherwise return that page with HTTP 200 and an HTML
    body, which is indistinguishable from a successful exchange until the JSON
    parse fails.
    """
    resp = requests.post(
        f"{base_url}/api/auth/cli-token",
        json={"session_token": session_token},
        headers={"Content-Type": "application/json"},
        timeout=10,
        allow_redirects=False,
    )
    if resp.status_code in _ENDPOINT_ABSENT_STATUSES:
        LOG.warning(
            "Token exchange endpoint POST %s/api/auth/cli-token unavailable "
            "(HTTP %d); falling back to legacy GET /api/auth/token bearer exchange.",
            base_url,
            resp.status_code,
        )
        resp = requests.get(
            f"{base_url}/api/auth/token",
            headers={"Authorization": f"Bearer {session_token}"},
            timeout=10,
            allow_redirects=False,
        )
    return resp


def exchange_session_for_jwt(session_token: str, auth_url: str) -> str:
    """Exchange a BetterAuth session token for a short-lived JWT.

    Posts the session token to ``{auth_url}/auth/cli-token`` (see
    :func:`_request_exchange`) and returns the JWT string.
    """
    base_url = auth_url.rstrip("/")
    try:
        resp = _request_exchange(session_token, base_url)
    except requests.ConnectionError:
        raise RuntimeError(
            f"Could not connect to auth server at {base_url}. "
            "Check that ZIPLINE_AUTH_URL is correct and the frontend is running. "
            f"Current ZIPLINE_AUTH_URL: {auth_url}"
        ) from None
    except requests.Timeout:
        raise RuntimeError(
            f"Timed out connecting to auth server at {base_url}. "
            "Check that ZIPLINE_AUTH_URL is correct and the frontend is reachable."
        ) from None
    except requests.RequestException as e:
        # Catch-all for the rest of the hierarchy — TooManyRedirects,
        # ChunkedEncodingError, ContentDecodingError, RetryError. Callers handle
        # RuntimeError; letting these escape as-is turns a transient network
        # hiccup into a traceback out of `zipline auth status`.
        raise RuntimeError(f"Token exchange at {base_url} failed: {e}") from None

    if resp.status_code == 401:
        raise RuntimeError(
            f"Token rejected by auth server at {base_url} (HTTP 401). "
            "The token may be expired, revoked, or does not belong to "
            "this auth server. Check that ZIPLINE_AUTH_URL matches the frontend "
            "where the token was created. If using a service principal, rotate "
            "the token in the admin UI. If using a personal session, run "
            "'zipline auth login' to re-authenticate."
        )
    if not resp.ok:
        raise RuntimeError(
            f"Token exchange failed (HTTP {resp.status_code}) at {base_url}. "
            f"Response: {resp.text[:200]}"
        )

    try:
        token = resp.json().get("token")
    except ValueError:
        # Not JSON. Usually something other than the app answered — a gateway
        # error page, or a proxy that served HTML with a 2xx.
        raise RuntimeError(
            f"Token exchange at {base_url} returned a non-JSON response "
            f"(HTTP {resp.status_code}, content-type "
            f"{resp.headers.get('content-type', 'unknown')}). This usually means an "
            "API gateway or proxy answered instead of the auth server. "
            f"Response: {resp.text[:200]}"
        ) from None

    if not token:
        raise RuntimeError(
            f"Token exchange at {base_url} succeeded but no JWT was returned. "
            "Check that the auth server is configured correctly."
        )
    if not is_jwt(token):
        raise RuntimeError(
            f"Token exchange at {base_url} returned a value that is not a JWT. "
            "Check that ZIPLINE_AUTH_URL points at the Zipline frontend and not "
            "at an intermediary that rewrites the response."
        )
    return token


# ---------------------------------------------------------------------------
# Cached resolver
# ---------------------------------------------------------------------------

_cache: dict[tuple[str, str], dict] = {}  # (session_token, auth_url) -> {"jwt": str, "exp": float}
_cache_lock = threading.Lock()


def resolve_token(token: str, auth_url: str | None = None) -> str:
    """Resolve a ``ZIPLINE_TOKEN`` value to a JWT suitable for API requests.

    * If *token* is already a JWT, return it as-is.
    * If *token* is a session token, exchange it for a JWT via *auth_url*.
      Exchanged JWTs are cached and re-used until within 60 s of expiry.

    Raises ``ValueError`` if *token* is a session token and *auth_url* is not
    provided.
    """
    if is_jwt(token):
        return token

    if not auth_url:
        raise ValueError(
            "ZIPLINE_AUTH_URL must be set when ZIPLINE_TOKEN is a session token "
            "(not a JWT). Set ZIPLINE_AUTH_URL to the frontend URL "
            "(e.g. https://zipline.example.com)."
        )

    key = (token, auth_url)
    with _cache_lock:
        cached = _cache.get(key)
        if cached and cached["exp"] - time.time() > 60:
            return cached["jwt"]

    jwt = exchange_session_for_jwt(token, auth_url)
    exp = _decode_jwt_exp(jwt)
    with _cache_lock:
        _cache[key] = {"jwt": jwt, "exp": exp}
    return jwt
