import base64
import json
from unittest.mock import Mock, patch

import pytest
import requests

from ai.chronon.repo.token_exchange import decode_jwt_claims, exchange_session_for_jwt


def _make_jwt(claims=None) -> str:
    """Build a fake (unsigned) JWT that passes `is_jwt` and decodes to *claims*."""

    def b64(obj: dict) -> str:
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode().rstrip("=")

    return f"{b64({'alg': 'EdDSA'})}.{b64(claims or {'sub': 'u1'})}.sig"


JWT = _make_jwt()


def _resp(status_code, *, json_body=None, text=None, content_type="application/json"):
    resp = Mock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.headers = {"content-type": content_type}
    if json_body is None and text is not None:
        resp.json.side_effect = ValueError("not json")
        resp.text = text
    else:
        resp.json.return_value = json_body or {}
        resp.text = json.dumps(json_body or {})
    return resp


def test_decode_jwt_claims():
    payload = {"email": "a@b.com", "name": "A B", "role": "admin", "exp": 123}
    b64 = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    assert decode_jwt_claims(f"header.{b64}.sig") == payload


def test_exchange_posts_token_in_body_without_authorization_header():
    """The opaque token must ride in the body, never the Authorization header —
    that is the whole point (JWT-only gateways reject non-JWT bearer tokens)."""
    post_resp = _resp(200, json_body={"token": JWT})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get") as mock_get,
    ):
        token = exchange_session_for_jwt("sess-abc", "https://hub.example.com")

    assert token == JWT
    mock_get.assert_not_called()
    args, kwargs = mock_post.call_args
    assert kwargs["json"] == {"session_token": "sess-abc"}
    assert "Authorization" not in (kwargs.get("headers") or {})


def test_exchange_targets_the_api_auth_namespace():
    """Load-bearing: perimeter gateways allowlist paths and methods, and
    /api/auth/** is the prefix the device flow already crosses. A path outside
    it is refused before it reaches the origin."""
    post_resp = _resp(200, json_body={"token": JWT})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        exchange_session_for_jwt("sess-abc", "https://hub.example.com")
    assert mock_post.call_args[0][0] == "https://hub.example.com/api/auth/cli-token"


def test_exchange_does_not_follow_redirects():
    """A gateway that bounces to a login page would otherwise return HTML with
    HTTP 200, which looks like success until the JSON parse fails."""
    post_resp = _resp(200, json_body={"token": JWT})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        exchange_session_for_jwt("sess-abc", "https://hub.example.com")
    assert mock_post.call_args[1]["allow_redirects"] is False


def test_exchange_strips_trailing_slash():
    post_resp = _resp(200, json_body={"token": JWT})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=post_resp) as mock_post,
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        exchange_session_for_jwt("sess-abc", "https://hub.example.com/")
    assert mock_post.call_args[0][0] == "https://hub.example.com/api/auth/cli-token"


@pytest.mark.parametrize("status", [403, 404, 405, 501])
def test_exchange_falls_back_to_legacy_endpoint_when_endpoint_absent(status):
    """A frontend that predates the endpoint answers 404; a gateway that
    allowlists paths/methods invents its own refusal (canary returns 405 with
    `allow: GET`). All of them mean "not there" — fall back rather than fail."""
    get_resp = _resp(200, json_body={"token": JWT})
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=_resp(status)),
        patch("ai.chronon.repo.token_exchange.requests.get", return_value=get_resp) as mock_get,
    ):
        token = exchange_session_for_jwt("sess-abc", "https://hub.example.com")

    assert token == JWT
    args, kwargs = mock_get.call_args
    assert args[0] == "https://hub.example.com/api/auth/token"
    assert kwargs["headers"]["Authorization"] == "Bearer sess-abc"


def test_exchange_does_not_fall_back_on_401():
    """401 is a verdict on the token, not a missing endpoint. Retrying it as a
    bearer would just send the opaque token back through the gateway."""
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=_resp(401)),
        patch("ai.chronon.repo.token_exchange.requests.get") as mock_get,
    ):
        with pytest.raises(RuntimeError, match="expired"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")
    mock_get.assert_not_called()


def test_exchange_raises_on_connection_error():
    with patch(
        "ai.chronon.repo.token_exchange.requests.post",
        side_effect=requests.ConnectionError("boom"),
    ):
        with pytest.raises(RuntimeError, match="Could not connect"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


@pytest.mark.parametrize(
    "exc",
    [
        requests.TooManyRedirects("loop"),
        requests.exceptions.ChunkedEncodingError("truncated"),
        requests.exceptions.ContentDecodingError("bad gzip"),
        requests.exceptions.RetryError("gave up"),
    ],
)
def test_exchange_wraps_remaining_request_exceptions(exc):
    """Callers only handle RuntimeError — anything escaping as a bare
    RequestException becomes a traceback out of `zipline auth status`."""
    with patch("ai.chronon.repo.token_exchange.requests.post", side_effect=exc):
        with pytest.raises(RuntimeError, match="failed"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


def test_exchange_raises_on_non_json_response():
    """A proxy serving an HTML page with a 2xx must not surface as a JSON
    decode traceback."""
    html = _resp(200, text="<html>login</html>", content_type="text/html")
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=html),
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        with pytest.raises(RuntimeError, match="non-JSON response"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


def test_exchange_raises_when_no_token_returned():
    with (
        patch("ai.chronon.repo.token_exchange.requests.post", return_value=_resp(200, json_body={})),
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        with pytest.raises(RuntimeError, match="no JWT was returned"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")


def test_exchange_raises_when_response_is_not_a_jwt():
    """Guards against an intermediary that returns its own `token` field."""
    with (
        patch(
            "ai.chronon.repo.token_exchange.requests.post",
            return_value=_resp(200, json_body={"token": "csrf-abc123"}),
        ),
        patch("ai.chronon.repo.token_exchange.requests.get"),
    ):
        with pytest.raises(RuntimeError, match="not a JWT"):
            exchange_session_for_jwt("sess-abc", "https://hub.example.com")
