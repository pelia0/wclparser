import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest

from wcl_archiver.wcl_client.auth import WCLAuthManager


def manager(tmp_path, handler=None):
    auth = WCLAuthManager(tmp_path / "session.json", transport=httpx.MockTransport(
        handler or (lambda request: httpx.Response(200, json={"access_token": "user-access", "refresh_token": "refresh-one", "expires_in": 3600}))
    ))
    auth.save_session_cookies({"wcl_session": "browser-cookie"}, client_id="client-id", client_secret="client-secret")
    return auth


def connect_state(auth, **changes):
    auth._state.update({"oauth_token": "legacy-client", "user_oauth_token": "old-user", "user_oauth_refresh_token": "old-refresh", "user_oauth_expires_at": 1, "user_oauth_client_id": "client-id", **changes})
    auth._save()


def test_begin_requires_credentials_and_uses_profile_scope(tmp_path):
    empty = WCLAuthManager(tmp_path / "empty.json")
    with pytest.raises(ValueError):
        empty.begin_user_oauth("http://localhost:8080/")
    auth = manager(tmp_path)
    result = auth.begin_user_oauth("http://localhost:8080/")
    parsed = urlsplit(result["authorization_url"])
    query = parse_qs(parsed.query)
    assert (parsed.scheme, parsed.netloc, parsed.path) == ("https", "www.warcraftlogs.com", "/oauth/authorize")
    assert query == {"response_type": ["code"], "client_id": ["client-id"], "redirect_uri": ["http://localhost:8080/"], "scope": ["view-user-profile"], "state": [result["state"]]}
    assert result["expires_in"] == 600
    assert len(result["state"]) >= 32
    assert result["state"] not in auth.session_file.read_text()


@pytest.mark.parametrize("uri", ["https://localhost:8080/", "http://example.com:8080/", "http://localhost.example.com:8080/", "http://user@localhost:8080/", "http://localhost:8080/?evil=1", "http://localhost:8080/#code", "http://localhost:0/", "http://localhost:65536/", "http://localhost:8080/other", "http://127.1:8080/", "http://localhost.:8080/", "http://localhost:8080\\@evil/", " http://localhost:8080/", "http://localhost:8080/?", "http://localhost:8080/#"])
def test_redirect_rejects_non_loopback_or_ambiguous_urls(tmp_path, uri):
    with pytest.raises(ValueError):
        manager(tmp_path).begin_user_oauth(uri)


@pytest.mark.parametrize("uri", ["http://localhost:8080/", "http://127.0.0.1:9090/api/auth/oauth/callback"])
@pytest.mark.asyncio
async def test_exchange_persists_separate_tokens_and_rejects_replay(tmp_path, uri):
    requests = []
    def handler(request):
        requests.append(request)
        return httpx.Response(200, json={"access_token": "user-access", "refresh_token": "refresh-one", "expires_in": 3600})
    auth = manager(tmp_path, handler)
    auth._state["oauth_token"] = "legacy-client"
    started = auth.begin_user_oauth(uri)
    status = await auth.complete_user_oauth("secret-code", started["state"])
    assert status["oauth_mode"] == "user"
    assert status["has_user_oauth_token"] is True
    assert status["has_client_credentials"] is True
    assert all(secret not in json.dumps(status) for secret in ("user-access", "refresh-one", "client-secret", "secret-code", started["state"]))
    request = requests[0]
    assert request.url.path == "/oauth/token"
    assert parse_qs(request.content.decode()) == {"grant_type": ["authorization_code"], "code": ["secret-code"], "redirect_uri": [uri]}
    assert "authorization" in request.headers
    reloaded = WCLAuthManager(auth.session_file)
    assert await reloaded.get_api_auth() == ("https://www.warcraftlogs.com/api/v2/user", "user-access")
    assert reloaded.get_headers_and_cookies()[0]["Authorization"] == "Bearer legacy-client"
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("secret-code", started["state"])
    assert len(requests) == 1


@pytest.mark.asyncio
async def test_wrong_expired_and_cancelled_state_never_exchange(tmp_path, monkeypatch):
    calls = []
    auth = manager(tmp_path, lambda request: calls.append(request) or httpx.Response(500))
    now = 1000
    monkeypatch.setattr("wcl_archiver.wcl_client.auth.time.time", lambda: now)
    started = auth.begin_user_oauth("http://localhost:8080/")
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", "wrong-state")
    now = 1601
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", started["state"])
    started = auth.begin_user_oauth("http://localhost:8080/")
    auth.cancel_user_oauth(started["state"])
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", started["state"])
    assert calls == []


@pytest.mark.parametrize("failure", ["http", "transport", "malformed"])
@pytest.mark.asyncio
async def test_failed_exchange_is_safe_single_use_and_preserves_previous_token(tmp_path, failure):
    def handler(request):
        if failure == "transport":
            raise httpx.ConnectError("secret-code client-secret", request=request)
        return httpx.Response(400 if failure == "http" else 200, text="secret-code client-secret")
    auth = manager(tmp_path, handler)
    connect_state(auth, user_oauth_expires_at=9999999999)
    started = auth.begin_user_oauth("http://localhost:8080/")
    with pytest.raises(RuntimeError) as error:
        await auth.complete_user_oauth("secret-code", started["state"])
    assert "secret-code" not in str(error.value)
    assert "client-secret" not in str(error.value)
    assert await auth.get_api_auth() == ("https://www.warcraftlogs.com/api/v2/user", "old-user")
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("secret-code", started["state"])


@pytest.mark.asyncio
async def test_refresh_single_flight_rotates_and_persists_tokens(tmp_path):
    calls = []
    async def handler(request):
        calls.append(parse_qs(request.content.decode()))
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"access_token": "new-user", "refresh_token": "new-refresh", "expires_in": 3600})
    auth = manager(tmp_path, handler)
    connect_state(auth)
    assert await asyncio.gather(*(auth.get_api_auth() for _ in range(8))) == [("https://www.warcraftlogs.com/api/v2/user", "new-user")] * 8
    assert calls == [{"grant_type": ["refresh_token"], "refresh_token": ["old-refresh"]}]
    saved = json.loads(auth.session_file.read_text())
    assert saved["user_oauth_refresh_token"] == "new-refresh"
    assert saved["user_oauth_token"] == "new-user"


@pytest.mark.parametrize("failure", ["invalid_grant", "transport", "missing_refresh"])
@pytest.mark.asyncio
async def test_expired_user_failure_never_downgrades_to_client(tmp_path, failure):
    def handler(request):
        if failure == "transport":
            raise httpx.ConnectError("old-refresh client-secret", request=request)
        return httpx.Response(400, json={"error": "invalid_grant", "error_description": "old-refresh client-secret"})
    auth = manager(tmp_path, handler)
    connect_state(auth, **({"user_oauth_refresh_token": ""} if failure == "missing_refresh" else {}))
    for _ in range(2):
        with pytest.raises(RuntimeError) as error:
            await auth.get_api_auth()
        assert "old-refresh" not in str(error.value)
        assert "client-secret" not in str(error.value)
        assert auth.get_auth_status()["oauth_mode"] == "user"
    if failure == "invalid_grant":
        assert auth.get_auth_status()["has_user_oauth_token"] is False
        assert auth._state["user_oauth_refresh_token"] == ""
    reloaded = WCLAuthManager(auth.session_file, transport=auth._transport)
    with pytest.raises(RuntimeError):
        await reloaded.get_api_auth()


@pytest.mark.asyncio
async def test_disconnect_preserves_client_cookie_and_invalidates_pending(tmp_path):
    auth = manager(tmp_path)
    connect_state(auth)
    started = auth.begin_user_oauth("http://localhost:8080/")
    status = auth.disconnect_user_oauth()
    assert status["oauth_mode"] == "client"
    assert status["has_user_oauth_token"] is False
    assert status["has_session_cookie"] is True
    assert await auth.get_api_auth() == ("https://www.warcraftlogs.com/api/v2/client", "legacy-client")
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", started["state"])


@pytest.mark.parametrize("method", ["save", "login"])
@pytest.mark.asyncio
async def test_changed_credentials_clear_user_and_pending_same_credentials_preserve(tmp_path, method):
    auth = manager(tmp_path)
    connect_state(auth, user_oauth_expires_at=9999999999)
    started = auth.begin_user_oauth("http://localhost:8080/")
    if method == "save":
        auth.save_session_cookies({"wcl_session": "new-cookie"}, client_id="client-id", client_secret="client-secret")
    else:
        await auth.login_with_credentials("", "", "client-id", "client-secret")
    assert auth.get_auth_status()["has_user_oauth_token"] is True
    if method == "save":
        auth.save_session_cookies({"wcl_session": "new-cookie"}, client_secret="changed-secret")
    else:
        await auth.login_with_credentials("", "", "client-id", "changed-secret")
    assert auth.get_auth_status()["has_user_oauth_token"] is False
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", started["state"])


@pytest.mark.asyncio
async def test_atomic_save_failure_preserves_previous_valid_state(tmp_path, monkeypatch):
    auth = manager(tmp_path)
    connect_state(auth, user_oauth_expires_at=9999999999)
    before = auth.session_file.read_bytes()
    started = auth.begin_user_oauth("http://localhost:8080/")
    def fail_replace(*args, **kwargs):
        raise OSError("disk error with user-access")
    monkeypatch.setattr("wcl_archiver.wcl_client.auth.os.replace", fail_replace)
    with pytest.raises(RuntimeError) as error:
        await auth.complete_user_oauth("code", started["state"])
    assert "user-access" not in str(error.value)
    assert auth.session_file.read_bytes() == before
    assert await auth.get_api_auth() == ("https://www.warcraftlogs.com/api/v2/user", "old-user")
    assert list(tmp_path.iterdir()) == [auth.session_file]


@pytest.mark.parametrize("payload", [
    {"access_token": "user-access"},
    {"access_token": "user-access", "expires_in": "3600"},
    {"access_token": "user-access", "expires_in": 0},
    {"access_token": "user-access", "expires_in": True},
    {"access_token": ["user-access"], "expires_in": 3600},
    {"access_token": "user-access", "refresh_token": ["secret-refresh"], "expires_in": 3600},
])
@pytest.mark.asyncio
async def test_malformed_token_payload_cannot_overwrite_existing_authorization(tmp_path, payload):
    auth = manager(tmp_path, lambda request: httpx.Response(200, json=payload))
    connect_state(auth, user_oauth_expires_at=9999999999)
    before = auth.session_file.read_bytes()
    started = auth.begin_user_oauth("http://localhost:8080/")
    with pytest.raises(RuntimeError):
        await auth.complete_user_oauth("code", started["state"])
    assert auth.session_file.read_bytes() == before


@pytest.mark.asyncio
async def test_refresh_without_rotation_preserves_existing_refresh_token(tmp_path):
    auth = manager(tmp_path, lambda request: httpx.Response(200, json={"access_token": "new-access", "expires_in": 3600}))
    connect_state(auth)
    assert await auth.get_api_auth() == ("https://www.warcraftlogs.com/api/v2/user", "new-access")
    assert json.loads(auth.session_file.read_text())["user_oauth_refresh_token"] == "old-refresh"


@pytest.mark.parametrize("operation", ["exchange", "refresh"])
@pytest.mark.parametrize("mutation", ["disconnect", "credentials"])
@pytest.mark.asyncio
async def test_inflight_token_request_cannot_reconnect_after_auth_changes(tmp_path, operation, mutation):
    entered, release = asyncio.Event(), asyncio.Event()
    async def handler(request):
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"access_token": "late-access", "refresh_token": "late-refresh", "expires_in": 3600})
    auth = manager(tmp_path, handler)
    connect_state(auth)
    started = auth.begin_user_oauth("http://localhost:8080/")
    task = asyncio.create_task(auth.complete_user_oauth("code", started["state"]) if operation == "exchange" else auth.get_api_auth())
    await entered.wait()
    if mutation == "disconnect":
        auth.disconnect_user_oauth()
    else:
        auth.save_session_cookies({"wcl_session": "cookie"}, client_secret="changed-secret")
    release.set()
    with pytest.raises((ValueError, RuntimeError)):
        await task
    assert auth.get_auth_status()["has_user_oauth_token"] is False
    assert "late-access" not in auth.session_file.read_text()


def test_legacy_modes_and_unknown_auth_have_safe_status(tmp_path):
    auth = WCLAuthManager(tmp_path / "session.json")
    assert auth.get_auth_status()["oauth_mode"] == "none"
    auth.save_session_cookies({"wcl_session": "cookie"})
    assert auth.get_auth_status()["oauth_mode"] == "browser"


@pytest.mark.asyncio
async def test_pending_state_is_bound_to_original_client_secret(tmp_path):
    requests = []
    auth = manager(tmp_path, lambda request: requests.append(request) or httpx.Response(500))
    started = auth.begin_user_oauth("http://localhost:8080/")
    auth._state["client_secret"] = "externally-changed-secret"
    with pytest.raises(ValueError):
        await auth.complete_user_oauth("code", started["state"])
    assert requests == []
