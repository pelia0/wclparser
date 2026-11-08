from pathlib import Path
import httpx
import pytest
from wcl_archiver.wcl_client.auth import WCLAuthManager


@pytest.mark.asyncio
async def test_wcl_auth_manager_csrf_login_and_session_persistence(tmp_path: Path):
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/login" and request.method == "GET":
            html = '<html><head><meta name="csrf-token" content="csrf_secret_123"></head></html>'
            return httpx.Response(
                200,
                text=html,
                headers={"set-cookie": "XSRF-TOKEN=xsrf_cookie_123; Path=/"},
            )
        if request.url.path == "/login" and request.method == "POST":
            return httpx.Response(
                200,
                text="Logged in as Pellia",
                headers={"set-cookie": "wcl_session=sub_session_abc; Path=/"},
            )
        if request.url.path == "/oauth/token" and request.method == "POST":
            return httpx.Response(200, json={"access_token": "oauth_bearer_xyz", "expires_in": 3600})
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    session_file = tmp_path / "auth_session.json"
    auth = WCLAuthManager(session_file=session_file, transport=transport)

    status = await auth.login_with_credentials(
        email="pellia@example.com",
        password="supersecretpassword",
        client_id="cid",
        client_secret="csecret",
    )

    assert status["authenticated"] is True
    assert status["has_oauth_token"] is True
    assert status["has_session_cookie"] is True
    assert session_file.exists()

    auth2 = WCLAuthManager(session_file=session_file, transport=transport)
    status2 = auth2.get_auth_status()
    assert status2["authenticated"] is True
    assert status2["email"] == "pellia@example.com"
    assert status2["has_session_cookie"] is True
    assert status2["has_oauth_token"] is True

    headers, cookies = auth2.get_headers_and_cookies()
    assert headers["Authorization"] == "Bearer oauth_bearer_xyz"
    assert headers["X-CSRF-TOKEN"] == "csrf_secret_123"
    assert headers["X-XSRF-TOKEN"] == "csrf_secret_123"
    assert cookies["wcl_session"] == "sub_session_abc"


def test_wcl_auth_manager_save_session_cookies_and_headers(tmp_path: Path):
    session_file = tmp_path / "cookies_session.json"
    auth = WCLAuthManager(session_file=session_file)

    initial_status = auth.get_auth_status()
    assert initial_status["authenticated"] is False

    status = auth.save_session_cookies(
        cookies={"wcl_session": "sess_999", "XSRF-TOKEN": "xsrf%20token%20val"},
        email="manual@example.com",
        client_id="cid_manual",
        client_secret="csec_manual",
    )
    assert status["authenticated"] is True
    assert status["email"] == "manual@example.com"
    assert status["has_session_cookie"] is True

    headers, cookies = auth.get_headers_and_cookies()
    assert cookies["wcl_session"] == "sess_999"
    assert headers["X-CSRF-TOKEN"] == "xsrf token val"
    assert headers["X-XSRF-TOKEN"] == "xsrf token val"
    assert "Authorization" not in headers


def test_wcl_auth_manager_corrupted_session_file_handled_gracefully(tmp_path: Path):
    session_file = tmp_path / "bad_session.json"
    session_file.write_text("NOT_JSON_DATA", encoding="utf-8")

    auth = WCLAuthManager(session_file=session_file)
    status = auth.get_auth_status()
    assert status["authenticated"] is False
    assert status["email"] == ""


@pytest.mark.asyncio
async def test_wcl_auth_manager_csrf_cookie_fallback_and_invalid_response_resets_auth(tmp_path: Path):
    def mock_handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/login" and request.method == "GET":
            # No meta tag, only cookie
            return httpx.Response(
                200,
                text="<html><body>Login Page Without Meta Tag</body></html>",
                headers={"set-cookie": "XSRF-TOKEN=cookie_token_999; Path=/"},
            )
        if request.url.path == "/login" and request.method == "POST":
            body = request.read().decode("utf-8")
            if "badpass" in body:
                return httpx.Response(
                    200,
                    text="Invalid username or password.",
                    headers={"set-cookie": "wcl_session=temp_guest_sess; Path=/"},
                )
            if "_token=cookie_token_999" in body:
                return httpx.Response(
                    200,
                    text="Logged in successfully",
                    headers={"set-cookie": "wcl_session=valid_session_ok; Path=/"},
                )
        return httpx.Response(404)

    transport = httpx.MockTransport(mock_handler)
    session_file = tmp_path / "fallback_session.json"
    auth = WCLAuthManager(session_file=session_file, transport=transport)

    # 1. Successful login using cookie fallback
    status1 = await auth.login_with_credentials(email="user@test.com", password="goodpassword")
    assert status1["authenticated"] is True
    assert status1["has_session_cookie"] is True

    # 2. Failed login with "Invalid" in response resets auth state to False
    status2 = await auth.login_with_credentials(email="user@test.com", password="badpass")
    assert status2["authenticated"] is False
