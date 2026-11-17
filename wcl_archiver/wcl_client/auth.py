import asyncio
import copy
import hashlib
import json
import math
import os
import re
import secrets
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote, urlencode
import httpx

CSRF_META_RE = re.compile(r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', re.I)


class _InvalidUserGrant(Exception):
    """Internal marker; provider response details must never leave the manager."""


class WCLAuthManager:
    BASE_URL = "https://www.warcraftlogs.com"

    def __init__(
        self,
        session_file: Path,
        transport: Optional[httpx.AsyncBaseTransport] = None,
    ):
        self.session_file = Path(session_file)
        self.session_file.parent.mkdir(parents=True, exist_ok=True)
        self._transport = transport
        self._oauth_lock = asyncio.Lock()
        self._pending_user_oauth: Dict[str, Dict[str, Any]] = {}
        self._oauth_generation = 0
        self._state: Dict[str, Any] = {
            "authenticated": False,
            "email": "",
            "cookies": {},
            "csrf_token": "",
            "client_id": "",
            "client_secret": "",
            "oauth_token": "",
            "user_oauth_token": "",
            "user_oauth_refresh_token": "",
            "user_oauth_expires_at": 0,
            "user_oauth_client_id": "",
            "status_message": "",
            "last_error": "",
        }
        self._load()
        self._persisted_state = copy.deepcopy(self._state)

    def _load(self) -> None:
        if self.session_file.exists():
            try:
                self._state.update(json.loads(self.session_file.read_text(encoding="utf-8")))
            except Exception:
                pass

    def _save(self) -> None:
        temporary_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.session_file.parent,
                prefix=f".{self.session_file.name}.", suffix=".tmp", delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                json.dump(self._state, temporary, indent=2)
                temporary.flush()
                os.fsync(temporary.fileno())
            os.replace(temporary_path, self.session_file)
        except Exception:
            self._state = copy.deepcopy(self._persisted_state)
            raise RuntimeError("Could not persist authentication settings.") from None
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)
        self._persisted_state = copy.deepcopy(self._state)

    def _oauth_mode(self) -> str:
        # The client id remains after a refresh rejection to keep user mode selected
        # until the user explicitly disconnects or changes application credentials.
        if self._state.get("user_oauth_client_id") or self._state.get("user_oauth_token"):
            return "user"
        if self._state.get("oauth_token"):
            return "client"
        if self._state.get("cookies"):
            return "browser"
        return "none"

    def get_auth_status(self) -> Dict[str, Any]:
        return {
            "authenticated": bool(self._state.get("authenticated") or self._state.get("user_oauth_token")),
            "email": self._state.get("email", ""),
            "has_session_cookie": "wcl_session" in self._state.get("cookies", {}),
            "has_oauth_token": bool(self._state.get("oauth_token")),
            "has_client_credentials": bool(self._state.get("client_id") and self._state.get("client_secret")),
            "has_user_oauth_token": bool(self._state.get("user_oauth_token")),
            "oauth_mode": self._oauth_mode(),
            "user_oauth_expires_at": self._state.get("user_oauth_expires_at", 0),
            "status_message": self._state.get("status_message", ""),
            "last_error": self._state.get("last_error", ""),
        }

    def _credential_identity(self) -> str:
        credentials = [self._state.get("client_id", ""), self._state.get("client_secret", "")]
        return hashlib.sha256(json.dumps(credentials).encode()).hexdigest()

    def _clear_user_oauth(self) -> None:
        for key in ("user_oauth_token", "user_oauth_refresh_token", "user_oauth_client_id"):
            self._state[key] = ""
        self._state["user_oauth_expires_at"] = 0
        self._pending_user_oauth.clear()
        self._oauth_generation += 1

    def _update_client_credentials(self, client_id: str, client_secret: str) -> None:
        client_id = client_id.strip() if client_id else self._state.get("client_id", "")
        client_secret = client_secret.strip() if client_secret else self._state.get("client_secret", "")
        if (client_id, client_secret) != (self._state.get("client_id", ""), self._state.get("client_secret", "")):
            self._clear_user_oauth()
            self._state["oauth_token"] = ""
        self._state["client_id"] = client_id
        self._state["client_secret"] = client_secret

    def begin_user_oauth(self, redirect_uri: str) -> Dict[str, Any]:
        """Create a short-lived, single-use authorization request for a loopback UI."""
        match = re.fullmatch(
            r"http://(?:localhost|127\.0\.0\.1)(?::([0-9]{1,5}))?(/|/api/auth/oauth/callback)",
            redirect_uri,
        ) if isinstance(redirect_uri, str) else None
        if not match or (match.group(1) is not None and not 1 <= int(match.group(1)) <= 65535):
            raise ValueError("OAuth redirect must use an HTTP loopback host and a supported callback path.")
        if not self._state.get("client_id") or not self._state.get("client_secret"):
            raise ValueError("Save your WCL Client ID and Client Secret before connecting an account.")
        now = time.time()
        self._pending_user_oauth = {
            key: value for key, value in self._pending_user_oauth.items()
            if value["expires_at"] > now
        }
        state = secrets.token_urlsafe(32)
        self._pending_user_oauth[hashlib.sha256(state.encode()).hexdigest()] = {
            "redirect_uri": redirect_uri,
            "expires_at": now + 600,
            "identity": self._credential_identity(),
            "generation": self._oauth_generation,
        }
        return {
            "authorization_url": f"{self.BASE_URL}/oauth/authorize?" + urlencode({
                "response_type": "code", "client_id": self._state["client_id"],
                "redirect_uri": redirect_uri, "scope": "view-user-profile", "state": state,
            }),
            "state": state,
            "expires_in": 600,
        }

    def _consume_user_oauth_state(self, state: str) -> Dict[str, Any]:
        pending = self._pending_user_oauth.pop(hashlib.sha256(state.encode()).hexdigest(), None) if isinstance(state, str) else None
        if (not pending or pending["expires_at"] <= time.time()
                or pending["identity"] != self._credential_identity()
                or pending["generation"] != self._oauth_generation):
            raise ValueError("OAuth state is invalid or expired. Start account authorization again.")
        return pending

    def cancel_user_oauth(self, state: str) -> None:
        self._consume_user_oauth_state(state)

    async def _request_user_token(self, data: Dict[str, str]) -> Dict[str, Any]:
        try:
            async with httpx.AsyncClient(transport=self._transport, timeout=30.0) as client:
                response = await client.post(
                    f"{self.BASE_URL}/oauth/token", data=data,
                    auth=(self._state["client_id"], self._state["client_secret"]),
                )
            payload = response.json()
            if response.status_code != 200:
                if isinstance(payload, dict) and payload.get("error") == "invalid_grant":
                    raise _InvalidUserGrant()
                raise RuntimeError("User OAuth token request failed. Connect your WCL account again.")
            if not isinstance(payload, dict):
                raise ValueError()
            token = payload.get("access_token")
            refresh = payload.get("refresh_token", "")
            expires_in = payload.get("expires_in")
            if (not isinstance(token, str) or not token or not isinstance(refresh, str)
                    or isinstance(expires_in, bool) or not isinstance(expires_in, (int, float))
                    or not math.isfinite(expires_in) or expires_in <= 0):
                raise ValueError()
            return payload
        except _InvalidUserGrant:
            raise
        except Exception:
            raise RuntimeError("User OAuth token request failed. Connect your WCL account again.") from None

    def _store_user_token(self, payload: Dict[str, Any], refresh_token: str = "") -> None:
        self._state.update({
            "user_oauth_token": payload["access_token"],
            "user_oauth_refresh_token": payload.get("refresh_token") or refresh_token,
            "user_oauth_expires_at": time.time() + payload["expires_in"],
            "user_oauth_client_id": self._state["client_id"],
            "status_message": "Connected WCL user account.", "last_error": "",
        })
        self._save()

    async def complete_user_oauth(self, code: str, state: str) -> Dict[str, Any]:
        async with self._oauth_lock:
            pending = self._consume_user_oauth_state(state)
            if not isinstance(code, str) or not code.strip():
                raise ValueError("OAuth authorization code is missing.")
            try:
                payload = await self._request_user_token({
                    "grant_type": "authorization_code", "code": code,
                    "redirect_uri": pending["redirect_uri"],
                })
            except _InvalidUserGrant:
                raise RuntimeError("User OAuth authorization was rejected. Connect your WCL account again.") from None
            if (pending["identity"] != self._credential_identity()
                    or pending["generation"] != self._oauth_generation):
                raise ValueError("OAuth authorization changed. Start account authorization again.")
            self._store_user_token(payload)
            return self.get_auth_status()

    def disconnect_user_oauth(self) -> Dict[str, Any]:
        self._clear_user_oauth()
        self._state["status_message"] = "Disconnected WCL user account."
        self._state["last_error"] = ""
        self._save()
        return self.get_auth_status()

    async def get_api_auth(self) -> Tuple[str, str]:
        async with self._oauth_lock:
            if self._oauth_mode() != "user":
                return f"{self.BASE_URL}/api/v2/client", self._state.get("oauth_token", "")
            if self._state.get("user_oauth_client_id") != self._state.get("client_id"):
                raise RuntimeError("WCL application credentials changed. Connect your WCL account again.")
            token = self._state.get("user_oauth_token", "")
            if token and self._state.get("user_oauth_expires_at", 0) > time.time():
                return f"{self.BASE_URL}/api/v2/user", token
            refresh_token = self._state.get("user_oauth_refresh_token", "")
            if not refresh_token:
                raise RuntimeError("WCL user authorization expired. Connect your WCL account again.")
            identity, generation = self._credential_identity(), self._oauth_generation
            try:
                payload = await self._request_user_token({"grant_type": "refresh_token", "refresh_token": refresh_token})
            except _InvalidUserGrant:
                if identity == self._credential_identity() and generation == self._oauth_generation:
                    self._state["user_oauth_token"] = ""
                    self._state["user_oauth_refresh_token"] = ""
                    self._state["user_oauth_expires_at"] = 0
                    self._state["last_error"] = "WCL user authorization expired. Connect your WCL account again."
                    self._save()
                raise RuntimeError("WCL user authorization expired. Connect your WCL account again.") from None
            if identity != self._credential_identity() or generation != self._oauth_generation:
                raise RuntimeError("WCL user authorization changed. Retry the request.")
            self._store_user_token(payload, refresh_token)
            return f"{self.BASE_URL}/api/v2/user", payload["access_token"]

    @staticmethod
    def parse_cookie_input(raw_cookie: str, xsrf_token: str = "") -> Dict[str, str]:
        raw = raw_cookie.strip()
        if not raw:
            return {}
        cookies: Dict[str, str] = {}
        if "=" in raw and (";" in raw or raw.startswith("wcl_session=") or raw.startswith("XSRF-TOKEN=")):
            for part in raw.split(";"):
                if "=" in part:
                    k, v = part.strip().split("=", 1)
                    cookies[k.strip()] = v.strip()
        else:
            cookies["wcl_session"] = raw
        if xsrf_token:
            cookies["XSRF-TOKEN"] = xsrf_token.strip()
        return cookies

    def save_session_cookies(
        self,
        cookies: Dict[str, str],
        xsrf_token: str = "",
        email: str = "Browser Session",
        client_id: str = "",
        client_secret: str = "",
    ) -> Dict[str, Any]:
        clean_cookies = {k: v for k, v in cookies.items() if v}
        self._state["cookies"] = clean_cookies
        self._state["csrf_token"] = xsrf_token or unquote(clean_cookies.get("XSRF-TOKEN", ""))
        self._state["email"] = email or "Browser Session"
        self._state["authenticated"] = bool(clean_cookies)
        self._state["status_message"] = (
            "Authenticated via browser session cookie." if clean_cookies else ""
        )
        self._state["last_error"] = "" if clean_cookies else "Empty session cookie provided."
        self._update_client_credentials(client_id, client_secret)
        self._save()
        return self.get_auth_status()

    async def login_with_credentials(
        self,
        email: str,
        password: str,
        client_id: str = "",
        client_secret: str = "",
    ) -> Dict[str, Any]:
        self._state["authenticated"] = False
        self._state["status_message"] = ""
        self._state["last_error"] = ""
        self._update_client_credentials(client_id, client_secret)

        browser_headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        async with httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=browser_headers,
            follow_redirects=True,
            transport=self._transport,
            timeout=30.0,
        ) as client:
            if email and password:
                try:
                    login_page = await client.get("/login")
                    csrf_match = CSRF_META_RE.search(login_page.text)
                    csrf_token = (
                        csrf_match.group(1)
                        if csrf_match
                        else unquote(client.cookies.get("XSRF-TOKEN", ""))
                    )

                    resp = await client.post(
                        "/login",
                        data={"email": email, "password": password, "_token": csrf_token},
                        headers={"Referer": f"{self.BASE_URL}/login", "Origin": self.BASE_URL},
                    )
                    cookies = dict(client.cookies.items())
                    lower_text = resp.text.lower()
                    has_cred_error = any(
                        phrase in lower_text
                        for phrase in (
                            "invalid username",
                            "invalid password",
                            "these credentials do not match",
                            "invalid email",
                            "incorrect password",
                        )
                    )
                    is_cloudflare_block = (
                        resp.status_code in (403, 503)
                        or "just a moment" in lower_text
                        or "cf-mitigated" in resp.headers
                    )
                    is_still_login_form = (
                        'name="password"' in lower_text
                        and "logout" not in lower_text
                        and "logged in" not in lower_text
                    )

                    if (
                        resp.status_code < 400
                        and not has_cred_error
                        and not is_cloudflare_block
                        and not is_still_login_form
                        and (
                            "wcl_session" in cookies
                            or "logout" in lower_text
                            or "logged in" in lower_text
                        )
                    ):
                        self._state["authenticated"] = True
                        self._state["email"] = email
                        self._state["cookies"] = cookies
                        self._state["csrf_token"] = csrf_token or unquote(cookies.get("XSRF-TOKEN", ""))
                        self._state["status_message"] = f"Authenticated as {email}."
                    elif is_cloudflare_block:
                        self._state["last_error"] = (
                            f"WarcraftLogs Cloudflare protection blocked direct password login (HTTP {resp.status_code}). "
                            "Please use Option A (API v2 Client ID & Secret from warcraftlogs.com/api/clients) "
                            "or Option B (paste wcl_session cookie from your logged-in browser)."
                        )
                    elif has_cred_error:
                        self._state["last_error"] = "Invalid WarcraftLogs email or password."
                    else:
                        self._state["last_error"] = (
                            f"Direct web login did not complete (HTTP {resp.status_code}, URL: {resp.url.path}). "
                            "WarcraftLogs uses Archon SSO / Cloudflare on web forms. "
                            "Please use API v2 Client ID & Secret or paste your wcl_session browser cookie."
                        )
                except Exception:
                    self._state["last_error"] = "Connection error during web login."

            if client_id and client_secret:
                try:
                    tok_resp = await client.post(
                        "/oauth/token",
                        data={"grant_type": "client_credentials"},
                        auth=(self._state["client_id"], self._state["client_secret"]),
                    )
                    if tok_resp.status_code == 200:
                        token_val = tok_resp.json().get("access_token", "")
                        if token_val:
                            self._state["oauth_token"] = token_val
                            self._state["authenticated"] = True
                            if not self._state.get("email"):
                                self._state["email"] = email or f"API v2 ({self._state['client_id'][:8]})"
                            self._state["status_message"] = "Authenticated with WCL API v2 OAuth2 token."
                            self._state["last_error"] = ""
                    else:
                        self._state["last_error"] = (
                            f"WCL API v2 OAuth2 failed (HTTP {tok_resp.status_code}). "
                            "Verify your Client ID and Client Secret from warcraftlogs.com/api/clients."
                        )
                except Exception:
                    self._state["last_error"] = "OAuth2 token request failed."

            if not email and not password and not (client_id and client_secret):
                self._state["last_error"] = (
                    "Please enter API v2 Client ID + Secret, a wcl_session cookie, or Email + Password."
                )

            self._save()
            return self.get_auth_status()

    def get_headers_and_cookies(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        cookies = dict(self._state.get("cookies", {}))
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) WCLArchiver/1.0",
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        }
        csrf = self._state.get("csrf_token") or unquote(cookies.get("XSRF-TOKEN", ""))
        if csrf:
            headers["X-CSRF-TOKEN"] = csrf
            headers["X-XSRF-TOKEN"] = csrf
        if self._state.get("oauth_token"):
            headers["Authorization"] = f"Bearer {self._state['oauth_token']}"
        return headers, cookies
