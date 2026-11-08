import json
import re
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from urllib.parse import unquote
import httpx

CSRF_META_RE = re.compile(r'<meta\s+name=["\']csrf-token["\']\s+content=["\']([^"\']+)["\']', re.I)


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
        self._state: Dict[str, Any] = {
            "authenticated": False,
            "email": "",
            "cookies": {},
            "csrf_token": "",
            "client_id": "",
            "client_secret": "",
            "oauth_token": "",
            "status_message": "",
            "last_error": "",
        }
        self._load()

    def _load(self) -> None:
        if self.session_file.exists():
            try:
                self._state.update(json.loads(self.session_file.read_text(encoding="utf-8")))
            except Exception:
                pass

    def _save(self) -> None:
        self.session_file.write_text(json.dumps(self._state, indent=2), encoding="utf-8")

    def get_auth_status(self) -> Dict[str, Any]:
        return {
            "authenticated": bool(self._state.get("authenticated")),
            "email": self._state.get("email", ""),
            "has_session_cookie": "wcl_session" in self._state.get("cookies", {}),
            "has_oauth_token": bool(self._state.get("oauth_token")),
            "status_message": self._state.get("status_message", ""),
            "last_error": self._state.get("last_error", ""),
        }

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
        if client_id:
            self._state["client_id"] = client_id
        if client_secret:
            self._state["client_secret"] = client_secret
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
                except Exception as exc:
                    self._state["last_error"] = f"Connection error during web login: {exc}"

            if client_id and client_secret:
                self._state["client_id"] = client_id.strip()
                self._state["client_secret"] = client_secret.strip()
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
                except Exception as exc:
                    self._state["last_error"] = f"OAuth2 token request error: {exc}"

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
