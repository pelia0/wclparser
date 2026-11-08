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
        }

    def save_session_cookies(
        self,
        cookies: Dict[str, str],
        xsrf_token: str = "",
        email: str = "Browser Session",
        client_id: str = "",
        client_secret: str = "",
    ) -> Dict[str, Any]:
        self._state["cookies"] = dict(cookies)
        self._state["csrf_token"] = xsrf_token or unquote(cookies.get("XSRF-TOKEN", ""))
        self._state["email"] = email
        self._state["authenticated"] = bool(cookies)
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
        async with httpx.AsyncClient(
            base_url=self.BASE_URL,
            follow_redirects=True,
            transport=self._transport,
            timeout=30.0,
        ) as client:
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
                headers={"Referer": f"{self.BASE_URL}/login"},
            )
            cookies = dict(client.cookies.items())
            if (
                resp.status_code < 400
                and "invalid" not in resp.text.lower()
                and ("wcl_session" in cookies or "logout" in resp.text.lower() or "logged in" in resp.text.lower())
            ):
                self._state["authenticated"] = True
                self._state["email"] = email
                self._state["cookies"] = cookies
                self._state["csrf_token"] = csrf_token or unquote(cookies.get("XSRF-TOKEN", ""))

            if client_id and client_secret:
                self._state["client_id"] = client_id
                self._state["client_secret"] = client_secret
                tok_resp = await client.post(
                    "/oauth/token",
                    data={"grant_type": "client_credentials"},
                    auth=(client_id, client_secret),
                )
                if tok_resp.status_code == 200:
                    self._state["oauth_token"] = tok_resp.json().get("access_token", "")

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
