import asyncio
from collections import OrderedDict
from contextlib import asynccontextmanager
import json
from email.utils import parsedate_to_datetime
from html import unescape
import math
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any, AsyncIterator, Callable, Coroutine, Dict, List, Optional, TypeVar
from wcl_archiver.wcl_client.rankings import REPORT_LINK_RE, RankingsDocument, RankingsPage

T = TypeVar("T")

BASE_URL = "https://www.warcraftlogs.com"
DEFAULT_CDP_PORT = 9222
RANKINGS_URL_TEMPLATE = (
    "https://www.warcraftlogs.com/zone/rankings/{zone}"
    "?boss={encounter}&class={class_name}&spec={spec_name}&partition={partition}&page={page}"
)
RANKINGS_ROW_SELECTOR = "table.rankings-table tbody tr, table.summary-table tbody tr"
ROW_TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
PLAYER_NAME_RE = re.compile(
    r'<a\b[^>]*class="[^"]*main-table-player[^"]*"[^>]*>\s*([^<]+?)\s*</a>',
    re.DOTALL | re.IGNORECASE,
)
KEYSTONE_CELL_RE = re.compile(
    r'class="[^"]*(?:klvl-value|keystone-level|keystone)[^"]*"[^>]*>\s*\+?(\d+)\s*<|(?<!\w)\+(\d{1,2})(?!\d)',
    re.IGNORECASE,
)
SCRIPT_TAG_RE = re.compile(r"<script\b[^>]*>.*?</script>", re.DOTALL | re.IGNORECASE)
HIDDEN_SPAN_RE = re.compile(r'<span\b[^>]*style="[^"]*display\s*:\s*none[^"]*"[^>]*>.*?</span>', re.DOTALL | re.IGNORECASE)
DURATION_CELL_RE = re.compile(
    r'class="[^"]*(?:players-table-duration|main-table-duration)[^"]*"[^>]*>(.*?)</td>',
    re.DOTALL | re.IGNORECASE,
)
DURATION_RE = re.compile(r"\b(\d{1,2}):(\d{2})(?:\.(\d{1,3}))?\b")
STRIP_TAGS_RE = re.compile(r"<[^>]+>")


class RateLimitError(RuntimeError):
    """A real HTTP or rendered-page rate limit; never an empty rankings page."""

    def __init__(self, message="WCL rate limit (429 Too Many Requests)", retry_after=0.0):
        super().__init__(message)
        self.retry_after = float(retry_after)


class NavigationHTTPError(RuntimeError):
    pass


def _visible_rankings_html(html: str) -> str:
    clean = re.sub(r"<!--.*?-->", " ", html or "", flags=re.DOTALL)
    clean = re.sub(r"<(script|style|template)\b[^>]*>.*?</\1\s*>", " ", clean,
                   flags=re.DOTALL | re.IGNORECASE)
    return HIDDEN_SPAN_RE.sub(" ", clean)


def is_rate_limit_page(html: str) -> bool:
    """Inspect rendered text, excluding embedded translation/script/style data."""
    visible = unescape(STRIP_TAGS_RE.sub(" ", _visible_rankings_html(html)))
    visible = re.sub(r"\s+", " ", visible).lower()
    return bool(re.search(r"\btoo many requests\b|\brate[ -]limit(?:ed| exceeded| reached)?\b|\brequest limit exceeded\b", visible))


def assert_valid_rankings_page(html: str) -> None:
    """Reject failure/loading pages before an empty result can exhaust a scan."""
    if is_rate_limit_page(html):
        raise RateLimitError()
    RankingsDocument(html).assert_valid()


def is_challenge_page(title: str, url: str, html: str) -> bool:
    title_l = (title or "").lower()
    url_l = (url or "").lower()
    html_l = (html or "").lower()
    if "just a moment" in title_l or "attention required" in title_l:
        return True
    if "challenges.cloudflare.com" in url_l or "/cdn-cgi/challenge-platform" in url_l:
        return True
    if (
        "challenges.cloudflare.com" in html_l
        or "cf-turnstile" in html_l
        or ("<title>just a moment" in html_l)
    ):
        return True
    return False


def is_login_page(title: str, url: str, html: str) -> bool:
    url_l = (url or "").lower()
    html_l = (html or "").lower()
    if "/login" in url_l:
        return True
    if 'action="/login"' in html_l or "name=\"password\"" in html_l and "login" in (title or "").lower():
        return True
    return False


def _parse_duration_ms(text: str) -> int:
    m = DURATION_RE.search(text or "")
    if not m:
        return 0
    mins = int(m.group(1))
    secs = int(m.group(2))
    frac = m.group(3) or "0"
    ms = int(frac.ljust(3, "0")[:3])
    return (mins * 60 + secs) * 1000 + ms


def parse_rankings_html(html: str) -> List[Dict[str, Any]]:
    if not html:
        return []

    table_match = re.search(
        r'<table\b[^>]*class="[^"]*(?:rankings-table|ranking-table|summary-table)[^"]*"[^>]*>(.*?)</table>',
        html,
        re.DOTALL | re.IGNORECASE,
    )
    search_area = table_match.group(1) if table_match else html

    results: List[Dict[str, Any]] = []
    seen = set()

    for row_html in ROW_TR_RE.findall(search_area):
        link_match = REPORT_LINK_RE.search(row_html)
        if not link_match:
            continue
        code = link_match.group(1)
        fight_id = int(link_match.group(2))
        key = (code, fight_id)
        if key in seen:
            continue
        seen.add(key)

        name_match = PLAYER_NAME_RE.search(row_html)
        player_name = name_match.group(1).strip() if name_match else "Player"

        key_level = 0
        key_match = KEYSTONE_CELL_RE.search(row_html)
        if key_match:
            val = key_match.group(1) or key_match.group(2)
            if val:
                key_level = int(val)

        dur_cell = DURATION_CELL_RE.search(row_html)
        dur_source = dur_cell.group(1) if dur_cell else row_html
        cleaned = SCRIPT_TAG_RE.sub(" ", dur_source)
        cleaned = HIDDEN_SPAN_RE.sub(" ", cleaned)
        plain_row = STRIP_TAGS_RE.sub(" ", cleaned)
        duration_ms = _parse_duration_ms(plain_row)

        results.append(
            {
                "report": {"code": code, "fightID": fight_id},
                "name": player_name,
                "keystoneLevel": key_level,
                "bracketData": key_level,
                "duration": duration_ms,
            }
        )

    if not results:
        for code, fid in REPORT_LINK_RE.findall(search_area):
            key = (code, int(fid))
            if key not in seen:
                seen.add(key)
                results.append(
                    {
                        "report": {"code": code, "fightID": int(fid)},
                        "name": "Player",
                        "keystoneLevel": 0,
                        "bracketData": 0,
                        "duration": 0,
                    }
                )

    return results


def parse_rankings_page(html: str, current_page: int = 1) -> RankingsPage:
    """Validate rendered output and retain raw row count plus paginator state."""
    if is_rate_limit_page(html):
        raise RateLimitError()
    document = RankingsDocument(html)
    document.assert_valid()
    refs = parse_rankings_html(document.tables[0].html()) if document.tables else []
    return RankingsPage(refs, row_count=document.row_count, has_next=document.has_next(current_page))


def _find_system_browser_exe() -> Optional[str]:
    candidates = [
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
        os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    for cmd in ("chrome", "google-chrome", "chromium", "msedge"):
        found = shutil.which(cmd)
        if found:
            return found
    return None


def _is_cdp_port_open(port: int) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1.0) as resp:
            return resp.status == 200
    except Exception:
        return False


class BrowserClient:
    BASE_URL = BASE_URL

    def __init__(
        self,
        profile_dir: Path,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        on_status: Optional[Callable[[str, str], None]] = None,
        should_stop: Optional[Callable[[], bool]] = None,
        cdp_port: int = DEFAULT_CDP_PORT,
        max_concurrent_requests: int = 4,
        scan_concurrency: int = 3,
        raw_archive=None,
    ):
        self.profile_dir = Path(profile_dir)
        self.min_delay = float(min_delay)
        self.max_delay = float(max_delay)
        self.on_status = on_status
        self.should_stop = should_stop
        self.cdp_port = int(cdp_port)
        self.max_concurrent_requests = max(1, int(max_concurrent_requests))
        self.scan_concurrency = max(1, int(scan_concurrency))
        self.raw_archive = raw_archive

        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None
        self._scan_tabs: List[Any] = []
        self._scan_tab_queue: Optional[asyncio.Queue[Any]] = None
        self._proc: Optional[subprocess.Popen[Any]] = None
        self._bg_loop: Optional[asyncio.AbstractEventLoop] = None
        self._bg_thread: Optional[threading.Thread] = None
        self._fight_boss_cache: Dict[tuple[str, int], int] = {}
        self._report_master_cache: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._master_cache_limit: int = 256
        self._master_inflight: Dict[str, asyncio.Task[Dict[str, Any]]] = {}
        self._req_sem: Optional[asyncio.Semaphore] = None
        self._nav_lock: Optional[asyncio.Lock] = None
        self._start_lock: Optional[asyncio.Lock] = None
        self._challenge_lock: Optional[asyncio.Lock] = None
        self._eval_cond: Optional[asyncio.Condition] = None
        self._active_evals: int = 0
        self._nav_pending: int = 0
        self._cooldown_until: float = 0.0
        self._challenge_cleared_seq: int = 0
        self.state: str = "not_started"
        self.status_message: str = "Browser not started"

    def _ensure_bg_sync(self) -> tuple[asyncio.Semaphore, asyncio.Lock, asyncio.Condition]:
        if self._req_sem is None:
            self._req_sem = asyncio.Semaphore(self.max_concurrent_requests)
        if self._nav_lock is None:
            self._nav_lock = asyncio.Lock()
        if self._start_lock is None:
            self._start_lock = asyncio.Lock()
        if self._challenge_lock is None:
            self._challenge_lock = asyncio.Lock()
        if self._eval_cond is None:
            self._eval_cond = asyncio.Condition()
        return self._req_sem, self._nav_lock, self._eval_cond

    def _cache_master(self, report_code: str, data: Dict[str, Any]) -> None:
        for f in data.get("fights", []):
            if "id" in f and "boss" in f:
                self._fight_boss_cache[(report_code, int(f["id"]))] = int(f.get("boss") or 0)
        self._report_master_cache[report_code] = data
        self._report_master_cache.move_to_end(report_code)
        while len(self._report_master_cache) > self._master_cache_limit:
            self._report_master_cache.popitem(last=False)

    @asynccontextmanager
    async def _eval_shared(self) -> AsyncIterator[None]:
        _, _, cond = self._ensure_bg_sync()
        async with cond:
            while self._nav_pending > 0:
                await cond.wait()
            self._active_evals += 1
        try:
            yield
        finally:
            async with cond:
                self._active_evals -= 1
                if self._active_evals == 0:
                    cond.notify_all()

    @asynccontextmanager
    async def _eval_exclusive(self) -> AsyncIterator[None]:
        _, nav_lock, cond = self._ensure_bg_sync()
        async with nav_lock:
            async with cond:
                self._nav_pending += 1
                while self._active_evals > 0:
                    await cond.wait()
            try:
                yield
            finally:
                async with cond:
                    self._nav_pending -= 1
                    cond.notify_all()

    def _ensure_bg_loop(self) -> asyncio.AbstractEventLoop:
        if self._bg_loop is not None and self._bg_loop.is_running():
            return self._bg_loop

        ready = threading.Event()

        def _thread_main() -> None:
            if sys.platform == "win32":
                loop = asyncio.ProactorEventLoop()
            else:
                loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._bg_loop = loop
            ready.set()
            loop.run_forever()

        self._bg_thread = threading.Thread(target=_thread_main, daemon=True)
        self._bg_thread.start()
        ready.wait()
        assert self._bg_loop is not None
        return self._bg_loop

    async def _run_in_browser_loop(self, coro: Coroutine[Any, Any, T]) -> T:
        caller_loop = asyncio.get_running_loop()
        bg_loop = self._ensure_bg_loop()
        if caller_loop is bg_loop:
            return await coro

        # A run_coroutine_threadsafe future becomes cancelled before the remote
        # task has finished its finally blocks. Keep a separate completion signal
        # that only the actual remote task's done callback can resolve.
        completed = caller_loop.create_future()
        remote_task = None
        cancellation_requested = False

        def deliver(task):
            cancelled = task.cancelled()
            error = None if cancelled else task.exception()
            result = None if cancelled or error is not None else task.result()
            def settle():
                if completed.done():
                    return
                if cancelled:
                    completed.cancel()
                elif error is not None:
                    completed.set_exception(error)
                else:
                    completed.set_result(result)
            caller_loop.call_soon_threadsafe(settle)

        def start_remote():
            nonlocal remote_task
            remote_task = bg_loop.create_task(coro)
            remote_task.add_done_callback(deliver)
            if cancellation_requested:
                remote_task.cancel()

        def cancel_remote():
            nonlocal cancellation_requested
            if not cancellation_requested:
                cancellation_requested = True
                if remote_task is not None:
                    remote_task.cancel()

        try:
            bg_loop.call_soon_threadsafe(start_remote)
        except BaseException:
            coro.close()
            raise
        try:
            return await asyncio.shield(completed)
        except asyncio.CancelledError:
            bg_loop.call_soon_threadsafe(cancel_remote)
            # Repeated caller cancellation must not interrupt remote cleanup or
            # inject a second cancellation into its finally blocks.
            while not completed.done():
                try:
                    await asyncio.shield(completed)
                except asyncio.CancelledError:
                    continue
                except BaseException:
                    break
            if not completed.cancelled():
                completed.exception()
            raise

    def _emit_status(self, state: str, message: str) -> None:
        self.state = state
        self.status_message = message
        if self.on_status:
            self.on_status(state, message)

    async def start(self) -> None:
        await self._run_in_browser_loop(self._start_impl())

    async def _start_impl(self) -> None:
        if self._context is not None and self._page is not None:
            try:
                if not self._page.is_closed():
                    return
            except Exception:
                pass

        from playwright.async_api import async_playwright

        self.profile_dir.mkdir(parents=True, exist_ok=True)
        exe = _find_system_browser_exe()

        if not _is_cdp_port_open(self.cdp_port) and exe:
            self._proc = subprocess.Popen(
                [
                    exe,
                    f"--remote-debugging-port={self.cdp_port}",
                    f"--user-data-dir={self.profile_dir.resolve()}",
                    "--no-first-run",
                    "--no-default-browser-check",
                    self.BASE_URL,
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            for _ in range(30):
                if _is_cdp_port_open(self.cdp_port):
                    break
                await asyncio.sleep(0.25)

        self._playwright = await async_playwright().start()
        if _is_cdp_port_open(self.cdp_port):
            self._browser = await self._playwright.chromium.connect_over_cdp(
                f"http://127.0.0.1:{self.cdp_port}"
            )
            contexts = self._browser.contexts
            self._context = contexts[0] if contexts else await self._browser.new_context()
        else:
            self._context = await self._playwright.chromium.launch_persistent_context(
                user_data_dir=str(self.profile_dir),
                headless=False,
            )

        pages = self._context.pages
        self._page = pages[0] if pages else await self._context.new_page()
        self._emit_status("ready", "Browser window open and ready")
        try:
            if self._page.url in ("about:blank", ""):
                await self._page.goto(self.BASE_URL, wait_until="domcontentloaded")
            title, url, html = await self._read_page_snapshot()
            if is_challenge_page(title, url, html):
                self._emit_status("waiting_challenge", "Solve the challenge in the browser window")
            elif is_login_page(title, url, html):
                self._emit_status("waiting_login", "Please log in in the browser window")
            else:
                self._emit_status("ready", "Browser window open and ready")
        except Exception:
            pass

    async def stop(self) -> None:
        if self._bg_loop is not None and self._bg_loop.is_running():
            await self._run_in_browser_loop(self._stop_impl())
        else:
            self._emit_status("not_started", "Browser window closed")

    async def _stop_impl(self) -> None:
        try:
            for tab in self._scan_tabs:
                try:
                    if tab is not self._page and not tab.is_closed():
                        await tab.close()
                except Exception:
                    pass
            if self._browser is not None:
                await self._browser.close()
            elif self._context is not None:
                await self._context.close()
        except Exception:
            pass
        finally:
            self._browser = None
            self._context = None
            self._page = None
            self._scan_tabs = []
            self._scan_tab_queue = None
            if self._playwright is not None:
                try:
                    await self._playwright.stop()
                except Exception:
                    pass
                finally:
                    self._playwright = None
            if self._proc is not None:
                try:
                    self._proc.terminate()
                except Exception:
                    pass
                finally:
                    self._proc = None
            self._req_sem = None
            self._nav_lock = None
            self._start_lock = None
            self._challenge_lock = None
            self._eval_cond = None
            self._active_evals = 0
            self._nav_pending = 0
            self._cooldown_until = 0.0
            self._master_inflight.clear()
            self._emit_status("not_started", "Browser window closed")

    async def _ensure_started(self) -> None:
        self._ensure_bg_sync()
        assert self._start_lock is not None
        if self._context is None or self._page is None or self._page.is_closed():
            async with self._start_lock:
                if self._context is None or self._page is None or self._page.is_closed():
                    await self._start_impl()

    async def _ensure_scan_tab_pool(self) -> Optional[asyncio.Queue[Any]]:
        await self._ensure_started()
        if (
            self.scan_concurrency <= 1
            or self._context is None
            or not hasattr(self._context, "new_page")
        ):
            return None
        self._ensure_bg_sync()
        assert self._start_lock is not None
        if self._scan_tab_queue is not None:
            return self._scan_tab_queue
        async with self._start_lock:
            if self._scan_tab_queue is not None:
                return self._scan_tab_queue
            q: asyncio.Queue[Any] = asyncio.Queue()
            existing = [p for p in getattr(self._context, "pages", []) if p is not self._page]
            tabs: List[Any] = []
            for p in existing:
                try:
                    if not p.is_closed():
                        tabs.append(p)
                except Exception:
                    pass
                if len(tabs) >= self.scan_concurrency:
                    break
            while len(tabs) < self.scan_concurrency:
                tab = await self._context.new_page()
                tabs.append(tab)
            self._scan_tabs = tabs
            for tab in tabs:
                q.put_nowait(tab)
            self._scan_tab_queue = q
            return q

    async def _read_page_snapshot(self, page: Any = None) -> tuple[str, str, str]:
        target = page if page is not None else self._page
        if target is None:
            return "", "", ""
        try:
            title = await target.title()
        except Exception:
            title = ""
        try:
            url = target.url
        except Exception:
            url = ""
        try:
            html = await target.content()
        except Exception:
            html = ""
        return title, url, html

    async def _wait_if_challenge(self, page: Any = None) -> None:
        target = page if page is not None else self._page
        title, url, html = await self._read_page_snapshot(target)
        if not is_challenge_page(title, url, html):
            return
        self._ensure_bg_sync()
        assert self._challenge_lock is not None
        async with self._challenge_lock:
            try:
                if target is not None and hasattr(target, "bring_to_front"):
                    await target.bring_to_front()
            except Exception:
                pass
            while True:
                if self.should_stop and self.should_stop():
                    return
                title, url, html = await self._read_page_snapshot(target)
                if not is_challenge_page(title, url, html):
                    break
                self._emit_status("waiting_challenge", "Solve the challenge in the browser window")
                await asyncio.sleep(2.0)
            if self.state == "waiting_challenge":
                self._emit_status("ready", "Challenge cleared, browser ready")

    async def _wait_if_logged_out(self, page: Any = None) -> None:
        target = page if page is not None else self._page
        title, url, html = await self._read_page_snapshot(target)
        if not is_login_page(title, url, html):
            return
        self._ensure_bg_sync()
        assert self._challenge_lock is not None
        async with self._challenge_lock:
            try:
                if target is not None and hasattr(target, "bring_to_front"):
                    await target.bring_to_front()
            except Exception:
                pass
            while True:
                if self.should_stop and self.should_stop():
                    return
                title, url, html = await self._read_page_snapshot(target)
                if not is_login_page(title, url, html):
                    break
                self._emit_status("waiting_login", "Please log in in the browser window")
                await asyncio.sleep(2.0)
            if self.state == "waiting_login":
                self._emit_status("ready", "Logged in, browser ready")

    async def _wait_for_cooldown(self) -> None:
        while self._cooldown_until > time.monotonic():
            if self.should_stop and self.should_stop():
                raise RuntimeError("Stopped by user")
            # Recheck after sleeping: another worker may extend the deadline.
            await asyncio.sleep(self._cooldown_until - time.monotonic())

    def _record_rate_limit(self, retry_after: str = "") -> float:
        self._rate_limit_streak = getattr(self, "_rate_limit_streak", 0) + 1
        delay = min(120.0, 2.0 * 2 ** min(self._rate_limit_streak - 1, 6))
        if retry_after:
            try:
                parsed_delay = float(retry_after)
            except (ValueError, TypeError):
                try:
                    parsed_delay = parsedate_to_datetime(retry_after).timestamp() - time.time()
                except (ValueError, TypeError, OverflowError):
                    parsed_delay = delay
            if math.isfinite(parsed_delay):
                # Honor seconds or HTTP dates, bounded to 50 ms..5 minutes.
                delay = max(0.05, min(300.0, parsed_delay))
        self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
        self._emit_status("rate_limited", f"WCL rate limit; pausing requests for {delay:.2f}s")
        return delay

    async def _capture_and_validate_rankings(self, page, url, current_page=1):
        _, _, html = await self._read_page_snapshot(page)
        if self.raw_archive is not None:
            self.raw_archive.capture('GET', url, html.encode('utf-8'), source='rendered_html')
        try:
            refs = parse_rankings_page(html, current_page)
        except RateLimitError as exc:
            exc.retry_after = self._record_rate_limit()
            raise
        self._rate_limit_streak = 0
        return refs

    async def _goto_page_resilient(
        self,
        page: Any,
        url: str,
        *,
        timeout_ms: int = 45000,
        attempts: int = 3,
        replace_scan_tab: bool = False,
    ) -> Any:
        current = page
        last_exc: Optional[Exception] = None
        for attempt in range(max(1, attempts)):
            if self.should_stop and self.should_stop():
                raise RuntimeError("Stopped by user")
            await self._wait_for_cooldown()
            try:
                try:
                    response = await current.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                except TypeError:
                    response = await current.goto(url, wait_until="domcontentloaded")
                status = int(getattr(response, "status", 200)) if response is not None else 200
                if status >= 400:
                    if self.raw_archive is not None:
                        _, _, html = await self._read_page_snapshot(current)
                        self.raw_archive.capture('GET', url, html.encode('utf-8'), status=status, source='rendered_html')
                    if status == 429:
                        headers = getattr(response, "headers", {}) or {}
                        retry_after = next((value for key, value in headers.items() if key.lower() == 'retry-after'), '')
                        delay = self._record_rate_limit(retry_after)
                        raise RateLimitError(f"WCL navigation HTTP 429: {url}", delay)
                    raise NavigationHTTPError(f"WCL navigation HTTP {status}: {url}")
                return current
            except NavigationHTTPError:
                raise
            except RateLimitError as exc:
                last_exc = exc
                if attempt >= attempts - 1:
                    break
                # Reuse the same tab/URL; shared cooldown gates every worker.
                continue
            except Exception as exc:
                last_exc = exc
                if self.should_stop and self.should_stop():
                    raise
                if attempt >= attempts - 1:
                    break
                if replace_scan_tab and self._context is not None and hasattr(self._context, "new_page"):
                    try:
                        if not current.is_closed():
                            await current.close()
                    except Exception:
                        pass
                    try:
                        new_tab = await self._context.new_page()
                        self._scan_tabs = [new_tab if t is current else t for t in self._scan_tabs]
                        current = new_tab
                    except Exception:
                        pass
                await asyncio.sleep(min(10.0, 1.5 * (2 ** attempt)))
        if last_exc is not None:
            raise last_exc
        return current

    async def _goto_unlocked(self, url: str) -> None:
        await self._ensure_started()
        await self._goto_page_resilient(self._page, url, timeout_ms=45000, attempts=3)
        await self._wait_if_challenge()
        await self._wait_if_logged_out()
        self._emit_status("ready", f"Loaded {url}")
        if self.max_delay > 0:
            delay = random.uniform(self.min_delay, max(self.min_delay, self.max_delay))
            if delay > 0:
                await asyncio.sleep(delay)

    async def _goto(self, url: str) -> None:
        async with self._eval_exclusive():
            await self._goto_unlocked(url)

    async def fetch_rankings_fight_refs(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str = "Any",
        spec_name: str = "Any",
        page: int = 1,
        difficulty: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        return await self._run_in_browser_loop(
            self._fetch_rankings_fight_refs_impl(
                zone_id=zone_id,
                encounter_id=encounter_id,
                partition=partition,
                class_name=class_name,
                spec_name=spec_name,
                page=page,
                difficulty=difficulty,
            )
        )

    async def _fetch_rankings_fight_refs_impl(
        self,
        zone_id: int,
        encounter_id: int,
        partition: int,
        class_name: str = "Any",
        spec_name: str = "Any",
        page: int = 1,
        difficulty: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        await self._ensure_started()
        url = RANKINGS_URL_TEMPLATE.format(
            zone=zone_id,
            encounter=encounter_id,
            class_name=class_name,
            spec_name=spec_name,
            partition=partition,
            page=page,
        )
        if difficulty and int(difficulty) > 0:
            url += f"&difficulty={int(difficulty)}"

        tab_queue = await self._ensure_scan_tab_pool()
        if tab_queue is not None:
            tab = await tab_queue.get()
            try:
                try:
                    if tab.is_closed():
                        new_tab = await self._context.new_page()
                        self._scan_tabs = [new_tab if t is tab else t for t in self._scan_tabs]
                        tab = new_tab
                except Exception:
                    pass
                tab = await self._goto_page_resilient(
                    tab, url, timeout_ms=45000, attempts=3, replace_scan_tab=True
                )
                await self._wait_if_challenge(tab)
                await self._wait_if_logged_out(tab)
                try:
                    await tab.wait_for_function(
                        """() => {
                            const loader = document.getElementById('graph-loading');
                            const tc = document.getElementById('table-container');
                            const loadingHidden = !loader || loader.style.display === 'none';
                            const hasContent = tc && tc.innerHTML.trim().length > 20;
                            return loadingHidden && hasContent;
                        }""",
                        timeout=12000,
                    )
                except Exception:
                    await self._wait_if_challenge(tab)
                    await self._wait_if_logged_out(tab)
                refs = await self._capture_and_validate_rankings(tab, url, page)
                if self.max_delay > 0:
                    delay = random.uniform(self.min_delay, max(self.min_delay, self.max_delay))
                    if delay > 0:
                        await asyncio.sleep(delay)
                return refs
            finally:
                tab_queue.put_nowait(tab)

        async with self._eval_exclusive():
            await self._goto_unlocked(url)
            try:
                await self._page.wait_for_function(
                    """() => {
                        const loader = document.getElementById('graph-loading');
                        const tc = document.getElementById('table-container');
                        const loadingHidden = !loader || loader.style.display === 'none';
                        const hasContent = tc && tc.innerHTML.trim().length > 20;
                        return loadingHidden && hasContent;
                    }""",
                    timeout=12000,
                )
            except Exception:
                await self._wait_if_challenge()
                await self._wait_if_logged_out()
            refs = await self._capture_and_validate_rankings(self._page, url, page)
        return refs

    async def _ensure_on_wcl_origin(self) -> None:
        await self._ensure_started()
        cur_url = self._page.url or ""
        if cur_url.startswith(self.BASE_URL):
            return
        async with self._eval_exclusive():
            await self._ensure_started()
            cur_url = self._page.url or ""
            if not cur_url.startswith(self.BASE_URL):
                await self._goto_page_resilient(self._page, self.BASE_URL, timeout_ms=45000, attempts=3)
                await self._wait_if_challenge()
                await self._wait_if_logged_out()

    async def _request_json(self, method: str, path: str) -> tuple[int, Any]:
        return await self._run_in_browser_loop(self._request_json_impl(method, path))

    async def _request_json_impl(self, method: str, path: str) -> tuple[int, Any]:
        req_sem, _, _ = self._ensure_bg_sync()
        rel_path = path if path.startswith("/") else f"/{path}"
        full_url = path if path.startswith("http") else f"{self.BASE_URL}{rel_path}"

        challenge_attempts = 0
        rate_attempts = 0
        for _ in range(7):
            await self._wait_for_cooldown()

            await self._ensure_on_wcl_origin()
            seq_before = self._challenge_cleared_seq
            retry_after_raw = ""
            try:
                async with self._eval_shared():
                    async with req_sem:
                        await self._wait_for_cooldown()
                        res = await self._page.evaluate(
                            """async ({url, method}) => {
                                const headers = {'X-Requested-With': 'XMLHttpRequest'};
                                const meta = document.querySelector('meta[name="csrf-token"]');
                                if (meta && meta.content) {
                                    headers['X-CSRF-TOKEN'] = meta.content;
                                }
                                const r = await fetch(url, {
                                    method,
                                    headers,
                                    credentials: 'same-origin'
                                });
                                const text = await r.text();
                                const retryAfter = r.headers ? (r.headers.get('Retry-After') || '') : '';
                                return {status: r.status, text, retryAfter};
                            }""",
                            {"url": rel_path, "method": method.upper()},
                        )
                        status = int(res.get("status", 0))
                        text = str(res.get("text", ""))
                        retry_after_raw = str(res.get("retryAfter", "") or "")
            except Exception as exc:
                status = 0
                text = str(exc)

            if self.raw_archive is not None and status:
                self.raw_archive.capture(method,full_url,text.encode('utf-8'),status=status,
                                         source='browser_fetch_text')

            if status == 429:
                rate_attempts += 1
                wait_s = self._record_rate_limit(retry_after_raw)
                if rate_attempts > 4:
                    raise RateLimitError(f"WCL request HTTP 429: {full_url}", wait_s)
                continue

            if status in (0, 401, 403) or is_challenge_page("", full_url, text):
                challenge_attempts += 1
                if challenge_attempts > 3:
                    return 403, text
                async with self._eval_exclusive():
                    if self._challenge_cleared_seq == seq_before:
                        await self._ensure_started()
                        await self._goto_page_resilient(self._page, full_url, timeout_ms=45000, attempts=3)
                        await self._wait_if_challenge()
                        await self._wait_if_logged_out()
                        self._challenge_cleared_seq += 1
                continue

            if status == 200:
                try:
                    return status, json.loads(text)
                except (ValueError, TypeError) as exc:
                    raise RuntimeError('Invalid JSON response from WCL') from exc
            return status, text
        return 403, {}

    async def unarchive_report(self, report_code: str) -> bool:
        return await self._run_in_browser_loop(self._unarchive_report_impl(report_code))

    async def _unarchive_report_impl(self, report_code: str) -> bool:
        has_source = getattr(self.raw_archive,'has_report_metadata',None)
        if report_code in self._report_master_cache and (not callable(has_source) or has_source(report_code)):
            return True

        status, data = await self._request_json_impl(
            "GET", f"/reports/fights-and-participants/{report_code}/0?lang=en&"
        )
        if status == 200 and isinstance(data, dict) and data.get("fights"):
            self._cache_master(report_code, data)
            return True

        async with self._eval_exclusive():
            await self._goto_unlocked(f"{self.BASE_URL}/reports/{report_code}")
            try:
                unarch_btn = await self._page.query_selector(
                    "button:has-text('Unarchive'), a:has-text('Unarchive'), input[value*='Unarchive']"
                )
                if unarch_btn:
                    await unarch_btn.click()
                    await asyncio.sleep(3.0)
            except Exception:
                pass

        status2, data2 = await self._request_json_impl(
            "GET", f"/reports/fights-and-participants/{report_code}/0?lang=en&"
        )
        if status2 == 200 and isinstance(data2, dict) and data2.get("fights"):
            self._cache_master(report_code, data2)
            return True
        return False

    async def fetch_report_master_and_fights(self, report_code: str) -> Dict[str, Any]:
        return await self._run_in_browser_loop(
            self._fetch_report_master_and_fights_impl(report_code)
        )

    async def _fetch_report_master_and_fights_impl(self, report_code: str) -> Dict[str, Any]:
        cached = self._report_master_cache.get(report_code)
        has_source = getattr(self.raw_archive,'has_report_metadata',None)
        if cached is not None and (not callable(has_source) or has_source(report_code)):
            self._report_master_cache.move_to_end(report_code)
            return cached

        inflight = self._master_inflight.get(report_code)
        if inflight is None:
            async def _fetch_one() -> Dict[str, Any]:
                status, data = await self._request_json_impl(
                    "GET", f"/reports/fights-and-participants/{report_code}/0?lang=en&"
                )
                if status != 200 or not isinstance(data, dict):
                    raise RuntimeError(
                        f"Failed to fetch report master for {report_code} (HTTP {status})"
                    )
                self._cache_master(report_code, data)
                return data

            inflight = asyncio.create_task(_fetch_one())
            self._master_inflight[report_code] = inflight
            try:
                return await inflight
            finally:
                if self._master_inflight.get(report_code) is inflight:
                    self._master_inflight.pop(report_code, None)
        return await inflight

    async def fetch_all_fight_events(
        self,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        return await self._run_in_browser_loop(
            self._fetch_with_source_archive(report_code, fight_id, start_time, end_time)
        )

    async def _fetch_with_source_archive(self, report_code, fight_id, start_time, end_time):
        if self.raw_archive is None:
            return await self._fetch_all_fight_events_impl(report_code,fight_id,start_time,end_time)
        with self.raw_archive.fight_capture(report_code,fight_id) as fetch_id:
            events = await self._fetch_all_fight_events_impl(report_code,fight_id,start_time,end_time)
            if self.should_stop and self.should_stop():
                raise asyncio.CancelledError()
            self.raw_archive.complete_fight(report_code,fight_id,fetch_id,start_time,end_time,len(events))
            return events

    async def _fetch_all_fight_events_impl(
        self,
        report_code: str,
        fight_id: int,
        start_time: int,
        end_time: int,
    ) -> List[Dict[str, Any]]:
        boss_id = self._fight_boss_cache.get((report_code, int(fight_id)), 0)
        if boss_id > 0:
            step_ms = 240000
            chunks: List[tuple[int, int]] = []
            cur = int(start_time)
            while cur <= int(end_time):
                seg_end = min(cur + step_ms - 1, int(end_time))
                chunks.append((cur, seg_end))
                cur = seg_end + 1

            async def _fetch_seg(c_start: int, c_end: int) -> Optional[List[Dict[str, Any]]]:
                if self.should_stop and self.should_stop():
                    return []
                path = f"/reports/replaysegment/{report_code}/{boss_id}/{c_start}/{c_end}/"
                status, data = await self._request_json_impl("GET", path)
                if status != 200 or not isinstance(data, dict) or "events" not in data:
                    return None
                return list(data.get("events", []))

            if chunks:
                seg_results = await asyncio.gather(*[_fetch_seg(c0, c1) for c0, c1 in chunks])
                if self.should_stop and self.should_stop():
                    return []
                if self.raw_archive is not None and any(sr is None for sr in seg_results):
                    raise RuntimeError('Incomplete replay segment download; retry this fight')
                if all(sr is not None for sr in seg_results):
                    replay_events: List[Dict[str, Any]] = []
                    for sr in seg_results:
                        if sr:
                            replay_events.extend(sr)
                    if replay_events and self.raw_archive is None:
                        return replay_events

        all_events: List[Dict[str, Any]] = []
        cursor: Optional[int] = int(start_time)

        while cursor is not None and cursor <= end_time:
            if self.should_stop and self.should_stop():
                raise asyncio.CancelledError()
            path = (
                f"/reports/summary-events/{report_code}/{fight_id}/{cursor}/{end_time}"
                f"/0/0/Any/0/-1.0.-1.-1/0"
            )
            status, data = await self._request_json_impl("GET", path)
            if status != 200 or not isinstance(data, dict):
                raise RuntimeError(
                    f"Failed to fetch fight events for {report_code}#{fight_id} (HTTP {status})"
                )
            batch = data.get("events", [])
            if not isinstance(data.get('events'),list):
                raise RuntimeError('Invalid canonical event page')
            all_events.extend(batch)
            next_ts = data.get("nextPageTimestamp")
            if next_ts is not None:
                if int(next_ts) <= cursor:
                    raise RuntimeError('Event pagination cursor did not advance')
                cursor = int(next_ts)
            else:
                cursor = None

        return all_events
