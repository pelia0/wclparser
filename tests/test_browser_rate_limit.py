import asyncio
from email.utils import formatdate
import time
from types import SimpleNamespace

import pytest

import wcl_archiver.wcl_client.browser_client as browser_module
from wcl_archiver.wcl_client.browser_client import BrowserClient


VALID_EMPTY = '<table class="rankings-table"><tbody></tbody></table>'
RATE_HTML = '<html><script>const translations={"rate":"Too Many Requests"}</script><div id="table-container"><h1>429 Too Many Requests</h1></div></html>'


class Page:
    def __init__(self, html=VALID_EMPTY, statuses=None, timeout=False):
        self.html = html
        self.statuses = list(statuses or [])
        self.timeout = timeout
        self.url = BrowserClient.BASE_URL
        self.visits = []
        self.requests = []

    def is_closed(self):
        return False

    async def goto(self, url, **kwargs):
        self.url = url
        self.visits.append((url, time.monotonic()))
        if not self.statuses:
            return None
        status, retry_after = self.statuses.pop(0)
        return SimpleNamespace(status=status, headers={"retry-after": retry_after})

    async def title(self):
        return "Rankings"

    async def content(self):
        return self.html

    async def wait_for_function(self, *args, **kwargs):
        if self.timeout:
            raise TimeoutError("ranking table did not load")

    async def evaluate(self, *args, **kwargs):
        self.requests.append(time.monotonic())
        return {"status": 200, "text": '{"events":[]}', "retryAfter": ""}


def client_for(tmp_path, page, **kwargs):
    client = BrowserClient(tmp_path / "profile", min_delay=0, max_delay=0, **kwargs)
    client._page = page
    client._context = object()
    return client


def test_rate_limit_guard_detects_visible_banner_but_ignores_translation_scripts():
    guard = getattr(browser_module, "assert_valid_rankings_page", None)
    assert callable(guard), "rankings pages require an explicit validity guard"
    error = browser_module.RateLimitError
    with pytest.raises(error, match="rate|429|Too Many",):
        guard(RATE_HTML)
    with pytest.raises(error):
        guard('<div class="alert">You have exceeded the rate limit. Please try again later.</div>' + VALID_EMPTY)
    guard('<script>const t="429 Too Many Requests"</script><style>.rate{content:"rate limit exceeded"}</style>' + VALID_EMPTY)
    with pytest.raises(RuntimeError):
        guard('<div id="table-container">Loading...</div>')
    guard(VALID_EMPTY)
    guard('<div class="no-data">No rankings found.</div>')


def test_retry_after_http_date_and_default_backoff_are_bounded(tmp_path):
    client = client_for(tmp_path, Page())
    assert client._record_rate_limit() == 2
    assert client._record_rate_limit() == 4
    assert client._record_rate_limit("invalid") == 8
    assert 8 <= client._record_rate_limit(formatdate(time.time() + 10, usegmt=True)) <= 10
    assert client._record_rate_limit("9999999") == 300
    assert client._record_rate_limit("nan") <= 120


async def test_navigation_429_retries_same_url_after_shared_retry_after(tmp_path):
    page = Page(statuses=[(429, "0.04"), (200, "")])
    client = client_for(tmp_path, page)
    assert await client._goto_page_resilient(page, "https://www.warcraftlogs.com/zone/rankings/9") is page
    assert len(page.visits) == 2, "HTTP 429 must not look like a successful navigation"
    assert page.visits[0][0] == page.visits[1][0]
    assert page.visits[1][1] - page.visits[0][1] >= 0.035
    assert client._cooldown_until >= page.visits[0][1] + 0.035


async def test_exhausted_navigation_429_raises_and_preserves_failed_page(tmp_path):
    page = Page(RATE_HTML, statuses=[(429, "0.01"), (429, "0.01")])
    client = client_for(tmp_path, page)
    captures = []
    client.raw_archive = SimpleNamespace(capture=lambda *args, **kwargs: captures.append((args, kwargs)))
    with pytest.raises(RuntimeError, match="429|rate|Too Many"):
        await client._goto_page_resilient(page, "https://www.warcraftlogs.com/zone/rankings/9", attempts=2)
    assert len(page.visits) == 2
    assert captures[-1][0][2] == RATE_HTML.encode()
    assert captures[-1][1]["status"] == 429
    assert client._cooldown_until > time.monotonic()


@pytest.mark.parametrize("status", [404, 500, 503])
async def test_http_navigation_errors_raise_instead_of_marking_empty(tmp_path, status):
    page = Page(statuses=[(status, "")])
    client = client_for(tmp_path, page)
    with pytest.raises(RuntimeError, match=str(status)):
        await client._goto_page_resilient(page, "https://www.warcraftlogs.com/zone/rankings/9", attempts=1)


@pytest.mark.parametrize("scan_pool", [False, True])
async def test_ajax_429_html_captured_then_raises_with_cooldown(tmp_path, scan_pool):
    page = Page(RATE_HTML)
    client = client_for(tmp_path, page)
    if scan_pool:
        queue = asyncio.Queue()
        queue.put_nowait(page)
        async def pool():
            return queue
        client._ensure_scan_tab_pool = pool
    captures = []
    client.raw_archive = SimpleNamespace(capture=lambda *args, **kwargs: captures.append((args, kwargs)))
    with pytest.raises(RuntimeError, match="429|rate|Too Many"):
        await client._fetch_rankings_fight_refs_impl(9, 11492, 5)
    assert captures[-1][0][2] == RATE_HTML.encode()
    assert client._cooldown_until > time.monotonic()


@pytest.mark.parametrize("html, valid", [('<div id="table-container">Loading...</div>', False), (VALID_EMPTY, True)])
async def test_loading_timeout_only_accepts_explicitly_valid_empty_page(tmp_path, html, valid):
    page = Page(html, timeout=True)
    client = client_for(tmp_path, page)
    if valid:
        assert await client._fetch_rankings_fight_refs_impl(9, 11492, 5) == []
    else:
        with pytest.raises(RuntimeError, match="rank|load|table"):
            await client._fetch_rankings_fight_refs_impl(9, 11492, 5)


async def test_navigation_workers_and_json_requests_honor_shared_cooldown(tmp_path):
    main, tab1, tab2 = Page(), Page(), Page()
    client = client_for(tmp_path, main)
    deadline = time.monotonic() + 0.05
    client._cooldown_until = deadline
    await asyncio.gather(client._goto_page_resilient(tab1, main.url),
                         client._goto_page_resilient(tab2, main.url),
                         client._request_json_impl("GET", "/reports/events"))
    assert tab1.visits[0][1] >= deadline
    assert tab2.visits[0][1] >= deadline
    assert main.requests[0] >= deadline


async def test_cooldown_extension_delays_navigation_already_waiting(tmp_path):
    page = Page()
    client = client_for(tmp_path, page)
    client._cooldown_until = time.monotonic() + 0.03
    task = asyncio.create_task(client._goto_page_resilient(page, page.url))
    await asyncio.sleep(0.01)
    deadline = time.monotonic() + 0.05
    client._cooldown_until = deadline
    await task
    assert page.visits[0][1] >= deadline


async def test_json_request_queued_for_semaphore_rechecks_new_cooldown(tmp_path):
    entered, release = asyncio.Event(), asyncio.Event()
    class BlockingPage(Page):
        async def evaluate(self, *args, **kwargs):
            if not entered.is_set():
                entered.set()
                await release.wait()
            return await super().evaluate(*args, **kwargs)
    page = BlockingPage()
    client = client_for(tmp_path, page, max_concurrent_requests=1)
    first = asyncio.create_task(client._request_json_impl("GET", "/first"))
    await entered.wait()
    second = asyncio.create_task(client._request_json_impl("GET", "/second"))
    await asyncio.sleep(0)
    client._record_rate_limit("0.05")
    deadline = client._cooldown_until
    release.set()
    await asyncio.gather(first, second)
    assert page.requests[1] >= deadline
