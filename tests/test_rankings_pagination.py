from pathlib import Path

import pytest

import wcl_archiver.wcl_client.browser_client as browser_module
from wcl_archiver.wcl_client.browser_client import BrowserClient, RateLimitError, assert_valid_rankings_page, parse_rankings_html


ACTIVE_NEXT = '<div id="pagination-hook"><ul class="pagination"><li class="page-item"><a class="page-link" rel="next" href="?page=2">Next »</a></li></ul></div>'
DISABLED_NEXT = '<ul class="pagination"><li class="page-item disabled" aria-disabled="true"><span class="page-link">Next »</span></li></ul>'


def document(count, pager=ACTIVE_NEXT, distinct=None):
    rows = []
    for index in range(count):
        fight = index + 1 if distinct is None else index % distinct + 1
        rows.append(f'<tr><td class="main-table-name"><a class="main-table-player Monk">Player</a><a href="/reports/abcdefghij#fight={fight}">Report</a></td></tr>')
    return '<table class="rankings-table"><tbody>' + ''.join(rows) + '</tbody></table>' + pager


def parse(html, current_page=1):
    parser = getattr(browser_module, "parse_rankings_page", None)
    assert callable(parser), "browser results must include row and paginator metadata"
    return parser(html, current_page)


@pytest.mark.parametrize("count, next_page", [(0, False), (69, False), (70, True), (71, True)])
def test_row_threshold_and_enabled_next(count, next_page):
    page = parse(document(count))
    assert isinstance(page, list)
    assert len(page) == count
    assert page.row_count == count
    assert page.has_next is next_page


@pytest.mark.parametrize("pager", ['', DISABLED_NEXT,
    '<a rel="next" href="?page=2">Next</a>',
    '<ul class="pagination"><li aria-disabled="true"><a href="?page=2"><span>Next</span> »</a></li></ul>',
    '<nav aria-label="Pagination"><button disabled>Next</button></nav>'])
def test_absent_or_disabled_real_paginator_ends_page(pager):
    page = parse(document(70, pager))
    assert page.row_count == 70
    assert page.has_next is False


@pytest.mark.parametrize("pager", [
    '<nav aria-label="Pagination"><ul><li><a href="?page=2"><span>Next</span><b> »</b></a></li></ul></nav>',
    '<ul class="pagination"><a href="?page=2"><span>Ne</span>xt »</a></ul>',
    '<ul class="pagination"><li><a href="?page=2">2</a></li></ul>',
    '<div id="pagination-hook"><a rel="next" href="?page=2" aria-label="Next page"><span>›</span></a></div>'])
def test_nested_next_or_later_page_link_is_enabled(pager):
    assert parse(document(70, pager)).has_next is True


def test_disabled_next_wins_over_other_future_page_links():
    pager = '<ul class="pagination"><li><a href="?page=5">5</a></li><li class="disabled"><span>Next »</span></li></ul>'
    assert parse(document(70, pager)).has_next is False


def test_disabled_next_label_split_across_markup_still_ends_page():
    pager = '<ul class="pagination"><a href="?page=5">5</a><li class="disabled"><span><b>Ne</b>xt »</span></li></ul>'
    assert parse(document(70, pager)).has_next is False


def test_duplicate_rows_count_before_fight_deduplication():
    page = parse(document(70, distinct=2))
    assert len(page) == 2
    assert page.row_count == 70
    assert page.has_next is True


def test_current_page_numbers_and_previous_links_do_not_advance():
    pager = '<ul class="pagination"><li><a rel="prev" href="?page=1">Previous</a></li><li class="active"><a href="?page=2">2</a></li></ul>'
    assert parse(document(70, pager), current_page=2).has_next is False


def test_hidden_rows_and_unrelated_report_links_do_not_count():
    html = document(69).replace('</tbody>', '<tr style="display:none"><td><a href="/reports/abcdefghij#fight=70">Hidden</a></td></tr></tbody>')
    html += '<footer><a href="/reports/abcdefghij#fight=71">Unrelated</a></footer>'
    page = parse(html)
    assert page.row_count == 69
    assert len(page) == 69
    assert page.has_next is False


def test_live_private_fights_empty_page_is_valid_and_terminal():
    html = (Path(__file__).parent / 'fixtures/rankings_private_empty.html').read_text()
    assert_valid_rankings_page(html)
    page = parse(html, current_page=2)
    assert page == []
    assert page.row_count == 0
    assert page.has_next is False


@pytest.mark.parametrize("message", ['No logs found.', 'No rankings available.', 'No results found.'])
def test_explicit_empty_markers_are_terminal(message):
    assert parse(f'<div id="table-container">{message}</div>') == []


@pytest.mark.parametrize("html, error", [
    ('<div id="table-container">429 Too Many Requests</div><div>No logs found.</div>', RateLimitError),
    ('<div id="table-container">Loading...</div>', RuntimeError),
    ('<form action="/login"><input type="password"></form><div>No logs found.</div>', RuntimeError),
    ('<h1>Login required</h1><div>No logs found.</div>', RuntimeError),
    ('<h1>Server Error</h1><div>No logs found.</div>', RuntimeError),
    ('<div id="graph-loading">Loading...</div>' + document(0), RuntimeError)])
def test_error_login_and_loading_pages_never_certify_empty(html, error):
    with pytest.raises(error):
        parse(html)


def test_translation_markers_in_script_do_not_certify_an_empty_page():
    with pytest.raises(RuntimeError):
        parse('<script>const t="No logs found.";</script><div>Loading...</div>')


async def test_browser_fetch_returns_list_compatible_page_metadata(tmp_path):
    html = (Path(__file__).parent / 'fixtures/rankings_private_empty.html').read_text()
    class Page:
        url = BrowserClient.BASE_URL
        def is_closed(self): return False
        async def goto(self, *args, **kwargs): return None
        async def title(self): return "Rankings"
        async def content(self): return html
        async def wait_for_function(self, *args, **kwargs): return None
    client = BrowserClient(tmp_path, min_delay=0, max_delay=0)
    client._context, client._page = object(), Page()
    page = await client._fetch_rankings_fight_refs_impl(9, 11492, 5, page=2)
    assert page == []
    assert page.row_count == 0
    assert page.has_next is False
    assert type(parse_rankings_html(document(2))) is list
