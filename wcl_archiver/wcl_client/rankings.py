"""Visible ranking rows and scoped pagination from rendered browser HTML."""

from html import escape
from html.parser import HTMLParser
import re
from urllib.parse import parse_qs, urlparse


REPORT_LINK_RE = re.compile(r"/reports/([A-Za-z0-9]{10,20})(?:#|\?[^\"']*?)fight=(\d+)")
TABLE_CLASSES = {"rankings-table", "ranking-table", "summary-table"}
PRIVATE_EMPTY = "All of the recorded fights for this boss, difficulty, size and region are private."


class RankingsPage(list):
    """Fight references plus page evidence, independent of fight deduplication."""

    def __init__(self, refs=(), *, row_count=0, has_next=False):
        super().__init__(refs)
        self.row_count = int(row_count)
        self.has_next = bool(has_next)


class _Node:
    def __init__(self, tag="", attrs=(), parent=None):
        self.tag = tag
        self.attrs = dict(attrs)
        self.parent = parent
        self.children = []

    @property
    def classes(self):
        return set((self.attrs.get("class") or "").lower().split())

    def ancestors(self):
        node = self
        while node is not None:
            yield node
            node = node.parent

    @property
    def visible(self):
        return not any(node.tag in {"script", "style", "template"}
                       or "hidden" in node.attrs
                       or (node.attrs.get("aria-hidden") or "").lower() == "true"
                       or re.search(r"(?:display\s*:\s*none|visibility\s*:\s*hidden)",
                                    node.attrs.get("style") or "", re.IGNORECASE)
                       or bool(node.classes & {"d-none", "hidden"})
                       for node in self.ancestors())

    @property
    def disabled(self):
        return any("disabled" in node.attrs or "disabled" in node.classes
                   or (node.attrs.get("aria-disabled") or "").lower() == "true"
                   for node in self.ancestors())

    def walk(self):
        if not self.visible:
            return
        yield self
        for child in self.children:
            if isinstance(child, _Node):
                yield from child.walk()

    def text(self):
        if not self.visible:
            return ""
        return "".join(child.text() if isinstance(child, _Node) else child for child in self.children)

    def html(self):
        if not self.visible:
            return ""
        attrs = ''.join(f' {key}="{escape(value or "", quote=True)}"' for key, value in self.attrs.items())
        body = ''.join(child.html() if isinstance(child, _Node) else escape(child) for child in self.children)
        return f"<{self.tag}{attrs}>{body}</{self.tag}>" if self.tag else body


class RankingsDocument(HTMLParser):
    def __init__(self, html):
        super().__init__(convert_charrefs=True)
        self.root = _Node()
        self.stack = [self.root]
        self.feed(html or "")
        self.close()
        self.nodes = list(self.root.walk())
        self.tables = [node for node in self.nodes if node.tag == "table" and node.classes & TABLE_CLASSES]

    def handle_starttag(self, tag, attrs):
        node = _Node(tag, attrs, self.stack[-1])
        self.stack[-1].children.append(node)
        if tag not in {"area", "base", "br", "col", "embed", "hr", "img", "input", "link", "meta", "param", "source", "track", "wbr"}:
            self.stack.append(node)

    def handle_startendtag(self, tag, attrs):
        self.handle_starttag(tag, attrs)
        if self.stack[-1].tag == tag:
            self.stack.pop()

    def handle_endtag(self, tag):
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                del self.stack[index:]
                break

    def handle_data(self, data):
        self.stack[-1].children.append(data)

    def assert_valid(self):
        for node in self.nodes:
            text = node.text().lower()
            if node.attrs.get("id") == "graph-loading":
                raise RuntimeError("Rankings table is still loading")
            if (node.tag == "form" and "/login" in (node.attrs.get("action") or "").lower()
                    or node.tag == "input" and (node.attrs.get("type") or "").lower() == "password"):
                raise RuntimeError("Rankings page requires login")
            if node.tag in {"title", "h1", "h2"} or node.attrs.get("role") == "alert":
                if re.search(r"\b(?:login required|log in required|authentication required|server error|access denied|unauthorized|forbidden)\b", text):
                    raise RuntimeError("Rankings page contains an authentication or server error")
        if self.tables:
            return
        visible = re.sub(r"\s+", " ", self.root.text()).strip()
        if PRIVATE_EMPTY.lower() in visible.lower():
            return
        if re.search(r"\bno (?:rankings|results|data|logs) (?:found|available|recorded)\b", visible, re.IGNORECASE):
            return
        raise RuntimeError("Rankings table did not load or page is not a valid rankings result")

    @property
    def row_count(self):
        if not self.tables:
            return 0
        return sum(1 for node in self.tables[0].walk() if node.tag == "tr"
                   and any(child.tag == "a" and REPORT_LINK_RE.search(child.attrs.get("href") or "")
                           for child in node.walk()))

    def has_next(self, current_page):
        if self.row_count < 70:
            return False
        controls = []
        for node in self.nodes:
            if node.tag not in {"a", "button", "span"}:
                continue
            if not any((parent.attrs.get("id") or "").startswith("pagination-hook")
                       or bool(parent.classes & {"pagination", "pager"})
                       or parent.tag == "nav" and "pagination" in (parent.attrs.get("aria-label") or "").lower()
                       for parent in node.ancestors()):
                continue
            controls.append(node)
        next_controls = [node for node in controls
                         if "next" in (node.attrs.get("rel") or "").lower().split()
                         or re.search(r"\bnext\b", node.text() + " " + (node.attrs.get("aria-label") or ""), re.IGNORECASE)]
        def enabled(node):
            return not node.disabled and (node.tag == "button" or node.tag == "a" and bool((node.attrs.get("href") or "").strip()))
        # An explicit disabled Next remains authoritative even if numbered links
        # happen to include larger page numbers elsewhere in that paginator.
        if next_controls:
            return any(enabled(node) for node in next_controls)
        for node in controls:
            if not enabled(node) or "prev" in (node.attrs.get("rel") or "").lower().split():
                continue
            values = parse_qs(urlparse(node.attrs.get("href") or "").query).get("page", [])
            label = node.text().strip()
            candidate = values[-1] if values else label
            if candidate.isdigit() and int(candidate) > int(current_page):
                return True
        return False
