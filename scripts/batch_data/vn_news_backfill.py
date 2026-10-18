"""
vn_news_backfill.py

Resumable, sitemap-driven crawler for Vietnamese news sites (Tuoi Tre, Thanh Nien,
VnEconomy). Companion to `guardian_api_backfill.py` - it writes the SAME Parquet
contract, so the existing BigQuery loader ingests the result:

    uv run python scripts/batch_data/vn_news_backfill.py --source tuoitre --from-date 2024-01-01
    uv run python scripts/batch_data/load_raw_to_bigquery.py --source vn      # (loader change - see below)

Enumeration is sitemap-driven (no listing-page scraping):
  * Every source publishes a sitemap index of date-bucketed archive sitemaps.
  * Tuoi Tre / Thanh Nien share one CMS: 5-day chunk sitemaps
    `sitemaps/sitemaps-YYYY-M-D1-D2.xml`, back to ~2016. The trailing 18-digit article
    id encodes the publish timestamp (`<prefix><YYMMDD><HHMMSS><seq>`), so article_id,
    web_publication_date and the dedupe key can all be read straight off the URL.
  * VnEconomy: monthly `sitemap/news-YYYY-MM.xml`, back to 2007. Slug-only URLs;
    the date comes from the sitemap <lastmod> and (authoritatively) the article page.

Design (mirrors the Guardian crawler):
  * State/lock/seen/output are all per-source, so the three sources run in PARALLEL
    with no coordination. Default state file: data/batch/vn_news_state.<source>.json
    (see scripts/batch_data/run_vn_backfill_all.sh).
  * Month-granularity cursor in the state file; a crash loses at most one month.
    The cursor never moves past the month of `--to-date` (minus `--tail-days`), so
    scheduled runs (the vn-ingestion Cloud Run Job) keep re-reading the live month.
  * `<state>.seen` sidecar lists every written article id -> re-runs skip
    already-fetched articles (VN months are large; a bare month cursor would re-fetch).
  * Each finished month is flushed to Parquet immediately, partitioned
    `<output-dir>/<source>/pub_year=YYYY/pub_month=MM/part-*.parquet`
    (the layout load_raw_to_bigquery.py --source vn wants).
  * Lock file (`<state>.lock`) refuses a second run against the same state file.
  * Concurrency: article pages for a month are fetched with a `--concurrency`-wide
    thread pool (pure I/O wait). Measured ceiling is ~5-6 articles/s per source and
    does NOT rise past --concurrency 8 - the sites throttle per-IP, so more workers
    only add connection resets. 8 is the knee (default). `--sleep` is a per-worker
    inter-request delay on top of that. Exponential backoff on 429/5xx/network
    errors. robots.txt for all three is `Allow: /` for a generic UA, no Crawl-delay;
    only search/print/api paths are disallowed, which this crawler never touches.

Parsing (`HtmlNewsSource.parse_article`): per field, JSON-LD NewsArticle -> og:/article:
meta -> DOM, in that order. Selectors verified against sample articles from 2015-2026 for
all three sources. Tuoi Tre / Thanh Nien carry a full JSON-LD NewsArticle; VnEconomy has
none but rich `article:*` meta (note: `article:published_time` is a `name=` meta there).
Body containers differ by CMS and by era (pre-2019 Vccorp posts are <br>-separated, not
<p>) - `_extract_body` falls back to raw text for those.

Downstream changes still needed before the Parquet can be loaded:
  1. load_raw_to_bigquery.py - add "vn" to `--source` choices and to SOURCE_DEFAULT_DIR
     (-> "data/batch/vn_news_parquet").
  2. raw.raw_data + RAW_DATA_SCHEMA - add `source_name` STRING and `language` STRING.
     This crawler already emits both columns; Guardian rows would backfill as
     source_name='guardian', language='en'.

Environment: none required (public sites). Honours HTTP(S)_PROXY if set.
"""

from __future__ import annotations

import argparse
import html as _html
import json
import logging
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import polars as pl
import requests

try:
    from bs4 import BeautifulSoup
except ModuleNotFoundError:  # --dry-run (sitemap walk only) still works without it
    BeautifulSoup = None  # type: ignore[assignment]

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

# Same 8 columns as guardian_api_backfill.OUTPUT_COLUMNS, plus the two new
# multi-source columns (see module docstring, downstream change #2).
OUTPUT_COLUMNS = [
    "article_id",
    "crawl_timestamp",
    "web_publication_date",
    "web_title",
    "body_text",
    "web_url",
    "section_name",
    "data_source",
    "source_name",
    "language",
]

# Vccorp-CMS 5-day sitemap chunks, used only when the index does not enumerate a
# month (older archives). Day 26-31 covers short months too (a 404 is skipped).
DAY_CHUNKS = [(1, 5), (6, 10), (11, 15), (16, 20), (21, 25), (26, 31)]


class FetchError(Exception):
    """A URL could not be retrieved after retries (or returned 4xx)."""


class SitemapError(Exception):
    """A sitemap that should exist could not be read completely, even after retries."""


# --------------------------------------------------------------------------- model


@dataclass
class Article:
    article_id: str
    web_url: str
    web_title: str
    body_text: str
    web_publication_date: datetime  # naive, UTC
    section_name: str


# ------------------------------------------------------------------------- sources


class NewsSource:
    name: str = ""
    language: str = "vi"
    base_url: str = ""
    sitemap_index_url: str = ""

    # -- enumeration ------------------------------------------------------
    def month_sitemap_urls(
        self, index_children: list[str], year: int, month: int
    ) -> list[str]:
        raise NotImplementedError

    # -- cheap per-URL metadata ---------------------------------------
    def article_id(self, url: str) -> str:
        """Default: the slug (URL path without host or extension)."""
        path = url.split("://", 1)[-1].split("/", 1)[-1]
        return re.sub(r"\.html?$", "", path).strip("/")

    def hint_publication_date(
        self, url: str, lastmod: datetime | None
    ) -> datetime | None:
        return lastmod

    # -- article page parse (STUB) ----------------------------------
    def parse_article(
        self, html: str, url: str, lastmod: datetime | None
    ) -> Article | None:
        raise NotImplementedError


def _meta(soup, *keys: str) -> str | None:
    """First non-empty <meta> content for any of `keys`, matched on either
    `property=` or `name=` (VnEconomy uses name= for article:published_time).
    Entities are unescaped - VnEconomy double-encodes the `+` in its timestamps
    as `&#x2B;`, which lxml does not always decode in attribute values."""
    for k in keys:
        el = soup.find("meta", attrs={"property": k}) or soup.find("meta", attrs={"name": k})
        if el and el.get("content"):
            return _html.unescape(el["content"]).strip()
    return None


def _jsonld_article(soup) -> dict:
    """The first NewsArticle / Article node from any ld+json block (incl. @graph)."""
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = s.string or s.get_text() or ""
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        stack = list(data) if isinstance(data, list) else [data]
        while stack:
            node = stack.pop(0)
            if not isinstance(node, dict):
                continue
            if isinstance(node.get("@graph"), list):
                stack.extend(node["@graph"])
            t = node.get("@type")
            t = t if isinstance(t, str) else (t[0] if isinstance(t, list) and t else "")
            if t in ("NewsArticle", "Article", "ReportageNewsArticle", "BlogPosting"):
                return node
    return {}


def _jsonld_breadcrumb_section(html: str) -> str:
    """Second item (the top-level section, after the home crumb) of the first JSON-LD
    BreadcrumbList, or ''."""
    soup = BeautifulSoup(html, "lxml" if _has_lxml() else "html.parser")
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        try:
            data = json.loads(s.string or s.get_text() or "")
        except (ValueError, TypeError):
            continue
        for node in data if isinstance(data, list) else [data]:
            if isinstance(node, dict) and node.get("@type") == "BreadcrumbList":
                items = sorted(node.get("itemListElement") or [],
                               key=lambda i: i.get("position", 0) if isinstance(i, dict) else 0)
                names = [i.get("name") or (i["item"].get("name") if isinstance(i.get("item"), dict) else "") or ""
                         for i in items if isinstance(i, dict)]
                if len(names) >= 2:
                    return _clean(names[1])
    return ""


_STRIP_IN_BODY = (
    "script, style, figure, figcaption, table, .article-actions, .box-advertisement, "
    "[class*='advertisement'], [class*='__related'], [class*='related-news'], "
    "[class*='comment'], .VCSortableInPreviewMode, .PhotoCMS_Caption, "
    ".detail-cmt, blockquote.tw, .zone--timeline, .box-tinlienquan"
)


def _extract_body(node) -> str:
    """Paragraph text from a body container. Falls back to a whitespace-collapsed
    get_text for the older Thanh Nien / Tuoi Tre template where paragraphs are
    not <p> elements."""
    for junk in node.select(_STRIP_IN_BODY):
        junk.decompose()
    paras = [p.get_text(" ", strip=True) for p in node.find_all("p")]
    paras = [re.sub(r"\s+", " ", p) for p in paras if len(p.strip()) > 1]
    text = "\n\n".join(paras)
    if len(text) < 200:  # pre-2019 template: <br>-separated, not <p>
        text = re.sub(r"\s+", " ", node.get_text(" ")).strip()
    return text.strip()


class HtmlNewsSource(NewsSource):
    """Real parser shared by all three sources. Priority for each field:
    JSON-LD NewsArticle -> og:/article: meta -> DOM. Verified against sample
    articles from 2015-2026 (tuoitre, thanhnien, vneconomy)."""

    # Ordered candidate body containers; first match wins. Per-source override.
    BODY_SELECTORS: tuple[str, ...] = ("article",)
    # Ordered candidate breadcrumb/section link selectors.
    SECTION_SELECTORS: tuple[str, ...] = ("[class*='breadcrumb'] a",)
    H1_SELECTORS: tuple[str, ...] = ("h1[itemprop='headline']", "h1")
    MIN_BODY_CHARS = 120

    def parse_article(self, html, url, lastmod):
        if BeautifulSoup is None:
            raise RuntimeError("beautifulsoup4 not installed - run: uv add beautifulsoup4")
        soup = BeautifulSoup(html, "lxml" if _has_lxml() else "html.parser")
        ld = _jsonld_article(soup)

        # -- title
        title = ld.get("headline") or _meta(soup, "og:title")
        for sel in self.H1_SELECTORS:
            h1 = soup.select_one(sel)
            if h1 and h1.get_text(strip=True):
                title = h1.get_text(" ", strip=True)
                break
        title = _clean(title)

        # -- publication date
        raw_dt = ld.get("datePublished") or _meta(
            soup, "article:published_time", "article:modified_time"
        )
        pub = _parse_dt(raw_dt) or self.hint_publication_date(url, lastmod)

        # -- section
        section = ld.get("articleSection") or _meta(soup, "article:section")
        if isinstance(section, list):
            section = section[0] if section else ""
        if not section:
            for sel in self.SECTION_SELECTORS:
                els = soup.select(sel)
                if els:
                    section = els[-1].get_text(" ", strip=True)
                    break
        section = _clean(section)

        # -- body
        body = ""
        for sel in self.BODY_SELECTORS:
            node = soup.select_one(sel)
            if node:
                body = _extract_body(node)
                if len(body) >= self.MIN_BODY_CHARS:
                    break

        if not title or pub is None or len(body) < self.MIN_BODY_CHARS:
            return None
        return Article(self.article_id(url), url, title, body, pub, section)


class VccorpStyleSource(HtmlNewsSource):
    """Tuoi Tre + Thanh Nien: one shared CMS. Article URL ends `slug-<digits>.htm`;
    18-digit ids on recent posts, shorter numeric ids pre ~2019."""

    _ID_RE = re.compile(r"-(\d{8,20})\.html?(?:$|[?#])")
    BODY_SELECTORS = ("div.detail-content", "div[itemprop='articleBody']", "div.detail-cmain")
    # tuoitre renders this as <nav class="detail-cate">, thanhnien as <div> - no tag constraint.
    SECTION_SELECTORS = (".detail-cate a", "[class*='breadcrumb'] a")
    H1_SELECTORS = ("h1.detail-title", "h1.article-title", "h1[itemprop='headline']", "h1")

    def month_sitemap_urls(self, index_children, year, month):
        # The index lists chunk sitemaps under /StaticSitemaps/ (tuoitre) or
        # /sitemaps/ (thanhnien); match on the filename tail either way.
        pat = re.compile(rf"/sitemaps-{year}-{month}-\d+-\d+\.xml$")
        hits = sorted(u for u in index_children if pat.search(u))
        if hits:
            return hits
        return [
            f"{self.base_url}/StaticSitemaps/sitemaps-{year}-{month}-{d1}-{d2}.xml"
            for d1, d2 in DAY_CHUNKS
        ]

    def _digits(self, url: str) -> str | None:
        m = self._ID_RE.search(url)
        return m.group(1) if m else None

    def article_id(self, url):
        return self._digits(url) or super().article_id(url)

    def hint_publication_date(self, url, lastmod):
        # The 18-digit id embeds a near-publish timestamp (creation, ~minutes
        # earlier than article:published_time) - fallback only.
        return (_decode_vccorp_ts(self._digits(url)) if self._digits(url) else None) or lastmod


class TuoiTre(VccorpStyleSource):
    name = "tuoitre"
    base_url = "https://tuoitre.vn"
    sitemap_index_url = "https://tuoitre.vn/sitemaps/index.rss"


class ThanhNien(VccorpStyleSource):
    name = "thanhnien"
    base_url = "https://thanhnien.vn"
    sitemap_index_url = "https://thanhnien.vn/sitemap.xml"


class VnEconomy(HtmlNewsSource):
    name = "vneconomy"
    base_url = "https://vneconomy.vn"
    sitemap_index_url = "https://vneconomy.vn/sitemap.xml"
    # No ld+json; rich article: meta (published_time via name=). Body class has a
    # typo in the site's own markup ("edtior").
    BODY_SELECTORS = (
        "div.article-editor",
        "[class*='article-editor']",
        "div.article-detail-section__main",
    )
    SECTION_SELECTORS = ("[class*='breadcrumb'] a", "[class*='bread-crumbs'] a")
    H1_SELECTORS = ("h1.article-header__title", "h1[itemprop='headline']")

    def month_sitemap_urls(self, index_children, year, month):
        want = f"/sitemap/news-{year}-{month:02d}.xml"
        hits = [u for u in index_children if u.endswith(want)]
        return hits or [f"{self.base_url}{want}"]


class TuoiTreEn(VccorpStyleSource):
    """Tuoi Tre News, the English edition (news.tuoitre.vn): same Vccorp CMS, ids and
    sitemap layout (/sitemaps/index.rss -> 5-day StaticSitemaps chunks) as tuoitre.vn.
    The category link sits in `.list-cate`. Pages declare <html lang="vi"> although
    the content is English, so the language comes from here, not from the page."""
    name = "tuoitre_en"
    language = "en"
    base_url = "https://news.tuoitre.vn"
    sitemap_index_url = "https://news.tuoitre.vn/sitemaps/index.rss"
    SECTION_SELECTORS = (".list-cate a", ".detail-cate a", "[class*='breadcrumb'] a")


class VnEconomyEn(VnEconomy):
    """VnEconomy's English edition (en.vneconomy.vn). A different template from
    vneconomy.vn (BEM classes; JSON-LD NewsArticle carries articleSection) and
    unpadded monthly sitemap names (sitemap/news-2026-9.xml). Archive from 2021-06."""
    name = "vneconomy_en"
    language = "en"
    base_url = "https://en.vneconomy.vn"
    sitemap_index_url = "https://en.vneconomy.vn/sitemap.xml"
    BODY_SELECTORS = ("div.detail__content",)
    SECTION_SELECTORS = ("[class*='breadcrumb'] a",)
    H1_SELECTORS = ("h1.detail__title", "h1")

    def month_sitemap_urls(self, index_children, year, month):
        want = f"/sitemap/news-{year}-{month}.xml"
        hits = [u for u in index_children if u.endswith(want)]
        return hits or [f"{self.base_url}{want}"]


class VietnamNet(HtmlNewsSource):
    """VietNamNet (vietnamnet.vn). Monthly article sitemaps in 1,000-URL parts
    (`sitemap-article-MM-YYYY-PP.xml`, back to 2005); JSON-LD NewsArticle carries
    headline / datePublished / articleSection. Section comes from JSON-LD only: the
    breadcrumb also holds the site's top navigation, so its last link is not the section.

    Ids are the trailing number (`...-2565868.html`), prefixed with the source name:
    VnExpress also uses bare 7-digit ids, and raw_data merges on article_id across
    all sources, so bare numbers could collide."""
    name = "vietnamnet"
    base_url = "https://vietnamnet.vn"
    sitemap_index_url = "https://vietnamnet.vn/sitemap.xml"
    BODY_SELECTORS = ("div.maincontent.main-content", "div.content-detail")
    SECTION_SELECTORS: tuple[str, ...] = ()
    H1_SELECTORS = ("h1.content-detail-title", "h1")
    _ID_RE = re.compile(r"-(\d{6,})\.html?(?:$|[?#])")

    def article_id(self, url):
        m = self._ID_RE.search(url)
        return f"{self.name}-{m.group(1)}" if m else super().article_id(url)

    def parse_article(self, html, url, lastmod):
        # ~5% of pages have no NewsArticle JSON-LD (so no articleSection), only a
        # BreadcrumbList ('Trang chủ' > 'Thời sự' > 'Pháp luật'); its second crumb is
        # the same top-level section. An empty one would land in the 'Other' group.
        art = super().parse_article(html, url, lastmod)
        if art is not None and not art.section_name:
            art.section_name = _jsonld_breadcrumb_section(html)
        return art

    def month_sitemap_urls(self, index_children, year, month):
        pat = re.compile(rf"/sitemap-article-{month:02d}-{year}-\d+\.xml$")
        return sorted(u for u in index_children if pat.search(u))


class VietnamNetEn(VietnamNet):
    """VietNamNet Global, the English edition (vietnamnet.vn/en). FORWARD-ONLY: its
    index lists monthly article sitemaps but they all 404, so the only enumeration is
    `/en/sitemap-news.xml` (~2.5 days of articles), read for the current month only.
    The pages carry no category (JSON-LD has no articleSection, only free-form tags),
    so the section is the breadcrumb's 'Vietnamnet global', which section_utils maps
    to National News."""
    name = "vietnamnet_en"
    language = "en"
    sitemap_index_url = "https://vietnamnet.vn/en/sitemap.xml"
    SECTION_SELECTORS = ("[class*='bread'] a",)
    DEFAULT_SECTION = "Vietnamnet global"

    def parse_article(self, html, url, lastmod):
        # An empty section would land in the 'Other' group, so fall back to the
        # edition's own label.
        art = super().parse_article(html, url, lastmod)
        if art is not None and not art.section_name:
            art.section_name = self.DEFAULT_SECTION
        return art

    def month_sitemap_urls(self, index_children, year, month):
        today = datetime.now(timezone.utc).date()
        if (year, month) != (today.year, today.month):
            return []
        return [f"{self.base_url}/en/sitemap-news.xml"]


class VnExpressRss(HtmlNewsSource):
    """Base for VnExpress: RSS-only and FORWARD-ONLY, by policy. Its sitemaps are
    served to search engines only (302 to the homepage for anyone else) and robots.txt
    disallows scraping/dataset crawlers, so we read only the RSS feeds it publishes
    for readers - never spoofed user agents, listing pages or id enumeration. Each
    feed holds its latest ~60 items (2-15 days for most categories), read for the
    current month only. Ids are the trailing 7-digit number, prefixed with the source
    name (vietnamnet uses bare numbers too)."""
    sitemap_index_url = ""
    FEEDS: tuple[str, ...] = ()
    _ID_RE = re.compile(r"-(\d{6,})\.html?(?:$|[?#])")

    def article_id(self, url):
        m = self._ID_RE.search(url)
        return f"{self.name}-{m.group(1)}" if m else super().article_id(url)

    def month_sitemap_urls(self, index_children, year, month):
        today = datetime.now(timezone.utc).date()
        if (year, month) != (today.year, today.month):
            return []
        return [f"{self.base_url}/rss/{f}.rss" for f in self.FEEDS]


class VnExpress(VnExpressRss):
    """vnexpress.net. Title h1.title-detail, body article.fck_detail; the section is
    the FIRST breadcrumb link (the last one is a sub-category, e.g. 'Vĩ mô').
    Left out: tin-moi-nhat (CDN-stale, ~3 h window), tin-xem-nhieu and spotlight
    (stale), startup (empty), vne-go."""
    name = "vnexpress"
    base_url = "https://vnexpress.net"
    FEEDS = (
        "thoi-su", "the-gioi", "kinh-doanh", "bat-dong-san", "khoa-hoc-cong-nghe", "suc-khoe",
        "the-thao", "giai-tri", "phap-luat", "giao-duc", "gia-dinh", "du-lich",
        "oto-xe-may", "y-kien", "goc-nhin", "tam-su", "thu-gian", "tin-noi-bat",
    )
    BODY_SELECTORS = ("article.fck_detail", "div.fck_detail")
    SECTION_SELECTORS = ("ul.breadcrumb > li:first-child a",)
    H1_SELECTORS = ("h1.title-detail", "h1")


class VnExpressEn(VnExpressRss):
    """VnExpress International (e.vnexpress.net). Title h1.title_post, body
    div.fck_detail. No breadcrumb, so the section comes from the URL path:
    /news/<section>/<sub>/slug-<id>.html -> <section> ('business', 'travel', ...),
    except the generic 'news' section, which uses <sub> ('environment', 'politics')."""
    name = "vnexpress_en"
    language = "en"
    base_url = "https://e.vnexpress.net"
    FEEDS = ("home", "news", "business", "world", "life", "travel", "sports", "tech", "perspectives")
    BODY_SELECTORS = ("div.fck_detail",)
    SECTION_SELECTORS: tuple[str, ...] = ()
    H1_SELECTORS = ("h1.title_post", "h1")

    def parse_article(self, html, url, lastmod):
        art = super().parse_article(html, url, lastmod)
        if art is not None and not art.section_name:
            parts = url.split("://", 1)[-1].split("/")[1:-1]  # path segments without the slug
            if parts and parts[0] == "news":
                parts = parts[1:]
            if parts and parts[0] == "news" and len(parts) > 1:
                parts = parts[1:]
            if parts:
                art.section_name = parts[0].replace("-", " ").title()
        return art


SOURCES: dict[str, NewsSource] = {
    s.name: s
    for s in (TuoiTre(), ThanhNien(), VnEconomy(), TuoiTreEn(), VnEconomyEn(), VietnamNet(),
              VietnamNetEn(), VnExpress(), VnExpressEn())
}


# ------------------------------------------------------------------ date helpers


def _has_lxml() -> bool:
    try:
        import lxml  # noqa: F401

        return True
    except ModuleNotFoundError:
        return False


def _clean(s) -> str:
    """Unescape HTML entities (JSON-LD headlines arrive entity-encoded) and
    collapse whitespace."""
    if not s:
        return ""
    return re.sub(r"\s+", " ", _html.unescape(str(s))).strip()


def _parse_dt(s: str | None) -> datetime | None:
    """Parse an ISO-ish or dd/mm/yyyy timestamp -> naive UTC."""
    if not s:
        return None
    s = _html.unescape(s).strip().replace("Z", "+00:00")
    # vietnamnet.vn/en writes "2026-10-17T18:03:00.000 +07:00" (space before offset).
    s = re.sub(r"\s+([+-]\d{2}:?\d{2})$", r"\1", s)
    try:
        dt = datetime.fromisoformat(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
    except ValueError:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S", "%H:%M %d/%m/%Y", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
        except ValueError:
            continue
    try:  # RFC 822, as in RSS <pubDate>: "Sat, 17 Oct 2026 23:28:53 +0700"
        dt = parsedate_to_datetime(s)
        return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt
    except (TypeError, ValueError):
        return None


def _decode_vccorp_ts(d: str | None) -> datetime | None:
    """`<prefix 2-4><YYMMDD><HHMMSS><seq>` -> naive UTC (site is +07:00).

    # TODO(verify): the prefix length is assumed 3 (fallbacks 2 and 4). Confirm
    # against a handful of known (url, publish time) pairs before trusting it.
    """
    if not d or len(d) < 14:
        return None
    for plen in (3, 2, 4):
        core = d[plen : plen + 12]
        if len(core) != 12:
            continue
        try:
            year = 2000 + int(core[0:2])
            local = datetime(
                year,
                int(core[2:4]),
                int(core[4:6]),
                int(core[6:8]),
                int(core[8:10]),
                int(core[10:12]),
            )
        except ValueError:
            continue
        if 2000 <= year <= date.today().year + 1:
            return local - timedelta(hours=7)  # +07:00 -> UTC
    return None


def _months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def _next_month(y: int, m: int) -> date:
    return date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)


# ------------------------------------------------------------------------- http


def make_session(pool: int = 16) -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "vi,en;q=0.8"})
    # Size the connection pool to the worker count, or concurrent workers thrash
    # it ("Connection pool is full, discarding connection") and stall on dead sockets.
    adapter = requests.adapters.HTTPAdapter(pool_connections=pool, pool_maxsize=pool)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


def fetch(
    session: requests.Session,
    url: str,
    *,
    sleep: float,
    timeout: tuple[int, int] = (10, 25),
    tries: int = 4,
) -> bytes:
    last: Exception | None = None
    for attempt in range(tries):
        try:
            r = session.get(url, timeout=timeout)
            if r.status_code in (429, 500, 502, 503, 504):
                wait = min(60, 5 * 2**attempt)
                logger.warning("%s on %s - backoff %ds", r.status_code, url, wait)
                last = FetchError(f"{r.status_code} {url}")
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(sleep)  # politeness: after every successful GET
            return r.content
        except requests.HTTPError as e:
            raise FetchError(f"{e.response.status_code} {url}") from e
        except requests.RequestException as e:
            # Timeout, ConnectionError, ChunkedEncodingError (truncated response -
            # tuoitre.vn does this), ContentDecodingError, etc. All retryable.
            last = e
            logger.warning(
                "net error %s (%s) retry %d/%d", url, type(e).__name__, attempt + 1, tries
            )
            time.sleep(2**attempt)
    raise FetchError(str(last))


def _xml_locs(xml_bytes: bytes) -> list[tuple[str, datetime | None]]:
    """Yield (loc, lastmod) for every <url>/<sitemap> node (sitemaps) and every RSS
    <item> (link, pubDate), namespace-agnostic.

    Raises SitemapError on unparseable XML instead of returning []: an empty list
    would read as "no articles in this chunk" and the month would count as complete,
    so a malformed response must fail (and be retried) rather than be skipped."""
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as e:
        raise SitemapError(f"unparseable sitemap ({e}) - truncated response?") from e
    out: list[tuple[str, datetime | None]] = []
    for node in root.iter():
        if node.tag.rsplit("}", 1)[-1] not in ("url", "sitemap", "item"):
            continue
        loc = lastmod = None
        for child in node:
            tag = child.tag.rsplit("}", 1)[-1]
            if tag in ("loc", "link"):
                loc = (child.text or "").strip()
            elif tag in ("lastmod", "pubDate"):
                lastmod = _parse_dt((child.text or "").strip())
        if loc:
            out.append((loc, lastmod))
    return out


def fetch_sitemap(
    session: requests.Session,
    url: str,
    *,
    sleep: float,
    listed: bool = True,
    tries: int = 8,
) -> list[tuple[str, datetime | None]]:
    """Entries of one sitemap, retrying truncated bodies and flaky errors.

    Only a 404 on a URL we guessed ourselves (listed=False, e.g. a day-chunk that
    may not exist) means "no such sitemap" and returns []. A 404 on a sitemap the
    index lists is retried like any other failure (news.tuoitre.vn flips the same
    URL between 200 and 404, roughly every other request - 4 tries still failed about
    one chunk per month, hence 8). Raises SitemapError when every attempt fails."""
    last: Exception | None = None
    for attempt in range(tries):
        try:
            return _xml_locs(fetch(session, url, sleep=sleep))
        except FetchError as e:
            if str(e).startswith("404") and not listed:
                return []
            last = e
        except SitemapError as e:
            last = e
        logger.warning("sitemap %s: %s - retry %d/%d", url, last, attempt + 1, tries)
        time.sleep(min(30, 2 * 2**attempt))
    raise SitemapError(f"{url}: {last}")


def load_sitemap_index(session: requests.Session, url: str, *, sleep: float) -> list[str]:
    """Raises SitemapError: an incomplete index would silently send months to guessed
    chunk URLs, so the run must fail instead."""
    return [loc for loc, _ in fetch_sitemap(session, url, sleep=sleep)]


# ------------------------------------------------------------------------ output


def write_month(rows: list[dict], output_dir: Path, run_tag: str) -> int:
    if not rows:
        return 0
    df = (
        pl.DataFrame(
            rows,
            schema_overrides={
                "crawl_timestamp": pl.Datetime("us"),
                "web_publication_date": pl.Datetime("us"),
            },
        )
        .select(OUTPUT_COLUMNS)
        .unique(subset=["article_id"], keep="last")
    )
    parted = df.with_columns(
        pl.col("web_publication_date").dt.year().alias("_y"),
        pl.col("web_publication_date").dt.month().alias("_m"),
    )
    for r in parted.select("_y", "_m").unique().iter_rows(named=True):
        y, m = r["_y"], r["_m"]
        sub = parted.filter((pl.col("_y") == y) & (pl.col("_m") == m)).drop("_y", "_m")
        d = output_dir / f"pub_year={y}" / f"pub_month={m:02d}"
        d.mkdir(parents=True, exist_ok=True)
        sub.write_parquet(d / f"part-{run_tag}.parquet", compression="zstd")
    return df.height


# ------------------------------------------------------------------------- state


def load_state(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, default=str))


def load_seen(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {ln.strip() for ln in path.read_text(encoding="utf-8").splitlines() if ln.strip()}


def append_seen(path: Path, ids: list[str]) -> None:
    if not ids:
        return
    with path.open("a", encoding="utf-8") as fh:
        fh.write("\n".join(ids) + "\n")


# -------------------------------------------------------------------------- crawl


def _is_article(loc: str) -> bool:
    return loc.endswith(".htm") or loc.endswith(".html")


def crawl_month(
    session: requests.Session,
    source: NewsSource,
    year: int,
    month: int,
    index_children: list[str],
    seen: set[str],
    *,
    sleep: float,
    concurrency: int,
    realtime_floor: datetime,
    budget: int | None,
    output_dir: Path,
    seen_path: Path,
    run_tag: str,
    flush_every: int = 100,
) -> tuple[int, bool, int]:
    """Return (n_written, ok, n_new_urls). ok is False on a sitemap failure or when the
    run budget truncated the month - the caller must NOT advance the cursor then.

    Article pages are fetched with a `concurrency`-wide thread pool (pure I/O wait);
    `sleep` is the per-worker inter-request delay, so the aggregate request rate is
    ~= concurrency / sleep.

    Rows are flushed to Parquet (+ the .seen sidecar) every `flush_every` completed
    articles, not just once at month-end - a month can be thousands of articles and
    take a long time at this crawler's real throughput, so incremental flushing is
    what makes data show up in the output directory while a month is still in
    progress, and it also means a kill mid-month loses at most one partial batch."""
    crawl_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    frontier: dict[str, tuple[str, datetime | None]] = {}
    sitemaps_ok = True
    listed = set(index_children)
    for sm in source.month_sitemap_urls(index_children, year, month):
        try:
            entries = fetch_sitemap(session, sm, sleep=sleep, listed=sm in listed)
        except SitemapError as e:
            # Month incomplete: the caller must not advance the cursor past it.
            logger.error("sitemap %s failed: %s", sm, e)
            sitemaps_ok = False
            break
        for loc, lastmod in entries:
            if not _is_article(loc):
                continue
            aid = source.article_id(loc)
            if aid in seen or aid in frontier:
                continue
            frontier[aid] = (loc, lastmod)

    items = list(frontier.items())
    truncated = budget is not None and len(items) > budget
    if truncated:
        items = items[:budget]

    logger.info(
        "[%s] %04d-%02d: %d new urls%s | %d workers",
        source.name, year, month, len(frontier),
        f", capped to {budget}" if truncated else "", max(1, concurrency),
    )

    def _one(item: tuple[str, tuple[str, datetime | None]]):
        _aid, (loc, lastmod) = item
        try:
            html = fetch(session, loc, sleep=sleep).decode("utf-8", "replace")
            return source.parse_article(html, loc, lastmod)
        except FetchError as e:
            logger.warning("article %s: %s", loc, e)
            return None
        except Exception as e:  # noqa: BLE001 - one bad page must not kill the month
            logger.warning("parse crash %s: %s", loc, e)
            return None

    written = 0
    parsed = 0
    failed = 0
    part = 0
    buffer: list[dict] = []

    def _flush():
        nonlocal written, part, buffer
        if not buffer:
            return
        part += 1
        n = write_month(buffer, output_dir, f"{run_tag}-{year}{month:02d}-p{part}")
        written += n
        ids = [r["article_id"] for r in buffer]
        append_seen(seen_path, ids)
        seen.update(ids)
        buffer = []

    if items:
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
            futures = [ex.submit(_one, item) for item in items]
            for fut in as_completed(futures):
                art = fut.result()
                if art is None:
                    failed += 1
                    continue
                parsed += 1
                buffer.append(
                    {
                        "article_id": art.article_id,
                        "crawl_timestamp": crawl_ts,
                        "web_publication_date": art.web_publication_date,
                        "web_title": art.web_title,
                        "body_text": art.body_text,
                        "web_url": art.web_url,
                        "section_name": art.section_name,
                        "data_source": "realtime"
                        if art.web_publication_date >= realtime_floor
                        else "batch",
                        "source_name": source.name,
                        "language": source.language,
                    }
                )
                if len(buffer) >= flush_every:
                    _flush()
                    logger.info(
                        "[%s] %04d-%02d: flushed %d/%d parsed so far",
                        source.name, year, month, parsed, len(items),
                    )
    _flush()
    if failed:
        logger.info(
            "[%s] %04d-%02d: %d parsed, %d failed/skipped",
            source.name, year, month, parsed, failed,
        )
    return written, (sitemaps_ok and not truncated), len(frontier)


def dry_run_month(session, source, index_children, year, month, *, sleep) -> int:
    n = 0
    listed = set(index_children)
    for sm in source.month_sitemap_urls(index_children, year, month):
        try:
            entries = fetch_sitemap(session, sm, sleep=sleep, listed=sm in listed)
        except SitemapError as e:
            logger.warning("[dry-run] %s unreadable (%s) - count is a lower bound", sm, e)
            continue
        n += sum(1 for loc, _ in entries if _is_article(loc))
    return n


# --------------------------------------------------------------------------- cli


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--source", required=True, choices=sorted(SOURCES))
    ap.add_argument("--from-date", default="2024-01-01", help="YYYY-MM-DD (month floor)")
    ap.add_argument("--to-date", default=datetime.now(timezone.utc).date().isoformat(),
                    help="YYYY-MM-DD, default today in UTC")
    ap.add_argument("--tail-days", type=int, default=2,
                    help="never park the cursor past the month of (to-date - tail-days + 1), "
                         "so scheduled runs keep re-reading the current month's sitemaps")
    ap.add_argument("--max-articles", type=int, default=5000, help="run cap (0 = unlimited)")
    ap.add_argument("--realtime-cutoff-days", type=int, default=7, help="newer than this -> data_source=realtime")
    ap.add_argument("--concurrency", type=int, default=8, help="parallel article fetches per source")
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="per-worker delay between requests; aggregate rate ~= concurrency/sleep",
    )
    ap.add_argument("--output-dir", type=Path, default=Path("data/batch/vn_news_parquet"))
    ap.add_argument(
        "--state-file",
        type=Path,
        default=None,
        help="default: data/batch/vn_news_state.<source>.json (per-source, so the "
        "three sources run in parallel with independent state/lock/seen)",
    )
    ap.add_argument("--dry-run", action="store_true", help="walk sitemaps + count urls only")
    ap.add_argument("--reset", action="store_true", help="ignore saved cursor + seen set")
    args = ap.parse_args()

    source = SOURCES[args.source]
    start = date.fromisoformat(args.from_date).replace(day=1)
    end = date.fromisoformat(args.to_date)

    state_file: Path = args.state_file or Path(
        f"data/batch/vn_news_state.{args.source}.json"
    )

    lock = state_file.with_suffix(state_file.suffix + ".lock")
    if lock.exists():
        logger.error("lock %s exists - another run active (or crashed). Remove if sure.", lock)
        return 1
    lock.parent.mkdir(parents=True, exist_ok=True)
    lock.write_text(f"pid={os.getpid()} source={args.source} started={datetime.now(timezone.utc).isoformat()}\n")

    seen_path = state_file.with_suffix(".seen")
    run_tag = datetime.now().strftime("%Y%m%dT%H%M%S")
    realtime_floor = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
        days=args.realtime_cutoff_days
    )

    stop = ""
    months_done = 0
    run_new = 0
    try:
        st = {} if args.reset else load_state(state_file)
        cursor = (
            date.fromisoformat(st["cursor_month"] + "-01")
            if st.get("cursor_month")
            else start
        )
        cursor = max(cursor, start)
        # The month cursor must never move past the month still being published
        # into, or every later run skips the rest of it (the same bug as the
        # Guardian crawler's day cursor, 2026-10-12). The .seen sidecar makes
        # re-reading the tail month cheap: only unseen URLs are fetched.
        tail_month = max(end - timedelta(days=args.tail_days - 1), start).replace(day=1)
        if cursor > tail_month:
            logger.info("[%s] saved cursor %s is past the tail month; re-reading from %s",
                        args.source, cursor.strftime("%Y-%m"), tail_month.strftime("%Y-%m"))
            cursor = tail_month
        seen = set() if args.reset else load_seen(seen_path)

        session = make_session(pool=max(16, args.concurrency + 4))
        logger.info("[%s] sitemap index: %s", args.source, source.sitemap_index_url)
        # RSS-only sources (VnExpress) have no sitemap index to load.
        index_children = (
            load_sitemap_index(session, source.sitemap_index_url, sleep=args.sleep)
            if source.sitemap_index_url else []
        )
        logger.info(
            "[%s] %d child sitemaps | crawl %s -> %s | seen=%d",
            args.source,
            len(index_children),
            cursor.strftime("%Y-%m"),
            end.strftime("%Y-%m"),
            len(seen),
        )

        totals = st.get("articles_total", 0)
        for y, m in _months(cursor, end):
            if args.dry_run:
                logger.info(
                    "[dry-run %s] %04d-%02d: ~%d article urls",
                    args.source, y, m, dry_run_month(session, source, index_children, y, m, sleep=args.sleep),
                )
                continue

            if args.max_articles and run_new >= args.max_articles:
                stop = f"hit --max-articles ({args.max_articles})"
                break

            budget = None if not args.max_articles else args.max_articles - run_new
            n, ok, _ = crawl_month(
                session, source, y, m, index_children, seen,
                sleep=args.sleep, concurrency=args.concurrency,
                realtime_floor=realtime_floor, budget=budget,
                output_dir=args.output_dir / args.source,
                seen_path=seen_path, run_tag=run_tag,
            )
            run_new += n
            totals += n
            months_done += 1

            if not ok:
                stop = f"month {y}-{m:02d} incomplete (budget or sitemap failure)"
                logger.warning("[%s] %s - cursor not advanced", args.source, stop)
                break

            st = {
                "source": args.source,
                "cursor_month": min(_next_month(y, m), tail_month).strftime("%Y-%m"),
                "months_done": st.get("months_done", 0) + 1,
                "articles_total": totals,
                "last_run": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state_file, st)
    except SitemapError as e:
        # Only the sitemap index raises this far (month sitemaps fail the month in
        # crawl_month). Exit non-zero so the job fails and the alert fires.
        logger.error("[%s] sitemap index unreadable: %s - state unchanged", args.source, e)
        return 1
    except KeyboardInterrupt:
        logger.warning("interrupted - state is saved at the last completed month")
        return 130
    finally:
        lock.unlink(missing_ok=True)

    logger.info("=" * 64)
    logger.info(
        "[%s] %s | %d months this run, %d new articles",
        args.source, "PAUSED" if stop else "DONE", months_done, run_new,
    )
    if stop:
        logger.info("stopped: %s  (just re-run to continue)", stop)
    logger.info(
        "Parquet under %s  ->  load with: "
        "uv run python scripts/batch_data/load_raw_to_bigquery.py --source vn   (after loader change)",
        args.output_dir / args.source,
    )
    logger.info("=" * 64)
    return 0


if __name__ == "__main__":
    sys.exit(main())
