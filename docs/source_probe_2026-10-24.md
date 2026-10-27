# Candidate news sites: feasibility probe (2026-10-24)

**Why:** users read different sites, so the agent got a "My sources" picker (`0879ec1`); new
sites add choice. Collected **forward-only** by default (no backfill), so each site costs
storage in proportion to its daily output. Pinecone (free plan, 2 GB) is the binding limit:
1.35 GB used (67%), full in ~5.3 months at today's ~1,500 articles/day, ~15 months after the
planned 256-dim switch (PROGRESS_LOG "Resume here" item 6).

**Method:** read-only, the crawler's user agent, 1 s between requests, ~15-25 requests per
site: robots.txt (rules for `*`, listed sitemaps), sitemap structure and recent URLs per day,
RSS, and one real article's metadata (`og:title`, `article:published_time`, JSON-LD type,
paragraph text in the HTML). Terms of use were not reviewed.

| Site | Lang | Usable feed | Articles/day (est.) | Article metadata | Verdict |
|---|---|---|---|---|---|
| dantri.com.vn (Dân Trí) | vi | `/rss/home.rss` (100 items ≈ 9 h); monthly sitemaps `/sitemaps/Article-YYYY-MM-01.xml` | ~250 | JSON-LD NewsArticle, `article:published_time` | easy |
| vir.com.vn (Vietnam Investment Review) | en | `/newssitemap-site-1.xml`; `/rss_feed/` | ~20-55 | NewsArticle, published time, `article:section` | easy |
| congthuong.vn (Công Thương) | vi | `/news-sitemap.xml` (RSS has only 7 items) | ~65-100 | NewsArticle, published time, section | easy |
| vietnamfinance.vn | vi | `/sitemap.xml` (1,000 recent, dated); `/home.rss` | ~55-70 | NewsArticle, published time | easy |
| cafef.vn | vi | 5-day sitemaps `/sitemaps/sitemaps-YYYY-M-D1-D2.xml` back to 2016 (Tuổi Trẻ's layout); `/google-news-sitemap.xml` | **~440** (Oct 6-9: 436-476) | NewsArticle, published time without offset (local) | easy, high volume |
| dtinews.dantri.com.vn (Dân Trí English) | en | `/rss/home.rss` (100 items ≈ 4 days) | ~25 | no JSON-LD, no published-time meta: date from the page | doable |
| ven.congthuong.vn (Công Thương English) | en | none (sitemap / RSS 404); ~30 article links on the homepage | low | — | needs a listing-page crawl mode |
| baodautu.vn (Báo Đầu tư) | vi | monthly sitemaps return "Not found act function sitemap_month"; RSS index link broken | ? | — | broken feeds; revisit |
| laodong.vn (Lao Động) | vi | every URL (incl. robots.txt) returns a JavaScript cookie challenge | ? | — | **skip**: passing it would circumvent bot protection |

All robots.txt files allow `*` (no crawl-delay); the disallowed paths (print / AMP / tag /
ajax pages) don't touch article URLs. No paywall markers found. No site's JSON-LD carries
`articleBody`, so each needs an HTML body selector, like the current crawlers.

**Cost per site:** ~420 embedding tokens per article (measured 2026-10-24) ≈ $0.08 per 1,000
articles; storage 2.73 KB per article now, 1.71 KB after the 256-dim switch.

| Plan (forward-only) | Extra/day | Months to full at 512 dims | At 256 dims |
|---|---|---|---|
| today's 10 sources | — | ~5.3 | ~15 |
| + Dân Trí, DTiNews, VIR | ~300 | ~4.4 | ~12.5 |
| + Công Thương, VietnamFinance | ~440 | ~4.1 | ~11.5 |
| + CafeF | ~880 | ~3.3 | ~9.5 |

**Outcome (2026-10-24):** first wave built (`4d746eb`). **DTiNews (`dantri_en`) and VIR
(`vir`) are live**, forward-only (first run: 100 + 33 articles; VIR's PR Newswire / Media
OutReach wires, about half its feed, are skipped). **Dân Trí is not:** from Cloud Run every
article page timed out (`ConnectTimeout`, 4 retries each) although its sitemap and RSS
loaded and the same pages load from a home connection, so it apparently refuses datacenter
addresses. Its crawler class stays; production runs without it (`d74d546`). Options: run it
from a non-datacenter machine, or ask Dân Trí for access. Not via a proxy / IP rotation
(that would be circumventing their block).

**Recommendation (before building):** first wave Dân Trí + DTiNews + VIR (widest Vietnamese audience + two
English options, ~300/day; fits this month's Pinecone write units); one finance site after
that (VietnamFinance cheap, CafeF only after the 256 switch and on demand); Công Thương as a
second-wave candidate. VEN and Báo Đầu tư need crawler work; Lao Động is out.

## Công Thương probe (2026-10-27)

Read-only probe for the next source; a draft `CongThuong` crawler class exists locally only
(not committed or deployed until VietnamFinance has had a clean run).

- **Access:** robots.txt `Allow: /` (only `/fv*/Watch` disallowed); sitemaps load in 1-2 s.
- **Enumeration:** `/news-sitemap.xml` = 203 URLs over ~2 days (**~100 articles/day**);
  `/sitemap-index.xml` lists `sitemap-article-YYYY-M-N.xml` parts back to **2015-01**
  (2026-09: 4 parts, 2,738 URLs), so unlike VietnamFinance a backfill is possible.
- **Parsing:** JSON-LD NewsArticle (headline, datePublished; no articleSection),
  `article:section` meta gives the section, body in `[itemprop=articleBody]`
  (`div.edittor-content`), title `h1.article-detail-title`. Ids `congthuong-<trailing digits>`
  (203/203 unique). 12/12 random pages parsed.
- **Not news, to skip (like VIR's wires):** lottery results (sections 'Xổ số miền Nam XSMN',
  'Xổ số Vietlott'…; ~8% of the feed, 16/203), horoscopes ('tử vi'), lunar calendar ('lịch âm').
  The draft returns `Skipped` for those (checked on 3 lottery pages).
- **Section groups:** most map already (Thời sự, Xã hội, Văn hóa - Giải trí, Xe và Công nghệ,
  Quản lý thị trường, Thị trường, Tài chính, Pháp luật). 'Năng lượng', 'Thương mại',
  'Công nghiệp' (+ 'Khuyến công') would land in Other: add them to Economy & Business in
  `section_utils` when the source goes live.
- **When adding:** forward-only like the others (current month = news sitemap), job loop
  4 workers / 1 s, state sync, accepted values, freshness only with its first rows, agent
  source list + parser ('Công Thương' is also the ministry's name: match "báo Công Thương" /
  congthuong, not the bare phrase).

## VEN probe (2026-10-27)

ven.congthuong.vn, Công Thương's English edition (Vietnam Economic News). Read-only probe; no
crawler code written yet.

- **Access:** robots.txt `Allow: /` (same file as congthuong.vn). No bot challenge.
- **Enumeration: listing pages only.** `/sitemap.xml`, `/sitemap-index.xml`, `/news-sitemap.xml`,
  `/rss`, `/rss.xml`, `/home.rss` all 404. The homepage lists 30 articles; the five text
  categories (`/industry`, `/news-and-events`, `/society`, `/tourism`, `/trade`) list ~40 each:
  185 unique articles, ids 57732-59963. Infographic / Photo / longform / video are formats, skip.
- **Volume: ~5 articles/day** (ids 59695 on 09-03 → 59963 on ~10-27). Too small to matter for
  Pinecone.
- **Parsing:** the draft `CongThuong` parser works unchanged (same CMS): 14/14 random pages
  parsed (title, date, section, body 1.9-11k chars). Ids `ven-<trailing digits>`.
- **Sections:** Society, News and Events, Industry, Trade, Tourism. Only Society maps today;
  'news and events' (→ National News), 'industry' / 'trade' (→ Economy & Business) and
  'tourism' (→ Lifestyle & Travel) need `section_utils` keywords, together with Công Thương's
  'Năng lượng' / 'Thương mại' / 'Công nghiệp'.
- **Crawler work needed:** a listing-page mode (the crawler only reads XML sitemaps / RSS):
  read the homepage + 5 category pages each run (6 requests), keep links matching
  `-<digits>.html`, fetch unseen ones. Forward-only: category pages reach back to 2025-11, so
  drop parsed articles older than `--from-date` on the first run (or accept ~185 older articles).
- **Order:** after Công Thương has had a clean run (one new source at a time).
