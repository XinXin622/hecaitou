#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import quote, unquote, urlsplit, urlunsplit

import requests


def _default_start_date(today: date) -> date:
    target_year = today.year - 3
    try:
        return date(target_year, today.month, today.day)
    except ValueError:
        # e.g. Feb 29 -> Feb 28
        return date(target_year, today.month, 28)


def _parse_yyyy_mm_dd(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def _parse_comma_list(value: str) -> list[str]:
    if not value.strip():
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _safe_print(s: str) -> None:
    sys.stdout.write(s + "\n")


@dataclass(frozen=True)
class FeedEntry:
    title: str
    url: str
    published: date
    labels: tuple[str, ...]
    content: str


def _extract_entry_url(entry: dict) -> Optional[str]:
    for link in entry.get("link", []):
        if link.get("rel") == "alternate":
            href = link.get("href")
            if href:
                return href
    return None


def normalize_url(url: str) -> str:
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = quote(unquote(parts.path), safe="/~%:@!$&'()*+,;=")
    query = quote(unquote(parts.query), safe="=&%")
    fragment = quote(unquote(parts.fragment), safe="")
    return urlunsplit((scheme, netloc, path, query, fragment))


def _fetch_feed_page(
    base_url: str, start_index: int, max_results: int, session: requests.Session
) -> dict:
    url = f"{base_url.rstrip('/')}/feeds/posts/default?alt=json&start-index={start_index}&max-results={max_results}"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def iter_recent_posts(
    base_url: str,
    start_date: date,
    end_date: date,
    session: requests.Session,
    max_results: int = 150,
    polite_delay_s: float = 0.2,
) -> Iterable[FeedEntry]:
    start_index = 1
    while True:
        payload = _fetch_feed_page(base_url, start_index, max_results, session)
        feed = payload.get("feed") or {}
        entries = feed.get("entry") or []
        if not entries:
            return

        page_entries: list[FeedEntry] = []
        for raw in entries:
            title = ((raw.get("title") or {}).get("$t") or "").strip()
            url = _extract_entry_url(raw)
            published_raw = ((raw.get("published") or {}).get("$t") or "").strip()
            if not (title and url and published_raw):
                continue

            content = ((raw.get("content") or {}).get("$t") or "").strip()
            if not content:
                content = ((raw.get("summary") or {}).get("$t") or "").strip()

            published_dt = datetime.fromisoformat(published_raw.replace("Z", "+00:00")).date()
            labels = tuple(
                sorted(
                    {
                        (c.get("term") or "").strip()
                        for c in (raw.get("category") or [])
                        if (c.get("term") or "").strip()
                    }
                )
            )
            page_entries.append(
                FeedEntry(title=title, url=normalize_url(url), published=published_dt, labels=labels, content=content)
            )

        # Feed is typically newest -> oldest; if even the oldest on this page is newer than start_date,
        # we still need more pages; if it's older, we can stop after processing.
        oldest_on_page = min((e.published for e in page_entries), default=None)

        for entry in page_entries:
            if start_date <= entry.published <= end_date:
                yield entry

        if oldest_on_page is None or oldest_on_page < start_date:
            return

        start_index += max_results
        if polite_delay_s > 0:
            time.sleep(polite_delay_s)


def read_local_urls(local_dir: Path) -> set[str]:
    urls: set[str] = set()
    for path in sorted(local_dir.glob("*.md")):
        text = path.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"^- 链接：(.*)$", text, flags=re.M)
        if not m:
            continue
        url = m.group(1).strip()
        if url:
            urls.add(normalize_url(url))
    return urls


def main(argv: list[str]) -> int:
    today = date.today()
    parser = argparse.ArgumentParser(
        description="核对本地 articles/ 是否完整覆盖和菜头近三年的读后/观后/推荐类文章（基于 Blogger JSON Feed）。"
    )
    parser.add_argument("--base-url", default="https://www.hecaitou.com")
    parser.add_argument("--from", dest="from_date", type=_parse_yyyy_mm_dd, default=_default_start_date(today))
    parser.add_argument("--to", dest="to_date", type=_parse_yyyy_mm_dd, default=today)
    parser.add_argument("--local-dir", type=Path, default=Path("articles"))
    parser.add_argument(
        "--labels",
        type=_parse_comma_list,
        default=["读后感", "观后感", "电影", "音乐"],
        help="命中这些标签即可认为属于目标集合（逗号分隔）。",
    )
    parser.add_argument(
        "--title-keywords",
        type=_parse_comma_list,
        default=["读后", "观后", "推荐", "书评", "影评"],
        help="标题包含这些关键词也计入（逗号分隔）。",
    )
    parser.add_argument(
        "--content-keywords",
        type=_parse_comma_list,
        default=["读后感", "观后感", "推荐观赏", "推荐阅读", "夜读推荐"],
        help="正文包含这些关键词也计入（逗号分隔，默认偏强信号，避免误报）。",
    )
    parser.add_argument("--format", choices=["text", "json", "csv"], default="text")
    parser.add_argument("--output", type=Path, default=None, help="输出文件（默认打印到 stdout）。")
    args = parser.parse_args(argv)

    if not args.local_dir.exists():
        _safe_print(f"local dir not found: {args.local_dir}")
        return 2

    include_labels = set(args.labels or [])
    title_keyword_re = None
    if args.title_keywords:
        title_keyword_re = re.compile("|".join(re.escape(k) for k in args.title_keywords))

    content_keyword_re = None
    if args.content_keywords:
        content_keyword_re = re.compile("|".join(re.escape(k) for k in args.content_keywords))

    local_urls = read_local_urls(args.local_dir)

    session = requests.Session()
    matched: list[FeedEntry] = []
    missing: list[FeedEntry] = []

    for entry in iter_recent_posts(args.base_url, args.from_date, args.to_date, session=session):
        if include_labels and any(label in include_labels for label in entry.labels):
            matched.append(entry)
        elif title_keyword_re and title_keyword_re.search(entry.title):
            matched.append(entry)
        elif content_keyword_re and content_keyword_re.search(entry.content):
            matched.append(entry)
        else:
            continue
        if entry.url not in local_urls:
            missing.append(entry)

    summary = {
        "base_url": args.base_url,
        "from": args.from_date.isoformat(),
        "to": args.to_date.isoformat(),
        "local_dir": str(args.local_dir),
        "local_count": len(local_urls),
        "matched_count": len(matched),
        "missing_count": len(missing),
    }

    def write_text(out):
        out.write(json.dumps(summary, ensure_ascii=False) + "\n")
        for e in sorted(missing, key=lambda x: (x.published, x.url)):
            labels = ",".join(e.labels)
            out.write(f"{e.published.isoformat()}\t{e.title}\t{e.url}\t{labels}\n")

    def write_json(out):
        out.write(
            json.dumps(
                {
                    **summary,
                    "missing": [
                        {"published": e.published.isoformat(), "title": e.title, "url": e.url, "labels": list(e.labels)}
                        for e in sorted(missing, key=lambda x: (x.published, x.url))
                    ],
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n"
        )

    def write_csv(out):
        writer = csv.DictWriter(out, fieldnames=["published", "title", "url", "labels"])
        writer.writeheader()
        for e in sorted(missing, key=lambda x: (x.published, x.url)):
            writer.writerow(
                {"published": e.published.isoformat(), "title": e.title, "url": e.url, "labels": ",".join(e.labels)}
            )

    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.format == "csv":
            with args.output.open("w", encoding="utf-8", newline="") as f:
                write_csv(f)
        else:
            with args.output.open("w", encoding="utf-8") as f:
                write_json(f) if args.format == "json" else write_text(f)
    else:
        if args.format == "csv":
            write_csv(sys.stdout)
        else:
            write_json(sys.stdout) if args.format == "json" else write_text(sys.stdout)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
