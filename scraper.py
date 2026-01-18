#!/usr/bin/env python3
"""
槽边往事博客爬虫 - 爬取推荐类文章（书籍、电影等）
"""

from __future__ import annotations

import os
import re
import time
import random
from datetime import datetime
from typing import Optional, List, Dict
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.hecaitou.com"

# 目标标签及其URL
TARGET_LABELS = {
    "读后感": "/search/label/读后感",
    "电影": "/search/label/电影",
    "观后感": "/search/label/观后感",
    "音乐": "/search/label/音乐",
}

# 补充搜索关键词（用于查找标签页可能遗漏的文章）
SEARCH_KEYWORDS = [
    "读后",
    "书评",
    "电影",
    "观后",
]

# 已知的文章URL（标签页索引可能遗漏的文章）
KNOWN_ARTICLES = [
    "https://www.hecaitou.com/2024/01/The-Happiest-Man-in-the-World-book-review.html",
]

# 筛选的年份范围（近三年）
YEAR_RANGE = (2023, 2025)

# 输出目录
OUTPUT_DIR = "articles"

# 请求头
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


def get_page(url: str) -> BeautifulSoup | None:
    """获取页面内容并解析为BeautifulSoup对象"""
    try:
        print(f"  正在访问: {url}")
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        response.encoding = "utf-8"
        return BeautifulSoup(response.text, "html.parser")
    except requests.RequestException as e:
        print(f"  请求失败: {e}")
        return None


def delay():
    """随机延迟1-2秒"""
    time.sleep(random.uniform(1, 2))


def parse_date_from_url(url: str) -> datetime | None:
    """从URL中提取日期（格式: /年/月/文章名.html）"""
    match = re.search(r"/(\d{4})/(\d{2})/", url)
    if match:
        year, month = int(match.group(1)), int(match.group(2))
        return datetime(year, month, 1)
    return None


def is_in_year_range(date: datetime | None) -> bool:
    """检查日期是否在目标年份范围内"""
    if date is None:
        return False
    return YEAR_RANGE[0] <= date.year <= YEAR_RANGE[1]


def get_articles_from_label_page(label: str, label_url: str) -> list[dict]:
    """从标签页获取文章列表"""
    articles = []
    full_url = urljoin(BASE_URL, label_url)

    soup = get_page(full_url)
    if not soup:
        return articles

    # Blogger标签页通常使用 .post-title 或 h3.post-title 显示文章标题
    # 也可能是 .entry-title 或直接在 <a> 标签中

    # 尝试多种选择器
    post_links = []

    # 方式1: 查找 post-title 类
    for title_elem in soup.select(".post-title a, h3.post-title a"):
        href = title_elem.get("href")
        if href:
            post_links.append((title_elem.get_text(strip=True), href))

    # 方式2: 查找 entry-title 类
    if not post_links:
        for title_elem in soup.select(".entry-title a"):
            href = title_elem.get("href")
            if href:
                post_links.append((title_elem.get_text(strip=True), href))

    # 方式3: 查找博客文章容器内的链接
    if not post_links:
        for post in soup.select(".blog-post, .post, article"):
            link = post.select_one("a[href*='hecaitou.com']")
            if link:
                href = link.get("href")
                title = link.get_text(strip=True) or "未知标题"
                if href and ".html" in href:
                    post_links.append((title, href))

    # 去重
    seen = set()
    for title, href in post_links:
        if href not in seen:
            seen.add(href)
            date = parse_date_from_url(href)
            if is_in_year_range(date):
                articles.append({
                    "title": title,
                    "url": href,
                    "date": date,
                    "label": label,
                })
                print(f"  找到文章: {title} ({date.strftime('%Y-%m') if date else '未知日期'})")

    return articles


def extract_article_content(url: str) -> dict | None:
    """从文章页面提取完整内容"""
    soup = get_page(url)
    if not soup:
        return None

    # 提取标题
    title = None
    title_elem = soup.select_one("h3.post-title, .post-title, h1.entry-title, .entry-title")
    if title_elem:
        title = title_elem.get_text(strip=True)
    if not title:
        title_elem = soup.select_one("title")
        if title_elem:
            title = title_elem.get_text(strip=True).split(":")[0].strip()

    # 提取日期
    date = None
    # 尝试从页面元素获取日期
    date_elem = soup.select_one(".date-header, .post-timestamp, time, .published")
    if date_elem:
        date_text = date_elem.get_text(strip=True)
        # 尝试解析日期
        date_match = re.search(r"(\d{4})[-年/](\d{1,2})[-月/](\d{1,2})", date_text)
        if date_match:
            date = datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)))

    # 如果页面没有日期，从URL提取
    if not date:
        date = parse_date_from_url(url)

    # 提取正文内容
    content = None
    content_elem = soup.select_one(".post-body, .entry-content, .post-content, article")
    if content_elem:
        # 移除脚本和样式
        for elem in content_elem.select("script, style, .post-footer, .comments"):
            elem.decompose()

        # 转换为文本，保留段落结构
        content = extract_text_with_structure(content_elem)

    # 提取标签
    labels = []
    for label_elem in soup.select(".post-labels a, .labels a, a[rel='tag']"):
        labels.append(label_elem.get_text(strip=True))

    return {
        "title": title,
        "date": date,
        "content": content,
        "labels": labels,
        "url": url,
    }


def extract_text_with_structure(elem) -> str:
    """提取文本并保留段落结构"""
    from bs4 import NavigableString, Tag

    lines = []

    # 如果是NavigableString，直接返回文本
    if isinstance(elem, NavigableString):
        return str(elem).strip()

    # 如果不是Tag，返回空
    if not isinstance(elem, Tag):
        return ""

    for child in elem.children:
        if isinstance(child, Tag):
            if child.name in ("p", "div"):
                text = child.get_text(strip=True)
                if text:
                    lines.append(text)
                    lines.append("")  # 空行分隔段落
            elif child.name in ("h1", "h2", "h3", "h4", "h5", "h6"):
                text = child.get_text(strip=True)
                if text:
                    level = int(child.name[1])
                    lines.append(f"{'#' * (level + 1)} {text}")
                    lines.append("")
            elif child.name == "br":
                lines.append("")
            elif child.name in ("ul", "ol"):
                for li in child.find_all("li", recursive=False):
                    text = li.get_text(strip=True)
                    if text:
                        lines.append(f"- {text}")
                lines.append("")
            elif child.name == "blockquote":
                text = child.get_text(strip=True)
                if text:
                    for line in text.split("\n"):
                        lines.append(f"> {line.strip()}")
                    lines.append("")
            elif child.name == "a":
                text = child.get_text(strip=True)
                href = child.get("href", "")
                if text and href:
                    lines.append(f"[{text}]({href})")
            elif child.name == "img":
                src = child.get("src", "")
                alt = child.get("alt", "图片")
                if src:
                    lines.append(f"![{alt}]({src})")
                    lines.append("")
            else:
                # 递归处理其他元素
                text = extract_text_with_structure(child)
                if text.strip():
                    lines.append(text)
        elif isinstance(child, NavigableString):
            text = str(child).strip()
            if text:
                lines.append(text)

    return "\n".join(lines)


def save_article_to_markdown(article: dict, output_dir: str):
    """将文章保存为Markdown文件"""
    if not article.get("content"):
        print(f"  跳过: 无内容 - {article.get('title', '未知标题')}")
        return

    # 生成文件名
    date_str = article["date"].strftime("%Y-%m-%d") if article.get("date") else "unknown-date"
    title = article.get("title", "未知标题")
    # 清理文件名中的非法字符
    safe_title = re.sub(r'[\\/:*?"<>|]', "", title)
    safe_title = safe_title[:50]  # 限制长度
    filename = f"{date_str}-{safe_title}.md"
    filepath = os.path.join(output_dir, filename)

    # 构建Markdown内容
    labels_str = ", ".join(article.get("labels", [])) or article.get("label", "")

    md_content = f"""# {title}

- 日期：{date_str}
- 链接：{article.get('url', '')}
- 标签：{labels_str}

---

{article.get('content', '')}
"""

    # 保存文件
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(md_content)

    print(f"  已保存: {filename}")


def get_articles_from_search(keyword: str) -> list[dict]:
    """通过搜索获取文章列表"""
    articles = []
    search_url = f"{BASE_URL}/search?q={keyword}"

    soup = get_page(search_url)
    if not soup:
        return articles

    # 查找搜索结果中的文章链接
    post_links = []

    for title_elem in soup.select(".post-title a, h3.post-title a, .entry-title a"):
        href = title_elem.get("href")
        if href:
            post_links.append((title_elem.get_text(strip=True), href))

    # 去重并筛选年份
    seen = set()
    for title, href in post_links:
        if href not in seen and ".html" in href:
            seen.add(href)
            date = parse_date_from_url(href)
            if is_in_year_range(date):
                articles.append({
                    "title": title,
                    "url": href,
                    "date": date,
                    "label": f"搜索:{keyword}",
                })
                print(f"  找到文章: {title} ({date.strftime('%Y-%m') if date else '未知日期'})")

    return articles


def main():
    """主函数"""
    print("=" * 60)
    print("槽边往事博客爬虫 - 推荐类文章")
    print(f"筛选年份: {YEAR_RANGE[0]} - {YEAR_RANGE[1]}")
    print("=" * 60)

    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 收集所有文章链接
    all_articles = []
    seen_urls = set()

    # 1. 从标签页获取文章
    for label, label_url in TARGET_LABELS.items():
        print(f"\n[标签: {label}]")
        articles = get_articles_from_label_page(label, label_url)
        for article in articles:
            if article["url"] not in seen_urls:
                seen_urls.add(article["url"])
                all_articles.append(article)
        delay()

    # 2. 通过搜索补充可能遗漏的文章
    for keyword in SEARCH_KEYWORDS:
        print(f"\n[搜索: {keyword}]")
        articles = get_articles_from_search(keyword)
        for article in articles:
            if article["url"] not in seen_urls:
                seen_urls.add(article["url"])
                all_articles.append(article)
                print(f"  新增文章: {article['title']}")
        delay()

    # 3. 添加已知的文章URL
    print(f"\n[已知文章]")
    for url in KNOWN_ARTICLES:
        if url not in seen_urls:
            date = parse_date_from_url(url)
            if is_in_year_range(date):
                seen_urls.add(url)
                all_articles.append({
                    "title": "待获取",
                    "url": url,
                    "date": date,
                    "label": "已知文章",
                })
                print(f"  添加: {url}")

    print(f"\n共找到 {len(all_articles)} 篇符合条件的文章")

    if not all_articles:
        print("未找到符合条件的文章，程序退出")
        return

    # 爬取每篇文章的完整内容
    print("\n" + "=" * 60)
    print("开始爬取文章内容...")
    print("=" * 60)

    saved_count = 0
    for i, article in enumerate(all_articles, 1):
        print(f"\n[{i}/{len(all_articles)}] {article['title']}")

        full_article = extract_article_content(article["url"])
        if full_article:
            # 合并信息
            full_article["label"] = article["label"]
            if not full_article.get("title"):
                full_article["title"] = article["title"]
            if not full_article.get("date"):
                full_article["date"] = article["date"]

            save_article_to_markdown(full_article, OUTPUT_DIR)
            saved_count += 1

        delay()

    print("\n" + "=" * 60)
    print(f"爬取完成！共保存 {saved_count} 篇文章到 {OUTPUT_DIR}/ 目录")
    print("=" * 60)


if __name__ == "__main__":
    main()
