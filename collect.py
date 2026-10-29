"""① 수집 노드 — Contemporary Classical Music (해외 RSS 피드).

참조: D00 뉴스레터 노드 완전판 13~14장. 노드 모양은 collect(state) -> {"collected", "log"}.
"""
import re
import sys
from datetime import datetime, timedelta, timezone

import feedparser
import requests

UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}

SOURCES = [
    ("I Care If You Listen", "https://icareifyoulisten.com/feed/"),
    ("Sequenza21",           "https://sequenza21.com/feed/"),
    ("VAN Magazine",         "https://van-magazine.com/feed/"),
    ("Guardian Classical",   "https://www.theguardian.com/music/classical-music-and-opera/rss"),
    ("Slipped Disc",         "https://slippedisc.com/feed/"),
]


# 일반 클래식 뉴스라 현대음악 글만 남겨야 하는 소스: 제목·요약에 키워드가 하나라도 있어야 통과
TOPIC_FILTER = {
    "Slipped Disc": re.compile(
        r"contemporary|new music|new work|world premiere|premiere[sd]?\b|composer|commission|"
        r"avant-garde|experimental|minimalis|electroacoustic|ensemble|festival|"
        r"\b(?:20th|21st)[- ]century|living composer",
        re.I),
}


def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "").strip()


def published_at(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None


def collect(s: dict) -> dict:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
    items, dead, empty, seen = [], [], [], set()
    filtered = 0
    for name, url in SOURCES:
        try:
            r = requests.get(url, headers=UA, timeout=20)
            r.raise_for_status()                    # 403·404·500도 실패로 잡는다
            feed = feedparser.parse(r.content)
        except Exception:
            dead.append(name)
            continue
        n_before = len(items)
        for e in feed.entries:
            at = published_at(e)
            if not at or at < cutoff:
                continue
            link = e.get("link", "")
            key = link.split("?")[0].rstrip("/")
            if not key or key in seen:
                continue
            summary = strip_tags(e.get("summary", ""))[:300]
            topic = TOPIC_FILTER.get(name)
            if topic and not topic.search(f"{e.get('title', '')} {summary}"):
                filtered += 1                       # 주제 밖이라 버린 글
                continue
            seen.add(key)
            items.append({"title": e.get("title", ""), "url": link, "source": name, "at": at,
                          "summary": summary})
        if len(items) == n_before:
            empty.append(name)                      # 응답은 왔지만 window 안에 0건
    log = f"① 수집   {s['hours']}시간 time window · {len(items)}건"
    if filtered:
        log += f" · 주제 필터로 {filtered}건 제외"
    if dead:
        log += f" · 응답 없음 {dead}"
    if empty:
        log += f" · 0건 {empty}"
    return {"collected": items, "log": [log]}


if __name__ == "__main__":
    hours = int(sys.argv[1]) if len(sys.argv) > 1 else 336
    out = collect({"hours": hours})
    for it in sorted(out["collected"], key=lambda x: x["at"], reverse=True):
        print(f"{it['at']:%m-%d %H:%M}  [{it['source']}] {it['title']}")
    print()
    print(out["log"][0])
