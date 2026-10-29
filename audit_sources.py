"""소스 후보를 같은 기준으로 재서 채택·탈락의 근거 데이터를 만든다 → store/source_audit.json.

    python audit_sources.py          # 후보 전체를 측정하고 JSON 저장 + 마크다운 표 출력
    python audit_sources.py --table  # 저장된 JSON에서 표만 다시 출력

판정 기준(REPORT.md의 소스 채택표와 같다)
  A 수집 가능   HTTP 200이고 글이 1건 이상 파싱된다
  B 살아 있음   수집 time window(audience.yaml의 pipeline.hours, 기본 336시간) 안에 올라온 글이 1건 이상이다
  C 본문 추출   최신 글 3건 중 trafilatura로 뽑은 본문이 body_min_chars(600자) 이상인 것이 2건 이상이다
  D 주제 적합   현대음악 키워드가 제목·요약에 든 글의 비율. 30% 이상이면 채택, 10~30%면 조건부 채택(선별 단계가 거름), 10% 이하면 탈락
판정은 이 기준만으로 내리고, 지금 audience.yaml에서 쓰는지 여부는 '사용' 열에 따로 적는다(둘이 다르면 눈에 띄게).
LLM은 쓰지 않는다. 피드는 계속 바뀌므로 측정 시각을 함께 기록한다.
"""
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
import trafilatura

import graph as G

OUT = Path(__file__).parent / "store" / "source_audit.json"

# 같은 기준으로 재 볼 후보. 채택한 소스는 audience.yaml에서 읽고, 나머지는 검토했던 후보다.
CANDIDATES = [
    ("Slipped Disc", "https://slippedisc.com/feed/", "클래식 업계 뉴스(처음엔 주제 필터를 붙여 채택했다가 측정 후 제외)"),
    ("NewMusicBox", "https://nmbx.newmusicusa.org/feed/", "미국 New Music USA 매거진"),
    ("theartsdesk", "https://www.theartsdesk.com/rss.xml", "영국 공연 리뷰"),
    ("Gramophone", "https://www.gramophone.co.uk/feed", "영국 클래식 음반 매거진"),
    ("Bachtrack", "https://bachtrack.com/rss", "클래식 공연 리뷰 포털"),
    ("The WholeNote", "https://www.thewholenote.com/feed", "캐나다 음악 매거진"),
    ("Classical Music Daily", "https://www.classicalmusicdaily.com/rss.xml", "클래식 일간 블로그"),
    ("NPR Music", "https://feeds.npr.org/1019/rss.xml", "NPR 음악 전체(장르 무관)"),
    ("NYT Arts·Music", "https://www.nytimes.com/svc/collections/v1/publish/https://www.nytimes.com/section/arts/music/rss.xml", "뉴욕타임스 음악 섹션(장르 무관)"),
]
# 국내 후보: 피드가 있는지부터 재야 해서 홈페이지에서 RSS·Atom 링크를 찾는다
DOMESTIC = [
    ("서울시립교향악단", "https://www.seoulphil.or.kr"),
    ("예술의전당", "https://www.sac.or.kr"),
    ("통영국제음악재단", "https://www.tongyeong.or.kr"),
]
CONTEMPORARY = re.compile(
    r"contemporary|new music|new work|world premiere|premiere[sd]?\b|composer|commission|avant-garde|experimental|"
    r"minimalis|electroacoustic|ensemble|living composer|\b(?:20th|21st)[- ]century", re.I)
RSS_LINK = re.compile(r'<link[^>]+type=["\']application/(?:rss|atom)\+xml["\'][^>]*>', re.I)


def hours_ago(dt):
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600 if dt else None


def measure_feed(name, url, note, adopted_cfg, window_h, min_chars):
    rec = {"name": name, "url": url, "note": note, "adopted_in_config": adopted_cfg is not None}
    try:
        r = requests.get(url, headers=G.UA, timeout=20)
        rec["http_status"] = r.status_code
        r.raise_for_status()
    except Exception as e:
        rec["http_status"] = rec.get("http_status")
        rec["error"] = type(e).__name__ if rec.get("http_status") is None else f"HTTP {rec['http_status']}"
        rec["A"] = False
        return rec
    feed = feedparser.parse(r.content)
    entries = feed.entries
    dates = [G.published_at(e) for e in entries]
    dated = [d for d in dates if d]
    rec["entries"] = len(entries)
    rec["A"] = len(entries) >= 1
    rec["newest_hours_ago"] = round(hours_ago(max(dated)), 1) if dated else None
    rec["oldest_hours_ago"] = round(hours_ago(min(dated)), 1) if dated else None
    rec["in_window"] = sum(1 for d in dated if hours_ago(d) <= window_h)
    span_days = (hours_ago(min(dated)) - (hours_ago(max(dated)) or 0)) / 24 if len(dated) > 1 else None
    rec["posts_per_week"] = round(len(dated) / span_days * 7, 1) if span_days and span_days > 0 else None
    summ = [len(G.strip_tags(e.get("summary", ""))) for e in entries]
    rec["rss_summary_avg_chars"] = round(sum(summ) / len(summ)) if summ else None
    texts = [f"{e.get('title', '')} {G.strip_tags(e.get('summary', ''))[:300]}" for e in entries]
    fit = sum(1 for t in texts if CONTEMPORARY.search(t)) / len(texts) if texts else 0
    rec["contemporary_fit"] = round(fit, 2)
    pol = re.compile(r"\b(?:" + "|".join(G.load_config()["exclusion_rules"]["politics"]) + r")\b", re.I)
    rec["political_rate"] = round(sum(1 for t in texts if pol.search(t)) / len(texts), 2) if texts else 0
    newest3 = [e for _, e in sorted(zip(dates, entries), key=lambda x: x[0] or datetime.min.replace(tzinfo=timezone.utc), reverse=True)][:3]
    lens = []
    for e in newest3:
        try:
            d0 = trafilatura.fetch_url(e.link)
            lens.append(len(trafilatura.extract(d0) or "") if d0 else 0)
        except Exception:
            lens.append(0)
    rec["body_chars_newest3"] = lens
    rec["B"] = rec["in_window"] >= 1
    rec["C"] = sum(1 for n in lens if n >= min_chars) >= 2
    rec["D"] = fit >= 0.30
    return rec


def verdict(rec):
    if rec.get("A") is False:
        return "탈락", "피드가 열리지 않음(A 미달)"
    if not rec["B"]:
        return "탈락", "최근 2주 안에 새 글이 없음(B 미달)"
    if not rec["C"]:
        return "탈락", "본문 추출이 안 됨(C 미달)"
    fit = rec["contemporary_fit"]
    if rec["D"]:
        return "채택", "A·B·C·D 모두 통과"
    if fit > 0.10:
        return "조건부 채택", f"A·B·C 통과, 현대음악 적합 {int(fit * 100)}%(10~30%)라 선별 단계의 LLM 판단으로 거름"
    return "탈락", f"현대음악 적합 {int(fit * 100)}%(10% 이하)라 일반 뉴스로 봄"


def audit_domestic(name, home):
    rec = {"name": name, "url": home, "kind": "domestic"}
    try:
        r = requests.get(home, headers=G.UA, timeout=20)
        rec["http_status"] = r.status_code
        links = RSS_LINK.findall(r.text)
        rec["rss_links_found"] = len(links)
    except Exception as e:
        rec["http_status"] = None
        rec["error"] = type(e).__name__
        rec["rss_links_found"] = 0
    return rec


def table(data):
    rows = ["| 소스 | HTTP | 글 수 | 최신 글 | 2주 내 | 본문(최신3, 자) | 현대음악 적합 | 판정 | 사용 | 근거 |", "|---|---|---|---|---|---|---|---|---|---|"]
    for r in data["feeds"]:
        age = r.get("newest_hours_ago")
        age_s = "-" if age is None else (f"{age:.0f}시간 전" if age < 72 else f"{age / 24:.0f}일 전")
        st = r.get("http_status") if r.get("http_status") else r.get("error", "-")
        used = "사용 중" if r["adopted_in_config"] else "-"
        rows.append(f"| {r['name']} | {st} | {r.get('entries', '-')} | {age_s} | {r.get('in_window', '-')} | "
                    f"{'/'.join(str(x) for x in r.get('body_chars_newest3', [])) or '-'} | "
                    f"{int(r['contemporary_fit'] * 100)}% | **{r['verdict']}** | {used} | {r['reason']} |" if "contemporary_fit" in r else
                    f"| {r['name']} | {st} | - | - | - | - | - | **{r['verdict']}** | {used} | {r['reason']} |")
    rows += ["", "| 국내 후보 | HTTP | 홈페이지에서 찾은 RSS·Atom 링크 |", "|---|---|---|"]
    for r in data["domestic"]:
        rows.append(f"| {r['name']} | {r.get('http_status') or r.get('error')} | {r['rss_links_found']}개 |")
    return "\n".join(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", action="store_true", help="저장된 JSON에서 표만 출력")
    args = ap.parse_args()
    if args.table:
        print(table(json.loads(OUT.read_text(encoding="utf-8"))))
        return 0
    cfg = G.load_config()
    window, min_chars = cfg["pipeline"]["hours"], cfg["pipeline"]["body_min_chars"]
    feeds = []
    for s in cfg["sources"]:
        print("측정:", s["name"], file=sys.stderr)
        feeds.append(measure_feed(s["name"], s["url"], "채택", s, window, min_chars))
    for name, url, note in CANDIDATES:
        print("측정:", name, file=sys.stderr)
        feeds.append(measure_feed(name, url, note, None, window, min_chars))
    for r in feeds:
        r["verdict"], r["reason"] = verdict(r)
    data = {"measured_at": datetime.now().isoformat(timespec="seconds"), "window_hours": window, "body_min_chars": min_chars,
            "criteria": {"A": "HTTP 200이고 글 1건 이상", "B": f"최근 {window}시간 안에 새 글 1건 이상",
                         "C": f"최신 글 3건 중 본문 {min_chars}자 이상이 2건 이상", "D": "현대음악 키워드 적합률 30% 이상"},
           "feeds": feeds, "domestic": [audit_domestic(n, h) for n, h in DOMESTIC]}
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(json.dumps(data, ensure_ascii=False, indent=1), encoding="utf-8")
    print(table(data))
    return 0


if __name__ == "__main__":
    sys.exit(main())
