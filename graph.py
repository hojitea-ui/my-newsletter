"""현대음악 뉴스레터 에이전트 — LangGraph 워크플로우 메인 로직

START → collect → select ─┬→ report ×N (기사마다 병렬) ─┐
                          └→ (선별 0건이면 바로) ───────┴→ verify → publish → END

노드                       하는 일
① collect  자료 수집      RSS 5곳에서 time window 안의 글을 모으고, 같은 주소는 한 번만 남긴다
② select   중요도 선별    예선(묶음마다 N건) → 본선(한 화면 비교)으로 5건, 같은 사건·매체 쏠림을 막는다
③ report   취재·요약      본문을 읽고 headline·summary·why 세 칸으로 요약, 형식은 코드로 검사
④ verify   자동 검수      요약이 원문에 근거하는지 대조, 통과 못 하면 버리고 사유를 남긴다
⑤ publish  최종 발행      검수 통과분만 Discord로 보낸다(기본은 보내지 않음)

기준·수치·소스는 audience.yaml에서 읽는다. 단계별 통계는 State의 stats에 모아 run.py가 store/metrics.jsonl에 남긴다.
"""
import json
import operator
import os
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, TypedDict

import feedparser
import requests
import trafilatura
import yaml
from langgraph.graph import END, START, StateGraph
from langgraph.types import Send
from openai import OpenAI
from pydantic import BaseModel, Field

UA = {"User-Agent": "Mozilla/5.0 (newsletter-agent-course)"}
DEFAULT_CONFIG = Path(__file__).with_name("audience.yaml")


# ── 설정 ────────────────────────────────────────────────────────
def load_config(path=DEFAULT_CONFIG) -> dict:
    return yaml.safe_load(Path(path).read_text(encoding="utf-8"))


def make_client() -> OpenAI:
    # 키는 환경변수 OPENAI_API_KEY에서 읽는다. brotli 응답 오류를 피하려고 gzip만 받는다
    return OpenAI(default_headers={"Accept-Encoding": "gzip, deflate"})


# ── 이미 보낸 기사 기록(중복 발행 방지) ─────────────────────────────
SENT_LOG = Path(__file__).parent / "store" / "sent.jsonl"


def _key(url: str) -> str:
    return url.split("?")[0].rstrip("/")                # 추적용 꼬리표를 떼고 같은 글인지 비교


def load_sent(path=None) -> set:
    """이미 발행한 기사 주소(정규화) 집합. 기록 파일이 없으면 빈 집합."""
    path = Path(path or SENT_LOG)
    if not path.exists():
        return set()
    out = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.add(_key(json.loads(line)["url"]))
        except Exception:                               # 깨진 줄은 건너뛴다
            continue
    return out


def mark_sent(articles: list, path=None) -> None:
    """실제로 발송에 성공한 기사만 기록한다(dry-run은 기록하지 않는다)."""
    path = Path(path or SENT_LOG)
    path.parent.mkdir(exist_ok=True)
    at = datetime.now().isoformat(timespec="seconds")
    with path.open("a", encoding="utf-8") as f:
        for a in articles:
            f.write(json.dumps({"url": a["url"], "headline": a["headline"], "sent_at": at}, ensure_ascii=False) + "\n")


# ── State ───────────────────────────────────────────────────────
def _merge(a: dict, b: dict) -> dict:
    return {**(a or {}), **(b or {})}


class Brief(TypedDict):
    hours:     int                                  # 수집 time window(시간)
    collected: list                                 # ① 수집한 기사
    picked:    list                                 # ② 선별해 남긴 기사
    drafted:   Annotated[list, operator.add]        # ③ 취재한 초안 — 워커들이 나눠 채운다
    verified:  list                                 # ④ 검수를 통과한 것(줄이는 칸이라 리듀서 없음)
    excluded:  Annotated[list, operator.add]        # ③에서 본문 부족·형식 위반으로 빠진 기사
    stats:     Annotated[dict, _merge]              # 단계별 통계(store/metrics.jsonl에 기록)
    log:       Annotated[list, operator.add]        # 무슨 일이 있었는지


class ReportIn(TypedDict):                          # report 워커가 받는 것은 기사 하나뿐
    item: dict


class Pick(BaseModel):
    index:  int = Field(description="후보 목록에서의 번호")
    title:  str = Field(description="고른 후보의 제목을 목록에 적힌 그대로 복사")
    reason: str = Field(description="왜 골랐는지 한국어 한 문장")
    event:  str = Field(description="이 기사가 다루는 사건을 짧은 한국어 라벨로. 같은 사건이면 같은 라벨")


class Shortlist(BaseModel):
    picks: list[Pick]


class Draft(BaseModel):
    headline: str = Field(description="한국어 헤드라인. 30자 이내, 마침표 없음. 무슨 일이 있었는지(공연·초연·발표 등)를 담고 '요약'·'후기'는 쓰지 않음")
    summary:  str = Field(description="정확히 세 문장. 문장마다 ~습니다/~합니다로 끝나는 합니다체. 과장 없이 건조하게")
    why:      str = Field(description="현대음악을 챙겨 보는 독자에게 왜 중요한지 정확히 한 문장, 80자 이내")


class Verdict(BaseModel):
    ok:       bool      = Field(description="요약이 원문에 근거하면 true")
    problems: list[str] = Field(description="원문에 근거가 없거나 원문과 다른 부분. 없으면 빈 목록")


# ── 순수 함수(LLM·네트워크 없이 시험할 수 있다) ───────────────────
def strip_tags(s):
    return re.sub(r"<[^>]+>", "", s or "").strip()


def published_at(entry):
    t = entry.get("published_parsed") or entry.get("updated_parsed")
    return datetime(*t[:6], tzinfo=timezone.utc) if t else None


SENT_END = re.compile(r"(?:다|요|죠)[.!?]['’”\"]?(?=\s|$)")     # 문장 끝(No. 2 같은 약어의 점은 세지 않는다)
FORMAL_END = re.compile(r"(?<!니)다[.!?]['’”\"]?(?=\s|$)")       # ~했다·~이다처럼 합니다체가 아닌 끝


def check_draft(d) -> list:
    """요약 형식 규칙을 어긴 항목 목록. 비어 있으면 통과. (프롬프트는 부탁이라 코드로 검사한다)"""
    issues = []
    if not re.search(r"[가-힣]", d.headline + d.summary):
        issues.append("한국어가 아님")
    if len(d.headline) > 40:                                    # 30자 이내 요청, 10자는 여유(영어 인명이 길다)
        issues.append(f"headline {len(d.headline)}자(30자 이내여야 함)")
    if re.search(r"요약|후기", d.headline):                       # 정보 없는 헤드라인('…공연 리뷰 요약')
        issues.append("headline에 '요약'·'후기'가 있음(무슨 일이 있었는지를 쓸 것)")
    n = len(SENT_END.findall(d.summary))
    if n != 3:
        issues.append(f"summary {n}문장(정확히 3문장이어야 함)")
    if FORMAL_END.search(d.summary):
        issues.append("summary가 합니다체가 아님(~했다·~이다 금지)")
    n_why = len(SENT_END.findall(d.why))
    if n_why != 1 or len(d.why) > 90:                           # 80자 이내 요청, 10자는 여유
        issues.append(f"why {n_why}문장·{len(d.why)}자(한 문장, 80자 이내여야 함)")
    return issues


def year_check(d: dict) -> list:
    """요약의 4자리 연도가 원문에 없으면 그 목록. 연도는 영어·한국어 모두 아라비아 숫자라 번역 오탐이 거의 없다.
    '1960년대'·'1960~70년대' 같은 연대 표현은, 그 연대 안의 연도(1969, 1970 등)가 원문에 있으면 통과시킨다."""
    text = d["headline"] + " " + d["summary"]
    body_years = {int(x) for x in re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", d["body"])}
    bad = []
    for m in re.finditer(r"(?<!\d)((?:19|20)\d{2})(?!\d)", text):
        y = int(m.group(1))
        if y in body_years:
            continue
        decade = re.match(r"(?:~(\d{2}))?년대", text[m.end():m.end() + 8])
        if decade:
            end = y + 9 if not decade.group(1) else (y // 100) * 100 + int(decade.group(1)) + 9
            if any(y <= by <= end for by in body_years):
                continue
        bad.append(str(y))
    return bad


def build_embeds(cfg: dict, run_id: str, lead: str, articles: list) -> list:
    """Discord embed 카드. 한도(카드 10개, 제목 256자, 본문 4096자, 합계 6000자)를 넘으면 보내기 전에 자른다."""
    DEFAULT = 0x5F7476
    TITLE_MAX, DESC_MAX, EMBED_MAX, TOTAL_MAX = 256, 4096, 10, 5800
    if not articles:                                    # 조용한 날에도 한 장은 보낸다
        return [{"title": f"🗞️ {run_id}", "color": DEFAULT, "description": "오늘은 조용합니다."}]
    suffix = cfg["publish"]["title_suffix"]
    embeds = [{"title": f"🗞️ {run_id} · {suffix}", "description": lead, "color": DEFAULT}]
    for i, a in enumerate(articles, 1):
        desc = a["summary"] + (f"\n\n💡 **{a['why']}**" if a.get("why") else "")
        embeds.append({
            "title": f"{i}. {a['headline']}"[:TITLE_MAX],
            "description": desc[:DESC_MAX],
            "url": a["url"],
            "color": DEFAULT,
            "footer": {"text": f"{a['source']} · {a['when']}"},
        })
    total = lambda es: sum(len(e.get("title", "")) + len(e.get("description", ""))
                           + len(e.get("footer", {}).get("text", "")) for e in es)
    while len(embeds) > EMBED_MAX or total(embeds) > TOTAL_MAX:
        embeds.pop()                                    # 뒤에서부터 덜어낸다
    return embeds


def send(cfg: dict, run_id: str, lead: str, articles: list, webhook=None, dry_run=True):
    """반환: None=보내지 않음(dry-run), True=성공, False=실패. (실패를 dry-run으로 표시하지 않는다)"""
    payload = {"username": cfg["publish"]["username"], "embeds": build_embeds(cfg, run_id, lead, articles)}
    if dry_run or not webhook:                          # 웹훅이 없어도 보내지 않는다
        print(f"[dry-run] embed {len(payload['embeds'])}개 · {len(json.dumps(payload, ensure_ascii=False))}자 — 보내지 않음")
        return None
    r = requests.post(webhook, json=payload, timeout=20)
    ok = r.status_code in (200, 204)
    print("발행:", "성공" if ok else f"실패 {r.status_code} {r.text[:120]}")
    return ok


# ── 그래프 ──────────────────────────────────────────────────────
def build_graph(cfg: dict = None, client: OpenAI = None):
    """설정과 LLM 클라이언트를 받아 컴파일된 LangGraph를 돌려준다."""
    cfg = cfg or load_config()
    client = client or make_client()
    P = cfg["pipeline"]
    model = P["model"]

    politics = re.compile(r"\b(?:" + "|".join(cfg["exclusion_rules"]["politics"]) + r")\b", re.I)
    topic_filters = {s["name"]: re.compile("|".join(s["topic_keywords"]), re.I)
                     for s in cfg["sources"] if s.get("topic_keywords")}

    body_th = cfg["exclusion_rules"].get("body_hits_threshold", 3)
    body_cache = {}                                     # 선별에서 읽은 본문을 취재에서 다시 쓴다

    def get_body(url):
        if url not in body_cache:
            d0 = trafilatura.fetch_url(url)
            body_cache[url] = trafilatura.extract(d0) if d0 else None
        return body_cache[url]

    imp = cfg["importance"]
    criteria = (f"독자는 {cfg['audience']['persona'].strip()}\n"
                + "".join(f"- {x}\n" for x in imp["include"])
                + "버릴 것: " + ", ".join(imp["exclude"]))
    sys_report = ("당신은 현대음악(Contemporary Classical Music) 뉴스레터 기자입니다.\n"
                  "아래 기사 본문을 읽고 헤드라인·요약·왜 중요한지를 한국어로 쓰세요.\n"
                  "본문에 없는 사실·숫자·평가는 쓰지 마세요.\n"
                  "'주목된다·기대를 모은다' 같은 기자체 표현은 쓰지 마세요.\n"
                  "형식 규칙(반드시 지키세요):\n"
                  "- headline: 30자 이내, 마침표 없음, 무슨 일이 있었는지(공연·초연·발표 등)를 담고 '요약'·'후기'·'리뷰 요약' 같은 군더더기는 쓰지 않음\n"
                  "- summary: 정확히 세 문장, 모든 문장이 '~습니다/~합니다'로 끝남(~했다·~이다 금지)\n"
                  "- why: 정확히 한 문장, 80자 이내\n"
                  "- 작품명·인명은 원문 표기 그대로 두세요")
    sys_check = ("요약이 원문에서 뒷받침되는지 판정하세요.\n"
                 "헤드라인과 요약만 보고 판단하고, 번역이나 단위 환산은 문제가 아닙니다.\n"
                 "원문에 없는 연도·숫자·사실, 원문과 다르게 옮긴 부분이 있으면 problems에 한 줄씩 적고 ok를 false로 하세요.")

    # ① 자료 수집 ───────────────────────────────────────────────
    def collect(s: dict) -> dict:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=s["hours"])
        items, dead, empty, seen = [], [], [], set()
        per_source, filtered = {}, 0
        sent, already = load_sent(), 0                  # 이미 보낸 기사는 다시 모으지 않는다
        for src in cfg["sources"]:
            name, url = src["name"], src["url"]
            try:
                r = requests.get(url, headers=UA, timeout=20)
                r.raise_for_status()                    # 403·404·500도 실패로 잡는다
                feed = feedparser.parse(r.content)
            except Exception:
                dead.append(name)                       # 한 곳이 죽어도 나머지는 계속
                continue
            n_before = len(items)
            for e in feed.entries:
                at = published_at(e)
                if not at or at < cutoff:               # time window 밖이거나 날짜가 없으면 버린다
                    continue
                key = _key(e.link)
                if key in seen:
                    continue
                if key in sent:
                    already += 1                        # 이미 발행한 글
                    continue
                summary = strip_tags(e.get("summary", ""))[:300]
                topic = topic_filters.get(name)
                if topic and not topic.search(f"{e.title} {summary}"):
                    filtered += 1                       # 주제 밖이라 버린 글
                    continue
                seen.add(key)
                items.append({"title": e.title, "url": e.link, "source": name, "at": at, "summary": summary})
            per_source[name] = len(items) - n_before
            if len(items) == n_before:
                empty.append(name)                      # 응답은 왔지만 window 안에 0건
        log = (f"① 수집   {s['hours']}시간 time window · {len(items)}건"
               + (f" · 주제 필터로 {filtered}건 제외" if filtered else "")
               + (f" · 이미 발행 {already}건 제외" if already else "")
               + (f" · 응답 없음 {dead}" if dead else "")
               + (f" · 0건 {empty}" if empty else ""))
        return {"collected": items, "log": [log],
                "stats": {"collect": {"window_hours": s["hours"], "collected": len(items), "per_source": per_source,
                                      "topic_filtered": filtered, "already_sent": already,
                                      "dead_sources": dead, "empty_sources": empty}}}

    # ② 중요도 선별 ─────────────────────────────────────────────
    def ask_picks(items, n):
        listing = "\n".join(f"{i}. [{it['source']}] {it['title']}" for i, it in enumerate(items))
        system = (f"{criteria}\n\n아래 목록에서 중요한 순서대로 {n}건을 고르세요.\n"
                  "reason은 한국어 한 문장으로 쓰세요.\n"
                  "같은 사건을 다룬 기사에는 같은 event 라벨을 붙이세요.")
        out = client.chat.completions.parse(
            model=model, temperature=0,
            messages=[{"role": "system", "content": system}, {"role": "user", "content": listing}],
            response_format=Shortlist).choices[0].message.parsed
        by_title = {it["title"]: i for i, it in enumerate(items)}
        seen, picks = set(), []
        for p in out.picks:                             # 없는 번호·겹친 번호는 버린다
            p.index = by_title.get(p.title.strip(), p.index)    # 제목이 일치하면 제목을 믿는다(번호는 가끔 어긋난다)
            if 0 <= p.index < len(items) and p.index not in seen:
                seen.add(p.index)
                picks.append(p)
        return picks

    def select(s: dict) -> dict:
        is_political = lambda it: bool(politics.search(f"{it['title']} {it['summary']}"))
        items = [it for it in s["collected"] if not is_political(it)]
        skipped = [it["title"] for it in s["collected"] if is_political(it)]
        BATCH, PASS, TARGET, PER_SOURCE = P["batch"], P["pass_per_batch"], P["target"], P["per_source"]
        if not items:
            return {"picked": [], "log": ["② 선별   0 → 0건" + (f" · 정치성 {len(skipped)}건 제외" if skipped else "")],
                    "stats": {"select": {"candidates": 0, "political_skipped": skipped, "picked": []}}}
        survivors, gates = [], []
        for i in range(0, len(items), BATCH):           # 예선 — 묶음마다 PASS건
            chunk = items[i:i + BATCH]
            ranked = ask_picks(chunk, PASS + 1)         # PASS+1등까지 받아 통과선과 첫 탈락을 남긴다
            survivors += [chunk[p.index] for p in ranked[:PASS]]
            gates.append({"pass_last": chunk[ranked[PASS - 1].index]["title"] if len(ranked) >= PASS else None,
                          "first_out": chunk[ranked[PASS].index]["title"] if len(ranked) > PASS else None})
        ranked = ask_picks(survivors, len(survivors))   # 본선 — 한 화면에 놓고 전부 순위(건너뛴 자리를 채울 예비)
        seen_events, per_src, finals, dropped, body_skipped = set(), Counter(), [], [], []

        def political_body(p):
            """최종 후보의 본문을 직접 읽어 정치 낱말이 body_th번 이상이면 그 목록(정치 중심 기사). 아니면 빈 목록."""
            it = survivors[p.index]
            hits = politics.findall(get_body(it["url"]) or "")
            if len(hits) >= body_th:
                body_skipped.append({"title": it["title"], "hits": len(hits), "words": sorted({h.lower() for h in hits})[:6]})
                return hits
            return []

        for p in ranked:
            label, src = p.event.strip().lower(), survivors[p.index]["source"]
            if len(finals) >= TARGET:
                dropped.append((p, "순위 밖"))
            elif label in seen_events:                  # 같은 사건은 한 건만
                dropped.append((p, "같은 사건"))
            elif per_src[src] >= PER_SOURCE:            # 한 매체가 자리를 다 차지하지 못하게
                dropped.append((p, "매체 상한"))
            elif (hits := political_body(p)):           # 본문이 정치 중심이면 건너뛰고 다음 순위로 채운다
                dropped.append((p, f"정치성 본문({len(hits)}회)"))
            else:
                seen_events.add(label)
                per_src[src] += 1
                finals.append(p)
        relaxed = 0
        for item in list(dropped):                      # 상한 때문에 TARGET건이 안 되면, 상한에 밀린 것부터 순위대로 채운다
            if len(finals) >= TARGET:
                break
            if item[1] == "매체 상한" and not political_body(item[0]):
                dropped.remove(item)
                finals.append(item[0])
                relaxed += 1
        finals.sort(key=ranked.index)
        picked = [{**survivors[p.index], "event": p.event, "reason": p.reason,
                   "body": body_cache.get(survivors[p.index]["url"])} for p in finals]
        ratio = len(finals) / len(survivors)
        log = (f"② 선별   {len(items)} → 예선 {len(survivors)} → {len(finals)}건 · 발행 비율 {ratio:.2f}"
               + (f" · 정치성 {len(skipped)}건 제외" if skipped else "")
               + (f" · 정치성 본문 {len(body_skipped)}건 제외" if body_skipped else "")
               + (f" · 매체 상한 완화 {relaxed}건" if relaxed else "")
               + (f" · {TARGET}건 미달({len(finals)}건)" if len(finals) < TARGET else ""))
        return {"picked": picked, "log": [log],
                "stats": {"select": {
                    "candidates": len(items), "political_skipped": skipped, "political_body_skipped": body_skipped,
                    "survivors": len(survivors),
                    "picked_count": len(finals), "ratio": round(ratio, 2), "relaxed": relaxed, "gates": gates,
                    "picked": [{"title": x["title"], "source": x["source"], "event": x["event"], "reason": x["reason"]} for x in picked],
                    "dropped": [{"title": survivors[p.index]["title"], "event": p.event, "reason": p.reason, "why": why}
                                for p, why in dropped]}}}

    # ③ 취재·요약 ───────────────────────────────────────────────
    def draft(body):
        """형식 검사를 통과할 때까지 최대 max_draft_tries번. 반환: (초안 또는 None, 마지막 위반 목록)"""
        sys_msg, issues = sys_report, []
        for k in range(P["max_draft_tries"]):
            d = client.chat.completions.parse(
                model=model, temperature=0 if k == 0 else 0.4,   # 재시도는 같은 답이 반복되지 않게 온도를 올린다
                messages=[{"role": "system", "content": sys_msg}, {"role": "user", "content": body[:P["body_max_chars"]]}],
                response_format=Draft).choices[0].message.parsed
            issues = check_draft(d)
            if not issues:
                return d, []
            sys_msg = sys_report + "\n\n앞선 답이 규칙을 어겼습니다: " + "; ".join(issues) + ". 고쳐서 다시 쓰세요."
        if len(issues) == 1 and issues[0].startswith("headline"):    # 헤드라인만 문제면 그 칸만 따로 고친다
            for _ in range(2):
                d.headline = client.chat.completions.create(
                    model=model, temperature=0, max_tokens=60,
                    messages=[{"role": "system", "content": "한국어 헤드라인을 뜻을 유지하며 25자 이내로 고치세요. "
                                                            "인명·작품명은 줄여도 되고, '요약'·'후기' 같은 말은 빼고 "
                                                            "무슨 일이 있었는지가 드러나게 하세요. 헤드라인만 답하세요."},
                              {"role": "user", "content": d.headline}]).choices[0].message.content.strip().rstrip(".")
                if not check_draft(d):
                    return d, []
        return None, issues

    def fan_report(s: dict):                            # 기사 수만큼 워커를 펼친다(fan-out)
        return [Send("report", {"item": it}) for it in s["picked"]] or "verify"   # 0건인 날에도 verify로 간다

    def report(s: ReportIn) -> dict:                    # 기사 한 건을 맡는다
        it = s["item"]
        body = it.get("body") or get_body(it["url"])     # 선별에서 읽은 본문이 있으면 다시 받지 않는다
        if not body or len(body) < P["body_min_chars"]:
            reason = f"본문 {len(body or '')}자(기준 {P['body_min_chars']}자 미만)"
            return {"drafted": [], "excluded": [{"source": it["source"], "title": it["title"], "reason": reason}],
                    "log": [f"   취재 제외 {it['source']} · {reason}"]}
        d, issues = draft(body)
        if d is None:                                   # 여러 번 써도 규칙을 못 지키면 빠진 이름을 남긴다
            return {"drafted": [], "excluded": [{"source": it["source"], "title": it["title"], "reason": f"요약 형식 위반 {issues}"}],
                    "log": [f"   취재 제외 {it['source']} · 요약 형식 위반 {issues}"]}
        return {"drafted": [{**it, "body": body[:P["body_max_chars"]], **d.model_dump()}]}

    # ④ 자동 검수 ───────────────────────────────────────────────
    def check(d: dict) -> Verdict:                      # 요약을 쓴 호출과 따로 부른다
        user = f"[원문]\n{d['body'][:P['body_max_chars']]}\n\n[헤드라인]\n{d['headline']}\n\n[요약]\n{d['summary']}"
        return client.chat.completions.parse(
            model=model, temperature=0,
            messages=[{"role": "system", "content": sys_check}, {"role": "user", "content": user}],
            response_format=Verdict).choices[0].message.parsed

    def verify(s: dict) -> dict:
        kept, notes, failed = [], [], []
        for d in s["drafted"]:
            bad_years = year_check(d)                   # 비용 0인 검사를 먼저
            if bad_years:
                notes.append(f"   불합격 {d['source']} · 원문에 없는 연도 {bad_years}")
                failed.append({"source": d["source"], "title": d["title"], "check": "year", "problems": bad_years})
                continue
            try:
                v = check(d)
            except Exception as e:                      # 판정이 실패한 기사는 보내지 않고 이름을 남긴다
                notes.append(f"   불합격 {d['source']} · 검수 호출 실패({type(e).__name__})")
                failed.append({"source": d["source"], "title": d["title"], "check": "call_error", "problems": [type(e).__name__]})
                continue
            if v.ok:
                kept.append(d)
            else:
                notes.append(f"   불합격 {d['source']} · {' / '.join(v.problems)[:120]}")
                failed.append({"source": d["source"], "title": d["title"], "check": "llm", "problems": v.problems})
        if s["drafted"] and all(f["check"] == "call_error" for f in failed) and len(failed) == len(s["drafted"]):
            # 판정을 한 건도 못 했다(API 오류·크레딧 소진 등). '오늘은 조용합니다'로 보내면 소식이 없는 날과 구분이 안 되니
            # 발송하지 않고 실패로 끝낸다. 보내지 못한 기사는 sent.jsonl에 없으므로 다음 실행에서 다시 고른다.
            raise RuntimeError(f"검수 호출이 모두 실패했습니다({failed[0]['problems'][0]}). 발송하지 않고 중단합니다.")
        return {"verified": kept,
                "log": [f"④ 검수   {len(s['drafted'])} → {len(kept)}건 · 불합격 {len(failed)}건"] + notes,
                "stats": {"verify": {"drafted": len(s["drafted"]), "verified": len(kept), "verify_fail": len(failed), "failed": failed}}}

    # ⑤ 최종 발행 ───────────────────────────────────────────────
    def publish(s: dict) -> dict:
        arts = [{"headline": a["headline"], "summary": a["summary"], "why": a["why"], "url": a["url"],
                 "source": a["source"], "when": a["at"].strftime("%m-%d %H:%M")} for a in s["verified"]]   # body는 빼고 필요한 칸만
        today = datetime.now().strftime("%Y-%m-%d")
        srcs = ", ".join(dict.fromkeys(a["source"] for a in arts))
        lead = f"오늘은 {len(arts)}건을 골랐습니다. ({srcs})" if arts else ""
        dry_run = os.environ.get("DRY_RUN", "1") == "1"                  # 기본은 보내지 않음
        sent = send(cfg, today, lead, arts, webhook=os.environ.get("DISCORD_WEBHOOK_URL"), dry_run=dry_run)
        if sent is True and arts:                        # 실제로 나간 기사만 '보낸 기사'로 기록 → 다음 실행에서 건너뛴다
            mark_sent(arts)
        state = "dry-run" if sent is None else ("sent" if sent else "failed")
        label = f"{len(arts)}건" if arts else "조용합니다"
        shown = {"dry-run": "dry-run", "sent": "보냄", "failed": "발송 실패 — 위 출력의 상태 코드 확인"}[state]
        return {"log": [f"⑤ 발행   {label} · {shown}"],
                "stats": {"publish": {"articles": len(arts), "state": state, "dry_run": dry_run}}}

    g = StateGraph(Brief)
    for name, fn in (("collect", collect), ("select", select), ("report", report), ("verify", verify), ("publish", publish)):
        g.add_node(name, fn)
    g.add_edge(START, "collect")
    g.add_edge("collect", "select")
    g.add_conditional_edges("select", fan_report, ["report", "verify"])
    g.add_edge("report", "verify")
    g.add_edge("verify", "publish")
    g.add_edge("publish", END)
    return g.compile()


def initial_state(cfg: dict = None) -> dict:
    cfg = cfg or load_config()
    return {"hours": cfg["pipeline"]["hours"], "collected": [], "picked": [], "drafted": [],
            "verified": [], "excluded": [], "stats": {}, "log": []}
