"""파이프라인 실행 스크립트.

    python run.py            # 수집→선별→취재→검수까지 돌리고 '보내지 않는다'(dry-run). 기본값
    python run.py --send     # 검수를 통과한 결과를 DISCORD_WEBHOOK_URL로 실제 발행
    python run.py --hours 72 # 수집 time window를 바꿔서(기본은 audience.yaml의 pipeline.hours)

안전장치: --send를 쓰지 않으면 .env에 DRY_RUN=0이 있어도 보내지 않는다.
실행할 때마다 단계별 수치와 검수 결과를 store/metrics.jsonl에 한 줄 남긴다.
"""
import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

import graph as G

ROOT = Path(__file__).parent
METRICS = ROOT / "store" / "metrics.jsonl"


def build_record(cfg: dict, final: dict, dry_run: bool, started: datetime, seconds: float, error: str = None) -> dict:
    """실행 한 번의 기록. 본문(body) 같은 큰 값과 웹훅 주소·키는 넣지 않는다."""
    st = final.get("stats", {}) if final else {}
    sel, ver, pub, col = st.get("select", {}), st.get("verify", {}), st.get("publish", {}), st.get("collect", {})
    p = cfg["pipeline"]
    return {
        "run_id": started.strftime("%Y-%m-%dT%H:%M:%S"),
        "dry_run": dry_run,
        "duration_sec": round(seconds, 1),
        "error": error,
        "config": {"window_hours": p["hours"], "batch": p["batch"], "pass_per_batch": p["pass_per_batch"],
                   "target": p["target"], "per_source": p["per_source"], "model": p["model"],
                   "sources": [s["name"] for s in cfg["sources"]]},
        # 각 단계를 지날 때마다 몇 건이 남았는지(깔때기)
        "funnel": {"collected": col.get("collected"), "after_politics_filter": sel.get("candidates"),
                   "preliminary_survivors": sel.get("survivors"), "picked": sel.get("picked_count"),
                   "drafted": ver.get("drafted"), "verified": ver.get("verified"), "published": pub.get("articles")},
        "collect": col,
        "select": {k: sel.get(k) for k in ("political_skipped", "political_body_skipped", "ratio", "relaxed", "gates", "picked", "dropped")},
        "report": {"excluded": final.get("excluded", []) if final else []},
        "verify": {"verify_fail": ver.get("verify_fail"), "failed": ver.get("failed", [])},
        "publish": pub,
        "result": [{"source": a["source"], "headline": a["headline"], "url": a["url"]} for a in (final or {}).get("verified", [])],
        "log": (final or {}).get("log", []),
    }


QUOTA_WORDS = ("spend limit", "insufficient_quota", "exceeded your current quota", "billing", "hard limit")


def _is(e: Exception, name: str) -> bool:
    return any(c.__name__ == name for c in type(e).__mro__)


def explain_error(e: Exception):
    """알려진 오류를 (제목, 한국어 안내 한 줄)로 바꾼다. 모르는 오류는 None → 원래 트레이스백을 그대로 보여 준다."""
    low = str(e).lower()
    if _is(e, "RateLimitError") or (isinstance(e, RuntimeError) and "RateLimitError" in str(e)):
        if any(w in low for w in QUOTA_WORDS):
            return ("OpenAI 한도 초과",
                    "OpenAI 사용 한도(크레딧 또는 월 한도)를 초과해 중단했습니다. 발송하지 않았고 발송 기록(sent.jsonl)도 바뀌지 않아 "
                    "한도를 풀면 다음 실행에서 이어서 처리합니다. platform.openai.com → Settings → Limits에서 한도와 사용량을 확인하세요.")
        return ("OpenAI 호출 거절(429)",
                "OpenAI가 요청을 거절했습니다(429). 한도 초과이거나 짧은 시간에 호출이 몰렸을 수 있습니다. 발송하지 않았고 발송 기록도 바뀌지 않았습니다. "
                "아래 원인 줄의 원문 메시지를 확인하세요.")
    if _is(e, "AuthenticationError") or _is(e, "PermissionDeniedError"):
        return ("OpenAI 키 오류",
                "OpenAI 키가 잘못됐거나 권한이 없습니다. GitHub Secrets의 OPENAI_API_KEY(로컬은 .env)를 확인하세요. 발송하지 않았습니다.")
    if _is(e, "APIConnectionError"):                       # 시간 초과(APITimeoutError)도 여기에 속한다
        return ("OpenAI 연결 실패", "OpenAI에 연결하지 못했습니다(네트워크 오류나 시간 초과). 잠시 뒤 다시 실행하세요. 발송하지 않았습니다.")
    if _is(e, "InternalServerError"):
        return ("OpenAI 서버 오류", "OpenAI 서버가 오류(5xx)를 냈습니다. 잠시 뒤 다시 실행하세요. 발송하지 않았습니다.")
    return None


def safe_error(e: Exception) -> str:
    """기록용 오류 문자열. 주소·키가 섞여 있을 수 있는 부분은 가리고 길이를 줄인다."""
    msg = re.sub(r"https?://\S+|/api/webhooks/\S+|sk-\S+", "***", str(e))
    return f"{type(e).__name__}: {msg}"[:300]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="현대음악 뉴스레터 에이전트 실행")
    ap.add_argument("--send", action="store_true", help="검수를 통과한 결과를 실제로 발행한다(기본은 보내지 않음)")
    ap.add_argument("--hours", type=int, help="수집 time window(시간). 기본은 audience.yaml 값")
    ap.add_argument("--no-metrics", action="store_true", help="store/metrics.jsonl에 기록하지 않는다")
    args = ap.parse_args(argv)

    load_dotenv(ROOT / ".env")
    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY가 없습니다. .env에 OPENAI_API_KEY=... 를 넣어 주세요.", file=sys.stderr)
        return 2
    os.environ["DRY_RUN"] = "0" if args.send else "1"      # --send가 있을 때만 실제 발행
    if args.send and not os.environ.get("DISCORD_WEBHOOK_URL"):
        print("--send인데 DISCORD_WEBHOOK_URL이 없습니다. .env에 넣어 주세요.", file=sys.stderr)
        return 2

    cfg = G.load_config()
    state = G.initial_state(cfg)
    if args.hours:
        state["hours"] = args.hours
        cfg["pipeline"]["hours"] = args.hours
    app = G.build_graph(cfg)

    started, t0, final, error = datetime.now(), time.time(), None, None
    try:
        final = app.invoke(state)
    except Exception as e:                                 # 죽은 실행도 기록에 남긴다
        error = safe_error(e)
        hint = explain_error(e)
        if hint:                                           # 알려진 오류는 긴 트레이스백 대신 한국어 안내 한 줄만 남긴다
            title, msg = hint
            if os.environ.get("GITHUB_ACTIONS") == "true":
                print(f"::error title={title}::{msg}")    # Actions 실행 요약 화면에 바로 보인다
            print(f"\n[실패] {title}: {msg}\n원인: {error}", file=sys.stderr)
            raise SystemExit(1) from None
        raise
    finally:
        if not args.no_metrics:
            METRICS.parent.mkdir(exist_ok=True)
            rec = build_record(cfg, final, not args.send, started, time.time() - t0, error)
            with METRICS.open("a", encoding="utf-8") as f:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    for line in final["log"]:
        print(line)
    print()
    for i, a in enumerate(final["verified"], 1):
        print(f"{i}. {a['headline']}  [{a['source']}]\n   {a['summary']}\n   💡 {a['why']}\n   {a['url']}\n")
    if not args.no_metrics:
        print(f"기록: {METRICS.relative_to(ROOT)} (+1줄)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
