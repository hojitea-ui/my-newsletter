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
        error = f"{type(e).__name__}: {e}"
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
