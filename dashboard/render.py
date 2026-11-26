"""Render the daily ops dashboard as a static HTML page.

Reads the mart directly (kpi_daily, kpi_weekly, dq_daily) - no CSV in between - and bakes the
numbers into site/days/<date>.html. The archive (site/index.html) is rebuilt from the files on disk,
and the git history of site/ is the record of what was published each day.

    python -m dashboard.render --date 2020-12-17
    python -m dashboard.render --date 2021-01-26 --dataset ga4_mart_test      # incident replay
    python -m dashboard.render --date 2020-12-17 --blocked-reason "dbt test failed: dq6_segment_sums"
"""
import argparse
import math
import os
from datetime import date
from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account
from jinja2 import Environment, FileSystemLoader

PROJECT = "bigquery-457902"
ROOT = Path(__file__).resolve().parent.parent
TEMPLATES = Path(__file__).resolve().parent / "templates"


def client() -> bigquery.Client:
    creds = service_account.Credentials.from_service_account_file(os.environ["GCP_KEYFILE"])
    return bigquery.Client(project=PROJECT, credentials=creds, location="US")


def rows(bq: bigquery.Client, sql: str, **params) -> list[dict]:
    qp = [bigquery.ScalarQueryParameter(k, "DATE", v) for k, v in params.items()]
    job = bq.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=qp))
    return [dict(r) for r in job.result()]


def load(bq: bigquery.Client, dataset: str, d: date) -> dict:
    ds = f"`{PROJECT}.{dataset}`"
    kpi = rows(bq, f"SELECT * FROM {ds}.kpi_daily WHERE kpi_date = @d", d=d)
    trend = rows(bq, f"""
        SELECT kpi_date, revenue, revenue_avg7, orders, sessions, order_cr, aov, is_excluded_day
        FROM {ds}.kpi_daily
        WHERE segment_type = 'all' AND kpi_date BETWEEN DATE_SUB(@d, INTERVAL 27 DAY) AND @d
        ORDER BY kpi_date""", d=d)
    weekly = rows(bq, f"""
        SELECT week_start, week_end, days_covered, sessions, orders, revenue, order_cr, aov
        FROM {ds}.kpi_weekly
        WHERE segment_type = 'all' AND week_end < @d   -- completed weeks only: never show data after @d
        ORDER BY week_start DESC LIMIT 8""", d=d)
    dq = rows(bq, f"SELECT * FROM {ds}.dq_daily WHERE dq_date = @d ORDER BY check_id, check_name", d=d)
    return {"kpi": {(r["segment_type"], r["segment_value"]): r for r in kpi},
            "trend": trend, "weekly": list(reversed(weekly)), "dq": dq}


# ---------- formatting ----------
def money(v):
    return "–" if v is None else f"${v:,.0f}"


def pct(v, digits=1):
    return "–" if v is None else f"{v * 100:.{digits}f}%"


def delta(cur, base):
    """Relative change cur vs base. None when the comparison is not available."""
    if cur is None or base in (None, 0):
        return None
    return cur / base - 1


def delta_pp(cur, base):
    if cur is None or base is None:
        return None
    return (cur - base) * 100


# ---------- view model ----------
CHECK_LABELS = {
    "DQ1": "데이터 도착 / 이벤트 총량",
    "DQ2": "거래번호 없는 구매 세션 비율",
    "DQ3": "이벤트별 수집량",
    "DQ4": "주문 매출 대사",
    "DQ5": "다중 주문 세션 비율",
}
EVENT_LABELS = {
    "view_item_sessions_vs_7d": "상품 조회",
    "add_to_cart_sessions_vs_7d": "장바구니",
    "begin_checkout_sessions_vs_7d": "결제 시작",
    "purchase_sessions_vs_7d": "구매",
}
STATUS = {
    "ok": ("정상", "✓", "good"),
    "warn": ("경고", "!", "warning"),
    "block": ("차단", "✕", "critical"),
    "insufficient_baseline": ("기준 부족", "–", "muted"),
}


def build_view(data: dict, d: date, blocked_reason: str | None) -> dict:
    statuses = [r["status"] for r in data["dq"]]
    if blocked_reason or "block" in statuses or not data["kpi"]:
        overall = "block"
    elif "warn" in statuses:
        overall = "warn"
    else:
        overall = "ok"

    dq_rows = []
    for r in data["dq"]:
        name = CHECK_LABELS[r["check_id"]]
        if r["check_id"] == "DQ3":
            name += f" · {EVENT_LABELS.get(r['check_name'], r['check_name'])}"
        label, icon, tone = STATUS[r["status"]]
        dq_rows.append({"id": r["check_id"], "name": name, "value": r["value"],
                        "value_fmt": pct(r["value"]) if r["value"] is not None else "–",
                        "num": r["numerator"], "den": r["denominator"],
                        "label": label, "icon": icon, "tone": tone, "detail": r["detail"]})

    view = {"date": d, "weekday": "월화수목금토일"[d.weekday()], "overall": overall,
            "overall_label": STATUS[overall][0], "overall_icon": STATUS[overall][1],
            "overall_tone": STATUS[overall][2], "blocked_reason": blocked_reason,
            "dq": dq_rows, "warns": [r for r in dq_rows if r["tone"] == "warning"]}
    if overall == "block":
        view["blocks"] = [r for r in dq_rows if r["tone"] == "critical"]
        return view

    a = data["kpi"][("all", "all")]
    last_wd = "월화수목금토일"[d.weekday()]
    recent = data["trend"][-14:]
    view["tiles"] = [
        tile("매출", a["revenue"], a["revenue_wow"], a["revenue_avg7"], money, last_wd,
             spark(recent, "revenue"), hero=True),
        tile("주문", a["orders"], a["orders_wow"], a["orders_avg7"], lambda x: f"{x:,.0f}", last_wd,
             spark(recent, "orders")),
        tile("세션", a["sessions"], a["sessions_wow"], a["sessions_avg7"], lambda x: f"{x:,.0f}", last_wd,
             spark(recent, "sessions")),
        tile("주문 전환율", a["order_cr"], a["order_cr_wow"], a["order_cr_avg7"], lambda x: pct(x, 2), last_wd,
             spark(recent, "order_cr"), rate=True),
        tile("객단가", a["aov"], a["aov_wow"], a["aov_avg7"], money, last_wd, spark(recent, "aov")),
    ]

    # 매출 = 세션 × 전환율 × 객단가 → ln(매출 변화) = 세 항의 ln 변화의 합 (7일 평균 대비, 정확히 분해됨)
    factors = []
    if a["revenue_avg7"]:
        for key, label, cur, base in [
            ("sessions", "세션", a["sessions"], a["sessions_avg7"]),
            ("order_cr", "주문 전환율", a["order_cr"], a["order_cr_avg7"]),
            ("aov", "객단가", a["aov"], a["aov_avg7"]),
        ]:
            r = delta(cur, base)
            factors.append({"label": label, "change": r,
                            "log": math.log(cur / base) if cur and base else 0.0})
        total_log = math.log(a["revenue"] / a["revenue_avg7"]) if a["revenue"] else None
        for f in factors:
            f["share"] = (f["log"] / total_log) if total_log else None
        top = max(factors, key=lambda f: abs(f["log"]))
        view["decomp_note"] = f"7일 평균 대비 가장 크게 움직인 항: {top['label']} ({signed_pct(top['change'])})"
    view["factors"] = factors

    segs = []
    for (t, v), label in [(("device", "desktop"), "데스크톱"), (("device", "mobile"), "모바일"),
                          (("visitor", "new"), "신규"), (("visitor", "returning"), "재방문")]:
        s = data["kpi"].get((t, v))
        if not s:
            continue
        segs.append({"group": "기기" if t == "device" else "방문", "label": label,
                     "sessions": s["sessions"], "orders": s["orders"], "revenue": s["revenue"],
                     "order_cr": s["order_cr"], "rev_avg7": delta(s["revenue"], s["revenue_avg7"]),
                     "cr_avg7_pp": delta_pp(s["order_cr"], s["order_cr_avg7"]),
                     "sessions_avg7": delta(s["sessions"], s["sessions_avg7"])})
    view["segments"] = segs

    view["funnel"] = {
        "steps": [
            {"label": "세션", "n": a["sessions"], "rate": None},
            {"label": "상품 조회", "n": a["view_sessions"], "rate": a["view_rate"]},
            {"label": "결제 시작", "n": a["checkout_sessions"], "rate": a["view_to_checkout"],
             "split": (a["checkout_with_cart"], a["checkout_direct"])},
            {"label": "구매 세션", "n": a["purchase_sessions"], "rate": a["checkout_to_purchase"]},
        ],
        "direct_share": a["direct_checkout_share"], "cart_rate": a["cart_rate"],
        "orders": a["orders"],
    }
    view["trend"] = trend_svg(data["trend"])
    view["weekly"] = data["weekly"]
    return view


def tile(label, cur, wow, avg7, vfmt, weekday, sparkline, hero=False, rate=False):
    """Stat tile: big value, one headline comparison (vs 7-day avg) with arrow, then the reference values."""
    if rate:
        w, a = delta_pp(cur, wow), delta_pp(cur, avg7)
        fmt = lambda x: None if x is None else f"{abs(round(x, 2)):.2f}%p"
        dirs = (direction(w, eps=0.05), direction(a, eps=0.05))   # %p: under 0.05%p counts as flat
    else:
        w, a = delta(cur, wow), delta(cur, avg7)
        fmt = lambda x: None if x is None else f"{abs(x) * 100:.1f}%"
        dirs = (direction(w), direction(a))
    arrow = {"up": "▲", "down": "▼", "flat": "≈", "na": ""}
    return {"label": label, "value": vfmt(cur), "hero": hero, "spark": sparkline,
            "avg7_dir": dirs[1], "avg7_arrow": arrow[dirs[1]], "avg7": fmt(a),
            "avg7_ref": vfmt(avg7) if avg7 is not None else None,
            "wow_dir": dirs[0], "wow_arrow": arrow[dirs[0]], "wow": fmt(w),
            "wow_ref": vfmt(wow) if wow is not None else None, "weekday": weekday}


def spark(points: list[dict], key: str) -> dict | None:
    """Tiny single-series trend for a stat tile (last 14 days). No axes; the tile names the metric."""
    vals = [p[key] for p in points if p[key] is not None]
    if len(vals) < 2:
        return None
    W, H, P = 120, 34, 3
    lo, hi = min(vals), max(vals)
    span = (hi - lo) or 1
    pts = [(P + (W - 2 * P) * i / (len(vals) - 1), P + (H - 2 * P) * (1 - (v - lo) / span)) for i, v in enumerate(vals)]
    return {"w": W, "h": H, "path": " ".join(("M" if i == 0 else "L") + f"{x:.1f},{y:.1f}" for i, (x, y) in enumerate(pts)),
            "x": pts[-1][0], "y": pts[-1][1], "days": len(vals)}


def signed_pct(v):
    return None if v is None else f"{v * 100:+.1f}%"


def direction(v, eps=0.005):
    if v is None:
        return "na"
    return "up" if v > eps else "down" if v < -eps else "flat"


def trend_svg(points: list[dict]) -> dict:
    """Line chart data: daily revenue (series 1) and its trailing 7-day average (reference)."""
    W, H, L, R, T, B = 640, 220, 56, 16, 16, 28
    if not points:
        return {"w": W, "h": H, "empty": True}
    vmax = max(max(p["revenue"] or 0, p["revenue_avg7"] or 0) for p in points)
    step = nice_step(vmax / 4)
    top = step * math.ceil(vmax / step)
    n = len(points)
    x = lambda i: L + (W - L - R) * (i / max(n - 1, 1))
    y = lambda v: T + (H - T - B) * (1 - v / top)
    rev = [(x(i), y(p["revenue"])) for i, p in enumerate(points)]
    avg = [(x(i), y(p["revenue_avg7"])) for i, p in enumerate(points) if p["revenue_avg7"] is not None]
    path = lambda pts: " ".join(("M" if j == 0 else "L") + f"{a:.1f},{b:.1f}" for j, (a, b) in enumerate(pts))
    grid = [{"y": y(step * k), "label": f"${step * k / 1000:,.0f}k"} for k in range(int(top / step) + 1)]
    ticks = [{"x": x(i), "label": f"{p['kpi_date'].month}/{p['kpi_date'].day}"}
             for i, p in enumerate(points) if i % 7 == (n - 1) % 7]
    dots = [{"x": x(i), "y": y(p["revenue"]), "date": f"{p['kpi_date'].month}/{p['kpi_date'].day}",
             "rev": money(p["revenue"]), "avg": money(p["revenue_avg7"])} for i, p in enumerate(points)]
    last = rev[-1]
    return {"w": W, "h": H, "left": L, "right": W - R, "bottom": H - B, "rev_path": path(rev),
            "avg_path": path(avg) if avg else "", "grid": grid, "ticks": ticks, "dots": dots,
            "last": {"x": last[0], "y": last[1]}, "avg_last": avg[-1] if avg else None}


def nice_step(raw: float) -> float:
    if raw <= 0:
        return 1
    mag = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


# ---------- output ----------
def env() -> Environment:
    e = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=True)
    e.filters.update(money=money, pct=pct, signed=signed_pct, num=lambda v: "–" if v is None else f"{v:,.0f}")
    return e


def write_index(site: Path) -> None:
    days = sorted((p.stem for p in (site / "days").glob("*.html")), reverse=True)
    (site / "index.html").write_text(env().get_template("index.html.j2").render(days=days), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, type=date.fromisoformat)
    ap.add_argument("--dataset", default="ga4_mart")
    ap.add_argument("--site", default=str(ROOT / "site"))
    ap.add_argument("--blocked-reason", default=None,
                    help="Force the '검수 실패' page (e.g. when dbt test failed on DQ6).")
    args = ap.parse_args()

    site = Path(args.site)
    (site / "days").mkdir(parents=True, exist_ok=True)
    view = build_view(load(client(), args.dataset, args.date), args.date, args.blocked_reason)
    out = site / "days" / f"{args.date.isoformat()}.html"
    out.write_text(env().get_template("dashboard.html.j2").render(v=view), encoding="utf-8")
    write_index(site)
    print(f"{out}  [{view['overall']}]")


if __name__ == "__main__":
    main()
