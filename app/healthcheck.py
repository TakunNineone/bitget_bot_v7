from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Any, List, Dict
import math


def dt_local(ms: Optional[int]) -> str:
    if ms is None:
        return "NULL"
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))


@dataclass
class CheckResult:
    name: str
    status: str  # OK/WARN/FAIL/INFO
    details: str


def q1(conn, sql, params=()):
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    return cur.fetchone()


def qv(conn, sql, params=()):
    cur = conn.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None


def table_exists(conn, name: str) -> bool:
    return q1(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,)) is not None


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None


def _quantiles(xs: List[float], qs: List[float]) -> Dict[float, float]:
    if not xs:
        return {}
    xs2 = sorted(xs)
    out: Dict[float, float] = {}
    n = len(xs2)
    for q in qs:
        if n == 1:
            out[q] = xs2[0]
            continue
        pos = q * (n - 1)
        lo = int(math.floor(pos))
        hi = int(math.ceil(pos))
        if lo == hi:
            out[q] = xs2[lo]
        else:
            w = pos - lo
            out[q] = xs2[lo] * (1 - w) + xs2[hi] * w
    return out


def healthcheck(db_path: str, symbols: List[str], freshness_sec: int, vol_gate: float):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now_ms = int(time.time() * 1000)
    fresh_ms = freshness_sec * 1000
    res: List[CheckResult] = []

    # ---------------- tables ----------------
    required = [
        "agg_1s_micro", "agg_1m_context", "agg_15m_trend",
        "raw_ws_trade", "raw_ws_ticker", "raw_ws_books15", "raw_ws_candle",
        "raw_rest_symbol_price", "raw_rest_open_interest", "raw_rest_current_fund_rate",
    ]
    missing = [t for t in required if not table_exists(conn, t)]
    if missing:
        res.append(CheckResult("tables", "FAIL", f"Missing: {missing}"))
        return res
    res.append(CheckResult("tables", "OK", "All required tables exist"))

    # ---------------- freshness (raw feeds) ----------------
    def _last_recv(table: str, col: str = "recv_ts") -> Optional[int]:
        try:
            return qv(conn, f"SELECT MAX({col}) FROM {table}")
        except Exception:
            return None

    feeds = [
        ("ws_trade", "raw_ws_trade", "recv_ts"),
        ("ws_ticker", "raw_ws_ticker", "recv_ts"),
        ("ws_books15", "raw_ws_books15", "recv_ts"),
        ("ws_candle15m", "raw_ws_candle", "recv_ts"),
        ("rest_symbol_price", "raw_rest_symbol_price", "recv_ts"),
        ("rest_open_interest", "raw_rest_open_interest", "recv_ts"),
        ("rest_current_fund_rate", "raw_rest_current_fund_rate", "recv_ts"),
    ]

    for name, table, col in feeds:
        last = _last_recv(table, col)
        if last is None:
            res.append(CheckResult(name, "WARN", "No rows"))
            continue
        lag = now_ms - int(last)
        if lag <= fresh_ms:
            res.append(CheckResult(name, "OK", f"Fresh. last_recv={dt_local(last)} lag_ms={lag}"))
        else:
            res.append(CheckResult(name, "WARN", f"Stale. last_recv={dt_local(last)} lag_ms={lag}"))

    # ---------------- per symbol ----------------
    for sym in symbols:
        last_sec = qv(conn, "SELECT MAX(sec_ts) FROM agg_1s_micro WHERE instId=?", (sym,))
        if not last_sec:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", "No rows"))
            continue

        cnt_10m = qv(
            conn,
            "SELECT COUNT(*) FROM agg_1s_micro WHERE instId=? AND sec_ts>=?",
            (sym, int(last_sec) - 10 * 60 * 1000),
        )
        if cnt_10m >= 520:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "OK", f"Rows last10m={cnt_10m}"))
        elif cnt_10m >= 300:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "WARN", f"Rows last10m={cnt_10m} (gaps)"))
        else:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", f"Rows last10m={cnt_10m} (aggregation broken)"))

        # ---- fast react snapshot (last 60s) ----
        row = q1(
            conn,
            """
            SELECT
              MIN(mid) AS mn,
              MAX(mid) AS mx,
              SUM(trade_count) AS flow
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts>=?
            """,
            (sym, int(last_sec) - 60 * 1000),
        )

        range_60s = None
        flow_60s = 0
        if row:
            flow_60s = int(row["flow"] or 0)
            if row["mn"] is not None and row["mx"] is not None:
                mid_last = qv(conn, "SELECT mid FROM agg_1s_micro WHERE instId=? ORDER BY sec_ts DESC LIMIT 1", (sym,))
                mid_last = _safe_float(mid_last)
                if mid_last and mid_last > 0:
                    range_60s = (float(row["mx"]) - float(row["mn"])) / mid_last

        row10 = q1(
            conn,
            "SELECT SUM(trade_count) AS flow FROM agg_1s_micro WHERE instId=? AND sec_ts>=?",
            (sym, int(last_sec) - 10 * 60 * 1000),
        )
        avg_flow_60s = (float(row10["flow"] or 0) / 10.0) if row10 else 0.0
        flow_spike = (flow_60s / avg_flow_60s) if avg_flow_60s > 0 else None

        fast_on = (
            (range_60s is not None and range_60s >= 0.0012) and
            (flow_spike is not None and flow_spike >= 1.6)
        )

        if range_60s is not None and flow_spike is not None:
            res.append(CheckResult(
                f"{sym}:fast_react",
                "OK" if fast_on else "WARN",
                f"range60s={range_60s:.4f} flow60s={flow_60s} spike={flow_spike:.2f}",
            ))
        else:
            res.append(CheckResult(f"{sym}:fast_react", "WARN", "insufficient data"))

        # ---- ATR regime from agg_15m_trend ----
        mid = qv(conn, "SELECT mid FROM agg_1s_micro WHERE instId=? ORDER BY sec_ts DESC LIMIT 1", (sym,))
        mid = _safe_float(mid)

        atr14 = qv(conn, "SELECT atr14 FROM agg_15m_trend WHERE instId=? ORDER BY candle_ts DESC LIMIT 1", (sym,))
        atr14 = _safe_float(atr14)

        if atr14 and mid and mid > 0:
            atr_pct = atr14 / mid
            if atr_pct >= vol_gate:
                res.append(CheckResult(f"{sym}:vol_regime", "OK", f"ATR_OK atr_pct={atr_pct:.4f}"))
            else:
                res.append(CheckResult(
                    f"{sym}:vol_regime",
                    "INFO" if fast_on else "WARN",
                    f"LOW_ATR atr_pct={atr_pct:.4f} (gate={vol_gate:.4f}) fast_react={'ON' if fast_on else 'OFF'}",
                ))
        else:
            res.append(CheckResult(f"{sym}:vol_regime", "WARN", "ATR or price missing"))

        # ---- agg_15m_trend freshness + stats ----
        last_candle = qv(conn, "SELECT MAX(candle_ts) FROM agg_15m_trend WHERE instId=?", (sym,))
        if not last_candle:
            res.append(CheckResult(f"{sym}:agg_15m_trend", "FAIL", "No rows"))
            continue

        lag = now_ms - int(last_candle)
        if lag <= 45 * 60 * 1000:
            res.append(CheckResult(f"{sym}:agg_15m_trend", "OK", f"Fresh. last={dt_local(int(last_candle))} lag_ms={lag}"))
        elif lag <= 3 * 60 * 60 * 1000:
            res.append(CheckResult(f"{sym}:agg_15m_trend", "WARN", f"Stale. last={dt_local(int(last_candle))} lag_ms={lag}"))
        else:
            res.append(CheckResult(f"{sym}:agg_15m_trend", "FAIL", f"Very stale. last={dt_local(int(last_candle))} lag_ms={lag}"))

        rows = conn.execute(
            """
            SELECT ema_diff, ema_slope, trend_dir
            FROM agg_15m_trend
            WHERE instId=?
            ORDER BY candle_ts DESC
            LIMIT 500
            """,
            (sym,),
        ).fetchall()

        diffs = [_safe_float(r[0]) for r in rows]
        slopes = [_safe_float(r[1]) for r in rows]
        dirs = [r[2] for r in rows]

        diffs = [x for x in diffs if x is not None]
        slopes = [x for x in slopes if x is not None]

        up = sum(1 for d in dirs if d == 1)
        down = sum(1 for d in dirs if d == -1)
        flat = sum(1 for d in dirs if d == 0)
        total = up + down + flat if (up + down + flat) > 0 else len(dirs)

        abs_slopes = [abs(x) for x in slopes]
        abs_diffs = [abs(x) for x in diffs]
        q = _quantiles(abs_slopes, [0.1, 0.25, 0.5, 0.75, 0.9])
        qd = _quantiles(abs_diffs, [0.1, 0.25, 0.5, 0.75, 0.9])

        def rate_ge(xs: List[float], thr: float) -> float:
            if not xs:
                return 0.0
            return sum(1 for x in xs if x >= thr) / len(xs)

        slope_gate_strict = 0.0008
        slope_gate_soft = 0.00035
        diff_gate = 0.0008

        slope_strict_rate = rate_ge(abs_slopes, slope_gate_strict)
        slope_soft_rate = rate_ge(abs_slopes, slope_gate_soft)
        diff_rate = rate_ge(abs_diffs, diff_gate)

        res.append(CheckResult(
            f"{sym}:trend_stats",
            "OK",
            f"last500 dir% up={up}/{total} down={down}/{total} flat={flat}/{total} | "
            f"|slope| q50={q.get(0.5,0):.6f} q75={q.get(0.75,0):.6f} | "
            f"pass(|slope|>=0.0008)={slope_strict_rate*100:.1f}% pass(|slope|>=0.00035)={slope_soft_rate*100:.1f}% "
            f"pass(|diff|>=0.0008)={diff_rate*100:.1f}%"
        ))

    conn.close()
    return res


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--freshness-sec", type=int, default=120)
    p.add_argument("--vol-gate", type=float, default=0.0018)
    args = p.parse_args()

    res = healthcheck(args.db, args.symbols, args.freshness_sec, args.vol_gate)

    order = {"OK": 0, "INFO": 1, "WARN": 2, "FAIL": 3}
    worst = "OK"
    for r in res:
        if order[r.status] > order[worst]:
            worst = r.status

    print("\n=== HEALTHCHECK v4 ===")
    for r in res:
        print(f"{r.status:4} | {r.name:24} | {r.details}")

    print(f"\nOverall: {worst}")
    if worst != "OK":
        print("Tip: look at WARN/FAIL — they explain why signals may be missing.")


if __name__ == "__main__":
    main()
