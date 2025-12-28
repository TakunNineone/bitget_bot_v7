from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Any, List, Tuple


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


def healthcheck(db_path: str, symbols: List[str], freshness_sec: int, vol_gate: float):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now_ms = int(time.time() * 1000)
    fresh_ms = freshness_sec * 1000
    res: List[CheckResult] = []

    # ---------------- tables ----------------
    required = [
        "agg_1s_micro", "agg_1m_context",
        "raw_ws_trade", "raw_ws_ticker", "raw_ws_books15", "raw_ws_candle",
        "raw_rest_symbol_price", "raw_rest_open_interest", "raw_rest_current_fund_rate",
    ]
    missing = [t for t in required if not table_exists(conn, t)]
    if missing:
        res.append(CheckResult("tables", "FAIL", f"Missing: {missing}"))
        return res
    res.append(CheckResult("tables", "OK", "All required tables exist"))

    # ---------------- per symbol ----------------
    for sym in symbols:
        last_sec = qv(conn, "SELECT MAX(sec_ts) FROM agg_1s_micro WHERE instId=?", (sym,))
        if not last_sec:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", "No rows"))
            continue

        # ---- aggregation density ----
        cnt_10m = qv(
            conn,
            "SELECT COUNT(*) FROM agg_1s_micro WHERE instId=? AND sec_ts>=?",
            (sym, last_sec - 10 * 60 * 1000),
        )
        if cnt_10m >= 520:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "OK", f"Rows last10m={cnt_10m}"))
        elif cnt_10m >= 300:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "WARN", f"Rows last10m={cnt_10m} (gaps)"))
        else:
            res.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", f"Rows last10m={cnt_10m} (broken)"))

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
            (sym, last_sec - 60 * 1000),
        )

        range_60s = None
        if row and row["mn"] and row["mx"]:
            mid_last = qv(
                conn,
                "SELECT mid FROM agg_1s_micro WHERE instId=? ORDER BY sec_ts DESC LIMIT 1",
                (sym,),
            )
            if mid_last and mid_last > 0:
                range_60s = (row["mx"] - row["mn"]) / mid_last

        flow_60s = int(row["flow"] or 0)

        row10 = q1(
            conn,
            """
            SELECT SUM(trade_count) AS flow
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts>=?
            """,
            (sym, last_sec - 10 * 60 * 1000),
        )
        avg_flow_60s = (row10["flow"] or 0) / 10 if row10 else 0
        flow_spike = (flow_60s / avg_flow_60s) if avg_flow_60s > 0 else None

        fast_on = (
            (range_60s is not None and range_60s >= 0.0012) and
            (flow_spike is not None and flow_spike >= 1.6)
        )

        res.append(
            CheckResult(
                f"{sym}:fast_react",
                "OK" if fast_on else "WARN",
                f"range60s={range_60s:.4f} flow60s={flow_60s} spike={flow_spike:.2f}"
                if range_60s and flow_spike
                else "insufficient data",
            )
        )

        # ---- ATR regime ----
        mid = qv(conn, "SELECT mid FROM agg_1s_micro WHERE instId=? ORDER BY sec_ts DESC LIMIT 1", (sym,))
        cur = conn.execute(
            "SELECT o,h,l,c FROM raw_ws_candle WHERE instId=? AND interval='candle15m' ORDER BY candle_ts DESC LIMIT 20",
            (sym,),
        )
        rows_raw = cur.fetchall()

        # cast to float safely
        rows = []
        for rr in rows_raw:
            try:
                o = float(rr[0]);
                h = float(rr[1]);
                l = float(rr[2]);
                c = float(rr[3])
                rows.append((o, h, l, c))
            except Exception:
                continue

        atr = None
        if len(rows) >= 15:
            trs = []
            prev = rows[-1][3]  # previous close
            for (_o, h, l, c) in reversed(rows[:-1]):
                tr = max(h - l, abs(h - prev), abs(l - prev))
                trs.append(tr)
                prev = c
            atr = sum(trs[:14]) / 14

        if atr and mid:
            atr_pct = atr / mid
            if atr_pct >= vol_gate:
                res.append(CheckResult(f"{sym}:vol_regime", "OK", f"ATR_OK atr_pct={atr_pct:.4f}"))
            else:
                if fast_on:
                    res.append(CheckResult(
                        f"{sym}:vol_regime",
                        "INFO",
                        f"LOW_ATR atr_pct={atr_pct:.4f} but FAST_REACT=ON",
                    ))
                else:
                    res.append(CheckResult(
                        f"{sym}:vol_regime",
                        "WARN",
                        f"LOW_ATR atr_pct={atr_pct:.4f} and FAST_REACT=OFF",
                    ))
        else:
            res.append(CheckResult(f"{sym}:vol_regime", "WARN", "ATR or price missing"))

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

    print("\n=== HEALTHCHECK v3 ===")
    for r in res:
        print(f"{r.status:4} | {r.name:24} | {r.details}")

    print(f"\nOverall: {worst}")
    if worst != "OK":
        print("Tip: look at WARN/FAIL — they explain why signals may be missing.")


if __name__ == "__main__":
    main()
