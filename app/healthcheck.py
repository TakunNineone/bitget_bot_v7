from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Any, List, Tuple


def dt_local(ms: Optional[int]) -> str:
    if ms is None:
        return "NULL"
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ms / 1000))
    except Exception:
        return str(ms)


@dataclass
class CheckResult:
    name: str
    status: str  # OK/WARN/FAIL
    details: str


def q1(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> Optional[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    cur = conn.execute(sql, params)
    return cur.fetchone()


def qv(conn: sqlite3.Connection, sql: str, params: Tuple = ()) -> Any:
    cur = conn.execute(sql, params)
    r = cur.fetchone()
    return r[0] if r else None


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    r = q1(conn, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return r is not None


def freshness_by_recv_ts(name: str, mx_recv: Optional[int], now_ms: int, fresh_ms: int) -> CheckResult:
    if mx_recv is None:
        return CheckResult(name, "FAIL", "No rows (MAX recv_ts is NULL)")
    lag = now_ms - int(mx_recv)
    if lag <= fresh_ms:
        return CheckResult(name, "OK", f"Fresh. last_recv={dt_local(int(mx_recv))} lag_ms={lag}")
    if lag <= fresh_ms * 5:
        return CheckResult(name, "WARN", f"Stale. last_recv={dt_local(int(mx_recv))} lag_ms={lag}")
    return CheckResult(name, "FAIL", f"Very stale. last_recv={dt_local(int(mx_recv))} lag_ms={lag}")


def healthcheck(db_path: str, symbols: List[str], freshness_sec: int = 120, vol_gate: float = 0.0040) -> List[CheckResult]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    results: List[CheckResult] = []

    now_ms = int(time.time() * 1000)
    fresh_ms = freshness_sec * 1000

    # ---- tables ----
    required_tables = [
        "raw_ws_trade", "raw_ws_ticker", "raw_ws_books15", "raw_ws_candle",
        "agg_1s_micro", "agg_1m_context",
        "raw_rest_symbol_price", "raw_rest_open_interest", "raw_rest_current_fund_rate"
    ]
    missing = [t for t in required_tables if not table_exists(conn, t)]
    if missing:
        results.append(CheckResult("tables", "FAIL", f"Missing tables: {', '.join(missing)}"))
        conn.close()
        return results
    results.append(CheckResult("tables", "OK", "All required tables exist"))

    # ---- WS freshness by recv_ts ----
    ws_trade_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_ws_trade")
    ws_tick_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_ws_ticker")
    ws_book_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_ws_books15")
    # candle15m is slower; use recv_ts from candle rows
    ws_c15_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_ws_candle WHERE interval='candle15m'")

    results.append(freshness_by_recv_ts("ws_trade", ws_trade_recv, now_ms, fresh_ms))
    results.append(freshness_by_recv_ts("ws_ticker", ws_tick_recv, now_ms, fresh_ms))
    results.append(freshness_by_recv_ts("ws_books15", ws_book_recv, now_ms, fresh_ms))

    if ws_c15_recv is None:
        results.append(CheckResult("ws_candle15m", "FAIL", "No candle15m rows (warmup/WS missing)"))
    else:
        lag = now_ms - int(ws_c15_recv)
        # allow 2*15m for candle15m freshness expectation
        if lag <= 2 * 15 * 60 * 1000:
            results.append(CheckResult("ws_candle15m", "OK", f"Present. last_recv={dt_local(int(ws_c15_recv))} lag_ms={lag}"))
        else:
            results.append(CheckResult("ws_candle15m", "WARN", f"Old candle15m recv. last_recv={dt_local(int(ws_c15_recv))} lag_ms={lag}"))

    # ---- REST freshness by recv_ts ----
    rest_px_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_rest_symbol_price")
    rest_oi_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_rest_open_interest")
    rest_fr_recv = qv(conn, "SELECT MAX(recv_ts) FROM raw_rest_current_fund_rate")

    results.append(freshness_by_recv_ts("rest_symbol_price", rest_px_recv, now_ms, fresh_ms))
    results.append(freshness_by_recv_ts("rest_open_interest", rest_oi_recv, now_ms, fresh_ms))

    if rest_fr_recv is None:
        results.append(CheckResult("rest_current_fund_rate", "FAIL", "No rows"))
    else:
        lag = now_ms - int(rest_fr_recv)
        if lag <= max(10 * 60 * 1000, fresh_ms):
            results.append(CheckResult("rest_current_fund_rate", "OK", f"Fresh-ish. last_recv={dt_local(int(rest_fr_recv))} lag_ms={lag}"))
        else:
            results.append(CheckResult("rest_current_fund_rate", "WARN", f"Stale. last_recv={dt_local(int(rest_fr_recv))} lag_ms={lag}"))

    # ---- Per-symbol checks ----
    for sym in symbols:
        last_sec = qv(conn, "SELECT MAX(sec_ts) FROM agg_1s_micro WHERE instId=?", (sym,))
        if last_sec is None:
            results.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", "No rows in agg_1s_micro"))
            continue

        cnt_10m = qv(conn,
                    "SELECT COUNT(*) FROM agg_1s_micro WHERE instId=? AND sec_ts>=?",
                    (sym, int(last_sec) - 10 * 60 * 1000)) or 0

        if cnt_10m >= 520:
            results.append(CheckResult(f"{sym}:agg_1s_micro", "OK", f"Rows last10m={cnt_10m} last={dt_local(int(last_sec))}"))
        elif cnt_10m >= 300:
            results.append(CheckResult(f"{sym}:agg_1s_micro", "WARN", f"Rows last10m={cnt_10m} (missing seconds?) last={dt_local(int(last_sec))}"))
        else:
            results.append(CheckResult(f"{sym}:agg_1s_micro", "FAIL", f"Rows last10m={cnt_10m} (aggregation broken) last={dt_local(int(last_sec))}"))

        # flow + ranges
        row = q1(conn, """
            SELECT
              MIN(spread_bps) AS min_spread,
              MAX(spread_bps) AS max_spread,
              MIN(imbalance15) AS min_imb,
              MAX(imbalance15) AS max_imb,
              SUM(CASE WHEN trade_count>0 THEN 1 ELSE 0 END) AS active_secs,
              COUNT(*) AS total_secs
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts>=?
        """, (sym, int(last_sec) - 10 * 60 * 1000))

        if row:
            active = int(row["active_secs"] or 0)
            total = int(row["total_secs"] or 0)
            if total > 0 and active == 0:
                results.append(CheckResult(f"{sym}:flow", "WARN", "No active seconds (trade_count>0) in last 10m"))
            else:
                results.append(CheckResult(f"{sym}:flow", "OK", f"active_secs={active}/{total} last10m"))

            min_imb = row["min_imb"]
            max_imb = row["max_imb"]
            if min_imb is not None and max_imb is not None and (min_imb < -1.05 or max_imb > 1.05):
                results.append(CheckResult(f"{sym}:imbalance_range", "WARN", f"imbalance out of expected [-1,1]: {min_imb}..{max_imb}"))
            else:
                results.append(CheckResult(f"{sym}:imbalance_range", "OK", f"imbalance {min_imb}..{max_imb}"))

        # context latest
        ctx = q1(conn, """
            SELECT min_ts, oi, doi, funding, mark_last, index_last
            FROM agg_1m_context
            WHERE instId=?
            ORDER BY min_ts DESC
            LIMIT 1
        """, (sym,))

        if not ctx:
            results.append(CheckResult(f"{sym}:context_latest", "FAIL", "No rows in agg_1m_context"))
        else:
            nulls = [k for k in ("oi", "doi", "funding", "mark_last", "index_last") if ctx[k] is None]
            if nulls:
                results.append(CheckResult(
                    f"{sym}:context_latest", "WARN",
                    f"Latest context NULL fields: {', '.join(nulls)} at {dt_local(int(ctx['min_ts']))}"
                ))
            else:
                results.append(CheckResult(f"{sym}:context_latest", "OK", f"Context OK at {dt_local(int(ctx['min_ts']))}"))

        # ---- ATR% snapshot (explains low_vol) ----
        # mid from agg_1s_micro
        mid = qv(conn, "SELECT mid FROM agg_1s_micro WHERE instId=? ORDER BY sec_ts DESC LIMIT 1", (sym,))
        # latest candle_ts for 15m
        candle_ts = qv(conn, "SELECT MAX(candle_ts) FROM raw_ws_candle WHERE instId=? AND interval='candle15m'", (sym,))
        if mid is None:
            results.append(CheckResult(f"{sym}:atr_pct", "WARN", "NO_PRICE (mid is NULL)"))
        elif candle_ts is None:
            results.append(CheckResult(f"{sym}:atr_pct", "WARN", "NO_CANDLES_15M (warmup missing?)"))
        else:
            # Pull last 100 candles and compute ATR(14) quickly in python
            cur = conn.execute(
                "SELECT o,h,l,c FROM raw_ws_candle "
                "WHERE instId=? AND interval='candle15m' AND candle_ts<=? "
                "ORDER BY candle_ts DESC LIMIT 100",
                (sym, int(candle_ts)),
            )
            rows = cur.fetchall()
            ohlc = []
            for r in reversed(rows):
                try:
                    o = float(r[0]); h = float(r[1]); l = float(r[2]); c = float(r[3])
                    ohlc.append((o,h,l,c))
                except Exception:
                    continue

            def _atr(ohlc_list, period=14) -> Optional[float]:
                if len(ohlc_list) < period + 1:
                    return None
                trs = []
                prev_close = ohlc_list[0][3]
                for (_o, h, l, c) in ohlc_list[1:]:
                    tr = max(h-l, abs(h-prev_close), abs(l-prev_close))
                    trs.append(tr)
                    prev_close = c
                atr0 = sum(trs[:period]) / period
                curv = atr0
                for tr in trs[period:]:
                    curv = (curv*(period-1) + tr) / period
                return curv

            atr_val = _atr(ohlc, 14)
            if atr_val is None:
                results.append(CheckResult(f"{sym}:atr_pct", "WARN", f"NO_ATR (need >=15 candles). candles={len(ohlc)}"))
            else:
                atr_pct = atr_val / float(mid) if float(mid) > 0 else None
                if atr_pct is None:
                    results.append(CheckResult(f"{sym}:atr_pct", "WARN", "Bad mid for atr_pct"))
                else:
                    if atr_pct >= vol_gate:
                        results.append(CheckResult(f"{sym}:atr_pct", "OK", f"atr_pct={atr_pct:.4f} (>= {vol_gate:.4f})"))
                    else:
                        results.append(CheckResult(f"{sym}:atr_pct", "WARN", f"LOW_VOL atr_pct={atr_pct:.4f} (< {vol_gate:.4f})"))

    conn.close()
    return results


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--freshness-sec", type=int, default=120)
    p.add_argument("--vol-gate", type=float, default=0.0040)
    args = p.parse_args()

    res = healthcheck(args.db, args.symbols, args.freshness_sec, args.vol_gate)

    order = {"OK": 0, "WARN": 1, "FAIL": 2}
    worst = "OK"
    for r in res:
        if order[r.status] > order[worst]:
            worst = r.status

    print("\n=== HEALTHCHECK v2 ===")
    for r in res:
        print(f"{r.status:4} | {r.name:24} | {r.details}")

    print(f"\nOverall: {worst}")
    if worst != "OK":
        print("Tip: look at FAIL/WARN items — they pinpoint the broken stage (WS/REST/aggregation/vol).")


if __name__ == "__main__":
    main()
