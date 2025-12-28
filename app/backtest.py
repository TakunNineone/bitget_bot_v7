from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Tuple

from .features import FeatureService, FeatureRow


# Backtest v2:
# - Saves closed trades to backtest_trades
# - FORCE_EXIT any open position at end of dataset
# - Prints summary stats (winrate, avg pnl, PF, max DD) per symbol
# - Execution model: entry/exit at mid (no slippage) per user request
# - Fees: round-trip percent of notional (fixed per trade)


@dataclass
class Signal:
    instId: str
    ts: int
    side: str  # "long" | "short" | "flat"
    reason: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_start: Optional[float] = None
    trailing_step: Optional[float] = None


@dataclass
class VirtualPosition:
    instId: str
    side: str
    entry_price: float
    entry_ts: int
    stop_loss: float
    take_profit: float
    trailing_start: float
    trailing_step: float

    best_pnl: float = 0.0
    worst_pnl: float = 0.0
    trail_active: bool = False
    trail_level: Optional[float] = None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pnl_frac(pos: VirtualPosition, price: float) -> float:
    if pos.side == "long":
        return price / pos.entry_price - 1.0
    return pos.entry_price / price - 1.0


def should_exit_and_update_trail(pos: VirtualPosition, price: float) -> Optional[str]:
    p = pnl_frac(pos, price)

    if p <= -pos.stop_loss:
        return "SL"
    if p >= pos.take_profit:
        return "TP"

    if (not pos.trail_active) and p >= pos.trailing_start:
        pos.trail_active = True
        if pos.side == "long":
            pos.trail_level = price * (1.0 - pos.trailing_step)
        else:
            pos.trail_level = price * (1.0 + pos.trailing_step)

    if pos.trail_active and pos.trail_level is not None:
        if pos.side == "long":
            new_level = price * (1.0 - pos.trailing_step)
            if new_level > pos.trail_level:
                pos.trail_level = new_level
            if price <= pos.trail_level:
                return "TRAIL"
        else:
            new_level = price * (1.0 + pos.trailing_step)
            if new_level < pos.trail_level:
                pos.trail_level = new_level
            if price >= pos.trail_level:
                return "TRAIL"

    return None


def decide(f: FeatureRow, vol_gate: float) -> Signal:
    if f.spread_bps is None or f.spread_bps > 8.0:
        return Signal(f.instId, f.ts, "flat", "spread")

    px = f.mid if f.mid is not None else f.vwap
    if px is None:
        return Signal(f.instId, f.ts, "flat", "no_price")
    if f.atr15m is None:
        return Signal(f.instId, f.ts, "flat", "no_atr")

    atr_pct = f.atr15m / px
    if atr_pct < vol_gate:
        return Signal(f.instId, f.ts, "flat", "low_vol")

    if f.trade_count_sum_5s < 5 or f.active_seconds_5s < 3:
        return Signal(f.instId, f.ts, "flat", "no_flow")

    dz = f.delta_rate_z or 0.0
    imb = f.imb_shift or 0.0
    mark_z = f.mark_dev_z
    dz_thr = 1.0 if mark_z is not None else 1.5

    long_ok = dz > dz_thr and imb > 0.05 and (mark_z is None or mark_z > -1.5)
    short_ok = dz < -dz_thr and imb < -0.05 and (mark_z is None or mark_z < 1.5)

    if not long_ok and not short_ok:
        return Signal(f.instId, f.ts, "flat", "no_trigger")

    side = "long" if long_ok else "short"

    sl = _clamp(0.9 * atr_pct, 0.0035, 0.0120)
    tp = _clamp(2.0 * sl, 0.0100, 0.0200)
    trailing_start = _clamp(0.8 * tp, 0.0060, 0.0150)
    trailing_step = _clamp(0.35 * sl, 0.0015, 0.0040)

    reason = f"dz={dz:.2f} imb={imb:.3f} atr={atr_pct:.4f} flow5s={f.trade_count_sum_5s}/{f.active_seconds_5s}"
    return Signal(f.instId, f.ts, side, reason, sl, tp, trailing_start, trailing_step)


def parse_dt_to_ms(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    ss = s.strip()
    if len(ss) == 10:
        dt = datetime.fromisoformat(ss + " 00:00:00")
    else:
        dt = datetime.fromisoformat(ss.replace("T", " "))
    return int(dt.timestamp() * 1000)


def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("""
    CREATE TABLE IF NOT EXISTS backtest_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instId TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_ts INTEGER NOT NULL,
        entry_price REAL NOT NULL,
        exit_ts INTEGER NOT NULL,
        exit_price REAL NOT NULL,
        gross_pnl_frac REAL NOT NULL,
        net_pnl_frac REAL NOT NULL,
        gross_pnl_usd REAL NOT NULL,
        net_pnl_usd REAL NOT NULL,
        fee_usd REAL NOT NULL,
        best_pnl_frac REAL NOT NULL,
        worst_pnl_frac REAL NOT NULL,
        exit_reason TEXT NOT NULL,
        duration_sec INTEGER NOT NULL,
        signal_reason TEXT,
        created_at INTEGER NOT NULL
    );
    """)
    conn.commit()


def clear_previous(conn: sqlite3.Connection, symbols: List[str], start_ms: Optional[int], end_ms: Optional[int]) -> None:
    # Optional cleanup: delete previous backtest rows for these symbols in the same time range
    # (keeps table from growing endlessly when you re-run).
    # This is conservative: only deletes by instId and overlapping entry_ts range if provided.
    if not symbols:
        return
    placeholders = ",".join("?" for _ in symbols)
    params: List = list(symbols)
    where = f"instId IN ({placeholders})"
    if start_ms is not None:
        where += " AND entry_ts>=?"
        params.append(start_ms)
    if end_ms is not None:
        where += " AND entry_ts<=?"
        params.append(end_ms)
    conn.execute(f"DELETE FROM backtest_trades WHERE {where}", tuple(params))
    conn.commit()


def _insert_trade(
    conn: sqlite3.Connection,
    sym: str,
    side: str,
    entry_ts: int,
    entry_price: float,
    exit_ts: int,
    exit_price: float,
    gross_pnl_frac: float,
    fee_usd: float,
    best_pnl_frac: float,
    worst_pnl_frac: float,
    exit_reason: str,
    duration_sec: int,
    signal_reason: Optional[str],
    notional_usd: float
) -> None:
    gross_pnl_usd = notional_usd * gross_pnl_frac
    net_pnl_usd = gross_pnl_usd - fee_usd
    net_pnl_frac = net_pnl_usd / notional_usd

    conn.execute(
        """
        INSERT INTO backtest_trades
        (instId, side, entry_ts, entry_price, exit_ts, exit_price,
         gross_pnl_frac, net_pnl_frac, gross_pnl_usd, net_pnl_usd, fee_usd,
         best_pnl_frac, worst_pnl_frac, exit_reason, duration_sec, signal_reason, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            sym, side, entry_ts, entry_price, exit_ts, exit_price,
            gross_pnl_frac, net_pnl_frac, gross_pnl_usd, net_pnl_usd, fee_usd,
            best_pnl_frac, worst_pnl_frac, exit_reason, duration_sec, signal_reason, int(time.time())
        )
    )


def _summarize(conn: sqlite3.Connection, sym: str) -> str:
    row = conn.execute(
        """
        SELECT
          COUNT(*) AS n,
          SUM(CASE WHEN net_pnl_usd > 0 THEN 1 ELSE 0 END) AS wins,
          SUM(net_pnl_usd) AS net_sum,
          AVG(net_pnl_usd) AS net_avg,
          SUM(CASE WHEN net_pnl_usd > 0 THEN net_pnl_usd ELSE 0 END) AS gross_profit,
          SUM(CASE WHEN net_pnl_usd < 0 THEN -net_pnl_usd ELSE 0 END) AS gross_loss
        FROM backtest_trades
        WHERE instId=?
        """,
        (sym,)
    ).fetchone()

    n = int(row[0] or 0)
    if n == 0:
        return f"{sym}: no closed trades"

    wins = int(row[1] or 0)
    winrate = wins / n * 100.0
    net_sum = float(row[2] or 0.0)
    net_avg = float(row[3] or 0.0)
    gross_profit = float(row[4] or 0.0)
    gross_loss = float(row[5] or 0.0)
    pf = (gross_profit / gross_loss) if gross_loss > 0 else float("inf")

    # Equity curve max drawdown from chronological net_pnl_usd
    cur = conn.execute(
        "SELECT net_pnl_usd FROM backtest_trades WHERE instId=? ORDER BY entry_ts ASC, id ASC",
        (sym,)
    )
    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for (pnl_usd,) in cur.fetchall():
        equity += float(pnl_usd or 0.0)
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > max_dd:
            max_dd = dd

    return (
        f"{sym}: trades={n} winrate={winrate:.1f}% "
        f"net_sum=${net_sum:.2f} net_avg=${net_avg:.2f} PF={pf:.2f} maxDD=${max_dd:.2f}"
    )


def backtest(
    db_path: str,
    symbols: List[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    step: int,
    fee_roundtrip_pct: float,
    notional_usd: float,
    vol_gate: float,
    cooldown_minutes: int,
    clear: bool
) -> None:
    fs = FeatureService(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    if clear:
        clear_previous(conn, symbols, start_ms, end_ms)

    fee_frac = fee_roundtrip_pct / 100.0
    fee_usd = notional_usd * fee_frac
    cooldown_ms = int(cooldown_minutes * 60 * 1000)

    for sym in symbols:
        where = "instId=?"
        params: List = [sym]
        if start_ms is not None:
            where += " AND sec_ts>=?"
            params.append(start_ms)
        if end_ms is not None:
            where += " AND sec_ts<=?"
            params.append(end_ms)

        cur = conn.execute(f"SELECT sec_ts FROM agg_1s_micro WHERE {where} ORDER BY sec_ts ASC", tuple(params))
        sec_ts_list = [int(r[0]) for r in cur.fetchall()]
        if not sec_ts_list:
            print(f"{sym}: no agg_1s_micro rows in range")
            continue

        pos: Optional[VirtualPosition] = None
        last_entry_ts: Optional[int] = None

        signals = 0
        closed = 0

        for i, sec_ts in enumerate(sec_ts_list):
            if step > 1 and (i % step) != 0:
                continue

            f = fs.compute_features(sym, sec_ts)
            if not f:
                continue

            px = f.mid if f.mid is not None else f.vwap
            if px is None:
                continue
            px = float(px)

            # --- manage open position ---
            if pos is not None:
                p = pnl_frac(pos, px)
                pos.best_pnl = max(pos.best_pnl, p)
                pos.worst_pnl = min(pos.worst_pnl, p)

                exit_reason = should_exit_and_update_trail(pos, px)
                if exit_reason:
                    duration_sec = int((sec_ts - pos.entry_ts) / 1000)
                    _insert_trade(
                        conn=conn,
                        sym=sym,
                        side=pos.side,
                        entry_ts=pos.entry_ts,
                        entry_price=pos.entry_price,
                        exit_ts=sec_ts,
                        exit_price=px,
                        gross_pnl_frac=p,
                        fee_usd=fee_usd,
                        best_pnl_frac=pos.best_pnl,
                        worst_pnl_frac=pos.worst_pnl,
                        exit_reason=exit_reason,
                        duration_sec=duration_sec,
                        signal_reason=None,
                        notional_usd=notional_usd
                    )
                    conn.commit()
                    closed += 1
                    pos = None
                continue

            # --- cooldown for new entries ---
            if last_entry_ts is not None and (sec_ts - last_entry_ts) < cooldown_ms:
                continue

            sig = decide(f, vol_gate=vol_gate)
            if sig.side == "flat":
                continue

            # Open new position at mid
            pos = VirtualPosition(
                instId=sym,
                side=sig.side,
                entry_price=px,
                entry_ts=sec_ts,
                stop_loss=float(sig.stop_loss or 0.0),
                take_profit=float(sig.take_profit or 0.0),
                trailing_start=float(sig.trailing_start or 0.0),
                trailing_step=float(sig.trailing_step or 0.0),
            )
            last_entry_ts = sec_ts
            signals += 1

        # --- FORCE EXIT at end of dataset ---
        if pos is not None:
            last_ts = sec_ts_list[-1]
            f_last = fs.compute_features(sym, last_ts)
            px_last = None
            if f_last:
                px_last = f_last.mid if f_last.mid is not None else f_last.vwap
            if px_last is not None:
                px_last = float(px_last)
                p = pnl_frac(pos, px_last)
                duration_sec = int((last_ts - pos.entry_ts) / 1000)
                _insert_trade(
                    conn=conn,
                    sym=sym,
                    side=pos.side,
                    entry_ts=pos.entry_ts,
                    entry_price=pos.entry_price,
                    exit_ts=last_ts,
                    exit_price=px_last,
                    gross_pnl_frac=p,
                    fee_usd=fee_usd,
                    best_pnl_frac=pos.best_pnl,
                    worst_pnl_frac=pos.worst_pnl,
                    exit_reason="FORCE_EXIT",
                    duration_sec=duration_sec,
                    signal_reason=None,
                    notional_usd=notional_usd
                )
                conn.commit()
                closed += 1
            pos = None

        print(f"{sym}: signals={signals} trades_closed={closed} fee_roundtrip={fee_roundtrip_pct:.3f}% notional={notional_usd:.2f}")
        print("  " + _summarize(conn, sym))

    fs.close()
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Backtest your strategy on stored DB history (paper execution) — v2.")
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--from", dest="start", default=None, help="Start datetime (local), e.g. 2025-12-28 00:00:00")
    p.add_argument("--to", dest="end", default=None, help="End datetime (local)")
    p.add_argument("--step", type=int, default=1, help="Use every N seconds from agg_1s_micro (speed/approx).")
    p.add_argument("--fee", type=float, default=0.12, help="Round-trip fee percent of notional (e.g. 0.12).")
    p.add_argument("--notional", type=float, default=1000.0, help="Notional in USD/USDT (e.g. 1000 for 100x10).")
    p.add_argument("--vol-gate", type=float, default=0.0018, help="ATR gate fraction (0.0018=0.18%).")
    p.add_argument("--cooldown-min", type=int, default=5, help="Cooldown between entries per symbol (minutes).")
    p.add_argument("--clear", action="store_true", help="Delete previous backtest_trades for these symbols/range before run.")

    args = p.parse_args()
    start_ms = parse_dt_to_ms(args.start)
    end_ms = parse_dt_to_ms(args.end)

    backtest(
        db_path=args.db,
        symbols=args.symbols,
        start_ms=start_ms,
        end_ms=end_ms,
        step=max(1, int(args.step)),
        fee_roundtrip_pct=float(args.fee),
        notional_usd=float(args.notional),
        vol_gate=float(args.vol_gate),
        cooldown_minutes=int(args.cooldown_min),
        clear=bool(args.clear),
    )

    print("\nDone. Results in table backtest_trades.")
    print("Example SQL:")
    print("  SELECT instId, side, datetime(entry_ts/1000,'unixepoch') AS entry_time, datetime(exit_ts/1000,'unixepoch') AS exit_time,")
    print("         net_pnl_usd, net_pnl_frac*100 AS net_pnl_pct, exit_reason, duration_sec")
    print("  FROM backtest_trades ORDER BY id DESC LIMIT 20;")


if __name__ == "__main__":
    main()
