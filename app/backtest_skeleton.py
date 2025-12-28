from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, List

from .features import FeatureService, FeatureRow
from .strategy_skeleton import decide, Signal
from datetime import datetime, timezone, timedelta

MSK = timezone(timedelta(hours=3))

@dataclass
class VirtualPosition:
    instId: str
    side: str
    entry_ts: int
    entry_price: float
    stop_loss: float
    take_profit: float
    trailing_start: float
    trailing_step: float

    best_pnl: float = 0.0
    worst_pnl: float = 0.0
    trail_active: bool = False
    trail_level: Optional[float] = None


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


def insert_trade(
    conn: sqlite3.Connection,
    sym: str,
    pos: VirtualPosition,
    exit_ts: int,
    exit_price: float,
    exit_reason: str,
    fee_usd: float,
    notional: float,
    signal_reason: Optional[str],
) -> None:
    gp = pnl_frac(pos, exit_price)
    gross_usd = notional * gp
    net_usd = gross_usd - fee_usd
    net_frac = net_usd / notional
    duration = int((exit_ts - pos.entry_ts) / 1000)

    conn.execute(
        """
        INSERT INTO backtest_trades
        (instId, side, entry_ts, entry_price, exit_ts, exit_price,
         gross_pnl_frac, net_pnl_frac, gross_pnl_usd, net_pnl_usd, fee_usd,
         best_pnl_frac, worst_pnl_frac, exit_reason, duration_sec, signal_reason, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            sym, pos.side, pos.entry_ts, pos.entry_price, exit_ts, exit_price,
            gp, net_frac, gross_usd, net_usd, fee_usd,
            pos.best_pnl, pos.worst_pnl, exit_reason, duration, signal_reason, int(time.time())
        )
    )


def backtest(
    db_path: str,
    symbols: List[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    step: int,
    fee_roundtrip_pct: float,
    notional: float,
    cooldown_min: int,
    clear: bool,
) -> None:
    fs = FeatureService(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)

    if clear:
        ph = ",".join("?" for _ in symbols)
        conn.execute(f"DELETE FROM backtest_trades WHERE instId IN ({ph})", tuple(symbols))
        conn.commit()

    fee_usd = notional * (fee_roundtrip_pct / 100.0)
    cooldown_ms = int(cooldown_min * 60 * 1000)

    for sym in symbols:
        where = "instId=?"
        params: List = [sym]
        if start_ms is not None:
            where += " AND sec_ts>=?"
            params.append(start_ms)
        if end_ms is not None:
            where += " AND sec_ts<=?"
            params.append(end_ms)

        sec_ts_list = [int(r[0]) for r in conn.execute(
            f"SELECT sec_ts FROM agg_1s_micro WHERE {where} ORDER BY sec_ts ASC",
            tuple(params),
        ).fetchall()]

        if not sec_ts_list:
            print(f"{sym}: no data")
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

            # manage open position
            if pos is not None:
                p = pnl_frac(pos, px)
                pos.best_pnl = max(pos.best_pnl, p)
                pos.worst_pnl = min(pos.worst_pnl, p)

                reason = should_exit_and_update_trail(pos, px)
                if reason:
                    insert_trade(conn, sym, pos, sec_ts, px, reason, fee_usd, notional, None)
                    conn.commit()
                    closed += 1
                    pos = None
                continue

            # cooldown for new entries
            if last_entry_ts is not None and (sec_ts - last_entry_ts) < cooldown_ms:
                continue

            sig: Signal = decide(f)  # <-- SINGLE SOURCE OF TRUTH
            if sig.side == "flat":
                continue

            # open
            pos = VirtualPosition(
                instId=sym,
                side=sig.side,
                entry_ts=sec_ts,
                entry_price=px,
                stop_loss=float(sig.stop_loss or 0.0),
                take_profit=float(sig.take_profit or 0.0),
                trailing_start=float(sig.trailing_start or 0.0),
                trailing_step=float(sig.trailing_step or 0.0),
            )
            last_entry_ts = sec_ts
            signals += 1
            # store reason on entry in-memory if you want; for now keep None in DB

        # force exit at end
        if pos is not None:
            last_ts = sec_ts_list[-1]
            f_last = fs.compute_features(sym, last_ts)
            px_last = None
            if f_last:
                px_last = f_last.mid if f_last.mid is not None else f_last.vwap
            if px_last is not None:
                insert_trade(conn, sym, pos, last_ts, float(px_last), "FORCE_EXIT", fee_usd, notional, None)
                conn.commit()
                closed += 1
            pos = None

        print(f"{sym}: signals={signals} trades_closed={closed} fee={fee_roundtrip_pct:.3f}% notional={notional:.2f}")

    fs.close()
    conn.close()


def main():
    p = argparse.ArgumentParser(description="Backtest that reuses app.strategy_skeleton.decide() (single source of truth).")
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--from", dest="start", default=None)
    p.add_argument("--to", dest="end", default=None)
    p.add_argument("--step", type=int, default=1)
    p.add_argument("--fee", type=float, default=0.12)
    p.add_argument("--notional", type=float, default=1000.0)
    p.add_argument("--cooldown-min", type=int, default=5)
    p.add_argument("--clear", action="store_true")
    args = p.parse_args()

    backtest(
        db_path=args.db,
        symbols=args.symbols,
        start_ms=parse_dt_to_ms(args.start),
        end_ms=parse_dt_to_ms(args.end),
        step=max(1, args.step),
        fee_roundtrip_pct=args.fee,
        notional=args.notional,
        cooldown_min=args.cooldown_min,
        clear=args.clear,
    )

    print("\nDone. Results in backtest_trades.")


if __name__ == "__main__":
    main()
