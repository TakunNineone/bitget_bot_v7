from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Optional, Dict, List

from .features import FeatureService, FeatureRow


@dataclass
class Signal:
    instId: str
    ts: int
    side: str  # "long" | "short" | "flat"
    reason: str
    stop_loss: Optional[float] = None          # fraction, e.g. 0.0035
    take_profit: Optional[float] = None        # fraction, e.g. 0.0100
    trailing_start: Optional[float] = None     # fraction
    trailing_step: Optional[float] = None      # fraction


@dataclass
class VirtualPosition:
    instId: str
    side: str                 # "long" or "short"
    entry_price: float
    entry_ts: int
    stop_loss: float          # fraction
    take_profit: float        # fraction
    trailing_start: float     # fraction
    trailing_step: float      # fraction

    best_pnl: float = 0.0
    worst_pnl: float = 0.0
    trail_active: bool = False
    trail_level: Optional[float] = None  # price level


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _pnl_frac(pos: VirtualPosition, price: float) -> float:
    """Unrealized PnL as fraction (e.g. 0.01 = +1%)."""
    if pos.side == "long":
        return price / pos.entry_price - 1.0
    return pos.entry_price / price - 1.0


def _should_exit_and_update_trail(pos: VirtualPosition, price: float) -> Optional[str]:
    """Updates trailing state and returns exit reason: SL/TP/TRAIL or None."""
    p = _pnl_frac(pos, price)

    # Hard SL / TP
    if p <= -pos.stop_loss:
        return "SL"
    if p >= pos.take_profit:
        return "TP"

    # Activate trailing
    if (not pos.trail_active) and p >= pos.trailing_start:
        pos.trail_active = True
        if pos.side == "long":
            pos.trail_level = price * (1.0 - pos.trailing_step)
        else:
            pos.trail_level = price * (1.0 + pos.trailing_step)

    # Update trailing & check trigger
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


def decide(f: FeatureRow) -> Signal:
    # ---- HARD GATES ----
    if f.spread_bps is None or f.spread_bps > 8.0:
        return Signal(f.instId, f.ts, "flat", "spread")

    px = f.mid if f.mid is not None else f.vwap
    if px is None:
        return Signal(f.instId, f.ts, "flat", "no_price")

    if f.atr15m is None:
        return Signal(f.instId, f.ts, "flat", "no_atr")

    atr_pct = f.atr15m / px

    # your current tuned gate
    if atr_pct < 0.0018:
        return Signal(f.instId, f.ts, "flat", "low_vol")

    # ---- FLOW GATE ----
    if f.trade_count_sum_5s < 5 or f.active_seconds_5s < 3:
        return Signal(f.instId, f.ts, "flat", "no_flow")

    # ---- TRIGGER ----
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

    reason = (
        f"dz={dz:.2f} imb={imb:.3f} atr={atr_pct:.4f} "
        f"flow5s={f.trade_count_sum_5s}/{f.active_seconds_5s}"
    )

    return Signal(
        f.instId, f.ts, side, reason,
        stop_loss=sl,
        take_profit=tp,
        trailing_start=trailing_start,
        trailing_step=trailing_step
    )


def latest_sec_ts(conn: sqlite3.Connection, instId: str) -> Optional[int]:
    cur = conn.execute("SELECT MAX(sec_ts) FROM agg_1s_micro WHERE instId=?", (instId,))
    r = cur.fetchone()
    return int(r[0]) if r and r[0] is not None else None


def run(sqlite_path: str, instIds: List[str], every_sec: int = 2) -> None:
    fs = FeatureService(sqlite_path)
    conn = sqlite3.connect(sqlite_path)

    cooldown_ms = 5 * 60 * 1000
    last_signal_ts: Dict[str, int] = {}

    positions: Dict[str, VirtualPosition] = {}
    last_pnl_print: Dict[str, float] = {}

    stats = {
        "spread": 0,
        "no_price": 0,
        "no_atr": 0,
        "low_vol": 0,
        "no_flow": 0,
        "no_trigger": 0,
        "cooldown": 0,
        "signals": 0,
        "entries": 0,
        "exits": 0,
    }

    last_report = time.time()

    try:
        while True:
            for instId in instIds:
                sec_ts = latest_sec_ts(conn, instId)
                if sec_ts is None:
                    continue

                f = fs.compute_features(instId, sec_ts)
                if not f:
                    continue

                px = f.mid if f.mid is not None else f.vwap

                # ---- PAPER PNL UPDATE (even during cooldown) ----
                if px is not None and instId in positions:
                    pos = positions[instId]
                    p = _pnl_frac(pos, float(px))
                    pos.best_pnl = max(pos.best_pnl, p)
                    pos.worst_pnl = min(pos.worst_pnl, p)

                    exit_reason = _should_exit_and_update_trail(pos, float(px))

                    now = time.time()
                    if (now - last_pnl_print.get(instId, 0.0)) >= 5.0:
                        trail = f"{pos.trail_level:.4f}" if pos.trail_level is not None else "-"
                        print(
                            f"[PNL] {instId} {pos.side.upper()} px={float(px):.4f} "
                            f"pnl={p*100:.2f}% best={pos.best_pnl*100:.2f}% worst={pos.worst_pnl*100:.2f}% "
                            f"trail={trail}"
                        )
                        last_pnl_print[instId] = now

                    if exit_reason:
                        print(
                            f"[EXIT] {instId} {pos.side.upper()} reason={exit_reason} "
                            f"pnl={p*100:.2f}% best={pos.best_pnl*100:.2f}% worst={pos.worst_pnl*100:.2f}%"
                        )
                        del positions[instId]
                        stats["exits"] += 1

                # One position per symbol in this skeleton
                if instId in positions:
                    continue

                # ---- COOLDOWN FOR NEW ENTRIES ----
                last_ts = last_signal_ts.get(instId)
                if last_ts is not None and (sec_ts - last_ts) < cooldown_ms:
                    stats["cooldown"] += 1
                    continue

                sig = decide(f)

                if sig.side == "flat":
                    stats[sig.reason] = stats.get(sig.reason, 0) + 1
                    continue

                last_signal_ts[instId] = sec_ts
                stats["signals"] += 1

                print(
                    f"[{time.strftime('%H:%M:%S')}] {sig.instId} {sig.side.upper()} "
                    f"SL={sig.stop_loss:.4f} TP={sig.take_profit:.4f} "
                    f"TR={sig.trailing_start:.4f}/{sig.trailing_step:.4f} :: {sig.reason}"
                )

                # ---- OPEN VIRTUAL POSITION ----
                if px is not None and sig.stop_loss and sig.take_profit and sig.trailing_start and sig.trailing_step:
                    positions[instId] = VirtualPosition(
                        instId=instId,
                        side=sig.side,
                        entry_price=float(px),
                        entry_ts=sec_ts,
                        stop_loss=float(sig.stop_loss),
                        take_profit=float(sig.take_profit),
                        trailing_start=float(sig.trailing_start),
                        trailing_step=float(sig.trailing_step),
                    )
                    stats["entries"] += 1
                    print(f"[ENTRY] {instId} {sig.side.upper()} entry={float(px):.4f}")

            if time.time() - last_report >= 30:
                print("\n[STATS 30s] " + " | ".join(f"{k}:{v}" for k, v in stats.items()))
                stats = {k: 0 for k in stats}
                last_report = time.time()

            time.sleep(every_sec)

    finally:
        fs.close()
        conn.close()


def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--every", type=int, default=2)
    args = p.parse_args()
    run(args.db, args.symbols, args.every)


if __name__ == "__main__":
    cli()
