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
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_start: Optional[float] = None
    trailing_step: Optional[float] = None


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def decide(f: FeatureRow) -> Signal:
    # ---- HARD GATES ----
    if f.spread_bps is None or f.spread_bps > 8.0:
        return Signal(f.instId, f.ts, "flat", "spread")

    # Price for ATR%: prefer mid; fallback to vwap
    px = f.mid if f.mid is not None else f.vwap
    if px is None:
        return Signal(f.instId, f.ts, "flat", "no_price")

    if f.atr15m is None:
        return Signal(f.instId, f.ts, "flat", "no_atr")

    atr_pct = f.atr15m / px

    # target 1–2% on 15m needs enough vol
    if atr_pct < 0.0025:
        return Signal(f.instId, f.ts, "flat", "low_vol")

    # ---- FLOW GATE ----
    print('trade_count_sum_5s',f.trade_count_sum_5s)
    print('active_seconds_5s',f.active_seconds_5s)
    if f.trade_count_sum_5s < 5 or f.active_seconds_5s < 3:
        return Signal(f.instId, f.ts, "flat", "no_flow")

    # ---- TRIGGER ----
    dz = f.delta_rate_z or 0.0
    imb = f.imb_shift or 0.0
    mark_z = f.mark_dev_z  # may be None early

    dz_thr = 1.0 if mark_z is not None else 1.5

    long_ok = dz > dz_thr and imb > 0.05 and (mark_z is None or mark_z > -1.5)
    short_ok = dz < -dz_thr and imb < -0.05 and (mark_z is None or mark_z < 1.5)

    if not long_ok and not short_ok:
        return Signal(f.instId, f.ts, "flat", "no_trigger")

    side = "long" if long_ok else "short"

    # ---- RISK PARAMS from ATR ----
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

    stats = {
        "spread": 0,
        "no_price": 0,
        "no_atr": 0,
        "low_vol": 0,
        "no_flow": 0,
        "no_trigger": 0,
        "cooldown": 0,
        "signals": 0,
    }

    last_report = time.time()

    try:
        while True:
            for instId in instIds:
                sec_ts = latest_sec_ts(conn, instId)
                if sec_ts is None:
                    continue

                last_ts = last_signal_ts.get(instId)
                if last_ts is not None and (sec_ts - last_ts) < cooldown_ms:
                    stats["cooldown"] += 1
                    continue

                f = fs.compute_features(instId, sec_ts)
                if not f:
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
