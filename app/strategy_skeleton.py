from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

from .features import FeatureService, FeatureRow
from datetime import datetime, timezone, timedelta

MSK = timezone(timedelta(hours=3))

# =========================
# Signal
# =========================

@dataclass
class Signal:
    instId: str
    ts: int
    side: str  # long / short / flat
    reason: str
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    trailing_start: Optional[float] = None
    trailing_step: Optional[float] = None


# =========================
# Decision logic
# =========================

def decide(f: FeatureRow, vol_gate: float = 0.0015) -> Signal:
    # ---- SPREAD ----
    if f.spread_bps is None or f.spread_bps > 8.0:
        return Signal(f.instId, f.ts, "flat", "spread")

    px = f.mid if f.mid is not None else f.vwap
    if px is None:
        return Signal(f.instId, f.ts, "flat", "no_price")

    if f.atr15m is None:
        return Signal(f.instId, f.ts, "flat", "no_atr")

    atr_pct = f.atr15m / px

    # ---- FAST REACT ----
    fast_vol = (
        (f.range_60s_pct is not None and f.range_60s_pct >= 0.0012) or
        (f.ret_std_60s is not None and f.ret_std_60s >= 0.00045)
    )
    fast_flow = f.flow_spike is not None and f.flow_spike >= 1.6

    # print('atr_pct:',atr_pct,'vol_gate:',vol_gate)
    if atr_pct < vol_gate:
        if not (fast_vol and fast_flow):
            return Signal(f.instId, f.ts, "flat", "low_vol")

    # ---- FLOW GATE (ROBUST) ----
    flow60 = f.flow_60s or 0
    spike = float(f.flow_spike or 0.0)

    # базовая ликвидность ~2 трейда/сек
    # print('flow60:',flow60,'spike:',spike)
    if flow60 < 120 and spike < 1.6:
        return Signal(f.instId, f.ts, "flat", "no_flow")

    # ускорение в последние секунды
    if f.trade_count_sum_5s < 2 and spike < 1.6:
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

    sl = max(0.0035, min(0.0120, 0.9 * atr_pct))
    tp = max(0.0100, min(0.0200, 2.0 * sl))
    tr_start = max(0.0060, min(0.0150, 0.8 * tp))
    tr_step = max(0.0015, min(0.0040, 0.35 * sl))

    reason = (
        f"dz={dz:.2f} imb={imb:.3f} atr={atr_pct:.4f} "
        f"flow5s={f.trade_count_sum_5s}/{f.active_seconds_5s} "
        f"flow60={flow60} spike={spike:.2f} "
        f"fastR={f.range_60s_pct or 0:.4f} fastStd={f.ret_std_60s or 0:.6f}"
    )

    return Signal(
        f.instId, f.ts, side, reason,
        stop_loss=sl,
        take_profit=tp,
        trailing_start=tr_start,
        trailing_step=tr_step,
    )


# =========================
# Runner
# =========================

def run(db_path: str, symbols: List[str], every: int):
    fs = FeatureService(db_path)
    conn = sqlite3.connect(db_path)

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

    last_signal_ts: Dict[str, int] = {}
    last_report = time.time()

    # debug snapshots
    last_flow_debug: Dict[str, dict] = {}

    instIds = symbols

    while True:
        for instId in instIds:
            row = conn.execute(
                "SELECT MAX(sec_ts) FROM agg_1s_micro WHERE instId=?",
                (instId,),
            ).fetchone()

            if not row or not row[0]:
                continue

            sec_ts = int(row[0])

            f = fs.compute_features(instId, sec_ts)
            if not f:
                continue

            # store snapshot for debug
            last_flow_debug[instId] = {
                "flow60": f.flow_60s,
                "spike": f.flow_spike,
                "tc5": f.trade_count_sum_5s,
                "act5": f.active_seconds_5s,
                "range60": f.range_60s_pct,
                "retstd60": f.ret_std_60s,
            }

            sig = decide(f)

            if sig.side == "flat":
                stats[sig.reason] = stats.get(sig.reason, 0) + 1
                continue

            # cooldown 5 minutes
            if instId in last_signal_ts and sec_ts - last_signal_ts[instId] < 5 * 60 * 1000:
                stats["cooldown"] += 1
                continue

            last_signal_ts[instId] = sec_ts
            stats["signals"] += 1
            stats["entries"] += 1

            print(
                f"[{time.strftime('%H:%M:%S')}] {instId.upper()} {sig.side.upper()} "
                f"SL={sig.stop_loss:.4f} TP={sig.take_profit:.4f} "
                f"TR={sig.trailing_start:.4f}/{sig.trailing_step:.4f} :: {sig.reason}"
            )

        # periodic report
        if time.time() - last_report >= 30:
            print("\n[STATS 30s] " + " | ".join(f"{k}:{v}" for k, v in stats.items()))

            def fmt(x, nd=2):
                if x is None:
                    return "NA"
                try:
                    return f"{float(x):.{nd}f}"
                except Exception:
                    return str(x)

            for sid in instIds:
                snap = last_flow_debug.get(sid)
                if not snap:
                    continue

                flow60 = snap.get("flow60")
                spike = snap.get("spike")
                tc5 = snap.get("tc5")
                act5 = snap.get("act5")
                r60 = snap.get("range60")
                s60 = snap.get("retstd60")

                print(
                    f"[FLOW] {sid} "
                    f"flow60={flow60 if flow60 is not None else 'NA'} "
                    f"spike={fmt(spike, 2)} "
                    f"tc5={tc5 if tc5 is not None else 'NA'} act5={act5 if act5 is not None else 'NA'} "
                    f"range60={fmt(r60, 4)} std60={fmt(s60, 6)}"
                )

            for k in stats:
                stats[k] = 0
            last_report = time.time()

        time.sleep(every)


# =========================
# CLI
# =========================

def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", nargs="+", required=True)
    p.add_argument("--every", type=int, default=2)
    args = p.parse_args()

    run(args.db, args.symbols, args.every)


if __name__ == "__main__":
    cli()
