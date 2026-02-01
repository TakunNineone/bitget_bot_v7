from __future__ import annotations

import argparse
import sqlite3
import time
from dataclasses import dataclass
from typing import Dict, List, Optional
from datetime import datetime, timezone, timedelta

from .features import FeatureService, FeatureRow

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

    # NEW: Break-even controls (optional; execution/backtest can use them)
    breakeven_at: Optional[float] = None        # profit fraction when BE triggers (e.g., 0.003 = +0.30%)
    breakeven_offset: Optional[float] = None    # new SL offset from entry in fraction (e.g., 0.0002 = +0.02%)


# =========================
# Decision logic (FLOW-FIRST)
# =========================

def decide(f: FeatureRow, vol_gate: float = 0.0015) -> Signal:
    if f.spread_bps is None or float(f.spread_bps) > 8.0:
        return Signal(f.instId, f.ts, "flat", "spread")

    px = f.mid if f.mid is not None else f.vwap
    if px is None:
        return Signal(f.instId, f.ts, "flat", "no_price")
    if f.atr15m is None:
        return Signal(f.instId, f.ts, "flat", "no_atr")

    px = float(px)
    atr_pct = float(f.atr15m) / px

    trend_dir = getattr(f, "trend_dir_15m", None)
    if trend_dir is None or int(trend_dir) == 0:
        return Signal(f.instId, f.ts, "flat", "no_trend")
    allowed_side = "long" if int(trend_dir) > 0 else "short"

    ema_diff = getattr(f, "ema_diff_15m", None)
    ema_slope = getattr(f, "ema_slope_15m", None)
    if ema_diff is None or ema_slope is None:
        return Signal(f.instId, f.ts, "flat", "no_trend_strength")

    ema_diff = float(ema_diff)
    ema_slope = float(ema_slope)

    # trend strength (keep)
    DIFF_MIN_LONG = 0.00050
    SLOPE_MIN_LONG = 0.00002
    DIFF_MIN_SHORT = 0.00080
    SLOPE_MIN_SHORT = 0.00035

    if allowed_side == "long":
        if not (ema_diff >= DIFF_MIN_LONG and ema_slope >= SLOPE_MIN_LONG):
            return Signal(f.instId, f.ts, "flat", "weak_uptrend")
    else:
        if not (ema_diff <= -DIFF_MIN_SHORT and ema_slope <= -SLOPE_MIN_SHORT):
            return Signal(f.instId, f.ts, "flat", "weak_downtrend")

    MAX_DIFF_LONG = 0.0105
    if allowed_side == "long" and ema_diff > MAX_DIFF_LONG:
        return Signal(f.instId, f.ts, "flat", "long_overextended")

    doi = getattr(f, "doi", None)
    if doi is None:
        return Signal(f.instId, f.ts, "flat", "no_doi")
    doi = float(doi)
    if allowed_side == "short" and doi < 0.0:
        return Signal(f.instId, f.ts, "flat", "short_doi_negative")

    # ===== flow / fast-react score =====
    flow60 = int(getattr(f, "flow_60s", 0) or 0)
    spike = float(getattr(f, "flow_spike", 0.0) or 0.0)
    tc5 = int(getattr(f, "trade_count_sum_5s", 0) or 0)
    act5 = int(getattr(f, "active_seconds_5s", 0) or 0)
    range60 = float(f.range_60s_pct) if f.range_60s_pct is not None else None
    std60 = float(f.ret_std_60s) if f.ret_std_60s is not None else None

    SPIKE_MIN_LONG = 1.40
    SPIKE_MIN_SHORT = 1.60
    spike_min = SPIKE_MIN_LONG if allowed_side == "long" else SPIKE_MIN_SHORT
    spike_ok = (spike is not None and spike >= spike_min)

    score = 0
    if flow60 >= 60:
        score += 1
    if flow60 >= 90:
        score += 1
    if tc5 >= 2 and act5 >= 1:
        score += 1
    if range60 is not None and range60 >= 0.00045:
        score += 1
    if std60 is not None and std60 >= 0.000040:
        score += 1
    if spike_ok:
        score += 1

    # ===== entry trigger first (needed for "soft flow gate" on longs) =====
    dz = float(f.delta_rate_z or 0.0)
    imb = float(f.imb_shift or 0.0)
    mark_z_now = getattr(f, "mark_dev_z", None)

    dz_thr = 1.05 if spike_ok else 1.15
    imb_thr = 0.050

    if allowed_side == "long":
        dz_thr += 0.10
        imb_thr += 0.015
    else:
        if not spike_ok:
            dz_thr += 0.20

    # ===== flow gate (asymmetric: LONGS softer) =====
    if allowed_side == "long":
        # allow score>=1 if trigger is strong (otherwise too few trades)
        strong_trigger = (dz >= (dz_thr + 0.35) and imb >= (imb_thr + 0.03))
        if score < 2 and not (score >= 1 and strong_trigger):
            return Signal(f.instId, f.ts, "flat", "no_flow_confirm")
    else:
        # SAFE short loosening:
        # allow score==1 only if impulse is clearly strong AND flow baseline is not dead.
        strong_impulse = (dz <= -(dz_thr + 0.40) and imb <= -(imb_thr + 0.05))
        flow_ok = (flow60 >= 55) or (tc5 >= 3 and act5 >= 2)

        if score < 2 and not (score >= 1 and flow_ok and strong_impulse):
            return Signal(f.instId, f.ts, "flat", "no_flow_confirm")

    # ATR gate secondary
    if atr_pct < vol_gate:
        fast_ok = (range60 is not None and range60 >= 0.00055) or (std60 is not None and std60 >= 0.000050)
        if not fast_ok:
            return Signal(f.instId, f.ts, "flat", "low_vol")

    # ===== long quality filters (relaxed but still effective) =====
    if allowed_side == "long":
        ret_15m_before = getattr(f, "ret_15m_before", None)
        dist_from_15m_high = getattr(f, "dist_from_15m_high", None)
        dz_mean_15s_before = getattr(f, "dz_mean_15s_before", None)
        mark_z = getattr(f, "mark_dev_z", None)

        # late entry keep
        if ret_15m_before is not None and float(ret_15m_before) > 0.015:
            return Signal(f.instId, f.ts, "flat", "long_late_entry_15m")

        # near-high tighter (0.10% -> 0.06%)
        if dist_from_15m_high is not None and float(dist_from_15m_high) < 0.0006:
            return Signal(f.instId, f.ts, "flat", "long_near_15m_high")

        # dz fading relaxed (2.0 -> 1.4)
        if dz_mean_15s_before is not None and float(dz_mean_15s_before) < 1.4:
            return Signal(f.instId, f.ts, "flat", "long_dz_fading")

        # mark overpriced: only kill if ALSO near-high (avoid deleting continuations)
        if (mark_z is not None and float(mark_z) > 0.8) and (dist_from_15m_high is not None and float(dist_from_15m_high) < 0.0015):
            return Signal(f.instId, f.ts, "flat", "long_mark_overpriced")

        # keep stretch guard
        if ema_diff > 0.0065 and dz < 3.0:
            return Signal(f.instId, f.ts, "flat", "long_weak_dz_in_stretch")

        ok = (dz >= dz_thr) and (imb >= imb_thr) and (mark_z_now is None or float(mark_z_now) > -1.0)
        if not ok:
            return Signal(f.instId, f.ts, "flat", "no_trigger")
        side = "long"
    else:
        # shorts unchanged
        ok = (dz <= -dz_thr) and (imb <= -imb_thr) and (mark_z_now is None or float(mark_z_now) < 1.8)
        if not ok:
            return Signal(f.instId, f.ts, "flat", "no_trigger")
        side = "short"

    # ===== exits / risk =====
    impulse = spike_ok or (score >= 4)
    sl_atr = max(0.0032, min(0.0100, 0.95 * atr_pct))

    if impulse:
        sl = max(sl_atr, 0.0055)
        sl = min(sl, 0.0140)
        tp = max(0.0130, min(0.0350, 2.1 * sl))
        tr_start = max(0.0060, min(0.0200, 0.55 * tp))
        tr_step = max(0.0016, min(0.0060, 0.26 * sl))
    else:
        sl = max(sl_atr, 0.0035)
        tp = max(0.0100, min(0.0220, 2.0 * sl))
        tr_start = max(0.0042, min(0.0120, 0.52 * tp))
        tr_step = max(0.0012, min(0.0045, 0.30 * sl))

    breakeven_at = 0.0030
    breakeven_offset = 0.0014

    reason = (
        f"trend={int(trend_dir)}({allowed_side}) doi={doi:+.2f} "
        f"diff={ema_diff:+.5f} slope={ema_slope:+.6f} "
        f"spike={spike:.2f}(ok={1 if spike_ok else 0}) score={score} imp={1 if impulse else 0} "
        f"dz={dz:.2f}(thr={dz_thr:.2f}) imb={imb:.3f}(thr={imb_thr:.3f}) "
        f"atr={atr_pct:.4f} flow60={flow60} tc5={tc5}/{act5} "
        f"R60={(range60 or 0.0):.4f} Std60={(std60 or 0.0):.6f}"
    )

    return Signal(
        f.instId, f.ts, side, reason,
        stop_loss=sl,
        take_profit=tp,
        trailing_start=tr_start,
        trailing_step=tr_step,
        breakeven_at=breakeven_at,
        breakeven_offset=breakeven_offset,
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
        "no_flow_confirm": 0,
        "no_flow_5s": 0,
        "low_vol": 0,
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
                "flow60": getattr(f, "flow_60s", None),
                "spike": getattr(f, "flow_spike", None),
                "tc5": getattr(f, "trade_count_sum_5s", None),
                "act5": getattr(f, "active_seconds_5s", None),
                "range60": getattr(f, "range_60s_pct", None),
                "retstd60": getattr(f, "ret_std_60s", None),
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
                f"[{datetime.now(MSK).strftime('%H:%M:%S')}] {instId.upper()} {sig.side.upper()} "
                f"SL={sig.stop_loss:.4f} TP={sig.take_profit:.4f} "
                f"TR={sig.trailing_start:.4f}/{sig.trailing_step:.4f} "
                f"BE={sig.breakeven_at:.4f}@{sig.breakeven_offset:.4f} :: {sig.reason}"
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
