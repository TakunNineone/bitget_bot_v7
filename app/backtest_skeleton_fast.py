from __future__ import annotations

import argparse
import sqlite3
import time
import math
from collections import deque
from dataclasses import dataclass
from typing import Optional, List, Tuple, Any, Dict

from .strategy_skeleton import decide, Signal


# -------------------------
# small utils for v6 features
# -------------------------
def _mean(xs: List[Optional[float]]) -> Optional[float]:
    ys = [x for x in xs if x is not None]
    if not ys:
        return None
    return sum(ys) / len(ys)


def pct_change(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or b == 0:
        return None
    return (a - b) / b


# ============================================================
# FeatureRowLite: stable duck-typed object for decide()
# (does NOT depend on features.FeatureRow constructor)
# ============================================================

@dataclass(slots=True)
class FeatureRowLite:
    instId: str
    ts: int

    # price
    mid: Optional[float] = None
    vwap: Optional[float] = None
    spread_bps: Optional[float] = None
    ret_1s: Optional[float] = None

    # micro
    trade_count: int = 0
    trade_count_sum_5s: int = 0
    active_seconds_5s: int = 0

    delta_vol: Optional[float] = None
    delta_rate_z: Optional[float] = None

    imbalance15: Optional[float] = None
    imb_shift: Optional[float] = None

    # fast-react / flow
    range_60s_pct: Optional[float] = None
    ret_std_60s: Optional[float] = None
    flow_60s: int = 0
    flow_spike: Optional[float] = None

    # 15m regime (required by decide)
    atr15m: Optional[float] = None
    ema_fast_15m: Optional[float] = None
    ema_slow_15m: Optional[float] = None
    ema_diff_15m: Optional[float] = None
    ema_slope_15m: Optional[float] = None
    trend_dir_15m: Optional[int] = None

    # 1m context
    mark_last: Optional[float] = None
    index_last: Optional[float] = None
    mark_dev_z: Optional[float] = None
    oi: Optional[float] = None
    doi: Optional[float] = None
    funding: Optional[float] = None

    # NEW: context before entry (for decide v6)
    ret_1m_before: Optional[float] = None
    ret_5m_before: Optional[float] = None
    ret_15m_before: Optional[float] = None
    dist_from_15m_high: Optional[float] = None
    dist_from_15m_low: Optional[float] = None
    dz_mean_5s_before: Optional[float] = None
    dz_mean_15s_before: Optional[float] = None
    imb_mean_5s_before: Optional[float] = None
    imb_mean_15s_before: Optional[float] = None
    flow_accel_30s: Optional[float] = None


# =========================
# Virtual position / execution
# =========================

@dataclass(slots=True)
class VirtualPosition:
    instId: str
    side: str
    entry_ts: int
    entry_price: float
    stop_loss: float
    take_profit: float
    trailing_start: float
    trailing_step: float
    breakeven_at: Optional[float] = None
    breakeven_offset: Optional[float] = None

    be_active: bool = False
    be_price: Optional[float] = None

    trail_active: bool = False
    trail_stop: Optional[float] = None


# =========================
# Rolling helpers
# =========================

class RollingFloatWindow:
    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.vals: List[float] = []

    def push(self, v: float) -> None:
        self.vals.append(v)
        if len(self.vals) > self.maxlen:
            self.vals.pop(0)

    def count(self) -> int:
        return len(self.vals)


class RollingIntWindow:
    maxlen: int
    vals: List[int]
    s: int

    def __init__(self, maxlen: int):
        self.maxlen = maxlen
        self.vals = []
        self.s = 0

    def push(self, v: int) -> None:
        self.vals.append(v)
        self.s += v
        if len(self.vals) > self.maxlen:
            old = self.vals.pop(0)
            self.s -= old

    def count(self) -> int:
        return len(self.vals)


# =========================
# DB loads
# =========================

def load_1s_micro(conn: sqlite3.Connection, instId: str, start_ms: Optional[int], end_ms: Optional[int]) -> List[sqlite3.Row]:
    q = """
        SELECT sec_ts, mid, vwap, spread_bps, ret_1s, trade_count, delta_vol, imbalance15
        FROM agg_1s_micro
        WHERE instId=?
    """
    params: List[Any] = [instId]
    if start_ms is not None:
        q += " AND sec_ts>=?"
        params.append(start_ms)
    if end_ms is not None:
        q += " AND sec_ts<=?"
        params.append(end_ms)
    q += " ORDER BY sec_ts ASC"
    return conn.execute(q, tuple(params)).fetchall()


def load_15m_candles(conn: sqlite3.Connection, instId: str) -> List[sqlite3.Row]:
    q = """
        SELECT candle_ts, o, h, l, c
        FROM raw_ws_candle
        WHERE instId=? AND interval='candle15m'
        ORDER BY candle_ts ASC
    """
    return conn.execute(q, (instId,)).fetchall()


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    r = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return r is not None


def load_15m_trend(conn: sqlite3.Connection, instId: str) -> List[sqlite3.Row]:
    """Load precomputed 15m regime from agg_15m_trend.

    Columns expected:
      candle_ts, atr14, ema20, ema50, ema_diff, ema_slope, trend_dir
    """
    q = """
        SELECT candle_ts, atr14, ema20, ema50, ema_diff, ema_slope, trend_dir
        FROM agg_15m_trend
        WHERE instId=?
        ORDER BY candle_ts ASC
    """
    return conn.execute(q, (instId,)).fetchall()


def load_1m_context(conn: sqlite3.Connection, instId: str) -> List[sqlite3.Row]:
    q = """
        SELECT min_ts, mark_last, index_last, oi, doi, funding
        FROM agg_1m_context
        WHERE instId=?
        ORDER BY min_ts ASC
    """
    return conn.execute(q, (instId,)).fetchall()


# =========================
# Regime calc (ATR + EMA)
# =========================

def compute_atr_series(candles15: List[sqlite3.Row], period: int = 14) -> Tuple[List[int], List[Optional[float]]]:
    if len(candles15) < period + 1:
        return [], []
    ts = [int(r["candle_ts"]) for r in candles15]
    ohlc = []
    for r in candles15:
        try:
            o = float(r["o"]); h = float(r["h"]); l = float(r["l"]); c = float(r["c"])
            ohlc.append((o, h, l, c))
        except Exception:
            ohlc.append((None, None, None, None))

    trs: List[Optional[float]] = [None] * len(ohlc)
    prev_close = ohlc[0][3]
    for i in range(1, len(ohlc)):
        o, h, l, c = ohlc[i]
        if h is None or l is None or c is None or prev_close is None:
            trs[i] = None
        else:
            trs[i] = max(h - l, abs(h - prev_close), abs(l - prev_close))
        prev_close = c

    atr: List[Optional[float]] = [None] * len(ohlc)
    # seed
    seed = [x for x in trs[1:period+1] if x is not None]
    if len(seed) < period:
        return ts, atr
    a = sum(seed) / period
    atr[period] = a
    for i in range(period + 1, len(trs)):
        tr = trs[i]
        if tr is None or atr[i-1] is None:
            atr[i] = atr[i-1]
        else:
            atr[i] = (atr[i-1] * (period - 1) + tr) / period
    return ts, atr


def ema_series(closes: List[float], period: int) -> List[Optional[float]]:
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1.0)
    out: List[Optional[float]] = [None] * len(closes)
    seed = sum(closes[:period]) / period
    ema = seed
    out[period-1] = ema
    for i in range(period, len(closes)):
        ema = (closes[i] - ema) * k + ema
        out[i] = ema
    return out


def compute_regime_series(candles15: List[sqlite3.Row]) -> Tuple[List[int], List[Optional[float]], List[Optional[float]], List[Optional[float]], List[Optional[float]], List[Optional[int]]]:
    if not candles15:
        return [], [], [], [], [], []

    ts = [int(r["candle_ts"]) for r in candles15]
    closes: List[float] = []
    for r in candles15:
        try:
            closes.append(float(r["c"]))
        except Exception:
            closes.append(float("nan"))

    ema_fast = ema_series(closes, 20)
    ema_slow = ema_series(closes, 50)

    ema_diff: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is None or ema_slow[i] is None:
            continue
        c = closes[i]
        if c and c > 0 and not math.isnan(c):
            ema_diff[i] = (ema_fast[i] - ema_slow[i]) / c

    # slope of fast EMA over last 12 points
    ema_slope: List[Optional[float]] = [None] * len(closes)
    for i in range(len(closes)):
        if ema_fast[i] is None:
            continue
        j = i - 11
        if j < 0 or ema_fast[j] is None:
            continue
        base = ema_fast[i]
        if base and base != 0:
            ema_slope[i] = (ema_fast[i] - ema_fast[j]) / abs(base) / 11.0

    trend_dir: List[Optional[int]] = [None] * len(closes)
    for i in range(len(closes)):
        d = ema_diff[i]
        if d is None:
            continue
        if d > 0.0004:
            trend_dir[i] = 1
        elif d < -0.0004:
            trend_dir[i] = -1
        else:
            trend_dir[i] = 0

    return ts, ema_fast, ema_slow, ema_diff, ema_slope, trend_dir


def compute_mark_dev_z(ctx_rows: List[sqlite3.Row], lookback: int = 60) -> Tuple[List[int], List[Optional[float]]]:
    if not ctx_rows:
        return [], []
    ts = [int(r["min_ts"]) for r in ctx_rows]
    out: List[Optional[float]] = [None] * len(ctx_rows)

    devs: List[Optional[float]] = [None] * len(ctx_rows)
    for i, r in enumerate(ctx_rows):
        m = r["mark_last"]
        ix = r["index_last"]
        if m is None or ix is None:
            continue
        try:
            m = float(m); ix = float(ix)
            if ix != 0:
                devs[i] = (m - ix) / ix
        except Exception:
            continue

    for i in range(len(ctx_rows)):
        if devs[i] is None:
            continue
        start = max(0, i - lookback)
        xs = [d for d in devs[start:i+1] if d is not None]
        if len(xs) < 20:
            continue
        mu = sum(xs) / len(xs)
        var = sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)
        if var <= 0:
            continue
        std = math.sqrt(var)
        if std > 0:
            out[i] = (devs[i] - mu) / std

    return ts, out


# =========================
# PnL and exits
# =========================

def pnl_frac(pos: VirtualPosition, px: float) -> float:
    if pos.side == "long":
        return (px - pos.entry_price) / pos.entry_price
    return (pos.entry_price - px) / pos.entry_price


def check_exit(pos: VirtualPosition, px: float, now_ts: int) -> Tuple[bool, str]:
    p = pnl_frac(pos, px)

    # activate breakeven
    if pos.breakeven_at is not None and pos.breakeven_offset is not None and not pos.be_active:
        if p >= pos.breakeven_at:
            pos.be_active = True
            if pos.side == "long":
                pos.be_price = pos.entry_price * (1.0 + pos.breakeven_offset)
            else:
                pos.be_price = pos.entry_price * (1.0 - pos.breakeven_offset)

    # trailing activation
    if not pos.trail_active and p >= pos.trailing_start:
        pos.trail_active = True
        if pos.side == "long":
            pos.trail_stop = px * (1.0 - pos.trailing_step)
        else:
            pos.trail_stop = px * (1.0 + pos.trailing_step)

    # update trailing stop
    if pos.trail_active and pos.trail_stop is not None:
        if pos.side == "long":
            new_stop = px * (1.0 - pos.trailing_step)
            if new_stop > pos.trail_stop:
                pos.trail_stop = new_stop
        else:
            new_stop = px * (1.0 + pos.trailing_step)
            if new_stop < pos.trail_stop:
                pos.trail_stop = new_stop

    # hard SL/TP
    if p <= -pos.stop_loss:
        return True, "SL"
    if p >= pos.take_profit:
        return True, "TP"

    # breakeven stop
    if pos.be_active and pos.be_price is not None:
        if pos.side == "long" and px <= pos.be_price:
            return True, "BE"
        if pos.side == "short" and px >= pos.be_price:
            return True, "BE"

    # trailing stop
    if pos.trail_active and pos.trail_stop is not None:
        if pos.side == "long" and px <= pos.trail_stop:
            return True, "TRAIL"
        if pos.side == "short" and px >= pos.trail_stop:
            return True, "TRAIL"

    return False, ""


# =========================
# Backtest main
# =========================

def ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS backtest_trades2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instId TEXT NOT NULL,
            side TEXT NOT NULL,
            entry_ts INTEGER NOT NULL,
            exit_ts INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            exit_price REAL NOT NULL,
            gross_pnl_frac REAL NOT NULL,
            gross_pnl_usd REAL NOT NULL,
            fee_frac REAL NOT NULL,
            fee_usd REAL NOT NULL,
            net_pnl_frac REAL NOT NULL,
            net_pnl_usd REAL NOT NULL,
            exit_reason TEXT NOT NULL,
            duration_sec INTEGER NOT NULL
        )
        """
    )


def backtest_fast(
    db_path: str,
    symbols: List[str],
    start_ms: Optional[int],
    end_ms: Optional[int],
    step: int,
    fee_pct: float,
    notional: float,
    cooldown_min: float,
    debug_flat: int,
    clear: bool,
) -> None:
    fee_frac_roundtrip = fee_pct / 100.0
    cooldown_ms = int(cooldown_min * 60 * 1000)

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    ensure_tables(conn)
    if clear:
        conn.execute("DELETE FROM backtest_trades2")
        conn.commit()

    for sym in symbols:
        micro = load_1s_micro(conn, sym, start_ms, end_ms)
        if not micro:
            print(f"{sym}: no agg_1s_micro rows")
            continue

        # ---- NEW: rolling buffers for v6 context features (fast, no per-row SQL) ----
        mid_hist = deque(maxlen=900)         # (sec_ts, mid) ~15m
        mid_map: Dict[int, float] = {}       # sec_ts -> mid (trimmed periodically)
        dz_hist = deque(maxlen=15)           # last 15 seconds delta_rate_z
        imb_hist = deque(maxlen=15)          # last 15 seconds imb_shift
        tc_hist_90 = deque(maxlen=90)        # last 90 seconds trade_count (for flow_accel_30s)

        # ---- 15m regime source ----
        # Prefer materialized agg_15m_trend (fast & consistent with live bot).
        # Fallback: compute from raw_ws_candle if agg_15m_trend is missing.
        use_db_trend = _table_exists(conn, "agg_15m_trend")
        trend_rows: List[sqlite3.Row] = load_15m_trend(conn, sym) if use_db_trend else []

        if trend_rows:
            trend_ts = [int(r["candle_ts"]) for r in trend_rows]
            atr_series = [float(r["atr14"]) if r["atr14"] is not None else None for r in trend_rows]
            ema_fast_s = [float(r["ema20"]) if r["ema20"] is not None else None for r in trend_rows]
            ema_slow_s = [float(r["ema50"]) if r["ema50"] is not None else None for r in trend_rows]
            ema_diff_s = [float(r["ema_diff"]) if r["ema_diff"] is not None else None for r in trend_rows]
            ema_slope_s = [float(r["ema_slope"]) if r["ema_slope"] is not None else None for r in trend_rows]
            trend_dir_s = [int(r["trend_dir"]) if r["trend_dir"] is not None else None for r in trend_rows]
        else:
            candles15 = load_15m_candles(conn, sym)
            if len(candles15) < 60:
                print(f"{sym}: not enough 15m candles ({len(candles15)}) and no agg_15m_trend")
                continue
            trend_ts, atr_series = compute_atr_series(candles15, period=14)
            _ts2, ema_fast_s, ema_slow_s, ema_diff_s, ema_slope_s, trend_dir_s = compute_regime_series(candles15)

        if not trend_ts:
            print(f"{sym}: no 15m regime rows (agg_15m_trend empty and ATR compute failed)")
            continue

        ctx_rows = load_1m_context(conn, sym)
        ctx_min_ts = [int(r["min_ts"]) for r in ctx_rows] if ctx_rows else []
        ctx_mark_z_ts, ctx_mark_z = compute_mark_dev_z(ctx_rows) if ctx_rows else ([], [])

        ptr_atr = 0
        ptr_ctx = 0

        # rolling windows
        last60_mid: List[float] = []
        w_flow60 = RollingIntWindow(60)
        w_flow600 = RollingIntWindow(600)
        w_deltas = RollingFloatWindow(600)
        last5_tc: List[int] = []

        prev_imb: Optional[float] = None

        pos: Optional[VirtualPosition] = None
        last_entry_ts = -10**18

        signals = 0
        trades_closed = 0
        flat_reasons: Dict[str, int] = {}

        for i, r in enumerate(micro):
            if step > 1 and (i % step) != 0:
                continue

            sec_ts = int(r["sec_ts"])

            # ---- attach 15m regime ----
            while ptr_atr + 1 < len(trend_ts) and trend_ts[ptr_atr + 1] <= sec_ts:
                ptr_atr += 1

            atr15m = atr_series[ptr_atr] if atr_series else None
            ema_fast_15m = ema_fast_s[ptr_atr] if ema_fast_s else None
            ema_slow_15m = ema_slow_s[ptr_atr] if ema_slow_s else None
            ema_diff_15m = ema_diff_s[ptr_atr] if ema_diff_s else None
            ema_slope_15m = ema_slope_s[ptr_atr] if ema_slope_s else None
            trend_dir_15m = trend_dir_s[ptr_atr] if trend_dir_s else None

            # ---- attach 1m context ----
            min_ts = sec_ts - (sec_ts % 60000)
            mark_last = index_last = oi = doi = funding = None
            mark_dev_z = None

            if ctx_rows:
                while ptr_ctx + 1 < len(ctx_min_ts) and ctx_min_ts[ptr_ctx + 1] <= min_ts:
                    ptr_ctx += 1
                cur = ctx_rows[ptr_ctx]
                try:
                    mark_last = float(cur["mark_last"]) if cur["mark_last"] is not None else None
                except Exception:
                    mark_last = None
                try:
                    index_last = float(cur["index_last"]) if cur["index_last"] is not None else None
                except Exception:
                    index_last = None
                try:
                    oi = float(cur["oi"]) if cur["oi"] is not None else None
                except Exception:
                    oi = None
                try:
                    doi = float(cur["doi"]) if cur["doi"] is not None else None
                except Exception:
                    doi = None
                try:
                    funding = float(cur["funding"]) if cur["funding"] is not None else None
                except Exception:
                    funding = None

                mark_dev_z = ctx_mark_z[ptr_ctx] if ptr_ctx < len(ctx_mark_z) else None

            # ---- micro parse ----
            mid = float(r["mid"]) if r["mid"] is not None else None
            vwap = float(r["vwap"]) if r["vwap"] is not None else None
            spread_bps = float(r["spread_bps"]) if r["spread_bps"] is not None else None
            ret_1s = float(r["ret_1s"]) if r["ret_1s"] is not None else None
            trade_count = int(r["trade_count"] or 0)
            delta_vol = float(r["delta_vol"]) if r["delta_vol"] is not None else None
            imbalance15 = float(r["imbalance15"]) if r["imbalance15"] is not None else None

            # 5s stats
            last5_tc.append(trade_count)
            if len(last5_tc) > 5:
                last5_tc.pop(0)
            trade_count_sum_5s = sum(last5_tc)
            active_seconds_5s = sum(1 for x in last5_tc if x > 0)

            # delta z
            if delta_vol is not None:
                w_deltas.push(delta_vol)

            delta_rate_z = None
            if delta_vol is not None and w_deltas.count() >= 20:
                mu = sum(w_deltas.vals) / len(w_deltas.vals)
                var = sum((x - mu) ** 2 for x in w_deltas.vals) / (len(w_deltas.vals) - 1)
                if var > 0:
                    std = math.sqrt(var)
                    if std > 0:
                        delta_rate_z = (delta_vol - mu) / std

            # imbalance shift
            imb_shift = None
            if imbalance15 is not None and prev_imb is not None:
                imb_shift = imbalance15 - prev_imb
            if imbalance15 is not None:
                prev_imb = imbalance15

            # ---- NEW: update rolling buffers (v6 context) ----
            if mid is not None:
                mid_hist.append((sec_ts, mid))
                mid_map[sec_ts] = mid
                # trim the map occasionally (cheap)
                if (i % 200) == 0:
                    cutoff = sec_ts - 15 * 60 * 1000
                    for k in list(mid_map.keys()):
                        if k < cutoff:
                            mid_map.pop(k, None)

            dz_hist.append(delta_rate_z)
            imb_hist.append(imb_shift)
            tc_hist_90.append(trade_count)

            # ---- NEW: compute v6 context features ----
            ret_1m_before = pct_change(mid, mid_map.get(sec_ts - 60_000)) if mid is not None else None
            ret_5m_before = pct_change(mid, mid_map.get(sec_ts - 5 * 60_000)) if mid is not None else None
            ret_15m_before = pct_change(mid, mid_map.get(sec_ts - 15 * 60_000)) if mid is not None else None

            dist_from_15m_high = None
            dist_from_15m_low = None
            if mid is not None and len(mid_hist) >= 30:
                mids15 = [m for (_t, m) in mid_hist if m is not None]
                if mids15 and mid > 0:
                    hi = max(mids15)
                    lo = min(mids15)
                    dist_from_15m_high = (hi - mid) / mid
                    dist_from_15m_low = (mid - lo) / mid

            dz_mean_5s_before = _mean(list(dz_hist)[-5:])
            dz_mean_15s_before = _mean(list(dz_hist)[-15:])
            imb_mean_5s_before = _mean(list(imb_hist)[-5:])
            imb_mean_15s_before = _mean(list(imb_hist)[-15:])

            flow_accel_30s = None
            if len(tc_hist_90) >= 90:
                arr = list(tc_hist_90)
                f_now = sum(arr[-60:])
                f_prev = sum(arr[-90:-30])
                flow_accel_30s = float(f_now - f_prev)

            # flow windows
            w_flow60.push(trade_count)
            w_flow600.push(trade_count)
            flow_60s = w_flow60.s

            # 60s mid range
            if mid is not None:
                last60_mid.append(mid)
                if len(last60_mid) > 60:
                    last60_mid.pop(0)

            range_60s_pct = None
            if len(last60_mid) >= 10 and mid is not None and mid > 0:
                mn = min(last60_mid)
                mx = max(last60_mid)
                range_60s_pct = (mx - mn) / mid

            # 60s return std
            ret_std_60s = None
            # approximate: std of last 60 ret_1s
            # reuse micro row ret_1s history by accumulating
            # (fast enough for skeleton)
            # We'll keep separate rolling list:
            # NOTE: minimal to stay compatible with decide logic
            # We approximate by using delta window if ret_1s is present.
            # For precision, a dedicated rolling ret list can be added.
            # Here we compute from last 60 rows in micro using indices (cost acceptable)
            # But we keep fast: compute only when needed.
            # We'll compute using last60_mid changes if enough points:
            if len(last60_mid) >= 20:
                # compute 1s returns from mids
                rets = []
                for j in range(1, len(last60_mid)):
                    a = last60_mid[j-1]
                    b = last60_mid[j]
                    if a and a != 0:
                        rets.append((b - a) / a)
                if len(rets) >= 20:
                    mu = sum(rets) / len(rets)
                    var = sum((x - mu) ** 2 for x in rets) / (len(rets) - 1)
                    if var > 0:
                        ret_std_60s = math.sqrt(var)

            # flow spike (last60 vs avg 60s across last ~10m)
            avg_flow_60s = w_flow600.s / max(1.0, w_flow600.count() / 60.0) if w_flow600.count() > 0 else 0.0
            flow_spike = (flow_60s / avg_flow_60s) if avg_flow_60s > 0 else None

            f = FeatureRowLite(
                instId=sym,
                ts=sec_ts,

                mid=mid,
                vwap=vwap,
                spread_bps=spread_bps,
                ret_1s=ret_1s,

                trade_count=trade_count,
                trade_count_sum_5s=trade_count_sum_5s,
                active_seconds_5s=active_seconds_5s,

                delta_vol=delta_vol,
                delta_rate_z=delta_rate_z,

                imbalance15=imbalance15,
                imb_shift=imb_shift,

                range_60s_pct=range_60s_pct,
                ret_std_60s=ret_std_60s,
                flow_60s=flow_60s,
                flow_spike=flow_spike,

                atr15m=atr15m,
                ema_fast_15m=ema_fast_15m,
                ema_slow_15m=ema_slow_15m,
                ema_diff_15m=ema_diff_15m,
                ema_slope_15m=ema_slope_15m,
                trend_dir_15m=trend_dir_15m,

                mark_last=mark_last,
                index_last=index_last,
                mark_dev_z=mark_dev_z,
                oi=oi,
                doi=doi,
                funding=funding,

                # v6 context
                ret_1m_before=ret_1m_before,
                ret_5m_before=ret_5m_before,
                ret_15m_before=ret_15m_before,
                dist_from_15m_high=dist_from_15m_high,
                dist_from_15m_low=dist_from_15m_low,
                dz_mean_5s_before=dz_mean_5s_before,
                dz_mean_15s_before=dz_mean_15s_before,
                imb_mean_5s_before=imb_mean_5s_before,
                imb_mean_15s_before=imb_mean_15s_before,
                flow_accel_30s=flow_accel_30s,
            )

            px = f.mid if f.mid is not None else f.vwap
            if px is None:
                continue
            px = float(px)

            # manage open position
            if pos is not None:
                should_exit, reason = check_exit(pos, px, sec_ts)
                if should_exit:
                    gross = pnl_frac(pos, px)
                    gross_usd = gross * notional
                    fee_frac = fee_frac_roundtrip
                    fee_usd = fee_frac * notional
                    net = gross - fee_frac
                    net_usd = net * notional
                    duration_sec = int((sec_ts - pos.entry_ts) / 1000)

                    conn.execute(
                        """
                        INSERT INTO backtest_trades2
                        (instId, side, entry_ts, exit_ts, entry_price, exit_price,
                         gross_pnl_frac, gross_pnl_usd, fee_frac, fee_usd,
                         net_pnl_frac, net_pnl_usd, exit_reason, duration_sec)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            sym, pos.side, pos.entry_ts, sec_ts, pos.entry_price, px,
                            gross, gross_usd, fee_frac, fee_usd,
                            net, net_usd, reason, duration_sec
                        ),
                    )
                    trades_closed += 1
                    pos = None

            # cooldown gate
            if pos is None and (sec_ts - last_entry_ts) < cooldown_ms:
                continue

            # decide entry
            if pos is None:
                sig: Signal = decide(f)  # decide uses duck-typed fields
                if sig.side == "flat":
                    if debug_flat > 0:
                        flat_reasons[sig.reason] = flat_reasons.get(sig.reason, 0) + 1
                    continue

                # open virtual position
                stop_loss = float(sig.stop_loss or 0.0035)
                take_profit = float(sig.take_profit or 0.0100)
                trailing_start = float(sig.trailing_start or 0.0080)
                trailing_step = float(sig.trailing_step or 0.0015)

                pos = VirtualPosition(
                    instId=sym,
                    side=sig.side,
                    entry_ts=sec_ts,
                    entry_price=px,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_start=trailing_start,
                    trailing_step=trailing_step,
                    breakeven_at=sig.breakeven_at,
                    breakeven_offset=sig.breakeven_offset,
                )
                last_entry_ts = sec_ts
                signals += 1

        conn.commit()

        print(f"{sym}: signals={signals} trades_closed={trades_closed} fee={fee_pct:.3f}% notional={notional:.2f}")
        if debug_flat > 0 and flat_reasons:
            print(f"{sym}: top flat reasons:")
            for k, v in sorted(flat_reasons.items(), key=lambda x: x[1], reverse=True)[:debug_flat]:
                print(f"  {k:<24} {v}")

    conn.close()


def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--symbols", required=True, help="comma-separated list")
    p.add_argument("--start-ms", type=int, default=None)
    p.add_argument("--end-ms", type=int, default=None)
    p.add_argument("--step", type=int, default=1)
    p.add_argument("--fee", type=float, default=0.12)
    p.add_argument("--notional", type=float, default=1000.0)
    p.add_argument("--cooldown-min", type=float, default=2.0)
    p.add_argument("--debug-flat", type=int, default=0)
    p.add_argument("--clear", action="store_true")
    args = p.parse_args()

    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    backtest_fast(
        db_path=args.db,
        symbols=symbols,
        start_ms=args.start_ms,
        end_ms=args.end_ms,
        step=args.step,
        fee_pct=args.fee,
        notional=args.notional,
        cooldown_min=args.cooldown_min,
        debug_flat=args.debug_flat,
        clear=args.clear,
    )


if __name__ == "__main__":
    cli()
