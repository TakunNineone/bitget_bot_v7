from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass
class TrendRow15m:
    instId: str
    candle_ts: int
    close: Optional[float]
    atr14: Optional[float]
    ema20: Optional[float]
    ema50: Optional[float]
    ema_diff: Optional[float]
    ema_slope: Optional[float]
    trend_dir: Optional[int]


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if math.isnan(f):
            return None
        return f
    except Exception:
        return None


def _ema_series(closes: List[float], period: int) -> List[Optional[float]]:
    """
    Same EMA implementation as backtest_skeleton_fast.
    Seed = SMA(period).
    """
    if len(closes) < period:
        return [None] * len(closes)
    k = 2.0 / (period + 1.0)
    out: List[Optional[float]] = [None] * len(closes)
    seed = sum(closes[:period]) / period
    ema = seed
    out[period - 1] = ema
    for i in range(period, len(closes)):
        ema = (closes[i] - ema) * k + ema
        out[i] = ema
    return out


def _compute_atr14(ohlc: List[Tuple[float, float, float, float]], period: int = 14) -> List[Optional[float]]:
    """
    Same Wilder ATR as backtest_skeleton_fast.compute_atr_series.
    """
    if len(ohlc) < period + 1:
        return [None] * len(ohlc)

    trs: List[Optional[float]] = [None] * len(ohlc)
    prev_close = ohlc[0][3]
    for i in range(1, len(ohlc)):
        _o, h, l, c = ohlc[i]
        if prev_close is None:
            trs[i] = None
        else:
            trs[i] = max(h - l, abs(h - prev_close), abs(l - prev_close))
        prev_close = c

    atr: List[Optional[float]] = [None] * len(ohlc)
    seed = [x for x in trs[1 : period + 1] if x is not None]
    if len(seed) < period:
        return atr

    a = sum(seed) / period
    atr[period] = a
    for i in range(period + 1, len(trs)):
        tr = trs[i]
        if tr is None or atr[i - 1] is None:
            atr[i] = atr[i - 1]
        else:
            atr[i] = (atr[i - 1] * (period - 1) + tr) / period
    return atr


def _compute_regime(
    closes: List[float],
    ema_fast: List[Optional[float]],
    ema_slow: List[Optional[float]],
) -> Tuple[List[Optional[float]], List[Optional[float]], List[Optional[int]]]:
    # ema_diff = (ema20 - ema50) / close
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

    # trend_dir by ema_diff thresholds
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

    return ema_diff, ema_slope, trend_dir


def _fetch_candles_15m(conn: sqlite3.Connection, instId: str, lookback: int) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    q = """
        SELECT candle_ts, o, h, l, c
        FROM raw_ws_candle
        WHERE instId=? AND interval='candle15m'
        ORDER BY candle_ts DESC
        LIMIT ?
    """
    rows = conn.execute(q, (instId, int(lookback))).fetchall()
    return list(reversed(rows))


def rebuild_15m_trend(conn: sqlite3.Connection, instId: str, lookback: int = 600) -> int:
    """
    (Re)build agg_15m_trend from last N 15m candles.
    Returns: number of rows upserted.
    """
    candles = _fetch_candles_15m(conn, instId, lookback)
    if not candles:
        return 0

    ts: List[int] = []
    closes: List[float] = []
    ohlc: List[Tuple[float, float, float, float]] = []

    for r in candles:
        candle_ts = int(r["candle_ts"])
        o = _safe_float(r["o"]) or 0.0
        h = _safe_float(r["h"]) or 0.0
        l = _safe_float(r["l"]) or 0.0
        c = _safe_float(r["c"]) or 0.0
        ts.append(candle_ts)
        closes.append(c)
        ohlc.append((o, h, l, c))

    atr14 = _compute_atr14(ohlc, period=14)
    ema20 = _ema_series(closes, 20)
    ema50 = _ema_series(closes, 50)
    ema_diff, ema_slope, trend_dir = _compute_regime(closes, ema20, ema50)

    out_rows: List[Tuple] = []
    for i in range(len(ts)):
        out_rows.append(
            (
                instId,
                ts[i],
                closes[i] if closes[i] != 0.0 else None,
                atr14[i],
                ema20[i],
                ema50[i],
                ema_diff[i],
                ema_slope[i],
                trend_dir[i],
            )
        )

    conn.executemany(
        """INSERT OR REPLACE INTO agg_15m_trend
           (instId,candle_ts,close,atr14,ema20,ema50,ema_diff,ema_slope,trend_dir)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        out_rows,
    )
    return len(out_rows)


def update_last_15m_trend(conn: sqlite3.Connection, instId: str, lookback: int = 300) -> int:
    """
    Incremental update: recompute trend on last N candles and upsert only last candle_ts.
    Returns: 1 if updated, 0 otherwise.
    """
    candles = _fetch_candles_15m(conn, instId, lookback)
    if len(candles) < 60:
        # need at least 50 for EMA50 seed + some for slope/ATR
        return 0

    ts: List[int] = []
    closes: List[float] = []
    ohlc: List[Tuple[float, float, float, float]] = []

    for r in candles:
        candle_ts = int(r["candle_ts"])
        o = _safe_float(r["o"]) or 0.0
        h = _safe_float(r["h"]) or 0.0
        l = _safe_float(r["l"]) or 0.0
        c = _safe_float(r["c"]) or 0.0
        ts.append(candle_ts)
        closes.append(c)
        ohlc.append((o, h, l, c))

    atr14 = _compute_atr14(ohlc, period=14)
    ema20 = _ema_series(closes, 20)
    ema50 = _ema_series(closes, 50)
    ema_diff, ema_slope, trend_dir = _compute_regime(closes, ema20, ema50)

    i = len(ts) - 1
    row = (
        instId,
        ts[i],
        closes[i] if closes[i] != 0.0 else None,
        atr14[i],
        ema20[i],
        ema50[i],
        ema_diff[i],
        ema_slope[i],
        trend_dir[i],
    )

    conn.execute(
        """INSERT OR REPLACE INTO agg_15m_trend
           (instId,candle_ts,close,atr14,ema20,ema50,ema_diff,ema_slope,trend_dir)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        row,
    )
    return 1
