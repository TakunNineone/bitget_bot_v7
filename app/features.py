from __future__ import annotations
"""
Feature engineering for micro-swing (15m context + microstructure).

Reads from SQLite tables:
- agg_1s_micro
- agg_1m_context
- raw_ws_candle (candle15m)

Key design:
- price for ATR% is NOT VWAP-only (VWAP can be None on empty seconds)
- delta z-score computed ONLY on "active seconds" where trade_count > 0
- flow quality features: trade_count_sum_5s, active_seconds_5s
"""
import math
import sqlite3
from dataclasses import dataclass
from typing import List, Optional, Tuple


def _safe_float(x) -> Optional[float]:
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    except Exception:
        return None


def zscore(xs: List[float]) -> Optional[float]:
    """Last value z-score over the window."""
    if not xs or len(xs) < 5:
        return None
    mu = sum(xs) / len(xs)
    var = sum((x - mu) ** 2 for x in xs) / (len(xs) - 1)
    sd = math.sqrt(var) if var > 0 else 0.0
    if sd == 0.0:
        return 0.0
    return (xs[-1] - mu) / sd


def atr(ohlc: List[Tuple[float, float, float, float]], period: int = 14) -> Optional[float]:
    """
    ATR from a list of (o,h,l,c) ordered oldest->newest.
    Returns last ATR value.
    """
    if len(ohlc) < period + 1:
        return None
    trs: List[float] = []
    prev_close = ohlc[0][3]
    for (_o, h, l, c) in ohlc[1:]:
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        trs.append(tr)
        prev_close = c

    # Wilder smoothing
    atr0 = sum(trs[:period]) / period
    cur = atr0
    for tr in trs[period:]:
        cur = (cur * (period - 1) + tr) / period
    return cur


@dataclass
class FeatureRow:
    instId: str
    ts: int  # second timestamp (ms, floored)

    # price sources (last second)
    mid: Optional[float]
    vwap: Optional[float]

    # micro (last second)
    spread_bps: Optional[float]
    imbalance15: Optional[float]
    delta_vol: Optional[float]
    ret_1s: Optional[float]
    trade_count: Optional[int]

    # micro (windowed flow quality)
    trade_count_sum_5s: int
    active_seconds_5s: int
    delta_rate_z: Optional[float]  # z-score of delta over *active* seconds in window

    # context (last minute)
    mark_last: Optional[float]
    index_last: Optional[float]
    doi: Optional[float]
    funding: Optional[float]

    # derived
    imb_shift: Optional[float]
    mark_dev_z: Optional[float]
    oi_z: Optional[float]
    atr15m: Optional[float]


class FeatureService:
    def __init__(self, sqlite_path: str):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _fetch_micro_window(self, instId: str, end_sec_ts: int, window_sec: int):
        start = end_sec_ts - (window_sec * 1000)
        cur = self.conn.execute(
            "SELECT * FROM agg_1s_micro "
            "WHERE instId=? AND sec_ts>=? AND sec_ts<=? "
            "ORDER BY sec_ts ASC",
            (instId, start, end_sec_ts),
        )
        return cur.fetchall()

    def _fetch_context(self, instId: str, min_ts: int):
        cur = self.conn.execute(
            "SELECT * FROM agg_1m_context WHERE instId=? AND min_ts=?",
            (instId, min_ts),
        )
        return cur.fetchone()

    def _fetch_15m_ohlc(self, instId: str, end_candle_ts: int, n: int = 256) -> List[Tuple[float, float, float, float]]:
        cur = self.conn.execute(
            "SELECT o,h,l,c FROM raw_ws_candle "
            "WHERE instId=? AND interval='candle15m' AND candle_ts<=? "
            "ORDER BY candle_ts DESC LIMIT ?",
            (instId, end_candle_ts, n),
        )
        rows = cur.fetchall()
        out: List[Tuple[float, float, float, float]] = []
        for r in reversed(rows):
            o = _safe_float(r["o"])
            h = _safe_float(r["h"])
            l = _safe_float(r["l"])
            c = _safe_float(r["c"])
            if None in (o, h, l, c):
                continue
            out.append((o, h, l, c))
        return out

    def _window_1m(self, instId: str, end_min_ts: int, col: str, minutes: int = 60) -> List[float]:
        start = end_min_ts - minutes * 60000
        cur = self.conn.execute(
            f"SELECT {col} FROM agg_1m_context "
            "WHERE instId=? AND min_ts>=? AND min_ts<=? "
            "ORDER BY min_ts ASC",
            (instId, start, end_min_ts),
        )
        xs: List[float] = []
        for r in cur.fetchall():
            v = _safe_float(r[0])
            if v is not None:
                xs.append(v)
        return xs

    def compute_features(
        self,
        instId: str,
        sec_ts: int,
        micro_window_sec: int = 120,
        context_min_ts: Optional[int] = None,
        candle15m_ts: Optional[int] = None,
    ) -> Optional[FeatureRow]:
        micro = self._fetch_micro_window(instId, sec_ts, micro_window_sec)
        if not micro:
            return None
        last = micro[-1]

        # ---- Micro flow quality ----
        active_rows = [r for r in micro if r["trade_count"] is not None and int(r["trade_count"]) > 0]
        deltas_active = [float(r["delta_vol"]) for r in active_rows if r["delta_vol"] is not None]
        delta_rate_z = zscore(deltas_active)

        # last 5 seconds activity
        last5 = micro[-5:] if len(micro) >= 5 else micro
        trade_count_sum_5s = sum(int(r["trade_count"]) for r in last5 if r["trade_count"] is not None)
        active_seconds_5s = sum(1 for r in last5 if r["trade_count"] is not None and int(r["trade_count"]) > 0)

        # imbalance shift (window)
        imbs = [float(r["imbalance15"]) for r in micro if r["imbalance15"] is not None]
        imb_shift = (imbs[-1] - imbs[0]) if len(imbs) >= 2 else None

        # ---- Context (1m) ----
        if context_min_ts is None:
            context_min_ts = sec_ts - (sec_ts % 60000)
        ctx = self._fetch_context(instId, context_min_ts)

        mark_last = _safe_float(ctx["mark_last"]) if ctx else None
        index_last = _safe_float(ctx["index_last"]) if ctx else None
        doi = _safe_float(ctx["doi"]) if ctx else None
        funding = _safe_float(ctx["funding"]) if ctx else None

        mark_dev_z = zscore(self._window_1m(instId, context_min_ts, "mark_last", 60))
        oi_z = zscore(self._window_1m(instId, context_min_ts, "doi", 60))

        # ---- ATR from 15m ----
        if candle15m_ts is None:
            candle15m_ts = sec_ts - (sec_ts % (15 * 60 * 1000))
        ohlc15 = self._fetch_15m_ohlc(instId, candle15m_ts, 256)
        atr15m = atr(ohlc15, 14)

        return FeatureRow(
            instId=instId,
            ts=sec_ts,
            mid=_safe_float(last["mid"]),
            vwap=_safe_float(last["vwap"]),
            spread_bps=_safe_float(last["spread_bps"]),
            imbalance15=_safe_float(last["imbalance15"]),
            delta_vol=_safe_float(last["delta_vol"]),
            ret_1s=_safe_float(last["ret_1s"]),
            trade_count=int(last["trade_count"]) if last["trade_count"] is not None else None,
            trade_count_sum_5s=trade_count_sum_5s,
            active_seconds_5s=active_seconds_5s,
            delta_rate_z=delta_rate_z,
            mark_last=mark_last,
            index_last=index_last,
            doi=doi,
            funding=funding,
            imb_shift=imb_shift,
            mark_dev_z=mark_dev_z,
            oi_z=oi_z,
            atr15m=atr15m,
        )
