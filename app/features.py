from __future__ import annotations

import sqlite3
import math
from dataclasses import dataclass
from typing import Optional, List, Any, Tuple


@dataclass
class FeatureRow:
    instId: str
    ts: int

    # price / micro
    mid: Optional[float]
    vwap: Optional[float]
    spread_bps: Optional[float]
    ret_1s: Optional[float]

    # flow / micro
    trade_count: int
    trade_count_sum_5s: int
    active_seconds_5s: int
    delta_vol: Optional[float]
    delta_rate_z: Optional[float]
    imbalance15: Optional[float]
    imb_shift: Optional[float]

    # higher TF
    atr15m: Optional[float]

    # context
    mark_last: Optional[float]
    index_last: Optional[float]
    mark_dev_z: Optional[float]
    oi: Optional[float]
    doi: Optional[float]
    funding: Optional[float]

    # fast react (≈60s)
    range_60s_pct: Optional[float]
    ret_std_60s: Optional[float]
    flow_60s: int
    flow_spike: Optional[float]
    flow_accel_30s: Optional[float]  # NEW

    # NEW: 15m trend (regime)
    ema_fast_15m: Optional[float]
    ema_slow_15m: Optional[float]
    ema_diff_15m: Optional[float]          # (fast - slow) / price
    ema_slope_15m: Optional[float]         # slope of fast EMA (pct per candle)
    trend_dir_15m: Optional[int]           # +1 up, -1 down, 0 unknown/flat

    # NEW: entry context (for better long filtering)
    ret_1m_before: Optional[float]
    ret_5m_before: Optional[float]
    ret_15m_before: Optional[float]
    dist_from_15m_high: Optional[float]
    dist_from_15m_low: Optional[float]
    dz_mean_5s_before: Optional[float]
    dz_mean_15s_before: Optional[float]
    imb_mean_5s_before: Optional[float]
    imb_mean_15s_before: Optional[float]


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


class FeatureService:
    def __init__(self, sqlite_path: str):
        self.conn = sqlite3.connect(sqlite_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        self.conn.close()

    def compute_features(self, instId: str, sec_ts: int) -> Optional[FeatureRow]:
        micro = self._fetch_micro(instId, sec_ts, 600)
        if not micro or len(micro) < 5:
            return None

        last = micro[-1]

        mid = _safe_float(last["mid"])
        vwap = _safe_float(last["vwap"])
        spread_bps = _safe_float(last["spread_bps"])
        ret_1s = _safe_float(last["ret_1s"])

        trade_count = int(last["trade_count"] or 0)

        # 5s flow
        last5 = micro[-5:]
        trade_count_sum_5s = sum(int(r["trade_count"] or 0) for r in last5)
        active_seconds_5s = sum(1 for r in last5 if (r["trade_count"] or 0) > 0)

        # delta series
        deltas = [_safe_float(r["delta_vol"]) for r in micro if r["delta_vol"] is not None]
        delta_vol = deltas[-1] if deltas else None

        # Prefer stored delta_rate_z if present in agg_1s_micro (faster + consistent)
        delta_rate_z = _safe_float(last.get("delta_rate_z")) if isinstance(last, dict) else None
        if delta_rate_z is None:
            # compute z on the fly (fallback)
            if delta_vol is not None and len(deltas) >= 20:
                mu = sum(deltas) / len(deltas)
                var = sum((x - mu) ** 2 for x in deltas) / (len(deltas) - 1)
                std = math.sqrt(var) if var > 0 else None
                if std and std > 0:
                    delta_rate_z = (delta_vol - mu) / std

        # imbalance shift
        imbs = [_safe_float(r["imbalance15"]) for r in micro if r["imbalance15"] is not None]
        imbalance15 = imbs[-1] if imbs else None

        imb_shift = None
        # Prefer stored imb_shift column if exists in agg_1s_micro
        try:
            imb_shift = _safe_float(last["imb_shift"])
        except Exception:
            imb_shift = (imbs[-1] - imbs[-2]) if len(imbs) >= 2 else None

        # 15m trend/regime (materialized in DB by app/trend_15m.py)
        atr15m, ema_fast_15m, ema_slow_15m, ema_diff_15m, ema_slope_15m, trend_dir_15m = self._fetch_15m_trend_row(instId, sec_ts)

        # Context
        ctx = self._fetch_context(instId, sec_ts)
        mark_last = ctx["mark_last"]
        index_last = ctx["index_last"]
        oi = ctx["oi"]
        doi = ctx["doi"]
        funding = ctx["funding"]
        mark_dev_z = ctx["mark_dev_z"]

        # FAST VOL / FLOW (≈60s)
        last60 = micro[-60:] if len(micro) >= 60 else micro

        mids: List[float] = []
        rets: List[float] = []
        flow_60s = 0

        for r in last60:
            m = _safe_float(r["mid"])
            if m is not None:
                mids.append(m)

            rr = _safe_float(r["ret_1s"])
            if rr is not None:
                rets.append(rr)

            tc = r["trade_count"]
            if tc is not None:
                flow_60s += int(tc)

        range_60s_pct = None
        if len(mids) >= 10:
            mn = min(mids)
            mx = max(mids)
            last_mid = mids[-1]
            if last_mid > 0:
                range_60s_pct = (mx - mn) / last_mid

        ret_std_60s = None
        if len(rets) >= 20:
            mu = sum(rets) / len(rets)
            var = sum((x - mu) ** 2 for x in rets) / (len(rets) - 1)
            if var > 0:
                ret_std_60s = math.sqrt(var)

        # flow spike: compare last 60s to average 60s over last ~10m
        last600 = micro  # up to 600s
        flow_10m = sum(int(r["trade_count"] or 0) for r in last600)
        avg_flow_60s = flow_10m / max(1.0, len(last600) / 60.0)
        flow_spike = (flow_60s / avg_flow_60s) if avg_flow_60s > 0 else None

        # NEW: flow accel 30s = flow60(now) - flow60(ending 30s ago)
        flow_accel_30s = None
        if len(last600) >= 90:
            win_now = last600[-60:]
            win_prev = last600[-90:-30]
            f_now = sum(int(r["trade_count"] or 0) for r in win_now)
            f_prev = sum(int(r["trade_count"] or 0) for r in win_prev)
            flow_accel_30s = float(f_now - f_prev)

        # NEW: returns before entry (need data older than 10m => query DB)
        ret_1m_before, ret_5m_before, ret_15m_before = self._fetch_returns_before(instId, sec_ts)

        # NEW: distance from 15m extremes
        dist_from_15m_high, dist_from_15m_low = self._fetch_dist_from_15m_extremes(instId, sec_ts)

        # NEW: micro means before (use stored columns if present; fallback)
        dz_mean_5s_before, dz_mean_15s_before = self._fetch_micro_mean_before(instId, sec_ts, ["delta_rate_z", "delta_vol"])
        imb_mean_5s_before, imb_mean_15s_before = self._fetch_micro_mean_before(instId, sec_ts, ["imb_shift", "imbalance15"])

        return FeatureRow(
            instId=instId,
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

            atr15m=atr15m,

            mark_last=mark_last,
            index_last=index_last,
            mark_dev_z=mark_dev_z,
            oi=oi,
            doi=doi,
            funding=funding,

            range_60s_pct=range_60s_pct,
            ret_std_60s=ret_std_60s,
            flow_60s=flow_60s,
            flow_spike=flow_spike,
            flow_accel_30s=flow_accel_30s,

            ema_fast_15m=ema_fast_15m,
            ema_slow_15m=ema_slow_15m,
            ema_diff_15m=ema_diff_15m,
            ema_slope_15m=ema_slope_15m,
            trend_dir_15m=trend_dir_15m,

            ret_1m_before=ret_1m_before,
            ret_5m_before=ret_5m_before,
            ret_15m_before=ret_15m_before,
            dist_from_15m_high=dist_from_15m_high,
            dist_from_15m_low=dist_from_15m_low,
            dz_mean_5s_before=dz_mean_5s_before,
            dz_mean_15s_before=dz_mean_15s_before,
            imb_mean_5s_before=imb_mean_5s_before,
            imb_mean_15s_before=imb_mean_15s_before,
        )

    # -------------------------
    # DB helpers
    # -------------------------
    def _fetch_micro(self, instId: str, sec_ts: int, limit: int):
        cur = self.conn.execute(
            """
            SELECT *
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts<=?
            ORDER BY sec_ts DESC
            LIMIT ?
            """,
            (instId, sec_ts, limit),
        )
        rows = cur.fetchall()
        rows.reverse()  # chronological
        return rows

    def _fetch_mid_at(self, instId: str, ts_ms: int) -> Optional[float]:
        r = self.conn.execute(
            """
            SELECT mid
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts<=?
            ORDER BY sec_ts DESC
            LIMIT 1
            """,
            (instId, ts_ms),
        ).fetchone()
        if not r:
            return None
        return _safe_float(r[0])

    def _fetch_returns_before(self, instId: str, sec_ts: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        px_e = self._fetch_mid_at(instId, sec_ts)
        if px_e is None or px_e == 0:
            return None, None, None

        px_1m = self._fetch_mid_at(instId, sec_ts - 60_000)
        px_5m = self._fetch_mid_at(instId, sec_ts - 5 * 60_000)
        px_15m = self._fetch_mid_at(instId, sec_ts - 15 * 60_000)

        def ch(a: Optional[float], b: Optional[float]) -> Optional[float]:
            if a is None or b is None or b == 0:
                return None
            return (a - b) / b

        return _safe_float(ch(px_e, px_1m)), _safe_float(ch(px_e, px_5m)), _safe_float(ch(px_e, px_15m))

    def _fetch_dist_from_15m_extremes(self, instId: str, sec_ts: int) -> Tuple[Optional[float], Optional[float]]:
        rows = self.conn.execute(
            """
            SELECT mid
            FROM agg_1s_micro
            WHERE instId=? AND sec_ts>? AND sec_ts<=? AND mid IS NOT NULL
            """,
            (instId, sec_ts - 15 * 60_000, sec_ts),
        ).fetchall()
        if not rows or len(rows) < 10:
            return None, None
        mids = [float(r[0]) for r in rows if r[0] is not None]
        if not mids:
            return None, None

        px = mids[-1]
        if px == 0:
            return None, None
        hi = max(mids)
        lo = min(mids)
        return _safe_float((hi - px) / px), _safe_float((px - lo) / px)

    def _fetch_micro_mean_before(self, instId: str, sec_ts: int, cols_try: List[str]) -> Tuple[Optional[float], Optional[float]]:
        # pick first existing column
        chosen = None
        for c in cols_try:
            try:
                self.conn.execute(f"SELECT {c} FROM agg_1s_micro LIMIT 1")
                chosen = c
                break
            except Exception:
                continue
        if chosen is None:
            return None, None

        def mean_window(ms_back: int) -> Optional[float]:
            r = self.conn.execute(
                f"""
                SELECT AVG({chosen})
                FROM agg_1s_micro
                WHERE instId=? AND sec_ts>? AND sec_ts<? AND {chosen} IS NOT NULL
                """,
                (instId, sec_ts - ms_back, sec_ts),
            ).fetchone()
            return _safe_float(r[0]) if r else None

        return mean_window(5_000), mean_window(15_000)

    def _fetch_15m_trend_row(
        self, instId: str, sec_ts: int
    ) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float], Optional[int]]:
        """Fetch the latest 15m regime row <= sec_ts from materialized table.

        Returns: (atr14, ema20, ema50, ema_diff, ema_slope, trend_dir)
        """
        try:
            r = self.conn.execute(
                """
                SELECT atr14, ema20, ema50, ema_diff, ema_slope, trend_dir
                FROM agg_15m_trend
                WHERE instId=? AND candle_ts<=?
                ORDER BY candle_ts DESC
                LIMIT 1
                """,
                (instId, sec_ts),
            ).fetchone()
        except Exception:
            r = None

        if not r:
            return None, None, None, None, None, None

        return (
            _safe_float(r[0]),
            _safe_float(r[1]),
            _safe_float(r[2]),
            _safe_float(r[3]),
            _safe_float(r[4]),
            int(r[5]) if r[5] is not None else None,
        )

    def _fetch_context(self, instId: str, sec_ts: int) -> dict:
        min_ts = sec_ts - (sec_ts % 60000)

        cur = self.conn.execute(
            """
            SELECT instId, min_ts, mark_last, index_last, oi, doi, funding
            FROM agg_1m_context
            WHERE instId=? AND min_ts<=?
            ORDER BY min_ts DESC
            LIMIT 120
            """,
            (instId, min_ts),
        )
        rows = cur.fetchall()

        if not rows:
            return {
                "mark_last": None,
                "index_last": None,
                "oi": None,
                "doi": None,
                "funding": None,
                "mark_dev_z": None,
            }

        latest = rows[0]
        mark_last = _safe_float(latest["mark_last"])
        index_last = _safe_float(latest["index_last"])
        oi = _safe_float(latest["oi"])
        doi = _safe_float(latest["doi"])
        funding = _safe_float(latest["funding"])

        devs: List[float] = []
        for r in rows[:60]:
            m = _safe_float(r["mark_last"])
            ix = _safe_float(r["index_last"])
            if m is None or ix is None or ix == 0:
                continue
            devs.append((m - ix) / ix)

        mark_dev_z = None
        if len(devs) >= 20 and mark_last is not None and index_last is not None and index_last != 0:
            cur_dev = (mark_last - index_last) / index_last
            mu = sum(devs) / len(devs)
            var = sum((x - mu) ** 2 for x in devs) / (len(devs) - 1)
            std = math.sqrt(var) if var > 0 else None
            if std and std > 0:
                mark_dev_z = (cur_dev - mu) / std

        return {
            "mark_last": mark_last,
            "index_last": index_last,
            "oi": oi,
            "doi": doi,
            "funding": funding,
            "mark_dev_z": mark_dev_z,
        }
