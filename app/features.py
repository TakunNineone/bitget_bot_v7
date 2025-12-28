from __future__ import annotations

import sqlite3
import math
from dataclasses import dataclass
from typing import Optional, List, Any


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

        # delta z (based on delta_vol window)
        deltas = [_safe_float(r["delta_vol"]) for r in micro if r["delta_vol"] is not None]
        delta_vol = deltas[-1] if deltas else None

        delta_rate_z = None
        if delta_vol is not None and len(deltas) >= 20:
            mu = sum(deltas) / len(deltas)
            var = sum((x - mu) ** 2 for x in deltas) / (len(deltas) - 1)
            std = math.sqrt(var) if var > 0 else None
            if std and std > 0:
                delta_rate_z = (delta_vol - mu) / std

        # imbalance shift
        imbs = [_safe_float(r["imbalance15"]) for r in micro if r["imbalance15"] is not None]
        imbalance15 = imbs[-1] if imbs else None
        imb_shift = (imbs[-1] - imbs[-2]) if len(imbs) >= 2 else None

        # ATR15m from raw candles
        atr15m = self._fetch_atr_15m(instId, sec_ts)

        # Context: mark/index/oi/doi/funding + mark_dev_z derived
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
        last600 = micro  # already up to 600s
        flow_10m = sum(int(r["trade_count"] or 0) for r in last600)
        avg_flow_60s = flow_10m / max(1.0, len(last600) / 60.0)
        flow_spike = (flow_60s / avg_flow_60s) if avg_flow_60s > 0 else None

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

    def _fetch_atr_15m(self, instId: str, sec_ts: int) -> Optional[float]:
        # ATR(14) computed from raw 15m candles
        cur = self.conn.execute(
            """
            SELECT o, h, l, c
            FROM raw_ws_candle
            WHERE instId=? AND interval='candle15m' AND candle_ts<=?
            ORDER BY candle_ts DESC
            LIMIT 100
            """,
            (instId, sec_ts),
        )
        rows = cur.fetchall()
        if not rows:
            return None

        ohlc = []
        for r in reversed(rows):
            try:
                o = float(r[0]); h = float(r[1]); l = float(r[2]); c = float(r[3])
                ohlc.append((o, h, l, c))
            except Exception:
                continue

        period = 14
        if len(ohlc) < period + 1:
            return None

        trs = []
        prev_close = ohlc[0][3]
        for (_o, h, l, c) in ohlc[1:]:
            tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
            trs.append(tr)
            prev_close = c

        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period

        return atr

    def _fetch_context(self, instId: str, sec_ts: int) -> dict:
        # align to minute
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

        # compute mark deviation zscore over up to last 60 minutes
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
