from __future__ import annotations
import json
from dataclasses import dataclass, field
from typing import Dict, Optional, List, Tuple, Any
from .orderbook import normalize_levels, mid_price, spread, microprice, imbalance
from .utils import safe_float

@dataclass
class BookState:
    bids: List[Tuple[str,str]] = field(default_factory=list)
    asks: List[Tuple[str,str]] = field(default_factory=list)
    checksum: Optional[int] = None

@dataclass
class TickerState:
    last: Optional[float] = None
    bid: Optional[float] = None
    ask: Optional[float] = None
    mark: Optional[float] = None
    index: Optional[float] = None

@dataclass
class TradeBucket:
    count: int = 0
    buy_vol: float = 0.0
    sell_vol: float = 0.0
    vwap_num: float = 0.0
    vwap_den: float = 0.0

    def add(self, price: float, size: float, side: str) -> None:
        self.count += 1
        if side == "buy":
            self.buy_vol += size
        else:
            self.sell_vol += size
        self.vwap_num += price * size
        self.vwap_den += size

    def vwap(self) -> Optional[float]:
        if self.vwap_den <= 0:
            return None
        return self.vwap_num / self.vwap_den

class MicroAggregator:
    """
    Собирает 1s агрегаты микроструктуры:
    mid/spread/microprice/imbalance15 + trades bucket (delta/vwap)
    """
    def __init__(self):
        self.books: Dict[str, BookState] = {}
        self.tickers: Dict[str, TickerState] = {}
        self.trades_1s: Dict[Tuple[str,int], TradeBucket] = {}
        self.last_price_1s: Dict[str, float] = {}

        # context for 1m
        self.oi_last: Dict[str, float] = {}
        self.funding_last: Dict[str, float] = {}
        self.mark_last: Dict[str, float] = {}
        self.index_last: Dict[str, float] = {}

    def on_books15(self, instId: str, bids_raw, asks_raw, checksum: Optional[int]) -> None:
        bids = normalize_levels(bids_raw or [], 15)
        asks = normalize_levels(asks_raw or [], 15)
        self.books[instId] = BookState(bids=bids, asks=asks, checksum=checksum)

    def on_ticker(self, instId: str, payload: Dict[str,Any]) -> None:
        st = self.tickers.get(instId) or TickerState()
        # payload structure depends on channel; we keep it robust
        for k in ("lastPr","last","lastPrice"):
            if k in payload: st.last = safe_float(payload.get(k))
        if "bidPr" in payload: st.bid = safe_float(payload["bidPr"])
        if "askPr" in payload: st.ask = safe_float(payload["askPr"])
        if "markPrice" in payload: st.mark = safe_float(payload["markPrice"])
        if "indexPrice" in payload: st.index = safe_float(payload["indexPrice"])
        self.tickers[instId] = st

    def on_trade(self, instId: str, ts_ms: int, price: str, size: str, side: str) -> None:
        sec_ts = ts_ms - (ts_ms % 1000)
        key = (instId, sec_ts)
        b = self.trades_1s.get(key)
        if b is None:
            b = TradeBucket()
            self.trades_1s[key] = b
        b.add(float(price), float(size), side)

    def on_rest_symbol_price(self, instId: str, payload: Dict[str,Any]) -> None:
        # Best-effort: data may be in payload["data"][0]
        try:
            d = payload.get("data")
            if isinstance(d, list) and d:
                d0 = d[0]
            else:
                d0 = d or {}
            if "markPrice" in d0: self.mark_last[instId] = safe_float(d0["markPrice"])
            if "indexPrice" in d0: self.index_last[instId] = safe_float(d0["indexPrice"])
        except Exception:
            pass

    def on_rest_open_interest(self, instId: str, payload: Dict[str, Any]) -> None:
        try:
            d = payload.get("data") or {}

            # V2 format:
            # data: { "openInterestList":[{"symbol":"BTCUSDT","size":"34278.06"}], "ts":"..." }
            if isinstance(d, dict) and "openInterestList" in d:
                lst = d.get("openInterestList") or []
                if isinstance(lst, list) and lst:
                    item = lst[0] if isinstance(lst[0], dict) else {}
                    if "size" in item:
                        self.oi_last[instId] = safe_float(item["size"])
                        return

            # fallback (на всякий)
            if isinstance(d, list) and d:
                d0 = d[0] if isinstance(d[0], dict) else {}
            elif isinstance(d, dict):
                d0 = d
            else:
                d0 = {}
            for k in ("openInterest", "size", "holdingAmount"):
                if k in d0:
                    self.oi_last[instId] = safe_float(d0[k])
                    return
        except Exception:
            pass

    def on_rest_funding(self, instId: str, payload: Dict[str,Any]) -> None:
        try:
            d = payload.get("data")
            if isinstance(d, list) and d:
                d0 = d[0]
            else:
                d0 = d or {}
            for k in ("fundingRate","fundRate"):
                if k in d0:
                    self.funding_last[instId] = safe_float(d0[k])
                    return
        except Exception:
            pass

    def flush_1s(self, sec_ts: int) -> Optional[Tuple]:
        """
        Возвращает строку для agg_1s_micro по одному символу невозможно.
        Вызови flush_for_symbols(sec_ts, symbols)
        """
        raise NotImplementedError

    def flush_for_symbols(self, sec_ts: int, symbols: List[str]) -> List[Tuple]:
        rows: List[Tuple] = []
        for instId in symbols:
            book = self.books.get(instId)
            tkr = self.tickers.get(instId)
            best_bid = book.bids[0] if book and book.bids else None
            best_ask = book.asks[0] if book and book.asks else None

            mid = mid_price(best_bid, best_ask)
            spr = spread(best_bid, best_ask)
            mp = microprice(best_bid, best_ask)
            imb = imbalance(book.bids, book.asks) if book else None
            top_bid_qty = float(best_bid[1]) if best_bid else None
            top_ask_qty = float(best_ask[1]) if best_ask else None

            tb = self.trades_1s.pop((instId, sec_ts), None)
            trade_count = tb.count if tb else 0
            buy_vol = tb.buy_vol if tb else 0.0
            sell_vol = tb.sell_vol if tb else 0.0
            delta = buy_vol - sell_vol
            vwap = tb.vwap() if tb else None

            # return 1s based on last price if available
            last_px = tkr.last if tkr and tkr.last is not None else None
            ret_1s = None
            if last_px is not None:
                prev = self.last_price_1s.get(instId)
                if prev and prev > 0:
                    ret_1s = (last_px / prev) - 1.0
                self.last_price_1s[instId] = last_px

            spread_bps = None
            if spr is not None and mid and mid > 0:
                spread_bps = (spr / mid) * 10000.0

            rows.append((
                instId, sec_ts,
                mid, spr, spread_bps, mp, imb,
                top_bid_qty, top_ask_qty,
                trade_count, buy_vol, sell_vol, delta, vwap, ret_1s
            ))
        return rows

    def flush_1m_context(self, min_ts: int, symbols: List[str]) -> List[Tuple]:
        rows: List[Tuple] = []
        for instId in symbols:
            mark = self.mark_last.get(instId)
            index = self.index_last.get(instId)
            oi = self.oi_last.get(instId)
            funding = self.funding_last.get(instId)

            # dOI vs previous minute cached in oi_last_prev
            # We store previous in a hidden dict to compute delta
            if not hasattr(self, "_oi_prev"):
                self._oi_prev = {}
            prev_oi = self._oi_prev.get(instId)
            doi = (oi - prev_oi) if (oi is not None and prev_oi is not None) else None
            if oi is not None:
                self._oi_prev[instId] = oi

            # deviations
            last = None
            tkr = self.tickers.get(instId)
            if tkr and tkr.last is not None:
                last = tkr.last
            mark_last = (mark - last) if (mark is not None and last is not None) else None
            index_last = (index - last) if (index is not None and last is not None) else None

            rows.append((instId, min_ts, mark_last, index_last, oi, doi, funding))
        return rows
