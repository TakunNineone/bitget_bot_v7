from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Dict, List, Callable, Awaitable
import aiohttp

from .utils import now_ms

log = logging.getLogger("rest")

class RestPoller:
    def __init__(
        self,
        base_url: str,
        product_type: str,
        symbols: List[str],
        intervals: Dict[str, int],
        on_payload: Callable[[str, str, Dict[str, Any], int], Awaitable[None]],
    ):
        self.base_url = base_url.rstrip("/")
        self.product_type = product_type
        self.symbols = symbols
        self.intervals = intervals
        self.on_payload = on_payload
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                asyncio.create_task(self._loop_symbol_price(session)),
                asyncio.create_task(self._loop_open_interest(session)),
                asyncio.create_task(self._loop_current_fund_rate(session)),
                asyncio.create_task(self._loop_funding_time(session)),
            ]
            try:
                await self._stop.wait()
            finally:
                for t in tasks:
                    t.cancel()

    async def _get(self, session: aiohttp.ClientSession, path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        async with session.get(url, params=params) as r:
            txt = await r.text()
            if r.status != 200:
                raise RuntimeError(f"HTTP {r.status} {url} {txt[:200]}")
            try:
                return json.loads(txt)
            except Exception:
                return {"raw": txt}

    async def _loop_symbol_price(self, session):
        sec = int(self.intervals.get("symbol_price_sec", 10))
        path = "/api/v2/mix/market/symbol-price"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("symbol-price", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("symbol-price %s error: %s", sym, e)
            await asyncio.sleep(sec)

    async def _loop_open_interest(self, session):
        sec = int(self.intervals.get("open_interest_sec", 20))
        path = "/api/v2/mix/market/open-interest"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("open-interest", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("open-interest %s error: %s", sym, e)
            await asyncio.sleep(sec)

    async def _loop_current_fund_rate(self, session):
        sec = int(self.intervals.get("current_fund_rate_sec", 120))
        path = "/api/v2/mix/market/current-fund-rate"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("current-fund-rate", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("current-fund-rate %s error: %s", sym, e)
            await asyncio.sleep(sec)

    async def _loop_funding_time(self, session):
        sec = int(self.intervals.get("funding_time_sec", 600))
        path = "/api/v2/mix/market/funding-time"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("funding-time", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("funding-time %s error: %s", sym, e)
            await asyncio.sleep(sec)
