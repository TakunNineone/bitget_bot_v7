from __future__ import annotations

import asyncio
import json
import logging
import random
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp

from .utils import now_ms

log = logging.getLogger("rest")


class RestPoller:
    """Simple REST poller with per-endpoint loops and resilient retries."""

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
        timeout = aiohttp.ClientTimeout(total=15, connect=8, sock_read=12)
        conn = aiohttp.TCPConnector(limit=50, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=conn) as session:
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

    async def _get_json(
        self,
        session: aiohttp.ClientSession,
        path: str,
        params: Dict[str, Any],
        *,
        retries: int = 3,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_err: Optional[BaseException] = None
        for attempt in range(retries + 1):
            try:
                async with session.get(url, params=params) as r:
                    txt = await r.text()
                    if r.status != 200:
                        raise RuntimeError(f"HTTP {r.status} {txt[:200]}")
                    try:
                        return json.loads(txt)
                    except Exception:
                        # Some Bitget endpoints occasionally return empty string on transient issues
                        if not txt.strip():
                            raise RuntimeError("empty-response")
                        return {"raw": txt}
            except asyncio.CancelledError:
                raise
            except Exception as e:
                last_err = e
                if attempt < retries:
                    delay = min(10.0, (0.5 * (2 ** attempt)) + random.random() * 0.3)
                    await asyncio.sleep(delay)
        raise RuntimeError(f"GET failed url={url} params={params} err={repr(last_err)}")

    async def _loop_symbol_price(self, session: aiohttp.ClientSession) -> None:
        sec = int(self.intervals.get("symbol_price_sec", 10))
        path = "/api/v2/mix/market/symbol-price"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get_json(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("symbol-price", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("symbol-price %s error: %r", sym, e)
            await asyncio.sleep(sec)

    async def _loop_open_interest(self, session: aiohttp.ClientSession) -> None:
        sec = int(self.intervals.get("open_interest_sec", 20))
        path = "/api/v2/mix/market/open-interest"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get_json(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("open-interest", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("open-interest %s error: %r", sym, e)
            await asyncio.sleep(sec)

    async def _loop_current_fund_rate(self, session: aiohttp.ClientSession) -> None:
        sec = int(self.intervals.get("current_fund_rate_sec", 120))
        path = "/api/v2/mix/market/current-fund-rate"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get_json(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("current-fund-rate", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("current-fund-rate %s error: %r", sym, e)
            await asyncio.sleep(sec)

    async def _loop_funding_time(self, session: aiohttp.ClientSession) -> None:
        sec = int(self.intervals.get("funding_time_sec", 600))
        path = "/api/v2/mix/market/funding-time"
        while True:
            for sym in self.symbols:
                recv_ts = now_ms()
                try:
                    payload = await self._get_json(session, path, {"symbol": sym, "productType": self.product_type})
                    await self.on_payload("funding-time", sym, payload, recv_ts)
                except Exception as e:
                    log.warning("funding-time %s error: %r", sym, e)
            await asyncio.sleep(sec)
