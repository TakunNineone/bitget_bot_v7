from __future__ import annotations
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Callable, Awaitable, Tuple
import websockets

from .utils import TokenBucket, now_ms

log = logging.getLogger("ws")

MessageHandler = Callable[[Dict[str, Any], int], Awaitable[None]]

class WSShard:
    def __init__(
        self,
        url: str,
        inst_type: str,
        symbols: List[str],
        channels_per_symbol: List[str],
        ping_interval_sec: int,
        reconnect_backoff_sec: int,
        max_msgs_per_second: int,
        on_message: MessageHandler,
        shard_id: int = 0,
    ):
        self.url = url
        self.inst_type = inst_type
        self.symbols = symbols
        self.channels_per_symbol = channels_per_symbol
        self.ping_interval_sec = ping_interval_sec
        self.reconnect_backoff_sec = reconnect_backoff_sec
        self.bucket = TokenBucket.per_second(rate=max_msgs_per_second, capacity=max_msgs_per_second)
        self.on_message = on_message
        self.shard_id = shard_id
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._connect_and_loop()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("WS shard %s error: %s", self.shard_id, e)
            await asyncio.sleep(self.reconnect_backoff_sec)

    async def _connect_and_loop(self) -> None:
        log.info("WS shard %s connecting (%d symbols) ...", self.shard_id, len(self.symbols))
        async with websockets.connect(self.url, ping_interval=None, close_timeout=5) as ws:
            # subscribe
            await self._subscribe(ws)

            # start ping loop
            ping_task = asyncio.create_task(self._ping_loop(ws))
            try:
                async for msg in ws:
                    recv_ts = now_ms()
                    if msg == "pong":
                        continue
                    if msg == "ping":
                        # some servers do reverse, answer anyway
                        await self._send(ws, "pong")
                        continue
                    try:
                        data = json.loads(msg)
                    except Exception:
                        log.debug("WS shard %s non-json: %s", self.shard_id, msg[:200])
                        continue
                    await self.on_message(data, recv_ts)
            finally:
                ping_task.cancel()
                with contextlib.suppress(Exception):
                    await ping_task

    async def _send(self, ws, payload: Any) -> None:
        await self.bucket.take(1)
        if isinstance(payload, str):
            await ws.send(payload)
        else:
            await ws.send(json.dumps(payload))

    async def _subscribe(self, ws) -> None:
        args = []
        for sym in self.symbols:
            for ch in self.channels_per_symbol:
                args.append({"instType": self.inst_type, "channel": ch, "instId": sym})
        sub = {"op": "subscribe", "args": args}
        await self._send(ws, sub)
        log.info("WS shard %s subscribed channels=%d", self.shard_id, len(args))

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_sec)
            await self._send(ws, "ping")

import contextlib
