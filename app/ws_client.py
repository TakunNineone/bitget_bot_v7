from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List

import websockets
from websockets.exceptions import ConnectionClosed

from .utils import TokenBucket, now_ms

log = logging.getLogger("ws")

MessageHandler = Callable[[Dict[str, Any], int], Awaitable[None]]


class WSShard:
    """Bitget public websocket shard.

    - Reconnect on any socket error.
    - Rate-limit outgoing frames (subscribe/ping) via TokenBucket.
    - Keepalive: periodic ping + watchdog (no inbound frames => reconnect).
    """

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
        read_timeout_sec: int = 70,
    ):
        self.url = url
        self.inst_type = inst_type
        self.symbols = symbols
        self.channels_per_symbol = channels_per_symbol
        self.ping_interval_sec = max(5, int(ping_interval_sec))
        self.reconnect_backoff_sec = max(1, int(reconnect_backoff_sec))
        self.bucket = TokenBucket.per_second(rate=max_msgs_per_second, capacity=max_msgs_per_second)
        self.on_message = on_message
        self.shard_id = shard_id
        self.read_timeout_sec = max(15, int(read_timeout_sec))
        self._stop = asyncio.Event()

    def stop(self) -> None:
        self._stop.set()

    async def run(self) -> None:
        backoff = self.reconnect_backoff_sec
        while not self._stop.is_set():
            try:
                await self._connect_and_loop()
                backoff = self.reconnect_backoff_sec  # reset after a clean loop
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.warning("WS shard %s reconnecting after error: %r", self.shard_id, e)

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _connect_and_loop(self) -> None:
        log.info("WS shard %s connecting (%d symbols) ...", self.shard_id, len(self.symbols))

        async with websockets.connect(
            self.url,
            ping_interval=None,   # manual ping
            close_timeout=5,
            max_queue=256,
        ) as ws:
            await self._subscribe(ws)

            ping_task = asyncio.create_task(self._ping_loop(ws))
            try:
                while True:
                    # Watchdog: no inbound frames for too long => reconnect
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=self.read_timeout_sec)
                    except asyncio.TimeoutError as e:
                        raise RuntimeError(f"read-timeout>{self.read_timeout_sec}s") from e

                    recv_ts = now_ms()

                    if msg == "pong":
                        continue
                    if msg == "ping":
                        await self._send(ws, "pong")
                        continue

                    try:
                        data = json.loads(msg)
                    except Exception:
                        log.debug("WS shard %s non-json: %s", self.shard_id, str(msg)[:200])
                        continue

                    # Subscribe ACK / error — don't forward to handler
                    if isinstance(data, dict) and "event" in data:
                        ev = data.get("event")
                        if ev in ("subscribe", "error"):
                            code = data.get("code")
                            msg2 = data.get("msg")
                            if ev == "error" or (code not in (None, "0", 0)):
                                raise RuntimeError(f"ws-subscribe-failed event={ev} code={code} msg={msg2}")
                            continue

                    await self.on_message(data, recv_ts)

            except ConnectionClosed as e:
                raise RuntimeError(f"ws-closed code={e.code} reason={e.reason}") from e
            finally:
                ping_task.cancel()
                with contextlib.suppress(Exception):
                    await ping_task

    async def _send(self, ws, payload: Any) -> None:
        await self.bucket.take(1)
        if isinstance(payload, str):
            await ws.send(payload)
        else:
            await ws.send(json.dumps(payload, separators=(",", ":")))

    async def _subscribe(self, ws) -> None:
        args = []
        for sym in self.symbols:
            for ch in self.channels_per_symbol:
                args.append({"instType": self.inst_type, "channel": ch, "instId": sym})
        sub = {"op": "subscribe", "args": args}
        await self._send(ws, sub)
        log.info("WS shard %s subscribed channels=%d", self.shard_id, len(args))

    async def _ping_loop(self, ws) -> None:
        # Bitget public WS accepts text "ping" and returns "pong"
        while True:
            await asyncio.sleep(self.ping_interval_sec)
            with contextlib.suppress(Exception):
                await self._send(ws, "ping")
