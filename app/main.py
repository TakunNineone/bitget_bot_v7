from __future__ import annotations
import argparse
import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Tuple

import aiohttp
import yaml

from .utils import now_ms
from .storage import SQLiteStore
from .ws_client import WSShard
from .rest_client import RestPoller
from .aggregator import MicroAggregator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("main")


def shard_symbols(symbols: List[str], channels_per_symbol: List[str], max_channels_per_conn: int) -> List[List[str]]:
    per_symbol = max(1, len(channels_per_symbol))
    max_symbols = max(1, max_channels_per_conn // per_symbol)
    shards = []
    for i in range(0, len(symbols), max_symbols):
        shards.append(symbols[i:i + max_symbols])
    return shards


async def warmup_candles_15m(
    store: SQLiteStore,
    inst_type: str,
    product_type: str,
    symbols: List[str],
    limit: int = 200,
) -> None:
    """
    Прогрев: тянем последние N свечей 15m через REST и кладём их в raw_ws_candle с interval='candle15m'.
    Это даёт ATR сразу после старта (не ждём 3-4 часа).
    """
    url = "https://api.bitget.com/api/v2/mix/market/candles"
    timeout = aiohttp.ClientTimeout(total=20)

    rows_total = 0
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for sym in symbols:
            params = {
                "symbol": sym,
                "productType": product_type,
                "granularity": "15m",
                "limit": limit,
            }
            try:
                async with session.get(url, params=params) as r:
                    txt = await r.text()
                    if r.status != 200:
                        log.warning("Warmup candles failed %s HTTP %s: %s", sym, r.status, txt[:200])
                        continue
                    payload = json.loads(txt)
            except Exception as e:
                log.warning("Warmup candles error %s: %s", sym, e)
                continue

            data = payload.get("data") or []
            recv_ts = now_ms()
            rows: List[Tuple] = []

            # Bitget candles often come as list of arrays: [ts, open, high, low, close, vol, ...]
            for item in data:
                if isinstance(item, list) and len(item) >= 6:
                    candle_ts = int(item[0])
                    o, h, l, c, v = map(str, item[1:6])
                    rows.append(
                        (inst_type, sym, "candle15m", candle_ts, recv_ts, o, h, l, c, v, json.dumps(item, separators=(",", ":")))
                    )

            if rows:
                store.put_raw_ws_candle(rows)
                rows_total += len(rows)
                log.info("Warmup %s: inserted %d 15m candles", sym, len(rows))

    log.info("Warmup done: inserted total %d 15m candles", rows_total)


async def main_async(cfg: Dict[str, Any]) -> None:
    data_dir = cfg.get("data_dir", "data")
    os.makedirs(data_dir, exist_ok=True)

    store = SQLiteStore(cfg.get("sqlite_path", os.path.join(data_dir, "bitget.db")))

    inst_type = cfg["inst_type"]
    symbols: List[str] = cfg["symbols"]
    channels_per_symbol: List[str] = cfg["channels_per_symbol"]
    ws_url = cfg["ws_public_url"]

    agg = MicroAggregator()

    # ---- WARMUP (15m candles) ----
    warmup_limit = int(cfg.get("warmup", {}).get("candles_15m_limit", 200))
    try:
        await warmup_candles_15m(
            store=store,
            inst_type=inst_type,
            product_type=cfg["rest_product_type"],
            symbols=symbols,
            limit=warmup_limit,
        )
    except Exception as e:
        log.warning("Warmup failed (continuing anyway): %s", e)

    # buffers
    buf_ticker: List[Tuple] = []
    buf_trade: List[Tuple] = []
    buf_books: List[Tuple] = []
    buf_candle: List[Tuple] = []

    async def on_ws_message(msg: Dict[str, Any], recv_ts: int) -> None:
        if "event" in msg:
            return
        arg = msg.get("arg") or {}
        channel = arg.get("channel")
        instId = arg.get("instId")
        if not channel or not instId:
            return
        data = msg.get("data")
        ts_outer = msg.get("ts")
        payload_str = json.dumps(msg, separators=(",", ":"))

        if channel == "ticker":
            if isinstance(data, list) and data:
                d0 = data[0]
            else:
                d0 = data if isinstance(data, dict) else {}
            ts = int(d0.get("ts") or ts_outer or recv_ts)
            buf_ticker.append((inst_type, instId, ts, recv_ts, payload_str))
            agg.on_ticker(instId, d0)

        elif channel == "trade":
            if isinstance(data, list):
                for d in data:
                    tradeId = str(d.get("tradeId", ""))
                    ts = int(d.get("ts") or ts_outer or recv_ts)
                    price = str(d.get("price", ""))
                    size = str(d.get("size", ""))
                    side = str(d.get("side", "")).lower()
                    buf_trade.append(
                        (inst_type, instId, tradeId, ts, recv_ts, price, size, side, json.dumps(d, separators=(",", ":")))
                    )
                    if tradeId and price and size and side:
                        agg.on_trade(instId, ts, price, size, side)

        elif channel.startswith("books"):
            d0 = None
            if isinstance(data, list) and data:
                d0 = data[0]
            elif isinstance(data, dict):
                d0 = data
            if not isinstance(d0, dict):
                return
            ts = int(d0.get("ts") or ts_outer or recv_ts)
            bids = d0.get("bids") or []
            asks = d0.get("asks") or []
            checksum = d0.get("checksum")
            buf_books.append(
                (inst_type, instId, ts, recv_ts, json.dumps(bids), json.dumps(asks), int(checksum) if checksum is not None else None, payload_str)
            )
            agg.on_books15(instId, bids, asks, int(checksum) if checksum is not None else None)

        elif channel.startswith("candle"):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, list) and len(item) >= 6:
                        candle_ts = int(item[0])
                        o, h, l, c, v = map(str, item[1:6])
                        buf_candle.append(
                            (inst_type, instId, channel, candle_ts, recv_ts, o, h, l, c, v, json.dumps(item, separators=(",", ":")))
                        )
                    elif isinstance(item, dict):
                        candle_ts = int(item.get("ts") or item.get("candleTime") or recv_ts)
                        o = str(item.get("open") or item.get("o") or "")
                        h = str(item.get("high") or item.get("h") or "")
                        l = str(item.get("low") or item.get("l") or "")
                        c = str(item.get("close") or item.get("c") or "")
                        v = str(item.get("vol") or item.get("v") or "")
                        buf_candle.append(
                            (inst_type, instId, channel, candle_ts, recv_ts, o, h, l, c, v, json.dumps(item, separators=(",", ":")))
                        )

        # flush raw buffers in batches
        if len(buf_ticker) >= 200:
            store.put_raw_ws_ticker(buf_ticker)
            buf_ticker.clear()
        if len(buf_trade) >= 500:
            store.put_raw_ws_trade(buf_trade)
            buf_trade.clear()
        if len(buf_books) >= 200:
            store.put_raw_ws_books15(buf_books)
            buf_books.clear()
        if len(buf_candle) >= 200:
            store.put_raw_ws_candle(buf_candle)
            buf_candle.clear()

    async def on_rest_payload(kind: str, sym: str, payload: Dict[str, Any], recv_ts: int) -> None:
        s = json.dumps(payload, separators=(",", ":"))
        ts = recv_ts
        try:
            d = payload.get("data")
            if isinstance(d, list) and d and isinstance(d[0], dict) and "ts" in d[0]:
                ts = int(d[0]["ts"])
            elif isinstance(d, dict) and "ts" in d:
                ts = int(d["ts"])
        except Exception:
            pass

        if kind == "symbol-price":
            store.put_raw_rest_symbol_price([(cfg["rest_product_type"], sym, ts, recv_ts, s)])
            agg.on_rest_symbol_price(sym, payload)
        elif kind == "open-interest":
            store.put_raw_rest_open_interest([(cfg["rest_product_type"], sym, ts, recv_ts, s)])
            agg.on_rest_open_interest(sym, payload)
        elif kind == "current-fund-rate":
            store.put_raw_rest_current_fund_rate([(cfg["rest_product_type"], sym, ts, recv_ts, s)])
            agg.on_rest_funding(sym, payload)
        elif kind == "funding-time":
            store.put_raw_rest_funding_time([(cfg["rest_product_type"], sym, ts, recv_ts, s)])

    # WS shards
    shards = shard_symbols(symbols, channels_per_symbol, int(cfg.get("max_channels_per_connection", 800)))
    ws_tasks = []
    for i, syms in enumerate(shards):
        ws = WSShard(
            url=ws_url,
            inst_type=inst_type,
            symbols=syms,
            channels_per_symbol=channels_per_symbol,
            ping_interval_sec=int(cfg.get("ping_interval_sec", 30)),
            reconnect_backoff_sec=int(cfg.get("reconnect_backoff_sec", 3)),
            max_msgs_per_second=int(cfg.get("max_msgs_per_second", 8)),
            on_message=on_ws_message,
            shard_id=i,
        )
        ws_tasks.append(asyncio.create_task(ws.run()))

    # REST poller
    rest = RestPoller(
        base_url="https://api.bitget.com",
        product_type=cfg["rest_product_type"],
        symbols=symbols,
        intervals=cfg.get("rest_poll", {}),
        on_payload=on_rest_payload,
    )
    rest_task = asyncio.create_task(rest.run())

    # aggregation loop
    async def agg_loop():
        last_sec = None
        last_min = None
        while True:
            await asyncio.sleep(0.05)
            now = now_ms()
            sec_ts = now - (now % 1000)
            min_ts = now - (now % 60000)

            if cfg.get("aggregations", {}).get("agg_1s_enabled", True):
                if last_sec is None:
                    last_sec = sec_ts
                if sec_ts > last_sec:
                    rows = agg.flush_for_symbols(last_sec, symbols)
                    store.put_agg_1s_micro(rows)
                    last_sec = sec_ts

            if cfg.get("aggregations", {}).get("agg_1m_enabled", True):
                if last_min is None:
                    last_min = min_ts
                if min_ts > last_min:
                    rows = agg.flush_1m_context(last_min, symbols)
                    store.put_agg_1m_context(rows)
                    last_min = min_ts

            # periodic flush raw buffers
            if len(buf_ticker):
                store.put_raw_ws_ticker(buf_ticker)
                buf_ticker.clear()
            if len(buf_trade):
                store.put_raw_ws_trade(buf_trade)
                buf_trade.clear()
            if len(buf_books):
                store.put_raw_ws_books15(buf_books)
                buf_books.clear()
            if len(buf_candle):
                store.put_raw_ws_candle(buf_candle)
                buf_candle.clear()

    agg_task = asyncio.create_task(agg_loop())

    try:
        await asyncio.gather(*ws_tasks, rest_task, agg_task)
    finally:
        store.close()


def load_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    args = p.parse_args()
    cfg = load_yaml(args.config)
    asyncio.run(main_async(cfg))


if __name__ == "__main__":
    cli()
