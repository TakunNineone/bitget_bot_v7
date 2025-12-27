from __future__ import annotations
import argparse
import asyncio
import json
import os
from typing import Any, Dict, List
import aiohttp
import yaml
from .storage import SQLiteStore
from .utils import now_ms

async def fetch(session, url, params):
    async with session.get(url, params=params) as r:
        r.raise_for_status()
        return await r.json()

async def main_async(cfg: Dict[str,Any], granularity: str, limit: int):
    store = SQLiteStore(cfg.get("sqlite_path", "data/bitget.db"))
    base_url = "https://api.bitget.com"
    path = "/api/v2/mix/market/candles"
    product_type = cfg["rest_product_type"]
    symbols: List[str] = cfg["symbols"]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=20)) as session:
        for sym in symbols:
            payload = await fetch(session, base_url+path, {"symbol": sym, "productType": product_type, "granularity": granularity, "limit": limit})
            recv_ts = now_ms()
            # store as raw in raw_ws_candle for convenience (interval=rest_<granularity>)
            data = payload.get("data") or []
            rows = []
            for item in data:
                if isinstance(item, list) and len(item) >= 6:
                    candle_ts = int(item[0])
                    o,h,l,c,v = map(str, item[1:6])
                    rows.append((cfg["inst_type"], sym, f"rest_{granularity}", candle_ts, recv_ts, o,h,l,c,v, json.dumps(item, separators=(",",":"))))
            store.put_raw_ws_candle(rows)
            print(f"{sym}: stored {len(rows)} candles ({granularity})")
    store.close()

def load_yaml(path: str) -> Dict[str,Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def cli():
    p = argparse.ArgumentParser()
    p.add_argument("--config", required=True)
    p.add_argument("--granularity", default="15m")
    p.add_argument("--limit", type=int, default=1000)
    args = p.parse_args()
    cfg = load_yaml(args.config)
    asyncio.run(main_async(cfg, args.granularity, args.limit))

if __name__ == "__main__":
    cli()
