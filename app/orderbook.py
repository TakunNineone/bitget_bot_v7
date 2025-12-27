from __future__ import annotations
import zlib
from typing import List, Tuple, Optional, Any

# bids/asks are list of [price, size] where values can be str/float
Level = Tuple[str, str]  # (price_str, size_str)

def _to_str(x: Any) -> str:
    # IMPORTANT: docs say keep original formatting (e.g. '0.5000' not '0.5')
    if isinstance(x, str):
        return x
    # fallback: keep as-is but avoid scientific notation
    return format(float(x), "f").rstrip("0").rstrip(".") if "." in format(float(x), "f") else str(x)

def normalize_levels(raw: List[List[Any]], depth: int) -> List[Level]:
    out: List[Level] = []
    for lvl in raw[:depth]:
        if len(lvl) < 2:
            continue
        out.append((_to_str(lvl[0]), _to_str(lvl[1])))
    return out

def build_checksum_string(bids: List[Level], asks: List[Level], n: int = 25) -> str:
    b = bids[:n]
    a = asks[:n]
    parts = []
    # alternate bid1, ask1, bid2, ask2...
    i = 0
    while i < len(b) or i < len(a):
        if i < len(b):
            parts.append(f"{b[i][0]}:{b[i][1]}")
        if i < len(a):
            parts.append(f"{a[i][0]}:{a[i][1]}")
        i += 1
    return ":".join(parts)

def crc32_signed(s: str) -> int:
    v = zlib.crc32(s.encode("utf-8"))
    # convert to signed 32-bit int
    if v & 0x80000000:
        v = -((~v & 0xFFFFFFFF) + 1)
    return int(v)

def verify_checksum(bids: List[Level], asks: List[Level], checksum: int) -> bool:
    s = build_checksum_string(bids, asks, 25)
    return crc32_signed(s) == int(checksum)

def mid_price(best_bid: Optional[Level], best_ask: Optional[Level]) -> Optional[float]:
    if not best_bid or not best_ask:
        return None
    try:
        return (float(best_bid[0]) + float(best_ask[0])) / 2.0
    except Exception:
        return None

def spread(best_bid: Optional[Level], best_ask: Optional[Level]) -> Optional[float]:
    if not best_bid or not best_ask:
        return None
    try:
        return float(best_ask[0]) - float(best_bid[0])
    except Exception:
        return None

def microprice(best_bid: Optional[Level], best_ask: Optional[Level]) -> Optional[float]:
    # microprice = (ask*bid_qty + bid*ask_qty) / (bid_qty + ask_qty)
    if not best_bid or not best_ask:
        return None
    try:
        bid_p, bid_q = float(best_bid[0]), float(best_bid[1])
        ask_p, ask_q = float(best_ask[0]), float(best_ask[1])
        denom = bid_q + ask_q
        if denom <= 0:
            return None
        return (ask_p * bid_q + bid_p * ask_q) / denom
    except Exception:
        return None

def imbalance(bids: List[Level], asks: List[Level]) -> Optional[float]:
    # (sum_bid - sum_ask) / (sum_bid + sum_ask)
    try:
        sb = sum(float(x[1]) for x in bids) if bids else 0.0
        sa = sum(float(x[1]) for x in asks) if asks else 0.0
        denom = sb + sa
        if denom <= 0:
            return None
        return (sb - sa) / denom
    except Exception:
        return None
