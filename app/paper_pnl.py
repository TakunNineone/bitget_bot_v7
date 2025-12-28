from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class VirtualPosition:
    instId: str
    side: str               # "long" or "short"
    entry_price: float
    entry_ts: int
    stop_loss: float        # as fraction, e.g. 0.0035
    take_profit: float      # as fraction, e.g. 0.0100
    trailing_start: float   # fraction
    trailing_step: float    # fraction

    # stats
    best_pnl: float = 0.0
    worst_pnl: float = 0.0
    trail_active: bool = False
    trail_level: Optional[float] = None  # price level


def pnl_frac(pos: VirtualPosition, price: float) -> float:
    if pos.side == "long":
        return price / pos.entry_price - 1.0
    return pos.entry_price / price - 1.0


def should_exit(pos: VirtualPosition, price: float) -> Optional[str]:
    p = pnl_frac(pos, price)

    # Hard SL / TP
    if p <= -pos.stop_loss:
        return "SL"
    if p >= pos.take_profit:
        return "TP"

    # Trailing activation
    if (not pos.trail_active) and p >= pos.trailing_start:
        pos.trail_active = True
        if pos.side == "long":
            pos.trail_level = price * (1.0 - pos.trailing_step)
        else:
            pos.trail_level = price * (1.0 + pos.trailing_step)

    # Trailing update & check
    if pos.trail_active and pos.trail_level is not None:
        if pos.side == "long":
            new_level = price * (1.0 - pos.trailing_step)
            if new_level > pos.trail_level:
                pos.trail_level = new_level
            if price <= pos.trail_level:
                return "TRAIL"
        else:
            new_level = price * (1.0 + pos.trailing_step)
            if new_level < pos.trail_level:
                pos.trail_level = new_level
            if price >= pos.trail_level:
                return "TRAIL"

    return None
