from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any

def now_ms() -> int:
    return int(time.time() * 1000)

@dataclass
class TokenBucket:
    """Простой лимитер сообщений (для subscribe/ping), чтобы не превышать N msg/sec."""
    rate: float
    capacity: float
    tokens: float
    last: float

    @classmethod
    def per_second(cls, rate: float, capacity: Optional[float] = None) -> "TokenBucket":
        cap = capacity if capacity is not None else rate
        t = time.monotonic()
        return cls(rate=rate, capacity=cap, tokens=cap, last=t)

    async def take(self, n: float = 1.0) -> None:
        while True:
            self._refill()
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep(0.01)

    def _refill(self) -> None:
        t = time.monotonic()
        dt = t - self.last
        self.last = t
        self.tokens = min(self.capacity, self.tokens + dt * self.rate)

def safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")
