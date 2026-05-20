"""
Real-time BTC price from Binance WebSocket.
No API key required — public stream.
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import Optional

import websockets
from loguru import logger


class BTCFeed:
    """
    Streams BTC/USDT trade prices from Binance.
    Provides current price + short-term momentum signal.
    """

    WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"

    def __init__(self, window: int = 60):
        self._price: Optional[float] = None
        self._history: deque[float] = deque(maxlen=window)  # last N prices
        self._callbacks: list = []
        self._running = False

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def price(self) -> Optional[float]:
        return self._price

    @property
    def momentum(self) -> float:
        """
        Returns a value in [-1, +1]:
          +1  = strongly trending up over last window
          -1  = strongly trending down
           0  = flat / not enough data
        """
        if len(self._history) < 10:
            return 0.0
        oldest = self._history[0]
        if oldest == 0:
            return 0.0
        change = (self._price - oldest) / oldest
        # Clamp to ±2% range → map to ±1
        return max(-1.0, min(1.0, change / 0.02))

    def on_price(self, callback) -> None:
        """Register a callback to be called on each new price."""
        self._callbacks.append(callback)

    async def run(self, stop_event: asyncio.Event) -> None:
        self._running = True
        logger.info("BTCFeed: connecting to Binance WebSocket")
        while not stop_event.is_set():
            try:
                async with websockets.connect(self.WS_URL, ping_interval=20) as ws:
                    logger.info("BTCFeed: connected")
                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        try:
                            data  = json.loads(raw)
                            price = float(data["p"])
                            self._price = price
                            self._history.append(price)
                            for cb in self._callbacks:
                                await cb(price)
                        except (KeyError, ValueError):
                            pass
            except Exception as exc:
                if stop_event.is_set():
                    break
                logger.warning(f"BTCFeed: disconnected ({exc}), reconnecting in 5s")
                await asyncio.sleep(5)
        logger.info("BTCFeed: stopped")
