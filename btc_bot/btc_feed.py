"""
Real-time BTC price feed with automatic fallback.

Priority:
  1. Kraken WebSocket  (works everywhere, no auth)
  2. Binance WebSocket (blocked in some regions — HTTP 451)
  3. CoinGecko REST    (poll every 10s — last resort)
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import Optional

import httpx
import websockets
from loguru import logger


class BTCFeed:
    """
    Streams BTC/USD price with automatic source fallback.
    Provides current price + short-term momentum signal.
    """

    _KRAKEN_URL  = "wss://ws.kraken.com/v2"
    _BINANCE_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    _COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"

    def __init__(self, window: int = 60):
        self._price: Optional[float] = None
        self._history: deque[float] = deque(maxlen=window)
        self._callbacks: list = []
        self._source: str = "none"

    # ── Public interface ──────────────────────────────────────────────────────

    @property
    def price(self) -> Optional[float]:
        return self._price

    @property
    def source(self) -> str:
        return self._source

    @property
    def momentum(self) -> float:
        """[-1, +1]: positive = uptrend, negative = downtrend."""
        if len(self._history) < 10 or not self._price:
            return 0.0
        oldest = self._history[0]
        if oldest == 0:
            return 0.0
        change = (self._price - oldest) / oldest
        return max(-1.0, min(1.0, change / 0.02))

    def on_price(self, callback) -> None:
        self._callbacks.append(callback)

    async def _emit(self, price: float) -> None:
        self._price = price
        self._history.append(price)
        for cb in self._callbacks:
            await cb(price)

    # ── Kraken feed ───────────────────────────────────────────────────────────

    async def _run_kraken(self, stop_event: asyncio.Event) -> None:
        """Kraken v2 WebSocket — global availability, no auth."""
        subscribe = json.dumps({
            "method": "subscribe",
            "params": {"channel": "ticker", "symbol": ["BTC/USD"]},
        })
        while not stop_event.is_set():
            try:
                async with websockets.connect(self._KRAKEN_URL,
                                              ping_interval=20) as ws:
                    await ws.send(subscribe)
                    self._source = "kraken"
                    logger.info("BTCFeed: connected to Kraken")
                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        try:
                            msg = json.loads(raw)
                            if msg.get("channel") == "ticker" and msg.get("data"):
                                price = float(msg["data"][0]["last"])
                                await self._emit(price)
                        except (KeyError, ValueError, TypeError):
                            pass
            except Exception as exc:
                if stop_event.is_set():
                    break
                logger.warning(f"BTCFeed [Kraken]: {exc} — retrying in 5s")
                await asyncio.sleep(5)

    # ── Binance feed ──────────────────────────────────────────────────────────

    async def _run_binance(self, stop_event: asyncio.Event) -> None:
        """Binance aggTrade stream — may be blocked in some regions (HTTP 451)."""
        while not stop_event.is_set():
            try:
                async with websockets.connect(self._BINANCE_URL,
                                              ping_interval=20) as ws:
                    self._source = "binance"
                    logger.info("BTCFeed: connected to Binance")
                    async for raw in ws:
                        if stop_event.is_set():
                            break
                        try:
                            data  = json.loads(raw)
                            price = float(data["p"])
                            await self._emit(price)
                        except (KeyError, ValueError):
                            pass
            except Exception as exc:
                if stop_event.is_set():
                    break
                err = str(exc)
                if "451" in err:
                    logger.warning("BTCFeed [Binance]: HTTP 451 — region blocked")
                    raise  # bubble up so caller can switch source
                logger.warning(f"BTCFeed [Binance]: {exc} — retrying in 5s")
                await asyncio.sleep(5)

    # ── CoinGecko REST fallback ───────────────────────────────────────────────

    async def _run_coingecko(self, stop_event: asyncio.Event) -> None:
        """Poll CoinGecko every 10s — last resort if WebSockets fail."""
        self._source = "coingecko"
        logger.info("BTCFeed: falling back to CoinGecko REST polling")
        async with httpx.AsyncClient(timeout=8.0) as http:
            while not stop_event.is_set():
                try:
                    resp = await http.get(
                        self._COINGECKO_URL,
                        params={"ids": "bitcoin", "vs_currencies": "usd"},
                    )
                    resp.raise_for_status()
                    price = float(resp.json()["bitcoin"]["usd"])
                    await self._emit(price)
                except Exception as exc:
                    logger.warning(f"BTCFeed [CoinGecko]: {exc}")
                await asyncio.sleep(10)

    # ── Main entry point ──────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        """Try Kraken → Binance → CoinGecko in order."""
        logger.info("BTCFeed: starting (Kraken → Binance → CoinGecko fallback)")

        # Try Kraken first
        try:
            await self._run_kraken(stop_event)
            return
        except Exception as exc:
            if stop_event.is_set():
                return
            logger.warning(f"BTCFeed: Kraken failed ({exc}), trying Binance")

        # Try Binance
        try:
            await self._run_binance(stop_event)
            return
        except Exception as exc:
            if stop_event.is_set():
                return
            logger.warning(f"BTCFeed: Binance failed ({exc}), falling back to CoinGecko")

        # Last resort: REST polling
        await self._run_coingecko(stop_event)
        logger.info("BTCFeed: stopped")
