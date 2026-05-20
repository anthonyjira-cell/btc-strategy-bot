"""
Real-time BTC price feed.

Sources tried in order:
  1. CoinGecko REST   — poll every 6s, works everywhere, no auth needed
  2. Kraken WebSocket — lower latency if REST fails
  3. Binance REST     — final fallback (public endpoint, no WebSocket)
"""
from __future__ import annotations

import asyncio
from collections import deque
from typing import Optional

import httpx
from loguru import logger


class BTCFeed:
    """
    Polls BTC/USD price with automatic source fallback.
    Provides current price + short-term momentum signal.
    """

    _COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
    _BINANCE_REST  = "https://api.binance.us/api/v3/ticker/price"
    _KRAKEN_REST   = "https://api.kraken.com/0/public/Ticker"

    def __init__(self, window: int = 60, poll_interval: float = 6.0):
        self._price: Optional[float] = None
        self._history: deque[float] = deque(maxlen=window)
        self._callbacks: list = []
        self._source: str = "none"
        self._poll_interval = poll_interval

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

    # ── Price fetchers ────────────────────────────────────────────────────────

    async def _fetch_coingecko(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(
                self._COINGECKO_URL,
                params={"ids": "bitcoin", "vs_currencies": "usd"},
                timeout=8.0,
            )
            r.raise_for_status()
            return float(r.json()["bitcoin"]["usd"])
        except Exception as exc:
            logger.debug(f"BTCFeed [CoinGecko]: {exc}")
            return None

    async def _fetch_kraken(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(
                self._KRAKEN_REST,
                params={"pair": "XBTUSD"},
                timeout=8.0,
            )
            r.raise_for_status()
            data = r.json()
            result = data.get("result", {})
            pair_data = next(iter(result.values()), {})
            return float(pair_data["c"][0])  # last trade price
        except Exception as exc:
            logger.debug(f"BTCFeed [Kraken REST]: {exc}")
            return None

    async def _fetch_binance_us(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(
                self._BINANCE_REST,
                params={"symbol": "BTCUSDT"},
                timeout=8.0,
            )
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as exc:
            logger.debug(f"BTCFeed [Binance.US REST]: {exc}")
            return None

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("BTCFeed: starting REST polling (CoinGecko → Kraken → Binance.US)")
        async with httpx.AsyncClient() as http:
            consecutive_failures = 0
            while not stop_event.is_set():
                price = await self._fetch_coingecko(http)
                if price:
                    if self._source != "coingecko":
                        logger.info(f"BTCFeed: using CoinGecko — ${price:,.0f}")
                    self._source = "coingecko"
                    consecutive_failures = 0
                    await self._emit(price)
                else:
                    price = await self._fetch_kraken(http)
                    if price:
                        if self._source != "kraken":
                            logger.info(f"BTCFeed: using Kraken REST — ${price:,.0f}")
                        self._source = "kraken"
                        consecutive_failures = 0
                        await self._emit(price)
                    else:
                        price = await self._fetch_binance_us(http)
                        if price:
                            if self._source != "binance.us":
                                logger.info(f"BTCFeed: using Binance.US — ${price:,.0f}")
                            self._source = "binance.us"
                            consecutive_failures = 0
                            await self._emit(price)
                        else:
                            consecutive_failures += 1
                            self._source = "none"
                            if consecutive_failures % 5 == 1:
                                logger.warning(
                                    f"BTCFeed: all sources failed "
                                    f"({consecutive_failures} attempts)"
                                )

                await asyncio.sleep(self._poll_interval)
        logger.info("BTCFeed: stopped")
