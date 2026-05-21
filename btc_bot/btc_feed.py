"""
Real-time BTC price feed.

Primary:  Binance + Kraken WebSockets run concurrently (~100ms latency)
          Whichever delivers the freshest tick wins.
Fallback: Kraken REST → Binance.US REST (if both WS feeds go stale)

WebSocket is critical for the dislocation strategy: we need to detect
BTC price moves within ~1s before Polymarket market makers reprice.
REST polling (5s intervals) is too slow — the market always reprices first.

Binance stream (wss://stream.binance.com:9443/ws/btcusdt@aggTrade):
  - Extremely high volume, highly reliable
  - BTCUSDT ≈ BTCUSD within 0.05% — fine for delta calculations

Kraken stream (wss://ws.kraken.com):
  - USD-native, good as second source
  - Occasionally drops on some hosting providers
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import Optional

import aiohttp
import httpx
from loguru import logger

KRAKEN_WS    = "wss://ws.kraken.com"
BINANCE_WS   = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
KRAKEN_REST  = "https://api.kraken.com/0/public/Ticker"
BINANCE_REST = "https://api.binance.us/api/v3/ticker/price"

# How long without any WS tick before we consider all feeds stale
WS_STALE_SECS = 12.0


class BTCFeed:
    """
    Real-time BTC/USD feed.
    Runs Binance + Kraken WebSockets concurrently; REST is last resort.
    """

    def __init__(self, window: int = 60, poll_interval: float = 5.0):
        self._price:    Optional[float] = None
        self._history:  deque[float]    = deque(maxlen=window)
        self._callbacks: list           = []
        self._source:   str             = "none"
        self._poll_interval             = poll_interval
        self._last_tick: float          = 0.0   # epoch of last price update

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

    async def _emit(self, price: float, source: str) -> None:
        import time as _time
        self._price = price
        self._last_tick = _time.time()
        self._history.append(price)
        if self._source != source:
            logger.info(f"BTCFeed: switched to {source} ✓")
        self._source = source
        for cb in self._callbacks:
            await cb(price)

    # ── Binance WebSocket (primary — most reliable) ───────────────────────────

    async def _run_binance_websocket(self, stop_event: asyncio.Event) -> None:
        """
        Stream BTC/USDT aggregate trades from Binance WebSocket.
        Extremely high trade frequency; rarely drops.
        """
        backoff = 1.0
        while not stop_event.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        BINANCE_WS,
                        heartbeat=20,
                        timeout=aiohttp.ClientTimeout(total=None, connect=10),
                    ) as ws:
                        logger.info("BTCFeed: Binance WebSocket connected ✓")
                        backoff = 1.0

                        async for msg in ws:
                            if stop_event.is_set():
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # aggTrade: {"e":"aggTrade","p":"price",...}
                                if data.get("e") == "aggTrade":
                                    price = float(data["p"])
                                    await self._emit(price, "binance-ws")
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                logger.warning("BTCFeed: Binance WS closed/error — reconnecting")
                                break

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(f"BTCFeed Binance WS error: {exc} — retry in {backoff:.0f}s")

            if not stop_event.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ── Kraken WebSocket (secondary) ─────────────────────────────────────────

    async def _run_kraken_websocket(self, stop_event: asyncio.Event) -> None:
        """
        Stream live BTC/USD trade ticks from Kraken WebSocket.
        Auto-reconnects on disconnect. Runs until stop_event is set.
        """
        backoff = 1.0
        while not stop_event.is_set():
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        KRAKEN_WS,
                        heartbeat=20,
                        timeout=aiohttp.ClientTimeout(total=None, connect=10),
                    ) as ws:
                        # Subscribe to individual trades for fastest updates
                        await ws.send_json({
                            "event": "subscribe",
                            "pair": ["XBT/USD"],
                            "subscription": {"name": "trade"},
                        })

                        logger.debug("BTCFeed: Kraken WebSocket connected")
                        backoff = 1.0

                        async for msg in ws:
                            if stop_event.is_set():
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # Trade messages: [channelID, [[price,vol,time,...]], "trade", "XBT/USD"]
                                if (
                                    isinstance(data, list)
                                    and len(data) == 4
                                    and data[2] == "trade"
                                ):
                                    trades = data[1]
                                    if trades:
                                        price = float(trades[-1][0])
                                        await self._emit(price, "kraken-ws")
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                logger.debug("BTCFeed: Kraken WS closed/error — reconnecting")
                                break

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.debug(f"BTCFeed Kraken WS error: {exc} — retry in {backoff:.0f}s")

            if not stop_event.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    # ── REST fallback (only when both WS feeds are stale) ────────────────────

    async def _run_rest_fallback(self, stop_event: asyncio.Event) -> None:
        """
        Poll REST sources every 5s — emergency fallback only.
        Skipped whenever any WS feed is delivering fresh ticks.
        """
        import time as _time
        async with httpx.AsyncClient() as http:
            while not stop_event.is_set():
                await asyncio.sleep(self._poll_interval)
                if stop_event.is_set():
                    break

                # Skip if any WS is delivering fresh ticks
                # (last_tick==0 means no tick yet — give WS time to warm up)
                age = _time.time() - self._last_tick if self._last_tick > 0 else 0.0
                if self._last_tick == 0.0 or age < WS_STALE_SECS:
                    continue

                # Both WS feeds stale — fall back to REST
                price = await self._fetch_kraken_rest(http)
                if price:
                    if "rest" not in self._source:
                        logger.warning(
                            f"BTCFeed: WS feeds stale ({age:.0f}s) — fell back to Kraken REST"
                        )
                    await self._emit(price, "kraken-rest")
                    continue

                price = await self._fetch_binance_rest(http)
                if price:
                    if "rest" not in self._source:
                        logger.warning("BTCFeed: fell back to Binance REST")
                    await self._emit(price, "binance-rest")

    async def _fetch_kraken_rest(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(KRAKEN_REST, params={"pair": "XBTUSD"}, timeout=8.0)
            r.raise_for_status()
            result = r.json().get("result", {})
            pair_data = next(iter(result.values()), {})
            return float(pair_data["c"][0])
        except Exception as exc:
            logger.debug(f"BTCFeed [Kraken REST]: {exc}")
            return None

    async def _fetch_binance_rest(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(BINANCE_REST, params={"symbol": "BTCUSDT"}, timeout=8.0)
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as exc:
            logger.debug(f"BTCFeed [Binance REST]: {exc}")
            return None

    # ── Main entry point ──────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("BTCFeed: starting (Binance WS + Kraken WS + REST fallback)")
        await asyncio.gather(
            self._run_binance_websocket(stop_event),
            self._run_kraken_websocket(stop_event),
            self._run_rest_fallback(stop_event),
            return_exceptions=True,
        )
        logger.info("BTCFeed: stopped")
