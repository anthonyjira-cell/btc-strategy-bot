"""
Real-time BTC price feed.

Primary:  Kraken WebSocket — real-time trade ticks (~100ms latency)
Fallback: Kraken REST → Binance.US REST (if WS disconnects)

WebSocket is critical for the dislocation strategy: we need to detect
BTC price moves within ~1s before Polymarket market makers reprice.
REST polling (5s intervals) is too slow — the market always reprices first.
"""
from __future__ import annotations

import asyncio
import json
from collections import deque
from typing import Optional

import aiohttp
import httpx
from loguru import logger

KRAKEN_WS   = "wss://ws.kraken.com"
KRAKEN_REST = "https://api.kraken.com/0/public/Ticker"
BINANCE_REST = "https://api.binance.us/api/v3/ticker/price"

# How long without a WS tick before we consider the feed stale
WS_STALE_SECS = 15.0


class BTCFeed:
    """
    Real-time BTC/USD feed via Kraken WebSocket with REST fallback.
    Provides current price + short-term momentum signal.
    """

    def __init__(self, window: int = 60, poll_interval: float = 5.0):
        self._price:    Optional[float] = None
        self._history:  deque[float]    = deque(maxlen=window)
        self._callbacks: list           = []
        self._source:   str             = "none"
        self._poll_interval             = poll_interval
        self._last_tick: float          = 0.0   # timestamp of last price update; set on first tick

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
        import time as _time
        self._price = price
        self._last_tick = _time.time()
        self._history.append(price)
        for cb in self._callbacks:
            await cb(price)

    # ── WebSocket feed (primary) ──────────────────────────────────────────────

    async def _run_websocket(self, stop_event: asyncio.Event) -> None:
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
                        # Subscribe to individual trades (not ticker) for fastest updates
                        await ws.send_json({
                            "event": "subscribe",
                            "pair": ["XBT/USD"],
                            "subscription": {"name": "trade"},
                        })

                        if self._source != "kraken-ws":
                            logger.info("BTCFeed: Kraken WebSocket connected ✓")
                        self._source = "kraken-ws"
                        backoff = 1.0  # reset on successful connect

                        async for msg in ws:
                            if stop_event.is_set():
                                return
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                # Trade messages: [channelID, [[price,vol,time,side,...]], "trade", "XBT/USD"]
                                if (
                                    isinstance(data, list)
                                    and len(data) == 4
                                    and data[2] == "trade"
                                ):
                                    trades = data[1]
                                    if trades:
                                        price = float(trades[-1][0])
                                        await self._emit(price)
                            elif msg.type in (
                                aiohttp.WSMsgType.CLOSED,
                                aiohttp.WSMsgType.ERROR,
                            ):
                                logger.warning("BTCFeed: WebSocket closed/error — reconnecting")
                                break

            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning(f"BTCFeed WS error: {exc} — retry in {backoff:.0f}s")

            if not stop_event.is_set():
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)   # exponential backoff, cap 30s

    # ── REST fallback (runs in parallel, kicks in when WS is stale) ──────────

    async def _run_rest_fallback(self, stop_event: asyncio.Event) -> None:
        """
        Poll REST sources every 5s as a heartbeat / fallback when WS is stale.
        Ensures the price is never more than 5s old even if WS disconnects.
        """
        import time as _time
        async with httpx.AsyncClient() as http:
            while not stop_event.is_set():
                await asyncio.sleep(self._poll_interval)
                if stop_event.is_set():
                    break

                # Skip REST poll if WebSocket is delivering fresh ticks,
                # OR if WS just connected but hasn't received its first tick yet
                # (last_tick=0 means WS hasn't ticked yet — give it WS_STALE_SECS to warm up)
                ws_fresh = (
                    self._source == "kraken-ws"
                    and (
                        self._last_tick == 0.0   # WS just connected, not ticked yet
                        or _time.time() - self._last_tick < WS_STALE_SECS
                    )
                )
                if ws_fresh:
                    continue

                # WebSocket stale or down — fall back to REST
                price = await self._fetch_kraken_rest(http)
                if price:
                    if self._source != "kraken-rest":
                        logger.warning("BTCFeed: WebSocket stale — fell back to Kraken REST")
                    self._source = "kraken-rest"
                    await self._emit(price)
                    continue

                price = await self._fetch_binance(http)
                if price:
                    if self._source != "binance.us":
                        logger.warning("BTCFeed: fell back to Binance.US REST")
                    self._source = "binance.us"
                    await self._emit(price)

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

    async def _fetch_binance(self, http: httpx.AsyncClient) -> Optional[float]:
        try:
            r = await http.get(BINANCE_REST, params={"symbol": "BTCUSDT"}, timeout=8.0)
            r.raise_for_status()
            return float(r.json()["price"])
        except Exception as exc:
            logger.debug(f"BTCFeed [Binance.US]: {exc}")
            return None

    # ── Main entry point ──────────────────────────────────────────────────────

    async def run(self, stop_event: asyncio.Event) -> None:
        logger.info("BTCFeed: starting (Kraken WebSocket + REST fallback)")
        await asyncio.gather(
            self._run_websocket(stop_event),
            self._run_rest_fallback(stop_event),
            return_exceptions=True,
        )
        logger.info("BTCFeed: stopped")
