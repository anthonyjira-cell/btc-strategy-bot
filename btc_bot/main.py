"""
Entry point for the BTC strategy bot.

Runs:
  - BTCFeed       — Binance WebSocket price stream
  - MarketFinder  — discovers + refreshes Polymarket BTC markets every 30s
  - BTCStrategy   — evaluates each market on every price tick
  - Web dashboard — aiohttp on $PORT (default 8080)
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys

from loguru import logger

from btc_bot.btc_feed import BTCFeed
from btc_bot.dashboard import create_app, start_dashboard
from btc_bot.market_finder import MarketFinder
from btc_bot.paper_trader import PaperTrader
from btc_bot.strategy import BTCStrategy

# ── Logging setup ─────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stderr,
    level="DEBUG" if not sys.stderr.isatty() else "INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <7}</level> | {message}",
    colorize=sys.stderr.isatty(),
)

MARKET_REFRESH_INTERVAL = 30   # seconds between price refreshes
MARKET_REDISCOVER_INTERVAL = 300  # seconds between full market rediscovery
PORT = int(os.environ.get("PORT", 8080))


async def main() -> None:
    stop_event = asyncio.Event()

    # Graceful shutdown
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for all signals
            pass

    feed   = BTCFeed()
    finder = MarketFinder()
    trader = PaperTrader()
    strategy = BTCStrategy(trader)

    markets_holder: list = []  # shared mutable list for dashboard

    # ── Callback: strategy evaluates every market on each BTC tick ────────────
    async def on_price(price: float) -> None:
        strategy.update_btc(price, feed.momentum)
        for market in list(markets_holder):
            await strategy.evaluate_market(market)

    feed.on_price(on_price)

    # ── Market refresh loop ───────────────────────────────────────────────────
    async def market_loop() -> None:
        discover_counter = 0
        while not stop_event.is_set():
            try:
                if discover_counter == 0:
                    # Full rediscovery
                    new_markets = await finder.find_btc_markets(limit=100)
                    if new_markets:
                        markets_holder.clear()
                        markets_holder.extend(new_markets)
                        logger.info(
                            f"Main: discovered {len(markets_holder)} BTC markets"
                        )
                    discover_counter = MARKET_REDISCOVER_INTERVAL // MARKET_REFRESH_INTERVAL
                else:
                    # Just refresh prices
                    for i, market in enumerate(list(markets_holder)):
                        markets_holder[i] = await finder.refresh_prices(market)
                    discover_counter -= 1
            except Exception as exc:
                logger.warning(f"Main: market_loop error: {exc}")

            await asyncio.sleep(MARKET_REFRESH_INTERVAL)

    # ── Dashboard ─────────────────────────────────────────────────────────────
    app    = create_app(strategy, feed, markets_holder)
    runner = await start_dashboard(app, PORT)
    logger.info(f"Main: dashboard running on http://0.0.0.0:{PORT}")

    # ── Launch tasks ──────────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(feed.run(stop_event),    name="btc-feed"),
        asyncio.create_task(market_loop(),            name="market-loop"),
    ]

    logger.info("Main: BTC strategy bot started (paper mode)")

    try:
        await stop_event.wait()
    finally:
        logger.info("Main: shutting down…")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        await finder.close()
        await trader.close()
        logger.info("Main: stopped")


if __name__ == "__main__":
    asyncio.run(main())
