"""
Entry point for the BTC strategy + general arb bot.

Runs:
  - BTCFeed        — REST polling for BTC price (CoinGecko → Kraken → Binance.US)
  - MarketFinder   — discovers + refreshes Polymarket BTC markets every 30s
  - ArbScanner     — scans ALL Polymarket markets for pure arb every 90s
  - BTCStrategy    — evaluates markets: BTC momentum + pure arb
  - Web dashboard  — aiohttp on $PORT (default 8080)
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys

from loguru import logger

from btc_bot.arb_scanner import ArbScanner
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

MARKET_REFRESH_INTERVAL   = 30    # seconds between BTC market price refreshes
MARKET_REDISCOVER_INTERVAL = 300  # seconds between full BTC market rediscovery
ARB_SCAN_INTERVAL          = 90   # seconds between general arb scans
PORT = int(os.environ.get("PORT", 8080))


async def main() -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    feed     = BTCFeed()
    finder   = MarketFinder()
    scanner  = ArbScanner()
    trader   = PaperTrader()
    strategy = BTCStrategy(trader)

    btc_markets:  list = []  # BTC-specific markets
    arb_markets:  list = []  # general arb candidates
    all_markets:  list = []  # combined list for dashboard

    def _rebuild_display() -> None:
        """Keep the dashboard list in sync."""
        all_markets.clear()
        all_markets.extend(btc_markets)
        # Add arb markets not already in BTC list
        btc_ids = {m.market_id for m in btc_markets}
        all_markets.extend(m for m in arb_markets if m.market_id not in btc_ids)

    # ── BTC price callback ────────────────────────────────────────────────────
    async def on_price(price: float) -> None:
        strategy.update_btc(price, feed.momentum)
        for market in list(btc_markets):
            await strategy.evaluate_market(market)

    feed.on_price(on_price)

    # ── BTC market refresh loop ───────────────────────────────────────────────
    async def btc_market_loop() -> None:
        discover_counter = 0
        while not stop_event.is_set():
            try:
                if discover_counter == 0:
                    new = await finder.find_btc_markets(limit=200)
                    if new:
                        btc_markets.clear()
                        btc_markets.extend(new)
                        _rebuild_display()
                        logger.info(f"Main: {len(btc_markets)} BTC markets loaded")
                    discover_counter = MARKET_REDISCOVER_INTERVAL // MARKET_REFRESH_INTERVAL
                else:
                    for i, m in enumerate(list(btc_markets)):
                        btc_markets[i] = await finder.refresh_prices(m)
                    _rebuild_display()
                    discover_counter -= 1
            except Exception as exc:
                logger.warning(f"Main: btc_market_loop error: {exc}")
            await asyncio.sleep(MARKET_REFRESH_INTERVAL)

    # ── General arb scan loop ─────────────────────────────────────────────────
    async def arb_scan_loop() -> None:
        while not stop_event.is_set():
            try:
                candidates = await scanner.find_arb_markets(fetch_limit=500)
                arb_markets.clear()
                arb_markets.extend(candidates)
                _rebuild_display()

                # Immediately try to enter any arb found
                for market in list(arb_markets):
                    await strategy.evaluate_arb_only(market)

            except Exception as exc:
                logger.warning(f"Main: arb_scan_loop error: {exc}")
            await asyncio.sleep(ARB_SCAN_INTERVAL)

    # ── Dashboard ─────────────────────────────────────────────────────────────
    app    = create_app(strategy, feed, all_markets)
    runner = await start_dashboard(app, PORT)
    logger.info(f"Main: dashboard on http://0.0.0.0:{PORT}")

    # ── Launch all tasks ──────────────────────────────────────────────────────
    tasks = [
        asyncio.create_task(feed.run(stop_event),    name="btc-feed"),
        asyncio.create_task(btc_market_loop(),        name="btc-markets"),
        asyncio.create_task(arb_scan_loop(),          name="arb-scan"),
    ]

    logger.info(
        "Main: hybrid BTC strategy + general arb scanner started (paper mode)"
    )

    try:
        await stop_event.wait()
    finally:
        logger.info("Main: shutting down…")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await runner.cleanup()
        await finder.close()
        await scanner.close()
        await trader.close()
        logger.info("Main: stopped")


if __name__ == "__main__":
    asyncio.run(main())
