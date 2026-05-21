"""
Entry point for the BTC strategy + general arb bot.

Runs:
  - BTCFeed        — REST polling for BTC price (CoinGecko → Kraken → Binance.US)
  - MarketFinder   — discovers + refreshes Polymarket BTC markets every 30s
  - ArbScanner     — scans ALL Polymarket markets for pure arb every 90s
  - BTCStrategy    — evaluates markets: BTC momentum + pure arb
  - Web dashboard  — aiohttp on $PORT (default 8080)

Live trading mode (set in Railway environment variables):
  LIVE_TRADING=true          — enable real order execution
  POLY_PRIVATE_KEY=0x...     — your Polymarket wallet private key
  LIVE_POSITION_SIZE=5       — dollars per trade (default $5, max suggested $10)
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
from decimal import Decimal

from loguru import logger

from btc_bot.arb_scanner import ArbScanner
from btc_bot.btc_binary_finder import BTCBinaryFinder
from btc_bot.btc_feed import BTCFeed
from btc_bot.dashboard import create_app, start_dashboard
from btc_bot.market_finder import MarketFinder
from btc_bot.paper_trader import PaperTrader
from btc_bot.strategy import BTCStrategy

# ── Logging setup ─────────────────────────────────────────────────────────────
logger.remove()
logger.add(
    sys.stdout,
    level="DEBUG",
    format="{time:HH:mm:ss} | {level: <7} | {message}",
    colorize=False,
    enqueue=True,
)

BINARY_POLL_INTERVAL       = 10   # seconds between 5-min window checks
MARKET_REFRESH_INTERVAL    = 60   # seconds between long-dated market refreshes
MARKET_REDISCOVER_INTERVAL = 300  # seconds between full market rediscovery
ARB_SCAN_INTERVAL          = 120  # seconds between general arb scans
PORT = int(os.environ.get("PORT", 8080))


async def main() -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    feed    = BTCFeed(poll_interval=5.0)   # 5s for intra-window sensitivity
    finder  = MarketFinder()
    scanner = ArbScanner()
    binary  = BTCBinaryFinder()

    # ── Choose paper or live trading ──────────────────────────────────────────
    live_mode    = os.environ.get("LIVE_TRADING", "").lower() == "true"
    private_key  = os.environ.get("POLY_PRIVATE_KEY", "")
    pos_size     = Decimal(os.environ.get("LIVE_POSITION_SIZE", "100"))

    if live_mode and private_key:
        from btc_bot.live_trader import LiveTrader
        try:
            trader = LiveTrader(private_key)
        except Exception as exc:
            print(f"FATAL: LiveTrader init failed: {exc}", flush=True)
            sys.exit(1)
        logger.warning(
            f"⚠️  LIVE TRADING ENABLED — ${pos_size} per trade — REAL MONEY"
        )
    else:
        trader = PaperTrader()
        if live_mode and not private_key:
            logger.warning(
                "LIVE_TRADING=true but POLY_PRIVATE_KEY not set — using paper mode"
            )
        else:
            logger.info("Main: paper trading mode")

    strategy = BTCStrategy(trader, position_size=pos_size)

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

    feed.on_price(on_price)

    # ── 5-minute binary window loop (PRIMARY ENGINE) ─────────────────────────
    async def binary_loop() -> None:
        while not stop_event.is_set():
            try:
                window = await binary.get_active_window()
                if window:
                    await strategy.evaluate_binary_window(window)
                    await strategy.evaluate_directional(window)
                    logger.debug(
                        f"Binary: UP={float(window.up_ask):.3f} "
                        f"DOWN={float(window.down_ask):.3f} "
                        f"spread={float(window.spread):.3f} "
                        f"{window.seconds_remaining:.0f}s left"
                    )
                # Settle any positions whose windows have now closed
                await strategy.settle_pending()
            except Exception as exc:
                logger.warning(f"Main: binary_loop error: {exc}")
            await asyncio.sleep(BINARY_POLL_INTERVAL)

    # ── Long-dated market refresh loop ───────────────────────────────────────
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
        asyncio.create_task(feed.run(stop_event),  name="btc-feed"),
        asyncio.create_task(binary_loop(),          name="binary-windows"),
        asyncio.create_task(btc_market_loop(),      name="btc-markets"),
        asyncio.create_task(arb_scan_loop(),        name="arb-scan"),
    ]

    logger.info("Main: BTC 5-min binary strategy started")

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
        await binary.close()
        await trader.close()
        logger.info("Main: stopped")


if __name__ == "__main__":
    asyncio.run(main())
