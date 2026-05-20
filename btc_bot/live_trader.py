"""
Live trader: executes real orders on Polymarket CLOB.

Activated when LIVE_TRADING=true env var is set.
Requires POLY_PRIVATE_KEY env var (your wallet's private key).

Position size is controlled by LIVE_POSITION_SIZE (default $5 per trade).
With $99 and max 6 positions at $5 each = $60 max deployed at once.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Optional

from loguru import logger

from btc_bot.models import Side

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137   # Polygon mainnet


class LiveTrader:
    """Places real limit orders on Polymarket CLOB via py-clob-client."""

    def __init__(self, private_key: str):
        # Import here so missing package only errors if live trading is enabled
        from py_clob_client.client import ClobClient

        self._client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=0,   # EOA (standard MetaMask-style wallet)
        )

        # Derive API credentials from the private key (one-time per session)
        try:
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            logger.info("LiveTrader: API credentials ready ✓")
        except Exception as exc:
            logger.error(f"LiveTrader: credential setup failed: {exc}")
            raise RuntimeError(f"LiveTrader init failed: {exc}") from exc

        self._loop = asyncio.get_event_loop()

    async def fill(
        self,
        market_id: str,
        side: Side,
        limit_price: Decimal,
        size: Decimal,          # size in USD → converted to shares
    ) -> Optional[Decimal]:
        """
        Place a real GTC limit order.  Returns limit_price on success, None on failure.
        size is dollars; converts to shares = dollars / price.
        """
        from py_clob_client.clob_types import OrderArgs, OrderType

        token_id = self._resolve_token(market_id, side)
        price_f  = float(limit_price)
        shares   = round(float(size) / price_f, 2)

        if shares < 1.0:
            logger.warning(f"LiveTrader: order too small ({shares} shares), skipping")
            return None

        order_args = OrderArgs(
            token_id=token_id,
            price=price_f,
            size=shares,
            side="BUY",
        )

        try:
            # py-clob-client is synchronous — run in thread executor
            order = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._client.create_order(order_args)
            )
            resp = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._client.post_order(order, OrderType.GTC)
            )

            if resp and resp.get("success"):
                logger.info(
                    f"LiveTrader: ✅ ORDER PLACED {side.value} "
                    f"token={token_id[:12]}… "
                    f"price={price_f:.4f} shares={shares:.2f} "
                    f"(~${float(size):.2f})"
                )
                return limit_price
            else:
                logger.warning(f"LiveTrader: order rejected: {resp}")
                return None

        except Exception as exc:
            logger.error(f"LiveTrader: order error: {exc}")
            return None

    @staticmethod
    def _resolve_token(market_id: str, side: Side) -> str:
        market_id = market_id.strip("'\"")
        if ":" in market_id:
            yes_id, no_id = market_id.split(":", 1)
            return yes_id if side == Side.YES else no_id
        return market_id

    async def close(self) -> None:
        pass
