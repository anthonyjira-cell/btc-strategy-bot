"""
Live trader: executes real orders on Polymarket CLOB.

Activated when LIVE_TRADING=true env var is set.
Requires POLY_PRIVATE_KEY env var (your wallet's private key).

Automatically detects neg_risk market type to avoid order_version_mismatch.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Dict, Optional

from loguru import logger

from btc_bot.models import Side

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137   # Polygon mainnet


class LiveTrader:
    """Places real limit orders on Polymarket CLOB via py-clob-client."""

    def __init__(self, private_key: str):
        from py_clob_client.client import ClobClient
        from eth_account import Account

        # Derive wallet address from private key
        wallet_address = Account.from_key(private_key).address
        logger.info(f"LiveTrader: wallet address {wallet_address}")

        # Try signature_type=1 (POLY_PROXY) — used by most Polymarket web users
        self._client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=1,   # POLY_PROXY (MetaMask / browser wallet)
            funder=wallet_address,
        )

        try:
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            logger.info("LiveTrader: API credentials ready ✓")
        except Exception as exc:
            logger.error(f"LiveTrader: credential setup failed: {exc}")
            raise RuntimeError(f"LiveTrader init failed: {exc}") from exc

        # Cache neg_risk per token so we don't retry every time
        self._neg_risk_cache: Dict[str, bool] = {}

    async def fill(
        self,
        market_id: str,
        side: Side,
        limit_price: Decimal,
        size: Decimal,
    ) -> Optional[Decimal]:
        """
        Place a real GTC limit order.
        Auto-detects neg_risk by trying False then True if version_mismatch.
        Returns actual fill price on success, None on failure.
        """
        from py_clob_client.clob_types import OrderArgs, OrderType

        token_id = self._resolve_token(market_id, side)
        price_f  = float(limit_price)
        shares   = round(float(size) / price_f, 2)

        if shares < 1.0:
            logger.warning(f"LiveTrader: order too small ({shares} shares), skipping")
            return None

        # Determine which neg_risk values to try
        if token_id in self._neg_risk_cache:
            neg_risk_values = [self._neg_risk_cache[token_id]]
        else:
            neg_risk_values = [False, True]  # try both, cache winner

        loop = asyncio.get_event_loop()

        for neg_risk in neg_risk_values:
            order_args = OrderArgs(
                token_id=token_id,
                price=price_f,
                size=shares,
                side="BUY",
                neg_risk=neg_risk,
            )
            try:
                order = await loop.run_in_executor(
                    None, lambda: self._client.create_order(order_args)
                )
                resp = await loop.run_in_executor(
                    None, lambda: self._client.post_order(order, OrderType.GTC)
                )

                if resp and resp.get("success"):
                    self._neg_risk_cache[token_id] = neg_risk
                    logger.info(
                        f"LiveTrader: ✅ ORDER PLACED {side.value} "
                        f"token={token_id[:12]}… "
                        f"price={price_f:.4f} shares={shares:.2f} "
                        f"neg_risk={neg_risk} (~${float(size):.2f})"
                    )
                    return limit_price

                err_str = str(resp)
                if "order_version_mismatch" in err_str:
                    logger.debug(f"LiveTrader: version mismatch with neg_risk={neg_risk}, trying other value")
                    continue  # try the other neg_risk value

                logger.warning(f"LiveTrader: order rejected: {resp}")
                return None

            except Exception as exc:
                err_str = str(exc)
                if "order_version_mismatch" in err_str:
                    logger.debug(f"LiveTrader: version mismatch with neg_risk={neg_risk}, trying other value")
                    continue
                logger.error(f"LiveTrader: order error: {exc}")
                return None

        logger.warning(f"LiveTrader: order_version_mismatch for both neg_risk values on {token_id[:16]}")
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
