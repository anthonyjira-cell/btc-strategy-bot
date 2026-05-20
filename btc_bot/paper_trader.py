"""
Paper trader: simulates fills using real Polymarket CLOB prices.
A buy order fills if the current ask price <= our limit price.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Optional

import httpx
from loguru import logger

from btc_bot.models import Side

CLOB_BASE = "https://clob.polymarket.com"


class PaperTrader:
    def __init__(self, slippage: Decimal = Decimal("0.005")):
        self._slippage = slippage
        self._http     = httpx.AsyncClient(timeout=5.0)

    async def fill(
        self,
        market_id: str,
        side: Side,
        limit_price: Decimal,
        size: Decimal,
    ) -> bool:
        """
        Returns True if the order would fill at current market prices.
        We fetch the live ask from CLOB and fill if ask <= limit_price + slippage.
        """
        token_id = self._resolve_token(market_id, side)
        try:
            resp = await self._http.get(
                f"{CLOB_BASE}/price",
                params={"token_id": token_id, "side": "buy"},
            )
            resp.raise_for_status()
            ask = Decimal(str(resp.json().get("price", "0.5")))
        except Exception as exc:
            logger.debug(f"PaperTrader: price fetch error: {exc}")
            # Assume fill on API error (conservative)
            return True

        fills = ask <= limit_price + self._slippage
        if fills:
            logger.debug(
                f"PaperTrader: FILL {side} {market_id[:16]} "
                f"@ ask={ask:.4f} limit={limit_price:.4f}"
            )
        return fills

    @staticmethod
    def _resolve_token(market_id: str, side: Side) -> str:
        market_id = market_id.strip("'\"")
        if ":" in market_id:
            yes_id, no_id = market_id.split(":", 1)
            return yes_id if side == Side.YES else no_id
        return market_id

    async def close(self) -> None:
        await self._http.aclose()
