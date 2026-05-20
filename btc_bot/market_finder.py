"""
Finds active BTC Up/Down binary markets on Polymarket.
Uses the Gamma API — no auth required.
"""
from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import List

import httpx
from loguru import logger

from btc_bot.models import BTCMarket

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

# Keywords that identify BTC direction markets
BTC_KEYWORDS = ["bitcoin", "btc", "will btc", "btc up", "btc down",
                "bitcoin up", "bitcoin down", "higher", "lower"]


class MarketFinder:
    def __init__(self, timeout: float = 10.0):
        self._http = httpx.AsyncClient(timeout=timeout)

    async def find_btc_markets(self, limit: int = 100) -> List[BTCMarket]:
        """Return active BTC Up/Down markets sorted by volume."""
        try:
            resp = await self._http.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "limit": limit,
                    "closed": "false",
                    "order": "volume",
                    "ascending": "false",
                },
            )
            resp.raise_for_status()
            markets = resp.json()
        except Exception as exc:
            logger.warning(f"MarketFinder: Gamma fetch error: {exc}")
            return []

        results: List[BTCMarket] = []
        for m in markets:
            question = m.get("question", "").lower()
            if not any(kw in question for kw in BTC_KEYWORDS):
                continue

            raw_tok = m.get("clobTokenIds") or []
            tokens  = json.loads(raw_tok) if isinstance(raw_tok, str) else raw_tok
            if len(tokens) < 2:
                continue

            raw_px = m.get("outcomePrices") or ["0.5", "0.5"]
            prices = json.loads(raw_px) if isinstance(raw_px, str) else raw_px
            yes_px = Decimal(str(prices[0])) if prices else Decimal("0.5")
            no_px  = Decimal(str(prices[1])) if len(prices) > 1 else Decimal("0.5")

            results.append(BTCMarket(
                market_id=f"{tokens[0]}:{tokens[1]}",
                question=m.get("question", ""),
                yes_ask=yes_px,
                no_ask=no_px,
                volume=float(m.get("volume") or 0),
                end_date=m.get("endDate", ""),
            ))

        logger.info(f"MarketFinder: found {len(results)} BTC markets")
        return results

    async def refresh_prices(self, market: BTCMarket) -> BTCMarket:
        """Fetch live CLOB prices for a single market."""
        yes_id, no_id = market.market_id.split(":", 1)
        try:
            yes_resp, no_resp = await asyncio.gather(
                self._http.get(f"{CLOB_BASE}/price",
                               params={"token_id": yes_id, "side": "buy"}),
                self._http.get(f"{CLOB_BASE}/price",
                               params={"token_id": no_id,  "side": "buy"}),
            )
            yes_resp.raise_for_status()
            no_resp.raise_for_status()
            market.yes_ask = Decimal(str(yes_resp.json().get("price", market.yes_ask)))
            market.no_ask  = Decimal(str(no_resp.json().get("price",  market.no_ask)))
        except Exception as exc:
            logger.debug(f"MarketFinder: price refresh error for {market.label}: {exc}")
        return market

    async def close(self) -> None:
        await self._http.aclose()
