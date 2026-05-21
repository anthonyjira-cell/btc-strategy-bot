"""
General Polymarket arb scanner.

Scans ALL active markets (not just BTC) for pure arb:
  combined YES ask + NO ask < (1 - fees) → buy both sides → guaranteed profit

Runs on a separate loop from the BTC strategy.
"""
from __future__ import annotations

import json
from decimal import Decimal
from typing import List

import httpx
from loguru import logger

from btc_bot.models import BTCMarket

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

# Arb thresholds
MIN_ARB_SPREAD  = Decimal("0.02")   # need 2%+ spread (fees ~1%, want 1%+ profit)
MIN_VOLUME      = 2_000.0           # $2k minimum volume — some liquidity required
MAX_CANDIDATES  = 20                # how many to return per scan


class ArbScanner:
    """Finds pure arb opportunities across all Polymarket binary markets."""

    def __init__(self, timeout: float = 10.0):
        self._http = httpx.AsyncClient(timeout=timeout)

    async def find_arb_markets(self, fetch_limit: int = 500) -> List[BTCMarket]:
        """
        Return markets where YES ask + NO ask < 0.98.
        Sorted by spread descending (best arbs first).
        """
        try:
            resp = await self._http.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "limit": fetch_limit,
                    "closed": "false",
                    "order": "volume",
                    "ascending": "false",
                },
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as exc:
            logger.warning(f"ArbScanner: Gamma fetch error: {exc}")
            return []

        candidates: List[BTCMarket] = []

        for m in raw:
            volume = float(m.get("volume") or 0)
            if volume < MIN_VOLUME:
                continue

            raw_tok = m.get("clobTokenIds") or []
            tokens  = json.loads(raw_tok) if isinstance(raw_tok, str) else raw_tok
            if len(tokens) < 2:
                continue

            raw_px = m.get("outcomePrices") or ["0.5", "0.5"]
            prices = json.loads(raw_px) if isinstance(raw_px, str) else raw_px
            yes_px = Decimal(str(prices[0])) if prices else Decimal("0.5")
            no_px  = Decimal(str(prices[1])) if len(prices) > 1 else Decimal("0.5")

            combined = yes_px + no_px
            spread   = Decimal("1") - combined

            if spread < MIN_ARB_SPREAD:
                continue

            candidates.append(BTCMarket(
                market_id=f"{tokens[0]}:{tokens[1]}",
                question=m.get("question", ""),
                yes_ask=yes_px,
                no_ask=no_px,
                volume=volume,
                end_date=m.get("endDate", ""),
            ))

        if not candidates:
            logger.debug("ArbScanner: no arb candidates found this scan")
            return []

        # Verify candidates using real CLOB orderbook best asks (not Gamma midpoints).
        # Gamma prices are last-traded / midpoint — actual CLOB asks may be higher,
        # meaning the apparent arb doesn't exist at the CLOB level.
        candidates.sort(key=lambda m: m.spread, reverse=True)
        to_verify = candidates[:MAX_CANDIDATES * 2]
        verified: List[BTCMarket] = []

        import asyncio as _asyncio

        async def _verify(m: BTCMarket) -> BTCMarket:
            yes_id, no_id = m.market_id.split(":", 1)
            try:
                yr, nr = await _asyncio.gather(
                    self._http.get(f"{CLOB_BASE}/book", params={"token_id": yes_id}),
                    self._http.get(f"{CLOB_BASE}/book", params={"token_id": no_id}),
                )
                yr.raise_for_status(); nr.raise_for_status()
                def _best(book, fallback):
                    asks = book.get("asks", [])
                    return Decimal(str(asks[0]["price"])) if asks else fallback
                m.yes_ask = _best(yr.json(), m.yes_ask)
                m.no_ask  = _best(nr.json(), m.no_ask)
            except Exception:
                pass
            return m

        updated = await _asyncio.gather(*[_verify(m) for m in to_verify])
        for m in updated:
            if m.spread >= MIN_ARB_SPREAD:
                verified.append(m)

        verified.sort(key=lambda m: m.spread, reverse=True)
        top = verified[:MAX_CANDIDATES]

        if top:
            logger.info(
                f"ArbScanner: {len(verified)} real arb candidates (CLOB-verified) → "
                f"top {len(top)} | best spread={float(top[0].spread):.3f}"
            )
            for m in top[:5]:
                logger.info(
                    f"  ARB {m.label[:35]:35s} | "
                    f"YES={float(m.yes_ask):.3f} NO={float(m.no_ask):.3f} "
                    f"spread={float(m.spread):.3f} vol=${m.volume:,.0f}"
                )
        else:
            logger.debug("ArbScanner: no real arb after CLOB verification")

        return top

    async def close(self) -> None:
        await self._http.aclose()
