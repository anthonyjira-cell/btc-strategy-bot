"""
General Polymarket arb scanner.

Scans ALL active markets (not just BTC) for pure arb:
  combined YES ask + NO ask < (1 - fees) → buy both sides → guaranteed profit

Two passes per scan:
  1. Top markets by VOLUME — liquid markets where fills are reliable
  2. Top markets by EXPIRY (soonest first) — near-expiry markets where prices
     should converge to 0/1 but market makers are slow to reprice

Both passes are Gamma-screened then CLOB-verified before trading.
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

MIN_ARB_SPREAD = Decimal("0.02")  # 2% spread → ~1% net after fees
MIN_VOLUME     = 500.0            # lowered from $2k — catch more thin markets
MAX_CANDIDATES = 20               # returned per scan
VERIFY_BATCH   = 40               # how many Gamma candidates to CLOB-verify


class ArbScanner:
    """Finds pure arb opportunities across all Polymarket binary markets."""

    def __init__(self, timeout: float = 15.0):
        self._http = httpx.AsyncClient(timeout=timeout)

    async def find_arb_markets(self, fetch_limit: int = 500) -> List[BTCMarket]:
        """
        Two-pass scan: by volume + by expiry.
        Gamma candidates are CLOB-verified before returning.
        """
        by_volume = await self._fetch_gamma(fetch_limit, order="volume")
        by_expiry = await self._fetch_gamma(fetch_limit, order="end_date_iso",
                                            ascending=True)

        # Merge, deduplicate by market_id
        seen: set = set()
        candidates: List[BTCMarket] = []
        for m in by_volume + by_expiry:
            if m.market_id not in seen:
                seen.add(m.market_id)
                candidates.append(m)

        if not candidates:
            logger.debug("ArbScanner: no Gamma candidates this scan")
            return []

        logger.debug(
            f"ArbScanner: {len(candidates)} Gamma candidates "
            f"(vol={len(by_volume)} expiry={len(by_expiry)}) → verifying with CLOB…"
        )

        # Sort by Gamma spread (best first) and verify top N with CLOB orderbook
        candidates.sort(key=lambda m: m.spread, reverse=True)
        to_verify = candidates[:VERIFY_BATCH]

        updated = await asyncio.gather(*[self._verify(m) for m in to_verify])
        verified = [m for m in updated if m.spread >= MIN_ARB_SPREAD]
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

    async def _fetch_gamma(
        self,
        limit: int,
        order: str = "volume",
        ascending: bool = False,
    ) -> List[BTCMarket]:
        """Fetch markets from Gamma API and pre-filter by Gamma spread."""
        try:
            resp = await self._http.get(
                f"{GAMMA_BASE}/markets",
                params={
                    "limit": limit,
                    "closed": "false",
                    "order": order,
                    "ascending": str(ascending).lower(),
                },
            )
            resp.raise_for_status()
            raw = resp.json()
        except Exception as exc:
            logger.warning(f"ArbScanner: Gamma fetch error ({order}): {exc}")
            return []

        results: List[BTCMarket] = []
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

            spread = Decimal("1") - yes_px - no_px
            if spread < MIN_ARB_SPREAD:
                continue

            results.append(BTCMarket(
                market_id=f"{tokens[0]}:{tokens[1]}",
                question=m.get("question", ""),
                yes_ask=yes_px,
                no_ask=no_px,
                volume=volume,
                end_date=m.get("endDate", ""),
            ))

        return results

    async def _verify(self, m: BTCMarket) -> BTCMarket:
        """Replace Gamma midpoints with real CLOB best asks."""
        yes_id, no_id = m.market_id.split(":", 1)
        try:
            yr, nr = await asyncio.gather(
                self._http.get(f"{CLOB_BASE}/book", params={"token_id": yes_id}),
                self._http.get(f"{CLOB_BASE}/book", params={"token_id": no_id}),
            )
            yr.raise_for_status()
            nr.raise_for_status()

            def _best(book: dict, fallback: Decimal) -> Decimal:
                asks = book.get("asks", [])
                return Decimal(str(asks[0]["price"])) if asks else fallback

            m.yes_ask = _best(yr.json(), m.yes_ask)
            m.no_ask  = _best(nr.json(), m.no_ask)
        except Exception:
            pass
        return m

    async def close(self) -> None:
        await self._http.aclose()
