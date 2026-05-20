"""
General Polymarket arb scanner.

Two-stage approach:
  Stage 1 — Gamma API: pre-filter markets where outcomePrices (midpoints)
             combined < PRE_FILTER_COMBINED.  Eliminates ~98% of markets fast.
  Stage 2 — CLOB API: fetch real ask prices for surviving candidates.
             Only markets where YES ask + NO ask < (1 - MIN_ARB_SPREAD) are
             returned as genuine arb opportunities.

This catches real arb even when Gamma midpoints look efficient, and avoids
fake paper profits from midpoint-only filtering.
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

# Stage-1 Gamma pre-filter — only CLOB-verify markets whose midpoints already
# suggest slack.  Set generously (0.995) so we don't miss edge cases where
# mid < ask but combined mids still close to 1.
PRE_FILTER_COMBINED = Decimal("0.995")

# Stage-2 threshold — actual CLOB ask prices must beat this to be tradeable
MIN_ARB_SPREAD  = Decimal("0.01")   # 1%+ net after fees
MIN_VOLUME      = 100.0             # very low floor — thin markets can arb too
MAX_CLOB_CHECKS = 150               # max concurrent CLOB verifications
MAX_CANDIDATES  = 50                # final result cap
CLOB_CONCURRENCY = 15              # simultaneous CLOB request pairs


class ArbScanner:
    """Finds pure arb opportunities across all Polymarket binary markets."""

    def __init__(self, timeout: float = 12.0):
        self._http = httpx.AsyncClient(timeout=timeout)

    async def find_arb_markets(self, fetch_limit: int = 2000) -> List[BTCMarket]:
        """
        Return markets where actual CLOB YES ask + NO ask < (1 - MIN_ARB_SPREAD).
        Sorted by spread descending (best arbs first).
        """
        # ── Stage 1: Gamma bulk fetch ─────────────────────────────────────────
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

        pre_candidates: List[BTCMarket] = []

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

            # Stage-1 pre-filter: midpoints must leave some room
            combined = yes_px + no_px
            if combined >= PRE_FILTER_COMBINED:
                continue

            pre_candidates.append(BTCMarket(
                market_id=f"{tokens[0]}:{tokens[1]}",
                question=m.get("question", ""),
                yes_ask=yes_px,
                no_ask=no_px,
                volume=volume,
                end_date=m.get("endDate", ""),
            ))

        # Sort by Gamma spread, take best MAX_CLOB_CHECKS for CLOB verification
        pre_candidates.sort(key=lambda m: m.spread, reverse=True)
        to_check = pre_candidates[:MAX_CLOB_CHECKS]

        logger.debug(
            f"ArbScanner: {len(pre_candidates)} Gamma pre-candidates "
            f"(combined<{PRE_FILTER_COMBINED}) → CLOB-checking top {len(to_check)}"
        )

        if not to_check:
            logger.debug("ArbScanner: no pre-candidates from Gamma — efficient market conditions")
            return []

        # ── Stage 2: CLOB ask-price verification ─────────────────────────────
        sem = asyncio.Semaphore(CLOB_CONCURRENCY)

        async def verify(market: BTCMarket) -> BTCMarket:
            async with sem:
                yes_id, no_id = market.market_id.split(":", 1)
                try:
                    yes_r, no_r = await asyncio.gather(
                        self._http.get(f"{CLOB_BASE}/price",
                                       params={"token_id": yes_id, "side": "buy"}),
                        self._http.get(f"{CLOB_BASE}/price",
                                       params={"token_id": no_id,  "side": "buy"}),
                    )
                    yes_r.raise_for_status()
                    no_r.raise_for_status()
                    market.yes_ask = Decimal(str(yes_r.json().get("price", market.yes_ask)))
                    market.no_ask  = Decimal(str(no_r.json().get("price",  market.no_ask)))
                except Exception as exc:
                    logger.debug(f"ArbScanner: CLOB price error for {market.label}: {exc}")
            return market

        verified = await asyncio.gather(*[verify(m) for m in to_check])

        # ── Filter by real CLOB spread ────────────────────────────────────────
        candidates = [m for m in verified if m.spread >= MIN_ARB_SPREAD]
        candidates.sort(key=lambda m: m.spread, reverse=True)
        top = candidates[:MAX_CANDIDATES]

        if top:
            logger.info(
                f"ArbScanner: {len(candidates)} CLOB-verified arb markets → "
                f"top {len(top)} | best spread={float(top[0].spread):.3f}"
            )
            for m in top[:5]:
                logger.info(
                    f"  ARB {m.label[:35]:35s} | "
                    f"YES={float(m.yes_ask):.3f} NO={float(m.no_ask):.3f} "
                    f"spread={float(m.spread):.3f} vol=${m.volume:,.0f}"
                )
        else:
            logger.info(
                f"ArbScanner: {len(to_check)} CLOB checks — "
                f"no arb above {float(MIN_ARB_SPREAD):.1%} right now"
            )

        return top

    async def close(self) -> None:
        await self._http.aclose()
