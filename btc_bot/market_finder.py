"""
Finds active BTC Up/Down binary markets on Polymarket.
Uses the Gamma API — no auth required.

Scoring logic:
  - Prefer markets expiring within 4–48 hours (most edge, market makers slow to reprice)
  - Require minimum volume ($1k) so there's real liquidity
  - Sort by composite score: nearness-to-expiry * volume
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional

import httpx
from loguru import logger

from btc_bot.models import BTCMarket

GAMMA_BASE = "https://gamma-api.polymarket.com"
CLOB_BASE  = "https://clob.polymarket.com"

# Keywords that identify BTC direction markets
BTC_KEYWORDS = [
    "bitcoin", "btc",
]

# Require at least one of these alongside a BTC keyword
BTC_DIRECTION_WORDS = [
    "price", "above", "below", "higher", "lower", "hit", "reach",
    "exceed", "end", "close", "up", "down", "worth", "over", "under",
    "between", "stay", "remain", "cross", "break", "surpass",
]

# Market selection parameters
MIN_VOLUME         = 50.0      # very low floor — catch thin BTC markets too
MAX_MARKETS        = 25        # track more markets for more trading opportunities
PREFER_EXPIRY_HRS  = (1, 720)  # wider window: 1h–30 days (catch monthly markets)


def _hours_to_expiry(end_date: str) -> Optional[float]:
    """Return hours until market expiry, or None if unparseable."""
    if not end_date:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(end_date, fmt).replace(tzinfo=timezone.utc)
            delta = (dt - datetime.now(timezone.utc)).total_seconds() / 3600
            return max(0.0, delta)
        except ValueError:
            continue
    return None


def _score(market: BTCMarket) -> float:
    """
    Composite score: higher = better trading opportunity.
    Near-expiry markets (1–72h) score highest because:
      - Prices should be near 0 or 1 as resolution approaches
      - Market makers are slowest to update these
      - Arb / directional edges are most reliable
    """
    hours = _hours_to_expiry(market.end_date)
    if hours is None:
        hours = 48.0  # unknown → treat as mid-range

    lo, hi = PREFER_EXPIRY_HRS
    if hours < lo:
        # Too close — might already be settling
        expiry_score = 0.1
    elif hours <= hi:
        # Sweet spot — linear peak at lo
        expiry_score = 1.0 - (hours - lo) / (hi - lo) * 0.7
    else:
        # Far out — lower edge
        expiry_score = 0.3

    # Normalise volume to 0–1 on a log scale (cap at $1M)
    vol_score = min(1.0, (market.volume / 1_000_000) ** 0.5)

    return expiry_score * 0.7 + vol_score * 0.3


class MarketFinder:
    def __init__(self, timeout: float = 10.0):
        self._http = httpx.AsyncClient(timeout=timeout)

    async def find_btc_markets(self, limit: int = 500) -> List[BTCMarket]:
        """Return top BTC Up/Down markets scored by expiry proximity + volume."""
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
            markets_raw = resp.json()
        except Exception as exc:
            logger.warning(f"MarketFinder: Gamma fetch error: {exc}")
            return []

        results: List[BTCMarket] = []
        for m in markets_raw:
            question = m.get("question", "").lower()
            # Must mention bitcoin/btc AND a direction/price word
            has_btc = any(kw in question for kw in BTC_KEYWORDS)
            has_dir = any(w in question for w in BTC_DIRECTION_WORDS)
            if not (has_btc and has_dir):
                continue

            raw_tok = m.get("clobTokenIds") or []
            tokens  = json.loads(raw_tok) if isinstance(raw_tok, str) else raw_tok
            if len(tokens) < 2:
                continue

            volume = float(m.get("volume") or 0)
            if volume < MIN_VOLUME:
                continue

            raw_px = m.get("outcomePrices") or ["0.5", "0.5"]
            prices = json.loads(raw_px) if isinstance(raw_px, str) else raw_px
            yes_px = Decimal(str(prices[0])) if prices else Decimal("0.5")
            no_px  = Decimal(str(prices[1])) if len(prices) > 1 else Decimal("0.5")

            end_date = m.get("endDate", "")
            hours    = _hours_to_expiry(end_date)

            # Skip markets that have already expired
            if hours is not None and hours <= 0:
                continue

            results.append(BTCMarket(
                market_id=f"{tokens[0]}:{tokens[1]}",
                question=m.get("question", ""),
                yes_ask=yes_px,
                no_ask=no_px,
                volume=volume,
                end_date=end_date,
            ))

        # Sort by composite score, take top N
        results.sort(key=_score, reverse=True)
        top = results[:MAX_MARKETS]

        logger.info(
            f"MarketFinder: {len(results)} BTC markets found → "
            f"top {len(top)} selected"
        )
        for m in top:
            h = _hours_to_expiry(m.end_date)
            hrs_str = f"{h:.1f}h" if h is not None else "?"
            logger.debug(
                f"  {m.label[:30]:30s} | vol=${m.volume:,.0f} | "
                f"expiry={hrs_str} | score={_score(m):.2f}"
            )
        return top

    async def refresh_prices(self, market: BTCMarket) -> BTCMarket:
        """Fetch live CLOB ask prices for YES and NO tokens."""
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
