"""
Two-mode strategy:

1. PURE ARB — YES ask + NO ask < 0.98 → buy both sides → guaranteed profit
   regardless of outcome. Only fires when a real CLOB arb exists.

2. NEAR-EXPIRY BTC DIRECTIONAL — For markets expiring within 36 hours,
   parse the strike price from the question, compute a fair probability
   using a log-normal BTC volatility model, and buy the winning side
   when the market is clearly mispricing it.

   Example: BTC=$77,900, strike=$75,000, 3h to expiry
   → model says YES worth 0.995, market prices it at 0.82 → buy YES

   This uses the bot's real edge: knowing BTC's actual current price
   vs what Polymarket traders have priced in.
"""
from __future__ import annotations

import math
import re
import time
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger

from btc_bot.models import BTCMarket, Side
from btc_bot import state_store


# ── Strategy parameters ───────────────────────────────────────────────────────

# Pure arb
MIN_ARB_SPREAD = Decimal("0.02")   # 2% spread → ~1% net after fees
FEE_RATE       = Decimal("0.005")  # 0.5% per leg

# Directional BTC bets
MIN_FAIR_VALUE   = 0.88   # model must say 88%+ probability of winning
MIN_EDGE         = 0.06   # market must be at least 6% below fair value
MAX_HOURS_EXPIRY = 36     # only look at markets expiring within 36h
BTC_ANNUAL_VOL   = 0.80   # 80% annualised vol (conservative for BTC)

# General
MAX_OPEN     = 6
ARB_COOLDOWN = 3600
NEAR_SETTLED = Decimal("0.03")


# ── Probability model ─────────────────────────────────────────────────────────

def _normal_cdf(x: float) -> float:
    """Standard normal CDF via Abramowitz & Stegun approximation."""
    t = 1.0 / (1.0 + 0.2316419 * abs(x))
    p = (0.319381530
         + t * (-0.356563782
         + t * (1.781477937
         + t * (-1.821255978
         + t * 1.330274429))))
    prob = 1.0 - (math.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)) * t * p
    return prob if x >= 0 else 1.0 - prob


def _fair_yes(btc_price: float, strike: float,
              hours: float, bullish: bool) -> float:
    """
    Probability that BTC stays above (bullish) or below (bearish) the strike
    at expiry, using a log-normal model.

    At 80% annual vol, 1h σ ≈ 0.85%, 4h ≈ 1.7%, 24h ≈ 4.2%.
    """
    if hours <= 0:
        return 1.0 if ((btc_price >= strike) == bullish) else 0.0
    sigma_t = BTC_ANNUAL_VOL * math.sqrt(hours / 8_760)
    if sigma_t < 1e-9:
        return 1.0 if ((btc_price >= strike) == bullish) else 0.0
    z = math.log(btc_price / strike) / sigma_t
    return _normal_cdf(z) if bullish else _normal_cdf(-z)


# ── Question parser ───────────────────────────────────────────────────────────

_PRICE_PATTERNS = [
    (re.compile(r'\$([0-9]{1,3}(?:,[0-9]{3})+(?:\.[0-9]+)?)'),  1),    # $80,000
    (re.compile(r'\$([0-9]+(?:\.[0-9]+)?)[kK]\b'),               1_000), # $80k
    (re.compile(r'\$([0-9]{4,7}(?:\.[0-9]+)?)'),                 1),    # $80000
]

_BEARISH_WORDS = [
    "below", "under", "lower", "fall", "drop",
    "less than", "not reach", "not hit", "not exceed",
]


def _parse_strike(question: str) -> Optional[float]:
    """Extract the first BTC-range price from a market question."""
    for pat, mult in _PRICE_PATTERNS:
        m = pat.search(question)
        if m:
            try:
                val = float(m.group(1).replace(",", "")) * mult
                if 1_000 < val < 10_000_000:   # sanity: plausible BTC range
                    return val
            except ValueError:
                pass
    return None


def _is_bullish(question: str) -> bool:
    """YES = BTC going UP unless the question uses bearish language."""
    q = question.lower()
    return not any(w in q for w in _BEARISH_WORDS)


# ── Strategy ──────────────────────────────────────────────────────────────────

class BTCStrategy:
    def __init__(self, trader, position_size: Decimal = Decimal("15")):
        self._trader    = trader
        self._size      = position_size
        self._btc_price: Optional[float] = None
        self._momentum:  float = 0.0
        self._last_entry: Dict[str, float] = {}
        self._positions: Dict[str, dict]   = {}   # market_id → position info

        saved = state_store.load()
        self._cum_pnl = Decimal(str(saved.get("cum_pnl", 0)))
        self._trades: List[dict] = saved.get("trades", [])

    def update_btc(self, price: float, momentum: float) -> None:
        self._btc_price = price
        self._momentum  = momentum

    def _on_cooldown(self, mid: str) -> bool:
        return time.time() - self._last_entry.get(mid, 0) < ARB_COOLDOWN

    def _record_entry(self, mid: str) -> None:
        self._last_entry[mid] = time.time()

    # ── Market evaluation ─────────────────────────────────────────────────────

    async def evaluate_market(self, market: BTCMarket) -> None:
        """Called every 30s with refreshed CLOB prices."""
        mid = market.market_id
        if mid in self._positions or self._on_cooldown(mid):
            return
        if len(self._positions) >= MAX_OPEN:
            return

        # Mode 1: pure arb
        if (market.yes_ask > NEAR_SETTLED and market.no_ask > NEAR_SETTLED
                and market.spread >= MIN_ARB_SPREAD):
            logger.info(
                f"Strategy: ARB '{market.label}' | "
                f"YES={float(market.yes_ask):.3f} NO={float(market.no_ask):.3f} "
                f"spread={float(market.spread):.3f}"
            )
            await self._enter_arb(market)
            return

        # Mode 2: near-expiry BTC directional
        await self._evaluate_directional(market)

    async def evaluate_arb_only(self, market: BTCMarket) -> None:
        """Used by the general arb scanner — pure arb only, no directional."""
        mid = market.market_id
        if mid in self._positions or self._on_cooldown(mid):
            return
        if len(self._positions) >= MAX_OPEN:
            return
        if (market.yes_ask > NEAR_SETTLED and market.no_ask > NEAR_SETTLED
                and market.spread >= MIN_ARB_SPREAD):
            logger.info(
                f"Strategy: ARB (scanner) '{market.label}' | "
                f"spread={float(market.spread):.3f}"
            )
            await self._enter_arb(market)

    async def _evaluate_directional(self, market: BTCMarket) -> None:
        """
        Buy the high-probability side when the model says 88%+ and the
        market is pricing it 6%+ below fair value.
        """
        if self._btc_price is None:
            return

        # Only near-expiry markets
        from btc_bot.market_finder import _hours_to_expiry
        hours = _hours_to_expiry(market.end_date)
        if hours is None or hours > MAX_HOURS_EXPIRY or hours <= 0:
            return

        strike = _parse_strike(market.question)
        if strike is None:
            return

        bullish = _is_bullish(market.question)
        fair    = _fair_yes(self._btc_price, strike, hours, bullish)

        if fair < MIN_FAIR_VALUE:
            return

        # Decide which side to buy
        if bullish:
            side       = Side.YES
            mkt_price  = market.yes_ask
        else:
            side       = Side.NO
            mkt_price  = market.no_ask

        edge = fair - float(mkt_price)
        if edge < MIN_EDGE:
            return

        logger.info(
            f"Strategy: DIRECTIONAL '{market.label}' | "
            f"BTC=${self._btc_price:,.0f} strike=${strike:,.0f} "
            f"hours={hours:.1f} fair={fair:.3f} mkt={float(mkt_price):.3f} "
            f"edge={edge:.3f}"
        )
        await self._enter_directional(market, side, mkt_price, fair)

    # ── Trade execution ───────────────────────────────────────────────────────

    async def _enter_arb(self, market: BTCMarket) -> None:
        yes_fill = await self._trader.fill(
            market.market_id, Side.YES, market.yes_ask, self._size
        )
        no_fill = await self._trader.fill(
            market.market_id, Side.NO, market.no_ask, self._size
        )

        if yes_fill is None or no_fill is None:
            if yes_fill is not None:
                logger.warning(
                    f"Strategy: YES filled but NO failed '{market.label}' — partial"
                )
            elif no_fill is not None:
                logger.warning(
                    f"Strategy: NO filled but YES failed '{market.label}' — partial"
                )
            return

        self._record_entry(market.market_id)
        gross = Decimal("1") - yes_fill - no_fill
        fees  = (yes_fill + no_fill) * FEE_RATE * 2
        net   = (gross - fees) * self._size
        self._cum_pnl += net
        self._record_trade(market, "arb", net)
        logger.info(
            f"Strategy: ✅ ARB FILLED '{market.label}' "
            f"YES@{yes_fill:.3f} NO@{no_fill:.3f} "
            f"net=${net:.2f} | cum=${self._cum_pnl:.2f}"
        )

    async def _enter_directional(
        self,
        market: BTCMarket,
        side: Side,
        mkt_price: Decimal,
        fair: float,
    ) -> None:
        fill = await self._trader.fill(
            market.market_id, side, mkt_price, self._size
        )
        if fill is None:
            return

        self._record_entry(market.market_id)
        self._positions[market.market_id] = {
            "side":  side,
            "price": fill,
            "fair":  fair,
            "label": market.label,
        }

        # Expected profit = (fair_value - fill_price) * size
        expected = (Decimal(str(fair)) - fill) * self._size
        self._record_trade(market, f"directional_{side.value}", expected)
        logger.info(
            f"Strategy: ✅ DIRECTIONAL {side.value} '{market.label}' "
            f"@ {fill:.3f} (fair={fair:.3f}) "
            f"expected=${expected:.2f} | cum=${self._cum_pnl:.2f}"
        )

    def _record_trade(self, market: BTCMarket, trade_type: str,
                      net: Decimal) -> None:
        self._trades.append({
            "time":    time.time(),
            "market":  market.label,
            "type":    trade_type,
            "net":     float(net),
            "cum_pnl": float(self._cum_pnl),
        })
        state_store.save(self._cum_pnl, self._trades)

    # ── Dashboard ─────────────────────────────────────────────────────────────

    @property
    def cumulative_pnl(self) -> Decimal:
        return self._cum_pnl

    @property
    def open_positions(self) -> int:
        return len(self._positions)

    @property
    def trade_count(self) -> int:
        return len(self._trades)

    @property
    def recent_trades(self) -> list:
        return self._trades[-10:]
