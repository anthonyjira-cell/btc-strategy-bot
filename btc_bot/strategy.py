"""
Pure arb strategy — ONLY enters when YES ask + NO ask < 0.97 on the CLOB.

No directional bets. No fair-value assumptions. Only locked-in profit.

Arb condition:
  yes_ask + no_ask < (1 - FEE_RATE * 2)
  → buy both sides → guaranteed $1 payout → net profit = spread - fees

P&L is only recorded when BOTH legs confirm a fill. Never on order submission.
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger

from btc_bot.models import BTCMarket, Position, PositionStatus, Side
from btc_bot import state_store


# ── Strategy parameters ───────────────────────────────────────────────────────
MIN_ARB_SPREAD = Decimal("0.02")   # need 2%+ spread (fees ~1%, want 1%+ net profit)
FEE_RATE       = Decimal("0.005")  # 0.5% per leg
MAX_OPEN       = 6                 # max concurrent open positions
ARB_COOLDOWN   = 3600              # seconds before re-entering same market
NEAR_SETTLED   = Decimal("0.03")   # skip markets where one side < 3%


class BTCStrategy:
    """
    Pure arb strategy. Only enters when combined CLOB asks < 0.97.
    Both legs are placed at the current best ask — taker orders that fill immediately.
    P&L is only counted when both fills are confirmed.
    """

    def __init__(self, trader, position_size: Decimal = Decimal("15")):
        self._trader = trader
        self._size   = position_size
        self._positions: Dict[str, Position] = {}
        self._last_entry: Dict[str, float]   = {}

        # BTC price (kept for dashboard compatibility — not used in trading logic)
        self._btc_price: Optional[float] = None
        self._momentum:  float = 0.0

        saved = state_store.load()
        self._cum_pnl = Decimal(str(saved.get("cum_pnl", 0)))
        self._trades: List[dict] = saved.get("trades", [])

    def update_btc(self, price: float, momentum: float) -> None:
        self._btc_price = price
        self._momentum  = momentum

    def _on_cooldown(self, market_id: str) -> bool:
        return time.time() - self._last_entry.get(market_id, 0) < ARB_COOLDOWN

    def _record_entry(self, market_id: str) -> None:
        self._last_entry[market_id] = time.time()

    def _has_arb(self, market: BTCMarket) -> bool:
        """True only when CLOB best asks leave a real profit after fees."""
        if market.yes_ask < NEAR_SETTLED or market.no_ask < NEAR_SETTLED:
            return False
        return market.spread >= MIN_ARB_SPREAD

    async def evaluate_market(self, market: BTCMarket) -> None:
        """Called every poll cycle. Only acts on genuine arb."""
        mid = market.market_id
        if mid in self._positions:
            return  # position already open, wait for it to settle
        if len(self._positions) >= MAX_OPEN:
            return
        if self._on_cooldown(mid):
            return
        if self._has_arb(market):
            logger.info(
                f"Strategy: ARB '{market.label}' | "
                f"YES={float(market.yes_ask):.3f} NO={float(market.no_ask):.3f} "
                f"combined={float(market.combined):.3f} spread={float(market.spread):.3f}"
            )
            await self._enter_arb(market)

    async def evaluate_arb_only(self, market: BTCMarket) -> None:
        """Same logic — used by the general arb scanner loop."""
        await self.evaluate_market(market)

    async def _enter_arb(self, market: BTCMarket) -> None:
        """
        Place both legs at current CLOB best asks.
        P&L is only recorded if BOTH fills return non-None.
        """
        yes_fill = await self._trader.fill(
            market.market_id, Side.YES, market.yes_ask, self._size
        )
        no_fill = await self._trader.fill(
            market.market_id, Side.NO, market.no_ask, self._size
        )

        if yes_fill is None or no_fill is None:
            # One or both legs failed — log which one, don't book any profit
            if yes_fill is not None:
                logger.warning(
                    f"Strategy: YES filled but NO failed on '{market.label}' "
                    f"— unhedged YES position at {yes_fill:.3f}"
                )
            elif no_fill is not None:
                logger.warning(
                    f"Strategy: NO filled but YES failed on '{market.label}' "
                    f"— unhedged NO position at {no_fill:.3f}"
                )
            else:
                logger.debug(f"Strategy: both legs failed for '{market.label}'")
            return

        # Both legs filled — book the real profit
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

    def _record_trade(self, market: BTCMarket, trade_type: str, net: Decimal) -> None:
        self._trades.append({
            "time":    time.time(),
            "market":  market.label,
            "type":    trade_type,
            "net":     float(net),
            "cum_pnl": float(self._cum_pnl),
        })
        state_store.save(self._cum_pnl, self._trades)

    # ── Dashboard properties ──────────────────────────────────────────────────

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
