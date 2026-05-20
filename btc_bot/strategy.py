"""
Hybrid BTC arb / market-making / hedging strategy.

Logic:
  1. For each BTC Up/Down market, calculate fair YES probability from
     BTC momentum + base rate.
  2. If market price < fair value - edge_threshold → mispriced → enter.
  3. On entry, immediately try to hedge with the opposite side.
     - If hedge fills → locked arb, net = 1 - entry - hedge - fees
     - If hedge doesn't fill → directional exposure, close if BTC moves our way
  4. Also watch for pure arb: combined ask < (1 - min_spread).
"""
from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Dict, List, Optional

from loguru import logger

from btc_bot.models import BTCMarket, Position, PositionStatus, Side
from btc_bot import state_store


# ── Strategy parameters ───────────────────────────────────────────────────────

EDGE_THRESHOLD   = Decimal("0.02")   # enter when price < fair - 2%
MIN_ARB_SPREAD   = Decimal("0.015")  # pure arb when combined < 0.985 (~0.5% net after fees)
MAX_POSITION_SIZE = Decimal("100")   # $ per position (paper)
FEE_RATE         = Decimal("0.005")  # 0.5% per leg
MAX_OPEN          = 6                # max concurrent open positions
HEDGE_TIMEOUT    = 10.0             # seconds to wait for hedge fill
DIRECTIONAL_STOP = Decimal("0.05")  # close directional if loss > 5%
ARB_COOLDOWN     = 1800             # seconds before re-entering same arb market (30 min)


class BTCStrategy:
    """
    Runs the hybrid strategy. Call `on_price_update()` whenever BTC price
    changes and `on_market_update()` when market prices refresh.
    """

    def __init__(self, paper_trader, position_size: Decimal = MAX_POSITION_SIZE):
        self._paper     = paper_trader
        self._size      = position_size
        self._positions: Dict[str, Position] = {}
        self._btc_price: Optional[float] = None
        self._momentum:  float = 0.0
        # Cooldown: track last arb entry time per market to avoid rapid re-entry
        self._arb_last: Dict[str, float] = {}

        # Load persisted state
        saved = state_store.load()
        self._cum_pnl = Decimal(str(saved.get("cum_pnl", 0)))
        self._trades: List[dict] = saved.get("trades", [])

    # ── Callbacks ─────────────────────────────────────────────────────────────

    def update_btc(self, price: float, momentum: float) -> None:
        self._btc_price = price
        self._momentum  = momentum

    async def evaluate_arb_only(self, market: BTCMarket) -> None:
        """
        Pure arb check for non-BTC markets — no directional logic.
        Only enters if spread >= MIN_ARB_SPREAD and cooldown has elapsed.
        """
        mid = market.market_id
        if mid in self._positions:
            await self._check_position(market)
            return
        if len(self._positions) >= MAX_OPEN:
            return
        # Cooldown: don't re-enter the same market within ARB_COOLDOWN seconds
        last = self._arb_last.get(mid, 0)
        if time.time() - last < ARB_COOLDOWN:
            return
        if market.spread >= MIN_ARB_SPREAD:
            logger.info(
                f"Strategy: ARB (general) '{market.label}' | "
                f"combined={float(market.combined):.4f} spread={float(market.spread):.4f}"
            )
            await self._enter_arb(market)

    async def evaluate_market(self, market: BTCMarket) -> None:
        """Called every poll cycle with fresh market prices."""
        mid = market.market_id

        # Update open positions
        if mid in self._positions:
            await self._check_position(market)
            return

        if len(self._positions) >= MAX_OPEN:
            return

        # ── Pure arb mode ─────────────────────────────────────────────────────
        last = self._arb_last.get(mid, 0)
        if market.spread >= MIN_ARB_SPREAD and time.time() - last >= ARB_COOLDOWN:
            logger.info(
                f"Strategy: ARB on '{market.label}' | "
                f"combined={market.combined:.4f} spread={market.spread:.4f}"
            )
            await self._enter_arb(market)
            return

        # ── Skip near-settled markets (already >90% one way) ─────────────────
        # Buying the cheap side of a near-settled market and hedging locks in
        # a loss once fees are applied (combined ~0.998, fees ~1%).
        if market.yes_ask >= Decimal("0.90") or market.no_ask >= Decimal("0.90"):
            return

        # ── Mispricing mode ───────────────────────────────────────────────────
        fair_yes = self._fair_yes(market)
        fair_no  = Decimal("1") - fair_yes

        yes_edge = fair_yes - market.yes_ask
        no_edge  = fair_no  - market.no_ask

        if yes_edge >= EDGE_THRESHOLD:
            logger.info(
                f"Strategy: YES mispriced on '{market.label}' | "
                f"ask={market.yes_ask:.3f} fair={fair_yes:.3f} edge={yes_edge:.3f}"
            )
            await self._enter_directional(market, Side.YES, market.yes_ask)
        elif no_edge >= EDGE_THRESHOLD:
            logger.info(
                f"Strategy: NO mispriced on '{market.label}' | "
                f"ask={market.no_ask:.3f} fair={fair_no:.3f} edge={no_edge:.3f}"
            )
            await self._enter_directional(market, Side.NO, market.no_ask)

    # ── Fair value ────────────────────────────────────────────────────────────

    def _fair_yes(self, market: BTCMarket) -> Decimal:
        """
        Simple fair value model for BTC Up/Down markets.
        Base = 0.50 (coin flip for short-term direction).
        Adjust ±2% for momentum (strong up trend → slightly favour YES).
        """
        base = Decimal("0.50")
        adj  = Decimal(str(self._momentum * 0.02))  # ±0.02 max
        q = market.question.lower()
        # Invert for "down" / "lower" markets
        if any(w in q for w in ["down", "lower", "below", "fall", "drop"]):
            adj = -adj
        return max(Decimal("0.1"), min(Decimal("0.9"), base + adj))

    # ── Trade execution (paper) ───────────────────────────────────────────────

    async def _enter_arb(self, market: BTCMarket) -> None:
        yes_fill = await self._paper.fill(market.market_id, Side.YES,
                                          market.yes_ask, self._size)
        no_fill  = await self._paper.fill(market.market_id, Side.NO,
                                          market.no_ask,  self._size)
        if yes_fill and no_fill:
            # Record cooldown so we don't re-enter this market for 1 hour
            self._arb_last[market.market_id] = time.time()
            gross = Decimal("1") - market.yes_ask - market.no_ask
            fees  = (market.yes_ask + market.no_ask) * FEE_RATE * 2
            net   = (gross - fees) * self._size
            self._cum_pnl += net
            self._record_trade(market, "arb", net)
            logger.info(
                f"Strategy: ARB FILLED '{market.label}' "
                f"net=+${net:.2f} | cum=${self._cum_pnl:.2f}"
            )

    async def _enter_directional(self, market: BTCMarket,
                                  side: Side, price: Decimal) -> None:
        fill = await self._paper.fill(market.market_id, side, price, self._size)
        if not fill:
            return

        pos = Position(
            market_id=market.market_id,
            question=market.question,
            entry_side=side,
            entry_price=price,
            size=self._size,
        )
        self._positions[market.market_id] = pos

        # Immediately try to hedge with opposite side
        hedge_side  = Side.NO if side == Side.YES else Side.YES
        hedge_price = market.no_ask if hedge_side == Side.NO else market.yes_ask
        hedge_fill  = await self._paper.fill(market.market_id, hedge_side,
                                             hedge_price, self._size)
        if hedge_fill:
            pos.hedge_side  = hedge_side
            pos.hedge_price = hedge_price
            pos.hedge_at    = time.time()
            net = pos.close_arb()
            self._cum_pnl += net
            del self._positions[market.market_id]
            self._record_trade(market, "directional+hedge", net)
            logger.info(
                f"Strategy: HEDGED '{market.label}' "
                f"entry={price:.3f} hedge={hedge_price:.3f} "
                f"net=+${net:.2f} | cum=${self._cum_pnl:.2f}"
            )
        else:
            logger.info(
                f"Strategy: DIRECTIONAL open '{market.label}' "
                f"{side} @ {price:.3f} (hedge pending)"
            )

    async def _check_position(self, market: BTCMarket) -> None:
        """Check if an open directional position can be hedged or stopped."""
        pos = self._positions.get(market.market_id)
        if not pos or pos.is_hedged:
            return

        hedge_side  = Side.NO if pos.entry_side == Side.YES else Side.YES
        hedge_price = market.no_ask if hedge_side == Side.NO else market.yes_ask

        # Try to lock in arb if combined is still favourable
        combined = pos.entry_price + hedge_price
        if combined < (Decimal("1") - FEE_RATE * 2):
            fill = await self._paper.fill(market.market_id, hedge_side,
                                          hedge_price, pos.size)
            if fill:
                pos.hedge_side  = hedge_side
                pos.hedge_price = hedge_price
                net = pos.close_arb()
                self._cum_pnl += net
                del self._positions[market.market_id]
                self._record_trade(market, "delayed_hedge", net)
                logger.info(
                    f"Strategy: HEDGE FILLED '{market.label}' "
                    f"net=${net:.2f} | cum=${self._cum_pnl:.2f}"
                )
                return

        # Stop out if directional loss too large
        current_price = (market.yes_ask if pos.entry_side == Side.YES
                         else market.no_ask)
        loss = (pos.entry_price - current_price) * pos.size
        if loss > DIRECTIONAL_STOP * pos.size:
            net = -loss - (pos.entry_price * FEE_RATE * pos.size)
            self._cum_pnl += net
            pos.status = PositionStatus.CANCELLED
            pos.net_profit = net
            del self._positions[market.market_id]
            self._record_trade(market, "stopped", net)
            logger.warning(
                f"Strategy: STOPPED OUT '{market.label}' "
                f"loss=${net:.2f} | cum=${self._cum_pnl:.2f}"
            )

    def _record_trade(self, market: BTCMarket, trade_type: str,
                      net: Decimal) -> None:
        self._trades.append({
            "time":       time.time(),
            "market":     market.label,
            "type":       trade_type,
            "net":        float(net),
            "cum_pnl":    float(self._cum_pnl),
        })
        state_store.save(self._cum_pnl, self._trades)

    # ── State for dashboard ───────────────────────────────────────────────────

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
