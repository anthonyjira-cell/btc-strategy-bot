"""
Three-engine trading strategy for Polymarket 5-minute BTC Up/Down binaries.

ENGINE 1 — DISLOCATION
  BTC moves >0.05% intra-window but the token price hasn't adjusted.
  Fair probability formula (from the original quant post):
    fair_prob = 0.5 + (|Δbtc%| / minutes_remaining) × 5.0
  Requires: edge >2% AND 10-min BTC trend agrees with direction.

ENGINE 2 — DIRECTIONAL (final 30 seconds)
  Composite confidence ≥0.45 AND BTC has confirmed direction by >0.03%.
  High-probability end-of-window confirmation trade.

ENGINE 3 — PURE ARB (bonus)
  UP ask + DOWN ask < 0.98 → buy both sides, guaranteed profit.
  Runs on all Polymarket markets via ArbScanner.

Position sizing: Kelly formula capped at 25% of bankroll.
  kelly = edge / (1 − token_price), max = bankroll × 0.25
"""
from __future__ import annotations

import json
import os
import time
from decimal import Decimal
from typing import Dict, List, Optional

import httpx
from loguru import logger

from btc_bot.btc_binary_finder import BinaryWindow
from btc_bot.models import BTCMarket, Side
from btc_bot import state_store

GAMMA_BASE = "https://gamma-api.polymarket.com"


# ── Parameters ────────────────────────────────────────────────────────────────

# Dislocation engine
DISLOC_MIN_BTC_MOVE   = 0.05   # % BTC must move to trigger dislocation check
DISLOC_MIN_EDGE       = 0.06   # token must be 6%+ below fair value
DISLOC_MIN_TOKEN      = 0.33   # don't buy tokens below this — market already moved too far
DISLOC_MAX_TOKEN      = 0.55   # don't buy tokens above this — break-even too high (need 55%+ win rate)
DISLOC_MAX_BET        = Decimal("2.50")   # cap dislocation bets — preserving $29 balance

# Directional engine (end of window)
# Only fires when the token is STILL cheap despite BTC having clearly moved.
# At 0.70+ the market has already priced the move — risk/reward is terrible.
DIRECT_SECONDS_LEFT   = 30     # only fire in final 30s
DIRECT_MIN_CONFIDENCE = 0.45   # composite_confidence = fair_prob - 0.5
DIRECT_BTC_CONFIRM    = 0.05   # % BTC must confirm direction (raised from 0.03)
DIRECT_MIN_TOKEN      = 0.33   # don't buy tokens below this — market has already moved too far
DIRECT_MAX_TOKEN      = 0.65   # skip if token already priced in (>0.65)
DIRECT_MIN_EDGE       = 0.10   # require 10%+ edge — final 30s formula is noisy
DIRECT_MAX_BET        = Decimal("2.00")   # cap directional bets

# Loss cooldown — skip N windows after a loss to avoid chasing reversals
LOSS_COOLDOWN_WINDOWS = 2      # wait 2 windows (10 min) after any loss before trading again

# Pure arb
ARB_MIN_SPREAD = Decimal("0.02")
FEE_RATE       = Decimal("0.005")

# General
MAX_OPEN     = 4
ARB_COOLDOWN = 300   # 5-min cooldown (= 1 window) between arb trades on same market
NEAR_SETTLED = Decimal("0.03")


# ── Kelly sizing ──────────────────────────────────────────────────────────────

def kelly_size(edge: float, token_price: float, bankroll: float) -> Decimal:
    """
    Fractional Kelly bet size, capped at 25% of bankroll.
    edge = fair_prob - token_price
    kelly_fraction = edge / (1 - token_price)
    """
    if edge <= 0 or token_price >= 0.99:
        return Decimal("0")
    fraction = edge / (1.0 - token_price)
    dollars   = fraction * bankroll
    capped    = min(dollars, bankroll * 0.25)
    return Decimal(str(round(max(1.0, capped), 2)))


# ── Dislocation fair-value formula ────────────────────────────────────────────

def dislocation_fair_prob(delta_pct: float, minutes_remaining: float) -> float:
    """
    Returns fair probability for the WINNING side (the direction BTC has moved).
    delta_pct: absolute % BTC has moved from window open (always positive)
    minutes_remaining: how many minutes left in the 5-min window
    """
    if minutes_remaining <= 0:
        return 1.0
    raw = 0.5 + (delta_pct / minutes_remaining) * 5.0
    return max(0.0, min(1.0, raw))


class BTCStrategy:
    def __init__(self, trader, bankroll: float = 99.0,
                 position_size: Decimal = Decimal("15")):
        # Allow runtime bankroll override via env var (e.g. CURRENT_BANKROLL=30)
        env_bankroll = os.environ.get("CURRENT_BANKROLL", "")
        if env_bankroll:
            try:
                bankroll = float(env_bankroll)
            except ValueError:
                pass
        self._trader   = trader
        self._bankroll = bankroll
        self._size     = position_size   # fallback fixed size

        # BTC state
        self._btc_price:    Optional[float] = None
        self._momentum:     float = 0.0
        self._window_open:  Optional[float] = None   # BTC at window start
        self._window_id:    int = 0                  # current window timestamp

        # Trade state
        self._traded_window: int = 0   # last window we traded in (avoid doubles)
        self._last_arb:  Dict[str, float] = {}
        self._positions: Dict[str, dict]  = {}

        # Session tracking (resets each boot — reflects current session only)
        self._session_pnl:    Decimal = Decimal("0")
        self._session_wins:   int = 0
        self._session_losses: int = 0
        self._session_start:  float = time.time()

        # Persistence
        saved = state_store.load()
        self._cum_pnl  = Decimal(str(saved.get("cum_pnl", 0)))
        self._trades:  List[dict] = saved.get("trades", [])
        # Pending positions waiting for settlement — keyed by window slug
        self._pending: Dict[str, dict] = saved.get("pending", {})
        # Loss cooldown — persisted so it survives redeploys
        self._loss_cooldown_until: int = saved.get("loss_cooldown_until", 0)
        # Write state immediately on startup so volume file exists from boot
        state_store.save(self._cum_pnl, self._trades, self._pending,
                         self._loss_cooldown_until)

    # ── State updates ─────────────────────────────────────────────────────────

    def update_btc(self, price: float, momentum: float) -> None:
        self._btc_price = price
        self._momentum  = momentum

    def on_window_start(self, window_start: int) -> None:
        """Called when a new 5-min window begins. Record opening BTC price."""
        if window_start == self._window_id:
            return
        self._window_id   = window_start
        self._window_open = self._btc_price

        # If we're joining a window late (>120s elapsed), skip trading it.
        # We don't know the real opening price, so delta calculations are wrong.
        import time as _time
        elapsed = _time.time() - window_start
        if elapsed > 120:
            self._traded_window = window_start   # block this window
            logger.info(
                f"Strategy: ⏭️  late join window {window_start} "
                f"({elapsed:.0f}s elapsed) — waiting for next fresh window"
            )
        else:
            logger.info(
                f"Strategy: 🕐 new window | BTC open=${self._btc_price:,.0f}"
                if self._btc_price else "Strategy: 🕐 new window"
            )

    # ── Engine 1: Dislocation ─────────────────────────────────────────────────

    async def evaluate_binary_window(self, window: BinaryWindow) -> None:
        """
        Main entry point called every ~10s with fresh window data.
        Runs dislocation check. Directional check runs separately at end of window.
        """
        self.on_window_start(window.window_start)

        if self._btc_price is None:
            return
        # Late-set window_open if BTC price wasn't available when window started
        if self._window_open is None:
            self._window_open = self._btc_price
            logger.debug(f"Strategy: late-set window_open=${self._btc_price:,.0f}")
        if self._traded_window == window.window_start:
            return   # already traded this window
        if self._pending:
            logger.debug(f"Strategy: {len(self._pending)} pending position(s) — waiting for settlement")
            return   # one trade at a time — wait for previous to settle
        if len(self._positions) >= MAX_OPEN:
            return
        if window.window_start <= self._loss_cooldown_until:
            logger.debug(f"Strategy: loss cooldown active — skipping window")
            return

        delta_pct = (self._btc_price - self._window_open) / self._window_open * 100
        btc_up    = delta_pct > 0
        minutes_left = window.minutes_remaining

        logger.debug(
            f"Strategy: Δbtc={delta_pct:+.3f}% "
            f"{'UP' if btc_up else 'DOWN'} | "
            f"UP@{float(window.up_ask):.3f} DOWN@{float(window.down_ask):.3f} | "
            f"{minutes_left:.1f}min left"
        )

        # Need minimum move to trigger
        if abs(delta_pct) < DISLOC_MIN_BTC_MOVE:
            return

        minutes_left = window.minutes_remaining
        if minutes_left <= 0.5:   # too close to expiry for dislocation (use directional)
            return

        fair = dislocation_fair_prob(abs(delta_pct), minutes_left)

        if btc_up:
            token_price = float(window.up_ask)
            if token_price <= 0:
                return   # no liquidity — empty book fallback
            if token_price < DISLOC_MIN_TOKEN:
                logger.debug(f"Strategy: DISLOC skip — UP token {token_price:.3f} below min {DISLOC_MIN_TOKEN}")
                return
            if token_price > DISLOC_MAX_TOKEN:
                logger.debug(f"Strategy: DISLOC skip — UP token {token_price:.3f} above max {DISLOC_MAX_TOKEN} (market priced in)")
                return
            edge        = fair - token_price
            side_label  = "UP"
        else:
            token_price = float(window.down_ask)
            if token_price <= 0:
                return   # no liquidity — empty book fallback
            if token_price < DISLOC_MIN_TOKEN:
                logger.debug(f"Strategy: DISLOC skip — DOWN token {token_price:.3f} below min {DISLOC_MIN_TOKEN}")
                return
            if token_price > DISLOC_MAX_TOKEN:
                logger.debug(f"Strategy: DISLOC skip — DOWN token {token_price:.3f} above max {DISLOC_MAX_TOKEN} (market priced in)")
                return
            fair        = 1.0 - (0.5 - (fair - 0.5))   # mirror for DOWN direction
            fair        = dislocation_fair_prob(abs(delta_pct), minutes_left)
            edge        = fair - token_price
            side_label  = "DOWN"

        if edge < DISLOC_MIN_EDGE:
            logger.debug(
                f"Strategy: DISLOC skip | Δbtc={delta_pct:+.3f}% "
                f"fair={fair:.3f} {side_label}@{token_price:.3f} edge={edge:.3f}"
            )
            return

        # Direction is already confirmed by delta sign — momentum check removed.
        # With WebSocket (60 history samples ≈ 12-30s), the old 5-min-calibrated
        # threshold was blocking valid trades by 0.001-0.002 momentum units.

        logger.info(
            f"Strategy: 🔥 DISLOCATION {side_label} | "
            f"Δbtc={delta_pct:+.3f}% {minutes_left:.1f}min left | "
            f"fair={fair:.3f} mkt={token_price:.3f} edge={edge:.3f}"
        )
        await self._place_binary(window, btc_up, edge, token_price, "dislocation",
                                 max_bet=DISLOC_MAX_BET)

    # ── Engine 2: Directional (final 30s) ────────────────────────────────────

    async def evaluate_directional(self, window: BinaryWindow) -> None:
        """
        Called in the final 30 seconds. Buy the confirmed direction
        if confidence ≥0.45 and BTC confirms by >0.03%.
        """
        if window.seconds_remaining > DIRECT_SECONDS_LEFT:
            return
        if self._btc_price is None:
            return
        if self._window_open is None:
            self._window_open = self._btc_price
        if self._traded_window == window.window_start:
            return
        if len(self._positions) >= MAX_OPEN:
            return

        delta_pct = (self._btc_price - self._window_open) / self._window_open * 100
        btc_up    = delta_pct > 0

        if abs(delta_pct) < DIRECT_BTC_CONFIRM:
            return

        minutes_left = window.minutes_remaining
        fair         = dislocation_fair_prob(abs(delta_pct), max(minutes_left, 0.01))
        confidence   = fair - 0.5

        if confidence < DIRECT_MIN_CONFIDENCE:
            return

        token_price = float(window.up_ask) if btc_up else float(window.down_ask)
        if token_price <= 0:
            return   # no liquidity — empty book fallback, don't trade
        if token_price < DIRECT_MIN_TOKEN:
            logger.debug(f"Strategy: DIRECT skip — token {token_price:.3f} below min {DIRECT_MIN_TOKEN} (market moved too far)")
            return
        if token_price > DIRECT_MAX_TOKEN:
            logger.debug(
                f"Strategy: DIRECT skip — token {token_price:.3f} above max {DIRECT_MAX_TOKEN} "
                f"(break-even >{DIRECT_MAX_TOKEN*100:.0f}% — terrible R/R)"
            )
            return
        edge        = fair - token_price
        side_label  = "UP" if btc_up else "DOWN"

        if edge < DIRECT_MIN_EDGE:
            logger.debug(
                f"Strategy: DIRECT skip — edge {edge:.3f} below min {DIRECT_MIN_EDGE}"
            )
            return

        logger.info(
            f"Strategy: ⚡ DIRECTIONAL {side_label} | "
            f"Δbtc={delta_pct:+.3f}% {window.seconds_remaining:.0f}s left | "
            f"confidence={confidence:.3f} edge={edge:.3f}"
        )
        await self._place_binary(
            window, btc_up, edge, token_price, "directional",
            max_bet=DIRECT_MAX_BET,
        )

    # ── Engine 3: Pure arb (general markets) ─────────────────────────────────

    async def evaluate_arb_only(self, market: BTCMarket) -> None:
        mid = market.market_id
        now = time.time()
        if now - self._last_arb.get(mid, 0) < ARB_COOLDOWN:
            return
        if len(self._positions) >= MAX_OPEN:
            return
        if (market.yes_ask > NEAR_SETTLED and market.no_ask > NEAR_SETTLED
                and market.spread >= ARB_MIN_SPREAD):
            logger.info(
                f"Strategy: ARB '{market.label}' | "
                f"YES={float(market.yes_ask):.3f} NO={float(market.no_ask):.3f} "
                f"spread={float(market.spread):.3f}"
            )
            await self._enter_arb(market)

    # ── Trade execution ───────────────────────────────────────────────────────

    async def _place_binary(
        self,
        window: BinaryWindow,
        btc_up: bool,
        edge: float,
        token_price: float,
        engine: str,
        max_bet: Optional[Decimal] = None,
    ) -> None:
        # Pass token ID directly — _resolve_token returns it as-is (no colon)
        token_id    = window.up_token_id if btc_up else window.down_token_id
        side        = Side.YES   # always buying a token (UP or DOWN)
        limit_price = window.up_ask if btc_up else window.down_ask
        size        = kelly_size(edge, token_price, self._bankroll)
        if max_bet is not None:
            size = min(size, max_bet)

        if size < Decimal("1"):
            logger.debug(f"Strategy: Kelly size too small ({size}), skipping")
            return

        fill = await self._trader.fill(token_id, side, limit_price, size)
        if fill is None:
            return

        self._traded_window = window.window_start
        shares = float(size) / float(fill)

        # Don't record PnL yet — wait for actual resolution
        self._pending[window.slug] = {
            "slug":         window.slug,
            "cost":         float(size),
            "shares":       shares,
            "window_end":   window.window_end,
            "is_up":        btc_up,
            "engine":       engine,
            "time":         time.time(),
            "up_token_id":  window.up_token_id,
            "down_token_id": window.down_token_id,
        }
        state_store.save(self._cum_pnl, self._trades, self._pending, self._loss_cooldown_until)

        logger.info(
            f"Strategy: ✅ {engine.upper()} {'UP' if btc_up else 'DOWN'} "
            f"@ {fill:.3f} x{shares:.1f} shares cost=${size} — pending settlement"
        )

    async def _enter_arb(self, market: BTCMarket) -> None:
        yes_fill = await self._trader.fill(
            market.market_id, Side.YES, market.yes_ask, self._size
        )
        no_fill = await self._trader.fill(
            market.market_id, Side.NO, market.no_ask, self._size
        )
        if yes_fill is None or no_fill is None:
            return
        self._last_arb[market.market_id] = time.time()
        gross = Decimal("1") - yes_fill - no_fill
        fees  = (yes_fill + no_fill) * FEE_RATE * 2
        net   = (gross - fees) * self._size
        self._cum_pnl += net
        self._trades.append({
            "time":    time.time(),
            "market":  market.label,
            "type":    "arb",
            "net":     float(net),
            "cum_pnl": float(self._cum_pnl),
        })
        state_store.save(self._cum_pnl, self._trades, self._pending, self._loss_cooldown_until)
        logger.info(
            f"Strategy: ✅ ARB '{market.label}' "
            f"YES@{yes_fill:.3f} NO@{no_fill:.3f} "
            f"net=${net:.2f} | cum=${self._cum_pnl:.2f}"
        )

    # ── Settlement — resolve pending positions against actual outcomes ─────────

    async def settle_pending(self) -> None:
        """
        For each pending position whose window has expired, fetch the Gamma
        market result and record actual PnL (win = shares×$1 − cost, loss = −cost).
        Called every binary_loop cycle so settlements happen within ~10s of expiry.
        Positions older than 2 hours are force-cleared as losses to prevent blocking.
        """
        now = time.time()
        to_check = [p for p in self._pending.values() if p["window_end"] <= now]
        if not to_check:
            return

        # Force-clear positions older than 30 min — Gamma never returns closed slugs
        FORCE_CLEAR_SECS = 1800
        for pos in list(to_check):
            if now - pos["window_end"] > FORCE_CLEAR_SECS:
                slug = pos["slug"]
                actual_pnl = Decimal(str(-pos["cost"]))
                self._cum_pnl += actual_pnl
                self._trades.append({
                    "time":    pos["time"],
                    "market":  slug,
                    "type":    pos["engine"],
                    "net":     float(actual_pnl),
                    "cum_pnl": float(self._cum_pnl),
                })
                del self._pending[slug]
                to_check = [p for p in to_check if p["slug"] != slug]
                state_store.save(self._cum_pnl, self._trades, self._pending, self._loss_cooldown_until)
                logger.warning(
                    f"Strategy: ⏰ FORCE-CLEARED {slug} (>2h old, no Gamma response) "
                    f"— recorded as loss ${actual_pnl:.2f}"
                )

        if not to_check:
            return

        async with httpx.AsyncClient(timeout=8.0) as http:
            for pos in to_check:
                slug = pos["slug"]
                try:
                    # Query by token ID — works for both open and closed markets.
                    # Slug-only queries return empty for resolved markets on Gamma.
                    up_tok = pos.get("up_token_id", "")
                    m = None
                    if up_tok:
                        resp = await http.get(
                            f"{GAMMA_BASE}/markets",
                            params={"clob_token_ids": up_tok}
                        )
                        if resp.status_code == 200:
                            data = resp.json()
                            if data:
                                m = data[0] if isinstance(data, list) else data
                    # Fallback: try slug (works for active markets)
                    if m is None:
                        resp = await http.get(
                            f"{GAMMA_BASE}/markets", params={"slug": slug}
                        )
                        resp.raise_for_status()
                        data = resp.json()
                        if not data:
                            logger.debug(f"Strategy: settle — no Gamma data for {slug}")
                            continue
                        m = data[0] if isinstance(data, list) else data

                    if not m.get("closed"):
                        logger.debug(f"Strategy: settle — {slug} not closed yet")
                        continue   # not resolved yet — check next cycle

                    raw_px = m.get("outcomePrices", '["0.5","0.5"]')
                    prices = json.loads(raw_px) if isinstance(raw_px, str) else raw_px
                    up_final = float(prices[0])   # 1.0 = UP won, 0.0 = UP lost

                    won = (up_final >= 0.99) if pos["is_up"] else (up_final <= 0.01)
                    actual_pnl = Decimal(str(
                        round(pos["shares"] - pos["cost"], 4) if won
                        else -pos["cost"]
                    ))

                    self._cum_pnl     += actual_pnl
                    self._session_pnl += actual_pnl
                    if won:
                        self._session_wins   += 1
                    else:
                        self._session_losses += 1
                    self._trades.append({
                        "time":    pos["time"],
                        "market":  slug,
                        "type":    pos["engine"],
                        "net":     float(actual_pnl),
                        "cum_pnl": float(self._cum_pnl),
                    })
                    # After a loss, sit out the next N windows
                    if not won:
                        self._loss_cooldown_until = (
                            pos["window_end"]
                            + LOSS_COOLDOWN_WINDOWS * 300
                        )
                        logger.info(
                            f"Strategy: 🛑 loss cooldown — skipping next "
                            f"{LOSS_COOLDOWN_WINDOWS} windows"
                        )
                    del self._pending[slug]
                    state_store.save(self._cum_pnl, self._trades, self._pending, self._loss_cooldown_until)

                    logger.info(
                        f"Strategy: {'✅ WIN' if won else '❌ LOSS'} "
                        f"{slug} ({'UP' if pos['is_up'] else 'DOWN'}) "
                        f"actual=${actual_pnl:.2f} | cum=${self._cum_pnl:.2f}"
                    )

                except Exception as exc:
                    logger.warning(f"Strategy: settle error for {slug}: {exc}")

    # ── Dashboard ─────────────────────────────────────────────────────────────

    async def evaluate_market(self, market: BTCMarket) -> None:
        """Compatibility shim — routes long-dated markets to arb check only."""
        await self.evaluate_arb_only(market)

    @property
    def cumulative_pnl(self) -> Decimal:
        return self._cum_pnl

    @property
    def session_pnl(self) -> Decimal:
        return self._session_pnl

    @property
    def session_wins(self) -> int:
        return self._session_wins

    @property
    def session_losses(self) -> int:
        return self._session_losses

    @property
    def session_start(self) -> float:
        return self._session_start

    @property
    def open_positions(self) -> int:
        return len(self._pending)

    @property
    def pending_positions(self) -> list:
        """Live positions waiting for settlement."""
        import time as _t
        now = _t.time()
        result = []
        for p in self._pending.values():
            secs_left = max(0, p["window_end"] - now)
            result.append({
                "slug":       p["slug"],
                "side":       "UP" if p["is_up"] else "DOWN",
                "cost":       p["cost"],
                "shares":     round(p["shares"], 2),
                "engine":     p["engine"],
                "secs_left":  round(secs_left),
            })
        return result

    @property
    def trade_count(self) -> int:
        return len(self._trades)

    @property
    def recent_trades(self) -> list:
        return self._trades[-10:]
