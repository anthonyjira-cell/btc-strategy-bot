"""
Live trader: executes real orders on Polymarket CLOB.

Flow per fill():
  1. Fetch current CLOB ask for the token
  2. Skip if ask > limit_price + slippage (won't fill anyway)
  3. Place a GTC limit order at the CLOB ask price
  4. Try neg_risk=True first (all current Polymarket markets use the NegRisk
     exchange). If order_version_mismatch, retry with neg_risk=False.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Optional

import httpx
from loguru import logger

from btc_bot.models import Side

CLOB_HOST  = "https://clob.polymarket.com"
CHAIN_ID   = 137        # Polygon mainnet
SLIPPAGE   = 0.005      # 0.5% — how much above limit we'll still accept


class LiveTrader:
    """Places real limit orders on Polymarket CLOB via py-clob-client."""

    def __init__(self, private_key: str):
        from py_clob_client.client import ClobClient
        from eth_account import Account

        wallet_address = Account.from_key(private_key).address
        logger.info(f"LiveTrader: wallet {wallet_address}")

        self._client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=1,   # POLY_PROXY — standard for MetaMask/browser wallets
            funder=wallet_address,
        )

        try:
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            logger.info("LiveTrader: API credentials ready ✓")
        except Exception as exc:
            raise RuntimeError(f"LiveTrader init failed: {exc}") from exc

        self._http = httpx.AsyncClient(timeout=5.0)

    async def fill(
        self,
        market_id: str,
        side: Side,
        limit_price: Decimal,
        size: Decimal,
    ) -> Optional[Decimal]:
        """
        1. Check current CLOB ask — skip if too far above limit.
        2. Place GTC order at CLOB ask (ensures immediate fill).
        3. Try neg_risk=True first, fall back to False on version_mismatch.
        Returns actual fill price or None.
        """
        from py_clob_client.clob_types import OrderArgs, OrderType, PartialCreateOrderOptions

        token_id = self._resolve_token(market_id, side)
        limit_f  = float(limit_price)

        # ── Step 1: fetch current CLOB ask ───────────────────────────────────
        try:
            resp = await self._http.get(
                f"{CLOB_HOST}/price",
                params={"token_id": token_id, "side": "buy"},
            )
            resp.raise_for_status()
            ask = float(resp.json().get("price", limit_f))
        except Exception as exc:
            logger.debug(f"LiveTrader: price fetch error: {exc} — using limit price")
            ask = limit_f

        # ── Step 2: price check ───────────────────────────────────────────────
        if ask > limit_f + SLIPPAGE:
            logger.debug(
                f"LiveTrader: SKIP {side.value} {token_id[:12]}… "
                f"ask={ask:.4f} > limit={limit_f:.4f}+slippage"
            )
            return None

        # Use actual ask as order price so it fills immediately
        order_price = ask
        shares      = round(float(size) / order_price, 2)

        if shares < 1.0:
            logger.debug(f"LiveTrader: order too small ({shares} shares)")
            return None

        logger.info(
            f"LiveTrader: attempting {side.value} {token_id[:12]}… "
            f"price={order_price:.4f} shares={shares:.2f} (~${float(size):.2f})"
        )

        # ── Step 3: place order — try neg_risk=True, fall back to False ───────
        loop = asyncio.get_running_loop()

        _client = self._client
        _token  = token_id
        _price  = order_price
        _shares = shares

        for neg_risk in (True, False):
            try:
                _neg = neg_risk
                order = await loop.run_in_executor(
                    None,
                    lambda: _client.create_order(
                        OrderArgs(
                            token_id=_token,
                            price=_price,
                            size=_shares,
                            side="BUY",
                        ),
                        PartialCreateOrderOptions(neg_risk=_neg),
                    )
                )
                resp = await loop.run_in_executor(
                    None,
                    lambda: _client.post_order(order, OrderType.GTC)
                )

                if resp and resp.get("success"):
                    logger.info(
                        f"LiveTrader: ✅ ORDER PLACED {side.value} "
                        f"{token_id[:12]}… @ {order_price:.4f} x{shares:.1f} shares "
                        f"(neg_risk={neg_risk})"
                    )
                    return Decimal(str(order_price))

                # Check if it's specifically a version mismatch → retry with other flag
                err = str(resp) if resp else ""
                if "order_version_mismatch" in err:
                    logger.debug(
                        f"LiveTrader: version mismatch with neg_risk={neg_risk}, retrying…"
                    )
                    continue

                logger.warning(f"LiveTrader: order rejected — {resp}")
                return None

            except Exception as exc:
                err = str(exc)
                if "order_version_mismatch" in err:
                    logger.debug(
                        f"LiveTrader: version mismatch with neg_risk={neg_risk}, retrying…"
                    )
                    continue
                logger.error(f"LiveTrader: order error: {exc}")
                return None

        logger.warning(f"LiveTrader: order failed on both neg_risk values — skipping")
        return None

    @staticmethod
    def _resolve_token(market_id: str, side: Side) -> str:
        market_id = market_id.strip("'\"")
        if ":" in market_id:
            yes_id, no_id = market_id.split(":", 1)
            return yes_id if side == Side.YES else no_id
        return market_id

    async def close(self) -> None:
        await self._http.aclose()
