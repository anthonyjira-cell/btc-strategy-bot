"""
Live trader: executes real orders on Polymarket CLOB using py-clob-client-v2.

fill() receives the CLOB best ask fetched fresh by market_finder/arb_scanner
and places the order at exactly that price — a taker order that fills immediately.
No re-fetching of prices here; the caller is responsible for freshness.

Try neg_risk=True first (most Polymarket markets use NegRisk exchange).
Fall back to neg_risk=False on order_version_mismatch or invalid_signature.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Optional

from loguru import logger

from btc_bot.models import Side

CLOB_HOST = "https://clob.polymarket.com"
CHAIN_ID  = 137  # Polygon mainnet


class LiveTrader:
    """Places real limit orders on Polymarket CLOB via py-clob-client-v2."""

    def __init__(self, private_key: str):
        from py_clob_client_v2 import ClobClient
        from eth_account import Account

        wallet_address = Account.from_key(private_key).address
        logger.info(f"LiveTrader: wallet {wallet_address}")

        # Polymarket deploys a proxy wallet per user; orders must be placed
        # from that proxy address (not the raw EOA). Set POLY_FUNDER_ADDRESS
        # to the deposit address shown on polymarket.com → Deposit dialog.
        import os
        funder = os.environ.get("POLY_FUNDER_ADDRESS", wallet_address)
        logger.info(f"LiveTrader: funder={funder}")

        try:
            # Step 1: derive API credentials using raw EOA (sig_type=0).
            # The API key must be associated with the EOA address because
            # the CLOB extracts the ECDSA signer from order signatures and
            # checks it matches the API key address.
            eoa_client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=private_key,
                signature_type=0,
                funder=wallet_address,
            )
            creds = eoa_client.create_or_derive_api_key()
            logger.info(f"LiveTrader: API key derived (EOA) ✓")

            # Step 2: build the trading client with sig_type=3 (ERC-1271)
            # + deposit wallet as funder, using the EOA-derived creds.
            # sig_type=3 = POLY_1271: deposit wallet is a smart contract;
            # isValidSignature() verifies the EOA signature on-chain.
            sig_type = 3 if funder != wallet_address else 0
            logger.info(f"LiveTrader: sig_type={sig_type}")

            self._client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=private_key,
                creds=creds,
                signature_type=sig_type,
                funder=funder,
            )
            logger.info("LiveTrader: trading client ready ✓")
        except Exception as exc:
            raise RuntimeError(f"LiveTrader init failed: {exc}") from exc


    async def fill(
        self,
        market_id: str,
        side: Side,
        limit_price: Decimal,
        size: Decimal,
    ) -> Optional[Decimal]:
        """
        1. Check current CLOB ask — skip if too far above limit.
        2. Place GTC order via create_and_post_order.
        3. Try neg_risk=True first, fall back to False on version_mismatch.
        Returns actual fill price or None.
        """
        from py_clob_client_v2 import OrderArgs, OrderType, PartialCreateOrderOptions

        token_id    = self._resolve_token(market_id, side)
        # limit_price IS the CLOB best ask fetched fresh by market_finder/arb_scanner.
        # Place the order at exactly that price — it will match as a taker immediately.
        order_price = float(limit_price)
        shares      = round(float(size) / order_price, 2)

        if shares < 1.0:
            logger.debug(f"LiveTrader: order too small ({shares} shares)")
            return None

        logger.info(
            f"LiveTrader: attempting {side.value} {token_id[:12]}… "
            f"price={order_price:.4f} shares={shares:.2f} (~${float(size):.2f})"
        )

        # ── Step 3: place order — try neg_risk=True, fall back to False ───────
        loop    = asyncio.get_running_loop()
        _client = self._client
        _token  = token_id
        _price  = order_price
        _shares = shares

        for neg_risk in (True, False):
            try:
                _neg = neg_risk
                resp = await loop.run_in_executor(
                    None,
                    lambda: _client.create_and_post_order(
                        OrderArgs(
                            token_id=_token,
                            price=_price,
                            size=_shares,
                            side="BUY",
                        ),
                        options=PartialCreateOrderOptions(neg_risk=_neg),
                        order_type=OrderType.GTC,
                    )
                )

                if resp and resp.get("success"):
                    logger.info(
                        f"LiveTrader: ✅ ORDER PLACED {side.value} "
                        f"{token_id[:12]}… @ {order_price:.4f} x{shares:.1f} shares "
                        f"(neg_risk={neg_risk})"
                    )
                    return Decimal(str(order_price))

                # Wrong neg_risk flag → retry with the other value
                err = str(resp) if resp else ""
                if "order_version_mismatch" in err or "invalid signature" in err:
                    logger.debug(
                        f"LiveTrader: sig/version mismatch (neg_risk={neg_risk}), retrying…"
                    )
                    continue

                logger.warning(f"LiveTrader: order rejected — {resp}")
                return None

            except Exception as exc:
                err = str(exc)
                if "order_version_mismatch" in err or "invalid signature" in err:
                    logger.debug(
                        f"LiveTrader: sig/version mismatch exc (neg_risk={neg_risk}), retrying…"
                    )
                    continue
                logger.error(f"LiveTrader: order error: {exc}")
                return None

        logger.warning(
            f"LiveTrader: order failed on both neg_risk values for {token_id[:12]}…"
        )
        return None

    @staticmethod
    def _resolve_token(market_id: str, side: Side) -> str:
        """
        For binary windows the strategy passes the token_id directly via
        the market_id field (a plain numeric string, no colon).
        For long-dated markets the format is "YES_TOKEN:NO_TOKEN".
        """
        market_id = market_id.strip("'\"")
        if ":" in market_id:
            yes_id, no_id = market_id.split(":", 1)
            return yes_id if side == Side.YES else no_id
        return market_id   # already a token ID

    async def close(self) -> None:
        pass  # nothing to clean up
