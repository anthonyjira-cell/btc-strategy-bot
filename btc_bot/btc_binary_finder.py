"""
Discovers and tracks Polymarket's 5-minute BTC Up/Down binary markets.

Market slug: btc-updown-5m-{window_start_unix_timestamp}
New market every 5 minutes at timestamps that are multiples of 300.
UP token = tokens[0], DOWN token = tokens[1].
Resolves via Chainlink BTC/USD: UP wins if close >= open, DOWN wins otherwise.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional, Tuple

import httpx
from loguru import logger

GAMMA_BASE     = "https://gamma-api.polymarket.com"
CLOB_BASE      = "https://clob.polymarket.com"
WINDOW_SECONDS = 300  # 5-minute windows


@dataclass
class BinaryWindow:
    slug:          str
    window_start:  int       # Unix timestamp of window open
    window_end:    int       # Unix timestamp of window close
    up_token_id:   str
    down_token_id: str
    up_ask:        Decimal = field(default=Decimal("0.5"))
    down_ask:      Decimal = field(default=Decimal("0.5"))

    @property
    def seconds_remaining(self) -> float:
        return max(0.0, self.window_end - time.time())

    @property
    def minutes_remaining(self) -> float:
        return self.seconds_remaining / 60.0

    @property
    def combined(self) -> Decimal:
        return self.up_ask + self.down_ask

    @property
    def spread(self) -> Decimal:
        return Decimal("1") - self.combined


class BTCBinaryFinder:
    """Fetches the current 5-min BTC binary window and its CLOB prices."""

    def __init__(self, timeout: float = 5.0):
        self._http = httpx.AsyncClient(timeout=timeout)
        self._slug_cache: Optional[str] = None
        self._window_cache: Optional[BinaryWindow] = None

    @staticmethod
    def current_window_start() -> int:
        return (int(time.time()) // WINDOW_SECONDS) * WINDOW_SECONDS

    async def get_active_window(self) -> Optional[BinaryWindow]:
        """
        Returns the currently active BTC 5-min window with fresh CLOB asks.
        Re-fetches market metadata only when the window rolls over.
        """
        window_start = self.current_window_start()
        slug = f"btc-updown-5m-{window_start}"

        if slug != self._slug_cache:
            logger.debug(f"BTCBinary: new window → {slug}")
            w = await self._fetch_window_meta(slug, window_start)
            if w is None:
                # Market might not be created yet — try previous window
                prev_start = window_start - WINDOW_SECONDS
                prev_slug  = f"btc-updown-5m-{prev_start}"
                w = await self._fetch_window_meta(prev_slug, prev_start)
                if w is None:
                    return None
            self._slug_cache   = slug
            self._window_cache = w

        # Refresh CLOB prices every call
        w = self._window_cache
        if w is None:
            return None
        up_ask, down_ask = await self._fetch_clob_asks(w.up_token_id, w.down_token_id)
        w.up_ask   = up_ask
        w.down_ask = down_ask
        return w

    async def _fetch_window_meta(
        self, slug: str, window_start: int
    ) -> Optional[BinaryWindow]:
        try:
            resp = await self._http.get(
                f"{GAMMA_BASE}/markets", params={"slug": slug}
            )
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            m = data[0] if isinstance(data, list) else data

            raw_tok = m.get("clobTokenIds") or []
            tokens  = json.loads(raw_tok) if isinstance(raw_tok, str) else raw_tok
            if len(tokens) < 2:
                return None

            return BinaryWindow(
                slug=slug,
                window_start=window_start,
                window_end=window_start + WINDOW_SECONDS,
                up_token_id=str(tokens[0]),
                down_token_id=str(tokens[1]),
            )
        except Exception as exc:
            logger.debug(f"BTCBinary: failed to fetch {slug}: {exc}")
            return None

    async def _fetch_clob_asks(
        self, up_id: str, down_id: str
    ) -> Tuple[Decimal, Decimal]:
        """Get best asks from CLOB orderbook for UP and DOWN tokens.
        Returns Decimal('0') for a side when the book is empty (no real ask).
        Strategy code treats 0 as 'no liquidity — skip trade'.
        """
        try:
            up_r, dn_r = await asyncio.gather(
                self._http.get(f"{CLOB_BASE}/book", params={"token_id": up_id}),
                self._http.get(f"{CLOB_BASE}/book", params={"token_id": down_id}),
            )
            up_r.raise_for_status()
            dn_r.raise_for_status()

            def _best(book: dict) -> Decimal:
                # Polymarket CLOB sorts asks HIGHEST-FIRST (descending),
                # so asks[-1] is the best (lowest) ask — the taker fill price.
                # Return 0 if book is empty — signals no liquidity to caller.
                asks = book.get("asks", [])
                return Decimal(str(asks[-1]["price"])) if asks else Decimal("0")

            return (
                _best(up_r.json()),
                _best(dn_r.json()),
            )
        except Exception as exc:
            logger.debug(f"BTCBinary: CLOB ask fetch error: {exc}")
            return Decimal("0"), Decimal("0")

    async def close(self) -> None:
        await self._http.aclose()
