"""Data models for the BTC strategy bot."""
from __future__ import annotations
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional
import time


class Side(str, Enum):
    YES = "YES"
    NO  = "NO"


class PositionStatus(str, Enum):
    OPEN      = "open"
    HEDGED    = "hedged"   # both legs filled → locked arb
    CLOSED    = "closed"
    CANCELLED = "cancelled"


@dataclass
class BTCMarket:
    market_id:  str          # "YES_TOKEN:NO_TOKEN"
    question:   str
    yes_ask:    Decimal
    no_ask:     Decimal
    volume:     float
    end_date:   str = ""

    @property
    def combined(self) -> Decimal:
        return self.yes_ask + self.no_ask

    @property
    def spread(self) -> Decimal:
        return Decimal("1") - self.combined

    @property
    def label(self) -> str:
        q = self.question[:28].rsplit(" ", 1)[0] if len(self.question) > 28 else self.question
        return q + "…" if len(self.question) > 28 else q


@dataclass
class Position:
    market_id:   str
    question:    str
    entry_side:  Side
    entry_price: Decimal
    size:        Decimal
    opened_at:   float = field(default_factory=time.time)

    hedge_side:  Optional[Side]    = None
    hedge_price: Optional[Decimal] = None
    hedge_at:    Optional[float]   = None

    status:      PositionStatus = PositionStatus.OPEN
    net_profit:  Optional[Decimal] = None

    @property
    def is_hedged(self) -> bool:
        return self.hedge_price is not None

    def close_arb(self) -> Decimal:
        """Lock in profit when both legs are filled."""
        if self.hedge_price is None:
            return Decimal("0")
        gross = Decimal("1") - self.entry_price - self.hedge_price
        fees  = (self.entry_price + self.hedge_price) * Decimal("0.01")  # ~1% total
        self.net_profit = (gross - fees) * self.size
        self.status = PositionStatus.HEDGED
        return self.net_profit
