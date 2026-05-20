"""
Persistent state store — saves/loads cumulative P&L and trade history
to a JSON file so the bot survives Railway restarts.

File location: STATE_FILE env var (default: /data/btc_bot_state.json).
Railway persistent volumes mount at /data; falls back to ./state.json locally.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import List

from loguru import logger

_DEFAULT_PATH = "/data/btc_bot_state.json"
_FALLBACK_PATH = "./state.json"


def _get_path() -> Path:
    custom = os.environ.get("STATE_FILE")
    if custom:
        return Path(custom)
    p = Path(_DEFAULT_PATH)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        return p
    except (PermissionError, OSError):
        return Path(_FALLBACK_PATH)


def load() -> dict:
    """Load saved state, return defaults if file doesn't exist or is corrupt."""
    path = _get_path()
    try:
        if path.exists():
            data = json.loads(path.read_text())
            logger.info(
                f"StateStore: loaded from {path} | "
                f"cum_pnl={data.get('cum_pnl', 0):.2f} | "
                f"trades={len(data.get('trades', []))}"
            )
            return data
    except Exception as exc:
        logger.warning(f"StateStore: failed to load {path}: {exc}")
    return {"cum_pnl": 0.0, "trades": []}


def save(cum_pnl: Decimal, trades: List[dict]) -> None:
    """Persist current state to disk."""
    path = _get_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "cum_pnl": float(cum_pnl),
            "trades": trades,
        }
        path.write_text(json.dumps(data, indent=2))
    except Exception as exc:
        logger.warning(f"StateStore: failed to save {path}: {exc}")
