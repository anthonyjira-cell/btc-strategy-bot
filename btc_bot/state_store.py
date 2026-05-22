"""
Persistent state store — saves/loads cumulative P&L, trade history,
and pending positions.

Storage priority:
  1. Upstash Redis  — if UPSTASH_REDIS_URL env var is set (survives redeploys)
  2. Local JSON file — /data/btc_bot_state.json (Railway volume) or ./state.json

Redis is preferred: free tier handles our ~10 writes/day easily, and state
survives every redeploy without needing a Railway persistent volume.
"""
from __future__ import annotations

import json
import os
from decimal import Decimal
from pathlib import Path
from typing import List

from loguru import logger

_REDIS_KEY      = "btc_bot_state"
_DEFAULT_PATH   = "/data/btc_bot_state.json"
_FALLBACK_PATH  = "./state.json"

# ── Redis client (lazy-initialised once) ─────────────────────────────────────

_redis = None

def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    url = os.environ.get("UPSTASH_REDIS_URL", "")
    if not url:
        return None
    try:
        import redis as redis_lib
        _redis = redis_lib.from_url(url, decode_responses=True, socket_timeout=5)
        _redis.ping()   # verify connection
        logger.info("StateStore: using Upstash Redis ✓")
        return _redis
    except Exception as exc:
        logger.warning(f"StateStore: Redis init failed ({exc}) — falling back to file")
        return None


# ── File path helper ──────────────────────────────────────────────────────────

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


# ── Public API ────────────────────────────────────────────────────────────────

def load() -> dict:
    """Load saved state; return defaults if not found or corrupt."""
    # Try Redis first
    r = _get_redis()
    if r is not None:
        try:
            raw = r.get(_REDIS_KEY)
            if raw:
                data = json.loads(raw) if isinstance(raw, str) else raw
                logger.info(
                    f"StateStore: loaded from Redis | "
                    f"cum_pnl={data.get('cum_pnl', 0):.2f} | "
                    f"trades={len(data.get('trades', []))} | "
                    f"pending={len(data.get('pending', {}))}"
                )
                return data
        except Exception as exc:
            logger.warning(f"StateStore: Redis load failed ({exc}) — trying file")

    # Fall back to file
    path = _get_path()
    try:
        if path.exists():
            data = json.loads(path.read_text())
            logger.info(
                f"StateStore: loaded from {path} | "
                f"cum_pnl={data.get('cum_pnl', 0):.2f} | "
                f"trades={len(data.get('trades', []))} | "
                f"pending={len(data.get('pending', {}))}"
            )
            return data
    except Exception as exc:
        logger.warning(f"StateStore: file load failed ({exc})")

    return {"cum_pnl": 0.0, "trades": [], "pending": {}}


def save(cum_pnl: Decimal, trades: List[dict], pending: dict = None,
         loss_cooldown_until: int = 0) -> None:
    """Persist current state (Redis if available, else file)."""
    data = {
        "cum_pnl":             float(cum_pnl),
        "trades":              trades,
        "pending":             pending or {},
        "loss_cooldown_until": loss_cooldown_until,
    }
    payload = json.dumps(data)

    # Try Redis first
    r = _get_redis()
    if r is not None:
        try:
            r.set(_REDIS_KEY, payload)
            return
        except Exception as exc:
            logger.warning(f"StateStore: Redis save failed ({exc}) — falling back to file")

    # Fall back to file
    path = _get_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload)
    except Exception as exc:
        logger.warning(f"StateStore: file save failed ({exc})")
