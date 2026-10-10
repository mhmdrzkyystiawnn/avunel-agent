"""Temporal guard: keep dated facts out of the built-in memory store.

The temporal provider OWNS time-bound facts (dated plans, appointments, events).
When the model calls the built-in ``memory`` tool with such a fact, this provider
vetoes the write through a ``pre_tool_call`` hook and points the model at the
temporal store instead. Because no independent copy ever lands in MEMORY.md,
``hermes temporal delete <id>`` cannot leave an active duplicate behind.

Deliberately conservative and fail-open:

  * A write is blocked ONLY on a STRONG, unambiguous time expression. A bare
    "minggu"/"bulan" or a lone weekday is NOT enough — a standing preference like
    "meeting tiap Senin" is a convention, not a dated fact.
  * ``remove`` is never blocked (deleting from built-in memory is always fine).
  * ``target="user"`` (identity/profile) is never blocked.
  * Any error returns "allow" so non-temporal built-in memory is never harmed.

Pure, side-effect-free, no I/O — unit-testable offline.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Actions that CREATE content. ``remove`` is intentionally absent: deleting is
# never a duplication risk.
WRITE_ACTIONS = frozenset({"add", "replace"})

# Strong, unambiguous time expressions. Narrow on purpose — every pattern here
# pins a fact to a specific day/period. Bare period words ("minggu", "bulan",
# "tahun") and a lone weekday are deliberately EXCLUDED to avoid vetoing
# standing conventions ("meeting tiap Jumat", "libur tiap minggu").
_TEMPORAL_PATTERNS = (
    r"\bhari\s+ini\b",
    r"\bbesok\b", r"\bbsk\b",
    r"\blusa\b",
    r"\bkemarin\b", r"\bkmrn\b",
    r"\btadi\s+(?:malam|pagi|siang|sore)\b",
    r"\bminggu\s+depan\b", r"\bminggu\s+lalu\b",
    r"\bbulan\s+depan\b", r"\bbulan\s+lalu\b",
    r"\btahun\s+depan\b", r"\btahun\s+lalu\b",
    # "N hari/minggu/bulan/tahun (yang) lalu|lagi|kemudian|depan"
    r"\b(?:satu|dua|tiga|empat|lima|enam|tujuh|delapan|sembilan|sepuluh|sebelas|\d{1,2})\s+"
    r"(?:hari|minggu|bulan|tahun)\s+(?:yang\s+)?(?:lalu|lagi|kemudian|depan)\b",
    r"\btanggal\s+\d{1,2}\b",
    # "12 september" / "12 sept"
    r"\b\d{1,2}\s+(?:januari|februari|maret|april|mei|juni|juli|agustus|september|"
    r"oktober|november|desember|jan|feb|mar|apr|jun|jul|agu|ags|sep|sept|okt|nov|des)\b",
    # 12/9 or 12/9/2026
    r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b",
)
_TEMPORAL_RE = re.compile("|".join(_TEMPORAL_PATTERNS), re.IGNORECASE)

BLOCK_MESSAGE = (
    "temporal guard: this looks like a dated fact (a plan/appointment/event with a "
    "time), which the temporal memory provider owns — it was NOT saved to built-in "
    "memory. Dated facts are captured automatically from the conversation and can be "
    "recalled with the `temporal_recall` tool. If this is a standing preference or a "
    "non-temporal note, rephrase without the date expression and retry."
)


def looks_temporal_fact(text: str) -> bool:
    """True only when *text* carries a STRONG time expression.

    Conservative by design: the caller vetoes a memory write on True, so a false
    positive costs the user a rejected non-temporal note. Anything uncertain
    returns False (let it through).
    """
    return bool(_TEMPORAL_RE.search(text or ""))


def _texts_for_op(op: Dict[str, Any]) -> List[str]:
    """Content strings an add/replace op would write (``content`` / ``new_text``)."""
    action = str(op.get("action") or "")
    if action not in WRITE_ACTIONS:
        return []
    return [v for k in ("content", "new_text") if isinstance(v := op.get(k), str) and v]


def memory_write_texts(args: Dict[str, Any]) -> List[str]:
    """Content strings a ``memory`` tool call would persist (single-op or batch).

    Only ``add``/``replace`` contribute. ``remove`` and any op without a write
    action are ignored.
    """
    if not isinstance(args, dict):
        return []
    ops = args.get("operations")
    if isinstance(ops, list) and ops:
        texts: List[str] = []
        for op in ops:
            if isinstance(op, dict):
                texts += _texts_for_op(op)
        return texts
    return _texts_for_op(args)


def should_block_memory_write(tool_name: str, args: Any) -> bool:
    """True when this tool call is a built-in ``memory`` write carrying a dated fact.

    Never raises: any unexpected shape returns False (fail-open).
    """
    try:
        if tool_name != "memory" or not isinstance(args, dict):
            return False
        # The user profile (identity/preferences) is never temporal.
        if str(args.get("target") or "memory") != "memory":
            return False
        return any(looks_temporal_fact(t) for t in memory_write_texts(args))
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug("temporal guard: should_block check failed (fail-open): %s", exc)
        return False
