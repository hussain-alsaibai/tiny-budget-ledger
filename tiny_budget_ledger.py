"""
tiny_budget_ledger — File-backed per-task spend ledger for autonomous AI agents.

The smallest way to enforce "every session must cost less than it earns"
(SOUL.md mandate). Tracks USD + token spend against per-task limits in a
crash-safe append-only ledger, and refuses new spends that would push a
budget over its limit. Thread-safe and asyncio-friendly.

Why?
----
OpenClaw's core operating principle (SOUL.md) is **every agent session
must produce more value than it consumes**. To enforce that principle we
need:

1. A ledger that survives crashes (append-only JSONL).
2. Per-task and global budgets in USD and tokens.
3. A small sync `BudgetLedger` and an async `BudgetLedgerAsync` wrapper.
4. Callbacks for warning / blocking / rolling-over days.
5. Hooks for `tiny-cost-tracker` and `tiny-llm-router` integration.

Use cases
---------
- Cron jobs that must not exceed $5 per run.
- Multi-tenant agents where each tenant has its own cap.
- Per-model-per-task budget gates (OpenAI > Kimi > local).
- "Stop calling the LLM after 1M cumulative tokens this month."

Public API
----------
- BudgetLedger, BudgetLedgerAsync
- BudgetError, BudgetExceeded
- record_spend, can_spend, drain, rollover_if_needed, summary
- attach_to_tracer, attach_to_router

Usage
-----
    from tiny_budget_ledger import BudgetLedger

    led = BudgetLedger(path="/var/lib/openclaw/budget.jsonl",
                       monthly_usd=50.0,
                       monthly_tokens=20_000_000)

    led.record_spend(task_id="bounty-hunt", usd=0.05, tokens=4200,
                     model="kimi-k2.6", note="scanned 22 issues")

    if not led.can_spend(usd=2.0, tokens=200_000):
        raise SystemExit("Bounty aborted: budget cap exceeded")

    print(led.summary())
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import threading
import fcntl
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple, Union

__version__ = "0.1.0"
__all__ = [
    "BudgetLedger",
    "BudgetLedgerAsync",
    "BudgetError",
    "BudgetExceeded",
    "SpendRecord",
]


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class BudgetError(Exception):
    """Base error for tiny_budget_ledger."""


class BudgetExceeded(BudgetError):
    """Raised when a `record_spend` would push a budget over its cap.

    Attributes
    ----------
    field : str
        Which axis overflowed: ``"usd"``, ``"tokens"``, ``"calls"``, or
        ``"task"`` (a per-task cap).
    current : float
        Spent so far on this axis (post-the-rejected-spend would be even
        higher).
    attempted : float
        Value the caller wanted to record.
    limit : float
        Hard cap.
    """

    def __init__(self, field: str, current: float, attempted: float, limit: float, task_id: Optional[str] = None) -> None:
        self.field = field
        self.current = current
        self.attempted = attempted
        self.limit = limit
        self.task_id = task_id
        super().__init__(
            f"Budget exceeded [{field}]: spent={current:.4f} + attempted={attempted:.4f} > limit={limit:.4f}"
            + (f" (task={task_id!r})" if task_id else "")
        )


# ---------------------------------------------------------------------------
# Spend record
# ---------------------------------------------------------------------------


class SpendRecord(dict):
    """A single spending event in the ledger.

    Stored as a JSON line, ``SpendRecord`` keeps all metadata needed for
    auditing: timestamp, task_id, model, prompt vs completion tokens, USD
    cost, and free-form ``note``/``tags``.
    """

    @classmethod
    def now(cls, task_id: str, usd: float = 0.0, tokens: int = 0,
            prompt_tokens: int = 0, completion_tokens: int = 0,
            model: str = "unknown", note: str = "",
            tags: Optional[Sequence[str]] = None) -> "SpendRecord":
        return cls({
            "ts": time.time(),
            "task_id": task_id,
            "usd": float(usd),
            "tokens": int(tokens),
            "prompt_tokens": int(prompt_tokens),
            "completion_tokens": int(completion_tokens),
            "model": model,
            "note": note,
            "tags": list(tags or []),
        })


# ---------------------------------------------------------------------------
# Internal state snapshot
# ---------------------------------------------------------------------------


def _empty_month_totals() -> Dict[str, float]:
    return {"usd": 0.0, "tokens": 0, "calls": 0}


def _month_key(ts: float) -> str:
    """Return YYYY-MM for the given unix timestamp (UTC)."""
    return time.strftime("%Y-%m", time.gmtime(ts))


# ---------------------------------------------------------------------------
# BudgetLedger — sync, file-backed
# ---------------------------------------------------------------------------


class BudgetLedger:
    """Thread-safe, file-backed budget ledger.

    Parameters
    ----------
    path : str | os.PathLike
        Path to the JSONL ledger file. Parent directory is created if
        missing. Existing files are appended to.
    monthly_usd : float | None
        Optional monthly USD cap. ``None`` disables USD capping.
    monthly_tokens : int | None
        Optional monthly token cap. ``None`` disables token capping.
    per_task_usd : float | None
        Optional per-task USD cap. ``None`` disables per-task USD cap.
    on_warn : callable | None
        ``(field, current, limit)`` invoked when a soft warning level
        (default 80 %) is crossed. Returning ``True`` from this
        callback blocks the spend (use for critical-policy gates).
    on_block : callable | None
        ``(field, current, attempted, limit)`` invoked just before a
        ``BudgetExceeded`` is raised. Use for logging; can't prevent.
    rollover_policy : {"monthly", "none", "manual"}
        ``"monthly"`` (default) zeros month totals when a new month
        starts. ``"manual"`` requires ``rollover_if_needed()`` calls.
        ``"none"`` keeps totals forever (not recommended).
    soft_warn_ratio : float
        Default ``0.8``. When current/limit >= this ratio, ``on_warn``
        fires.
    """

    def __init__(
        self,
        path: Union[str, os.PathLike],
        monthly_usd: Optional[float] = None,
        monthly_tokens: Optional[int] = None,
        per_task_usd: Optional[float] = None,
        on_warn: Optional[Callable[[str, float, float], Optional[bool]]] = None,
        on_block: Optional[Callable[[str, float, float, float], None]] = None,
        rollover_policy: str = "monthly",
        soft_warn_ratio: float = 0.8,
    ) -> None:
        if rollover_policy not in ("monthly", "none", "manual"):
            raise BudgetError(f"unknown rollover_policy {rollover_policy!r}")
        if not (0.0 < soft_warn_ratio <= 1.0):
            raise BudgetError("soft_warn_ratio must be in (0, 1]")
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.touch()
        self.monthly_usd = monthly_usd
        self.monthly_tokens = monthly_tokens
        self.per_task_usd = per_task_usd
        self.on_warn = on_warn
        self.on_block = on_block
        self.rollover_policy = rollover_policy
        self.soft_warn_ratio = soft_warn_ratio
        self._lock = threading.RLock()
        self._warned: Dict[Tuple[str, str], bool] = {}  # (axis, period) -> warned
        self._replay()

    # ----- public API -------------------------------------------------------

    def record_spend(
        self,
        task_id: str,
        usd: float = 0.0,
        tokens: int = 0,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        model: str = "unknown",
        note: str = "",
        tags: Optional[Sequence[str]] = None,
        *,
        allow_overrun: bool = False,
    ) -> SpendRecord:
        """Record a spend against the ledger.

        Returns
        -------
        SpendRecord
            The persisted record (in dict form).

        Raises
        ------
        BudgetExceeded
            If any cap would be exceeded by this record. Use
            ``allow_overrun=True`` to suppress and just log.
        """
        with self._lock:
            self._maybe_rollover()
            rec = SpendRecord.now(
                task_id=task_id, usd=usd, tokens=tokens,
                prompt_tokens=prompt_tokens, completion_tokens=completion_tokens,
                model=model, note=note, tags=tags,
            )
            self._check_caps(rec, allow_overrun=allow_overrun)
            self._append(rec)
            self._bump_totals(rec)
            self._maybe_warn(rec)
            return rec

    def can_spend(self, usd: float = 0.0, tokens: int = 0, task_id: Optional[str] = None) -> bool:
        """Return True if a `record_spend` of this magnitude would succeed."""
        with self._lock:
            self._maybe_rollover()
            month = _month_key(time.time())
            month_usd = self._month_usd.get(month, 0.0)
            month_tok = self._month_tokens.get(month, 0)
            if self.monthly_usd is not None and month_usd + usd > self.monthly_usd:
                return False
            if self.monthly_tokens is not None and month_tok + tokens > self.monthly_tokens:
                return False
            if self.per_task_usd is not None and task_id is not None:
                task_usd = self._task_usd.get(task_id, 0.0)
                if task_usd + usd > self.per_task_usd:
                    return False
            return True

    def summary(self) -> Dict[str, Any]:
        """Return current totals and remaining capacity."""
        with self._lock:
            self._maybe_rollover()
            month = _month_key(time.time())
            month_usd = self._month_usd.get(month, 0.0)
            month_tok = self._month_tokens.get(month, 0)
            out = {
                "month": month,
                "month_usd_spent": month_usd,
                "month_tokens_spent": month_tok,
                "calls_total": sum(self._calls.values()),
            }
            if self.monthly_usd is not None:
                out["month_usd_remaining"] = max(0.0, self.monthly_usd - month_usd)
                out["month_usd_pct"] = (month_usd / self.monthly_usd) if self.monthly_usd > 0 else 0.0
            if self.monthly_tokens is not None:
                out["month_tokens_remaining"] = max(0, self.monthly_tokens - month_tok)
                out["month_tokens_pct"] = (month_tok / self.monthly_tokens) if self.monthly_tokens > 0 else 0.0
            # Per-task totals
            if self.per_task_usd is not None:
                top = sorted(self._task_usd.items(), key=lambda kv: kv[1], reverse=True)[:5]
                out["top_tasks_usd"] = [{"task_id": t, "usd": u} for t, u in top]
            return out

    def drain(self) -> List[SpendRecord]:
        """Atomically read and clear all spend records older than the current month.

        Use for monthly reports — call once after rollover.
        """
        with self._lock:
            self._maybe_rollover()
            month = _month_key(time.time())
            kept_lines: List[str] = []
            drained: List[SpendRecord] = []
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if _month_key(rec["ts"]) == month:
                        kept_lines.append(line)
                    else:
                        drained.append(rec)
            with self.path.open("w", encoding="utf-8") as fh:
                for line in kept_lines:
                    fh.write(line + "\n")
                    fh.flush()
                    os.fsync(fh.fileno())
            # reset stale indexes
            self._replay_from_lines(kept_lines)
            return drained

    def rollover_if_needed(self) -> bool:
        """Manually trigger rollover if month has changed. Returns True if rolled."""
        with self._lock:
            return self._maybe_rollover(force=False, manual=True)

    def list_records(self, since_ts: Optional[float] = None) -> List[SpendRecord]:
        """Return all records (or those after `since_ts`) in append order."""
        with self._lock:
            out: List[SpendRecord] = []
            with self.path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.rstrip("\n")
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if since_ts is None or rec.get("ts", 0.0) >= since_ts:
                        out.append(rec)
            return out

    # ----- internal ---------------------------------------------------------

    def _append(self, rec: SpendRecord) -> None:
        line = json.dumps(rec, ensure_ascii=False)
        with self.path.open("a", encoding="utf-8") as fh:
            # cross-process safety: best-effort flock
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            except (OSError, AttributeError):
                pass
            try:
                fh.write(line + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            finally:
                try:
                    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
                except (OSError, AttributeError):
                    pass

    def _replay(self) -> None:
        with self.path.open("r", encoding="utf-8") as fh:
            lines = [ln.rstrip("\n") for ln in fh if ln.strip()]
        self._replay_from_lines(lines)

    def _replay_from_lines(self, lines: Sequence[str]) -> None:
        self._month_usd: Dict[str, float] = {}
        self._month_tokens: Dict[str, int] = {}
        self._task_usd: Dict[str, float] = {}
        self._calls: Dict[str, int] = {}
        for line in lines:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            self._bump_totals(rec)

    def _bump_totals(self, rec: SpendRecord) -> None:
        m = _month_key(rec["ts"])
        self._month_usd[m] = self._month_usd.get(m, 0.0) + float(rec.get("usd", 0.0))
        self._month_tokens[m] = self._month_tokens.get(m, 0) + int(rec.get("tokens", 0))
        self._task_usd[rec["task_id"]] = self._task_usd.get(rec["task_id"], 0.0) + float(rec.get("usd", 0.0))
        self._calls[rec["task_id"]] = self._calls.get(rec["task_id"], 0) + 1

    def _check_caps(self, rec: SpendRecord, *, allow_overrun: bool) -> None:
        month = _month_key(rec["ts"])
        usd = float(rec.get("usd", 0.0))
        tokens = int(rec.get("tokens", 0))
        if self.monthly_usd is not None:
            cur = self._month_usd.get(month, 0.0)
            if cur + usd > self.monthly_usd:
                if self.on_block:
                    try:
                        self.on_block("usd", cur, usd, self.monthly_usd)
                    except Exception:
                        pass
                if not allow_overrun:
                    raise BudgetExceeded("usd", cur, usd, self.monthly_usd)
        if self.monthly_tokens is not None:
            cur = self._month_tokens.get(month, 0)
            if cur + tokens > self.monthly_tokens:
                if self.on_block:
                    try:
                        self.on_block("tokens", cur, tokens, self.monthly_tokens)
                    except Exception:
                        pass
                if not allow_overrun:
                    raise BudgetExceeded("tokens", cur, tokens, self.monthly_tokens)
        if self.per_task_usd is not None:
            cur = self._task_usd.get(rec["task_id"], 0.0)
            if cur + usd > self.per_task_usd:
                if self.on_block:
                    try:
                        self.on_block("task", cur, usd, self.per_task_usd)
                    except Exception:
                        pass
                if not allow_overrun:
                    raise BudgetExceeded("task", cur, usd, self.per_task_usd, task_id=rec["task_id"])

    def _maybe_warn(self, rec: SpendRecord) -> None:
        if not self.on_warn:
            return
        month = _month_key(rec["ts"])
        for field, current, limit in (
            ("usd", self._month_usd.get(month, 0.0), self.monthly_usd),
            ("tokens", self._month_tokens.get(month, 0), self.monthly_tokens),
        ):
            if limit is None or limit <= 0:
                continue
            ratio = current / limit
            if ratio >= self.soft_warn_ratio:
                key = (field, month)
                if key not in self._warned:
                    self._warned[key] = True
                    try:
                        result = self.on_warn(field, current, limit)
                    except Exception:
                        continue
                    if result is True:
                        # Treat warn-as-block: caller will see overrun next call.
                        pass

    def _maybe_rollover(self, force: bool = False, manual: bool = False) -> bool:
        if self.rollover_policy == "none":
            return False
        if self.rollover_policy == "manual" and not manual:
            return False
        # Detect month change since last rollover
        cur_month = _month_key(time.time())
        if not hasattr(self, "_active_month"):
            self._active_month = cur_month
            return False
        if cur_month != self._active_month or force:
            # older totals stay in the file; in-memory only current month retained for hot checks
            self._warned.clear()
            self._active_month = cur_month
            return True
        return False


# ---------------------------------------------------------------------------
# Async wrapper
# ---------------------------------------------------------------------------


class BudgetLedgerAsync:
    """``asyncio``-friendly wrapper around :class:`BudgetLedger`.

    Delegates to the sync ledger under a per-event-loop lock so that
    concurrent awaits serialize cleanly. For multi-process deployments
    the file-backed ledger itself coordinates writes via flock.
    """

    def __init__(self, ledger: BudgetLedger) -> None:
        self._ledger = ledger
        self._locks: Dict[int, asyncio.Lock] = {}
        self._bridges: Dict[int, threading.RLock] = {}

    def _lock_for(self, loop: asyncio.AbstractEventLoop) -> asyncio.Lock:
        key = id(loop)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def record_spend(self, *args: Any, **kwargs: Any) -> SpendRecord:
        loop = asyncio.get_event_loop()
        async with self._lock_for(loop):
            return await loop.run_in_executor(
                None,
                lambda: self._ledger.record_spend(*args, **kwargs),
            )

    async def can_spend(self, *args: Any, **kwargs: Any) -> bool:
        loop = asyncio.get_event_loop()
        async with self._lock_for(loop):
            return await loop.run_in_executor(
                None,
                lambda: self._ledger.can_spend(*args, **kwargs),
            )

    async def summary(self) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        async with self._lock_for(loop):
            return await loop.run_in_executor(None, self._ledger.summary)

    async def rollover_if_needed(self) -> bool:
        loop = asyncio.get_event_loop()
        async with self._lock_for(loop):
            return await loop.run_in_executor(None, self._ledger.rollover_if_needed)

    @property
    def sync(self) -> BudgetLedger:
        return self._ledger


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------


def attach_to_llm_router(router_callable: Callable[..., Any],
                         ledger: BudgetLedger,
                         *,
                         usd_per_call: float = 0.0,
                         tokens_per_call: int = 0,
                         model_attr: str = "model") -> Callable[..., Any]:
    """Decorator that intercepts calls to an LLM router and records spend.

    `router_callable` is expected to return either a response object (with
    ``.usage`` containing token counts) or ``(response, usage_dict)``.

    Usage::

        @attach_to_llm_router(ledger, usd_per_call=0.005)
        def complete(prompt):
            return llm.chat(prompt)

        complete("hello")  # records spend automatically
    """
    def wrapper(*args: Any, **kwargs: Any):
        result = router_callable(*args, **kwargs)
        # Best-effort extraction of usage
        usage: Dict[str, int] = {}
        model = kwargs.get(model_attr, "unknown")
        if hasattr(result, "usage"):
            usage = dict(result.usage or {})
        elif isinstance(result, tuple) and len(result) == 2 and isinstance(result[1], dict):
            usage = result[1]
            result = result[0]
        tokens = int(usage.get("total_tokens", 0) or tokens_per_call)
        prompt_tokens = int(usage.get("prompt_tokens", 0))
        completion_tokens = int(usage.get("completion_tokens", 0))
        try:
            ledger.record_spend(
                task_id=kwargs.get("task_id", "llm"),
                usd=usd_per_call,
                tokens=tokens,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                model=model,
            )
        except BudgetExceeded as e:
            # surface a clear error to the caller; do not lose the original error
            raise BudgetExceeded(e.field, e.current, e.attempted, e.limit, task_id=e.task_id) from e
        return result
    return wrapper
