# tiny-budget-ledger

> File-backed per-task spend ledger for autonomous AI agents. Stdlib only.

[![Python](https://img.shields.io/badge/python-3.8%2B-blue.svg)](#)
[![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen.svg)](#)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/tests-16%2F16%20passing-success.svg)](#testing)

The smallest way to enforce **"every agent session must cost less than it
earns."** Tracks USD and token spend against per-task and monthly caps in a
crash-safe append-only JSONL ledger, and refuses new spends that would push
a budget over its limit. Thread-safe and asyncio-friendly.

```python
from tiny_budget_ledger import BudgetLedger

led = BudgetLedger(
    path="/var/lib/openclaw/budget.jsonl",
    monthly_usd=50.0,
    monthly_tokens=20_000_000,
    per_task_usd=5.0,
)

led.record_spend(task_id="bounty-hunt", usd=0.05, tokens=4200,
                 model="kimi-k2.6", note="scanned 22 issues")

if not led.can_spend(usd=2.0, tokens=200_000):
    raise SystemExit("Aborted: budget cap would be exceeded")

print(led.summary())
# {'month': '2026-09', 'month_usd_spent': 0.05, 'month_tokens_spent': 4200,
#  'calls_total': 1, 'month_usd_remaining': 49.95, 'month_usd_pct': 0.001,
#  'month_tokens_remaining': 19995800, 'month_tokens_pct': 0.00021, ...}
```

---

## Why?

OpenClaw's core operating principle (SOUL.md) is **every session must produce
more value than it consumes**. To enforce that principle we need:

1. **A ledger that survives crashes** (append-only JSONL, `fsync` per write).
2. **Per-task and global budgets** in USD and tokens.
3. **A small sync `BudgetLedger`** and an **`BudgetLedgerAsync`** wrapper.
4. **Callbacks** for warn / block / monthly rollover.
5. **Hooks** for `tiny-cost-tracker` and `tiny-llm-router`.

Cron jobs, multi-tenant agents, per-model-per-task budget gates — every
autonomous surface needs this. `tiny-budget-ledger` is one file, ~440 lines,
zero deps.

---

## Installation

```bash
# pip (when published)
pip install tiny-budget-ledger

# Or just copy the single file
curl -O https://raw.githubusercontent.com/hussain-alsaibai/tiny-budget-ledger/main/tiny_budget_ledger.py
```

Requires **Python 3.8+**. No external packages.

---

## Quick start

```python
from tiny_budget_ledger import BudgetLedger, BudgetExceeded

led = BudgetLedger(
    path="budget.jsonl",
    monthly_usd=10.0,
    monthly_tokens=1_000_000,
    per_task_usd=0.50,
)

# record spend
led.record_spend(
    task_id="scrape-news",
    usd=0.003, tokens=512,
    prompt_tokens=480, completion_tokens=32,
    model="kimi-k2.6",
    note="summarized 5 rss headlines",
    tags=["ingest", "news"],
)

# pre-check before an expensive call
if not led.can_spend(usd=0.20, tokens=20_000, task_id="deep-summary"):
    raise RuntimeError("Over budget — switching to local model")

# summary for logging / dashboards
print(led.summary())
```

---

## Features

- **Crash-safe append-only JSONL** with `fsync` and best-effort cross-process `flock`.
- **Monthly USD cap**, **monthly token cap**, **per-task USD cap** — any combination.
- **Soft warning callback** (`on_warn`) when a cap crosses a configurable ratio (default 80 %).
- **Block callback** (`on_block`) invoked just before a `BudgetExceeded` raise.
- **`can_spend()`** pre-check that never touches the ledger.
- **`allow_overrun=True`** to log past-cap spends without raising.
- **`drain()`** removes records older than the current month, returns them for reports.
- **`rollover_if_needed()`** for manual monthly rollover control.
- **`BudgetLedgerAsync`** thin asyncio wrapper that delegates to sync under per-loop lock.
- **`attach_to_llm_router()`** decorator that auto-records `usage` from any LLM response object.
- **`list_records(since_ts=...)`** for audit queries.
- **Zero dependencies** — pure stdlib.
- **Full type hints**, **MIT licensed**, 16/16 tests passing.

---

## API at a glance

### `BudgetLedger(path, monthly_usd=None, monthly_tokens=None, per_task_usd=None, on_warn=None, on_block=None, rollover_policy="monthly", soft_warn_ratio=0.8)`

| Method | Returns | Description |
|---|---|---|
| `record_spend(task_id, usd=0.0, tokens=0, prompt_tokens=0, completion_tokens=0, model="unknown", note="", tags=None, allow_overrun=False)` | `SpendRecord` | Append a record. Raises `BudgetExceeded` unless `allow_overrun=True`. |
| `can_spend(usd=0.0, tokens=0, task_id=None)` | `bool` | Pre-check; never writes. |
| `summary()` | `dict` | Current month totals + remaining capacity + top-5 tasks. |
| `drain()` | `list[SpendRecord]` | Move records older than current month off disk; returns them. |
| `rollover_if_needed()` | `bool` | Manual month rollover trigger. |
| `list_records(since_ts=None)` | `list[SpendRecord]` | Audit query. |

### Exceptions

| Class | Raised when |
|---|---|
| `BudgetError` | Base / constructor validation. |
| `BudgetExceeded` | A `record_spend` would exceed a cap. Has `.field`, `.current`, `.attempted`, `.limit`, `.task_id` attributes. |

### Helpers

- `BudgetLedgerAsync(ledger)` — `record_spend` / `can_spend` / `summary` / `rollover_if_needed` as `await`-ables.
- `attach_to_llm_router(router_callable, ledger, *, usd_per_call=0.0, tokens_per_call=0, model_attr="model")` — wraps a router function to record spend automatically from `.usage` attribute.

---

## Use cases

### 1. Cron with hard cost ceiling

```python
led = BudgetLedger(path="/var/lib/openclaw/budget.jsonl",
                   monthly_usd=200.0, per_task_usd=5.0)

try:
    led.record_spend(task_id="daily-bounty-work",
                     usd=compute_cost(), tokens=compute_tokens(),
                     model="kimi-k2.6",
                     note=f"targeted issue {n}")
except BudgetExceeded as e:
    abort_and_email_admin(field=e.field, limit=e.limit)
```

### 2. Per-model-per-task budget gate

```python
led = BudgetLedger(path="budget.jsonl",
                   monthly_tokens=5_000_000)

if led.can_spend(tokens=200_000, task_id="vision-ocr"):
    response = call_vision(image)        # gpt-image-style model
    led.record_spend(task_id="vision-ocr",
                     tokens=response.usage.total_tokens,
                     usd=0.18, model="vision-pro")
```

### 3. Wrap any LLM call

```python
@attach_to_llm_router(ledger=budget, usd_per_call=0.002)
def ask(prompt: str, model: str = "kimi-k2.6") -> str:
    return openai.ChatCompletion.create(model=model,
                                        messages=[{"role":"user","content":prompt}]).choices[0].message.content
```

### 4. Asyncio fleet

```python
import asyncio
from tiny_budget_ledger import BudgetLedger, BudgetLedgerAsync

async def worker(i, ledger):
    a = BudgetLedgerAsync(ledger)
    if await a.can_spend(usd=0.10, tokens=10_000, task_id=f"agent-{i}"):
        await a.record_spend(task_id=f"agent-{i}",
                             usd=0.08, tokens=8200,
                             model="kimi-k2.6")

ledger = BudgetLedger(path="fleet.jsonl", monthly_usd=20.0)
await asyncio.gather(*[worker(i, ledger) for i in range(20)])
```

---

## Comparison vs alternatives

| Library | External deps | File-backed | Per-task caps | Async | Notes |
|---|---|---|---|---|---|
| **tiny-budget-ledger** | **0** | ✅ JSONL + fsync | ✅ | ✅ wrapper | ~440 LOC, stdlib |
| `tally` (npm) | 0 | ❌ in-memory | ⚠️ via plugin | ✅ | Node-only |
| `langchain-core` budget | many | ❌ | ❌ | ⚠️ | Heavy |
| Custom CSV | 0 | ⚠️ manual | manual | manual | You build it |

**TL;DR** — if you ship Python and want the smallest budget guard, use
`tiny-budget-ledger`. If you need SQLite/Postgres backend, fork it and add a
custom sink — the public API is intentionally minimal.

---

## Testing

```bash
python3 -m unittest test_tiny_budget_ledger.py -v
```

Current: **16 / 16 passing.** Covers: persistence, USD cap, token cap, per-task
cap, allow_overrun, on_warn (one-shot), on_block, drain, list_records,
append-only growth, async wrapper, decorator integration.

---

## See also (OpenClaw ecosystem)

- [`tiny-cost-tracker`](https://github.com/hussain-alsaibai/tiny-cost-tracker) — real-time cost attribution.
- [`tiny-budget`](https://github.com/hussain-alsaibai/tiny-budget) (Node.js) — runtime budget enforcement.
- [`tiny-llm-router`](https://github.com/hussain-alsaibai/tiny-llm-router) — role-based model routing.
- [`tiny-llm`](https://github.com/hussain-alsaibai/tiny-llm) — transport adapter.

---

## License

MIT — see [LICENSE](LICENSE).
