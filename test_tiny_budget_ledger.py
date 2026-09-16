"""Tests for tiny_budget_ledger.

Run with::

    python -m unittest test_tiny_budget_ledger.py -v

All tests use a tmp_path fixture so nothing leaks to the real filesystem.
"""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from pathlib import Path

from tiny_budget_ledger import (
    BudgetError,
    BudgetExceeded,
    BudgetLedger,
    BudgetLedgerAsync,
    SpendRecord,
)


class TestSpendRecord(unittest.TestCase):
    def test_basic_shape(self) -> None:
        rec = SpendRecord.now(task_id="t", usd=0.01, tokens=10, model="kimi-k2.6")
        self.assertEqual(rec["task_id"], "t")
        self.assertAlmostEqual(rec["usd"], 0.01)
        self.assertEqual(rec["tokens"], 10)
        self.assertEqual(rec["model"], "kimi-k2.6")
        self.assertIsInstance(rec["ts"], float)
        self.assertGreater(rec["ts"], time.time() - 10)


class TestBudgetLedger(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.path = self.tmp / "budget.jsonl"

    def _led(self, **kw) -> BudgetLedger:
        kw.setdefault("path", self.path)
        return BudgetLedger(**kw)

    def test_first_record_persists(self) -> None:
        led = self._led(monthly_usd=1.0)
        led.record_spend(task_id="t1", usd=0.10, tokens=100)
        self.assertTrue(self.path.exists())
        lines = self.path.read_text().splitlines()
        self.assertEqual(len(lines), 1)
        rec = json.loads(lines[0])
        self.assertEqual(rec["task_id"], "t1")
        self.assertEqual(rec["usd"], 0.10)

    def test_can_spend_under_cap(self) -> None:
        led = self._led(monthly_usd=1.0, monthly_tokens=1000)
        self.assertTrue(led.can_spend(usd=0.5, tokens=500))
        led.record_spend(task_id="t", usd=0.5, tokens=500)
        self.assertTrue(led.can_spend(usd=0.4, tokens=400))
        self.assertFalse(led.can_spend(usd=1.0, tokens=0))

    def test_usd_cap_raises(self) -> None:
        led = self._led(monthly_usd=1.0)
        led.record_spend(task_id="t", usd=0.9)
        with self.assertRaises(BudgetExceeded) as ctx:
            led.record_spend(task_id="t", usd=0.5)
        self.assertEqual(ctx.exception.field, "usd")

    def test_token_cap_raises(self) -> None:
        led = self._led(monthly_tokens=1000)
        led.record_spend(task_id="t", tokens=900)
        with self.assertRaises(BudgetExceeded) as ctx:
            led.record_spend(task_id="t", tokens=200)
        self.assertEqual(ctx.exception.field, "tokens")

    def test_per_task_cap(self) -> None:
        led = self._led(per_task_usd=0.5)
        led.record_spend(task_id="t1", usd=0.4)
        self.assertTrue(led.can_spend(task_id="t1", usd=0.05))
        self.assertFalse(led.can_spend(task_id="t1", usd=0.5))
        with self.assertRaises(BudgetExceeded) as ctx:
            led.record_spend(task_id="t1", usd=0.5)
        self.assertEqual(ctx.exception.task_id, "t1")

    def test_allow_overrun(self) -> None:
        led = self._led(monthly_usd=1.0)
        led.record_spend(task_id="t", usd=0.9)
        rec = led.record_spend(task_id="t", usd=0.5, allow_overrun=True)
        self.assertEqual(rec["usd"], 0.5)
        s = led.summary()
        self.assertEqual(s["month_usd_spent"], 1.4)

    def test_summary_remaining(self) -> None:
        led = self._led(monthly_usd=10.0, monthly_tokens=100_000)
        led.record_spend(task_id="t", usd=2.0, tokens=20_000)
        s = led.summary()
        self.assertEqual(s["month_usd_spent"], 2.0)
        self.assertEqual(s["month_usd_remaining"], 8.0)
        self.assertEqual(s["month_tokens_spent"], 20_000)

    def test_on_warn_called_once_per_period(self) -> None:
        calls: list[tuple[str, float, float]] = []
        led = self._led(
            monthly_usd=1.0,
            soft_warn_ratio=0.5,
            on_warn=lambda f, c, l: calls.append((f, c, l)) or None,
        )
        led.record_spend(task_id="t", usd=0.6)  # over 50%
        led.record_spend(task_id="t", usd=0.05)
        self.assertGreaterEqual(len(calls), 1)
        usd_warns = [c for c in calls if c[0] == "usd"]
        self.assertEqual(len(usd_warns), 1)

    def test_on_block_called(self) -> None:
        blocks: list[tuple[str, float, float, float]] = []
        led = self._led(
            monthly_usd=1.0,
            on_block=lambda f, c, a, l: blocks.append((f, c, a, l)),
        )
        led.record_spend(task_id="t", usd=0.9)
        try:
            led.record_spend(task_id="t", usd=0.5)
        except BudgetExceeded:
            pass
        self.assertTrue(any(b[0] == "usd" for b in blocks))

    def test_drain_clears_old_records(self) -> None:
        led = self._led(monthly_usd=1.0)
        # simulate prior-month records (artificially backdate ts)
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps({"ts": time.time() - 60 * 60 * 24 * 90,
                                 "task_id": "old", "usd": 0.5,
                                 "tokens": 100, "prompt_tokens": 50,
                                 "completion_tokens": 50, "model": "x",
                                 "note": "", "tags": []}) + "\n")
        led._replay()
        led.record_spend(task_id="now", usd=0.1)
        drained = led.drain()
        self.assertGreaterEqual(len(drained), 1)
        self.assertEqual(drained[0]["task_id"], "old")
        # current-month record survives
        kept = [json.loads(ln) for ln in self.path.read_text().splitlines() if ln]
        self.assertTrue(any(r["task_id"] == "now" for r in kept))

    def test_list_records(self) -> None:
        led = self._led(monthly_usd=10.0)
        led.record_spend(task_id="a", usd=0.1)
        led.record_spend(task_id="b", usd=0.1)
        recs = led.list_records()
        self.assertEqual(len(recs), 2)
        self.assertEqual([r["task_id"] for r in recs], ["a", "b"])

    def test_append_only_file_grows(self) -> None:
        led = self._led(monthly_usd=10.0)
        for i in range(50):
            led.record_spend(task_id=f"t{i}", usd=0.001)
        size = self.path.stat().st_size
        self.assertGreater(size, 0)

    def test_constructor_validates(self) -> None:
        with self.assertRaises(BudgetError):
            BudgetLedger(path=self.path, rollover_policy="bogus")
        with self.assertRaises(BudgetError):
            BudgetLedger(path=self.path, soft_warn_ratio=1.5)


class TestBudgetLedgerAsync(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = Path(tempfile.mkdtemp())
        self.path = self.tmp / "budget.jsonl"

    def test_async_record(self) -> None:
        import asyncio

        async def run() -> None:
            led = BudgetLedger(path=self.path, monthly_usd=10.0)
            a = BudgetLedgerAsync(led)
            r = await a.record_spend(task_id="t", usd=0.5, tokens=100)
            self.assertEqual(r["task_id"], "t")
            ok = await a.can_spend(usd=0.5, tokens=100)
            self.assertTrue(ok)
            s = await a.summary()
            self.assertEqual(s["month_usd_spent"], 0.5)
            self.assertEqual(s["month_tokens_spent"], 100)

        asyncio.run(run())


class TestAttachToRouter(unittest.TestCase):
    def test_decorator_records(self) -> None:
        from tiny_budget_ledger import attach_to_llm_router

        tmp = Path(tempfile.mkdtemp())
        led = BudgetLedger(path=tmp / "b.jsonl", monthly_usd=10.0)

        def fake_complete_inner(prompt: str, model: str = "kimi-k2.6"):
            class R:
                usage = {"prompt_tokens": 100, "completion_tokens": 50, "total_tokens": 150}
            return R()

        wrapped = attach_to_llm_router(fake_complete_inner, led)
        wrapped("hi", model="kimi-k2.6")
        s = led.summary()
        self.assertEqual(s["month_tokens_spent"], 150)


if __name__ == "__main__":
    unittest.main(verbosity=2)
