#!/usr/bin/env python3
"""Run every task's test suite and print one summary table.

    python run_tests.py            # all tasks
    python run_tests.py task2 task4

Each suite runs from its own directory in its own process, because the tasks
are independent projects and several of them have a module named gateway.py.
No API key or network access is needed: every test uses local fakes.
"""
from __future__ import annotations

import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SUITES = {
    "task1": ["test_task1.py", "test_stdio.py"],
    "task2": [],
    "task3": [],
    "task4": [],
}


def run(task: str, args: list) -> tuple:
    start = time.perf_counter()
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", *args],
        cwd=ROOT / task, capture_output=True, text=True,
    )
    elapsed = time.perf_counter() - start
    summary = (proc.stdout.strip().splitlines() or ["(no output)"])[-1]
    counts = dict((kind, int(n)) for n, kind in re.findall(r"(\d+) (passed|failed|error|errors|skipped)", summary))
    return proc.returncode, counts, elapsed, proc.stdout + proc.stderr


def main(argv: list) -> int:
    tasks = argv or list(SUITES)
    unknown = [t for t in tasks if t not in SUITES]
    if unknown:
        print("unknown task(s): %s (choose from %s)" % (", ".join(unknown), ", ".join(SUITES)), file=sys.stderr)
        return 2

    print("Python %s\n" % sys.version.split()[0])
    print("%-6s  %7s  %6s  %7s" % ("task", "passed", "failed", "time"))
    passed_total, failures = 0, []
    for task in tasks:
        code, counts, elapsed, output = run(task, SUITES[task])
        failed = counts.get("failed", 0) + counts.get("error", 0) + counts.get("errors", 0)
        ok = code == 0
        passed_total += counts.get("passed", 0)
        print("%-6s  %7d  %6d  %6.1fs  %s" % (task, counts.get("passed", 0), failed, elapsed, "✅" if ok else "❌"))
        if not ok:
            failures.append((task, output))

    print("%-6s  %7d" % ("total", passed_total))
    for task, output in failures:
        print("\n" + "=" * 30 + " %s output " % task + "=" * 30)
        print(output)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
