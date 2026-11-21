from __future__ import annotations

from signal_service.eval.harness import MIN_ROWS, run_eval


def test_golden_set_clears_the_threshold():
    report = run_eval()
    assert report["n"] >= MIN_ROWS
    assert report["n"] >= 50
    assert report["passed"] is True
    assert report["accuracy"]["topic"] >= 0.85
    assert report["schema_errors"] == 0
