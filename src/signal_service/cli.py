from __future__ import annotations

import argparse
import json
from pathlib import Path

from signal_service.config import load_settings
from signal_service.demo.generate import build_demo
from signal_service.demo.snowflake_deploy import deploy_sandbox
from signal_service.eval.harness import run_eval
from signal_service.grok.client import LiveGrokClient, MockGrokClient
from signal_service.orchestration.pipeline import _guard, run_pipeline
from signal_service.warehouse.duckdb import DuckDBWarehouse
from signal_service.warehouse.snowflake import SnowflakeIPBlocked, SnowflakeWarehouse, token_source


def _client(settings):
    import os

    key = os.environ.get("XAI_API_KEY", "")
    guard = _guard(settings)
    if key:
        return LiveGrokClient(
            key,
            guard,
            model=settings.grok_model,
            timeout=settings.grok_timeout_seconds,
            max_retries=settings.grok_max_retries,
            voice_id=settings.voice_id,
        )
    return MockGrokClient(guard)


def _json_default(value):
    if hasattr(value, "isoformat"):
        return value.isoformat()
    raise TypeError(type(value))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="signal-service")
    sub = parser.add_subparsers(dest="command", required=True)

    generate = sub.add_parser("generate", help="Write the demo DuckDB warehouse")
    generate.add_argument("--out", default="data/warehouse/demo.duckdb")

    demo = sub.add_parser("run-demo", help="Run detection, diagnosis, signals, and the brief")
    demo.add_argument("--db", default="data/warehouse/demo.duckdb")
    demo.add_argument("--out", default="artifacts/demo-run.json")
    demo.add_argument("--rebuild", action="store_true")
    demo.add_argument("--backend", choices=("duckdb", "snowflake"), default="duckdb")

    sub.add_parser("deploy-snowflake", help="Create MCP_DB.SIGNAL_SANDBOX and load the demo dataset")

    sub.add_parser("eval", help="Score the fallback classifier on the golden set")

    serve = sub.add_parser("serve", help="Serve the HTTP API")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.add_argument("--db", default="data/warehouse/demo.duckdb")

    ingest = sub.add_parser("ingest", help="Land a batch in bronze and propagate it")
    ingest.add_argument("--batch", nargs="+", default=["new"], help="new, seed, or: file PATH")
    ingest.add_argument("--db", default="data/warehouse/demo.duckdb")
    ingest.add_argument("--backend", choices=("duckdb", "snowflake"), default="duckdb")
    ingest.add_argument("--hours", type=int, default=None)
    ingest.add_argument(
        "--rerun",
        action="store_true",
        help="Reprocess this batch_id even if gold was marked success. Does not DROP or TRUNCATE.",
    )

    rerun = sub.add_parser(
        "rerun",
        help="Reprocess one batch_id without DROP or TRUNCATE, even after a false gold success",
    )
    rerun.add_argument("batch_id")
    rerun.add_argument("--db", default="data/warehouse/demo.duckdb")
    rerun.add_argument("--backend", choices=("duckdb", "snowflake"), default="duckdb")

    args = parser.parse_args(argv)
    settings = load_settings()

    if args.command == "generate":
        path = build_demo(args.out)
        print(path)
        return 0

    if args.command == "eval":
        report = run_eval()
        print(json.dumps(report, indent=2))
        return 0 if report["passed"] else 1

    if args.command == "deploy-snowflake":
        return _deploy_snowflake()

    if args.command == "run-demo":
        if args.backend == "snowflake":
            warehouse = SnowflakeWarehouse()
        else:
            db_path = Path(args.db)
            if args.rebuild or not db_path.exists():
                build_demo(db_path)
            warehouse = DuckDBWarehouse(db_path)
        try:
            result = run_pipeline(warehouse, _client(settings), settings, simulate_build_approval=True)
        except SnowflakeIPBlocked as exc:
            _report_ip_block(exc)
            return 2
        finally:
            warehouse.close()
        return _write_demo(result, Path(args.out))

    if args.command == "ingest":
        return _ingest(args)

    if args.command == "rerun":
        return _rerun(args)

    if args.command == "serve":
        import uvicorn

        from signal_service.api.app import create_app

        db_path = Path(args.db)
        if not db_path.exists():
            build_demo(db_path)
        warehouse = DuckDBWarehouse(db_path)
        uvicorn.run(create_app(warehouse), host=args.host, port=args.port)
        return 0

    return 2


def _open_warehouse(args):
    if args.backend == "snowflake":
        return SnowflakeWarehouse()
    db_path = Path(args.db)
    if not db_path.exists():
        build_demo(db_path)
    return DuckDBWarehouse(db_path)


def _ingest(args) -> int:
    from signal_service.pipeline.ingest import ingest

    kind, path = _parse_batch(args.batch)
    warehouse = _open_warehouse(args)
    try:
        report = ingest(warehouse, batch=kind, path=path, hours=args.hours, rerun=args.rerun)
    except SnowflakeIPBlocked as exc:
        _report_ip_block(exc)
        return 2
    finally:
        warehouse.close()
    print(json.dumps(report, indent=2, default=_json_default))
    return 0 if report.get("status") in {"success", "noop", "landed"} else 1


def _rerun(args) -> int:
    from signal_service.pipeline.ingest import rerun_batch

    warehouse = _open_warehouse(args)
    try:
        report = rerun_batch(warehouse, args.batch_id)
    except SnowflakeIPBlocked as exc:
        _report_ip_block(exc)
        return 2
    finally:
        warehouse.close()
    print(json.dumps(report, indent=2, default=_json_default))
    return 0 if report.get("status") in {"success", "noop", "landed"} else 1


def _parse_batch(values: list[str]) -> tuple[str, str | None]:
    if not values or values == ["new"]:
        return "new", None
    if values == ["seed"]:
        return "seed", None
    if values[0] == "file" and len(values) == 2:
        return "file", values[1]
    raise SystemExit("usage: ingest --batch new | ingest --batch seed | ingest --batch file PATH")


def _deploy_snowflake() -> int:
    warehouse = SnowflakeWarehouse()
    if not warehouse.token:
        print("SNOWFLAKE_PAT_TOKEN is not set")
        _print_public_ip()
        return 2
    try:
        report = deploy_sandbox(warehouse)
    except SnowflakeIPBlocked as exc:
        _report_ip_block(exc)
        return 2
    finally:
        warehouse.close()
    report["token_source"] = token_source()
    print(json.dumps(report, indent=2))
    return 0


def _report_ip_block(exc: SnowflakeIPBlocked) -> None:
    print("Snowflake rejected the connection from this host.")
    _print_public_ip()
    print(str(exc)[:300])


def _print_public_ip() -> None:
    import urllib.request

    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=15) as response:
            address = response.read().decode("utf-8").strip()
    except Exception:
        address = "unavailable"
    print(f"public_ip={address}")


def _write_demo(result: dict, destination: Path) -> int:
    evaluation = run_eval()
    result["eval"] = evaluation
    result["verify_pr"] = {
        "merge": False,
        "eval_topic_accuracy": evaluation["accuracy"]["topic"],
        "eval_threshold": evaluation["topic_threshold"],
        "schema_diff": "signals.ticket_topics, signals.external_events, signals.spend_log",
        "cost_estimate_usd": result["spend"]["live_estimate_usd"]["total_usd"],
        "note": "tests_passed is reported by CI. This command does not mark tests as passed.",
    }
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2, default=_json_default), encoding="utf-8")
    markdown = Path(str(destination).replace(".json", ".md"))
    markdown.write_text(_render(result), encoding="utf-8")
    print(markdown)
    if "case_card" not in result:
        return 1
    return 0 if evaluation["passed"] else 1


def _render(result: dict) -> str:
    sections = ["# Demo run", ""]
    if "case_card" not in result:
        sections.append("No case was opened.")
        sections.append("")
        sections.append("```json")
        sections.append(json.dumps({key: result.get(key) for key in ("error", "alerts")}, indent=2))
        sections.append("```")
        return "\n".join(sections) + "\n"
    sections.append("## Case Card")
    sections.append("")
    sections.append("```json")
    sections.append(json.dumps(result["case_card"], indent=2))
    sections.append("```")
    sections.append("")
    sections.append("## Diagnosis before signals")
    sections.append("")
    sections.append("```json")
    sections.append(json.dumps(result["diagnosis_before_signals"], indent=2))
    sections.append("```")
    sections.append("")
    sections.append("## Diagnosis Pack")
    pack = {
        "hypotheses": result["diagnosis_pack"]["hypotheses"],
        "capability_gap": result["diagnosis_pack"]["capability_gap"],
        "external_signals": result["diagnosis_pack"]["external_signals"],
        "evidence": result["diagnosis_pack"]["evidence"],
    }
    sections.append("")
    sections.append("```json")
    sections.append(json.dumps(pack, indent=2))
    sections.append("```")
    sections.append("")
    sections.append("## Brief")
    sections.append("")
    sections.append(result["brief"]["markdown"])
    sections.append("## Voice script")
    sections.append("")
    sections.append(result["brief"]["voice_script"])
    sections.append("")
    return "\n".join(sections)


if __name__ == "__main__":
    raise SystemExit(main())
