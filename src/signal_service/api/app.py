from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from signal_service.config import load_settings
from signal_service.demo.generate import build_demo
from signal_service.eval.harness import run_eval
from signal_service.grok.client import LiveGrokClient, MockGrokClient
from signal_service.orchestration.pipeline import _guard, run_pipeline
from signal_service.pipeline.ingest import ingest
from signal_service.pipeline.propagate import PipelineError
from signal_service.safety.cost import CircuitBreakerOpen
from signal_service.warehouse.duckdb import DuckDBWarehouse


class IngestBody(BaseModel):
    batch: str = "new"
    path: str | None = None
    hours: int | None = Field(default=None, ge=1, le=48)

_STATE: dict = {}


def _client(settings):
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


def create_app(warehouse: DuckDBWarehouse | None = None) -> FastAPI:
    settings = load_settings()
    app = FastAPI(title="signal-service", version="0.1.0")
    app.state.settings = settings
    app.state.warehouse = warehouse

    def warehouse_or_create() -> DuckDBWarehouse:
        if app.state.warehouse is not None:
            return app.state.warehouse
        path = Path(os.environ.get("SIGNAL_DUCKDB_PATH", "data/warehouse/demo.duckdb"))
        if not path.exists():
            build_demo(path)
        app.state.warehouse = DuckDBWarehouse(path)
        return app.state.warehouse

    @app.get("/health")
    def health():
        key = bool(os.environ.get("XAI_API_KEY"))
        return {
            "status": "ok",
            "warehouse": "duckdb",
            "grok": "live" if key else "mock",
        }

    @app.post("/v1/pipeline/run")
    def pipeline_run(simulate_build_approval: bool = True):
        try:
            return run_pipeline(
                warehouse_or_create(),
                _client(settings),
                settings,
                simulate_build_approval=simulate_build_approval,
            )
        except CircuitBreakerOpen as exc:
            raise HTTPException(status_code=429, detail=str(exc)) from exc

    @app.get("/v1/eval")
    def eval_route():
        return run_eval()

    @app.post("/v1/ingest")
    def ingest_route(body: IngestBody | None = None):
        payload = body or IngestBody()
        if payload.batch == "file" and payload.path:
            from signal_service.paths import data_path

            root = data_path().resolve()
            candidate = Path(payload.path)
            if not candidate.is_absolute():
                candidate = (root / candidate).resolve()
            else:
                candidate = candidate.resolve()
            if root != candidate and root not in candidate.parents:
                raise HTTPException(status_code=400, detail="batch file must live under data/")
            file_path = str(candidate)
        elif payload.batch == "file":
            raise HTTPException(status_code=400, detail="file ingest requires path")
        else:
            file_path = None
        try:
            report = ingest(
                warehouse_or_create(),
                batch=payload.batch,
                path=file_path,
                hours=payload.hours,
            )
        except PipelineError as exc:
            raise HTTPException(status_code=500, detail={"layer": exc.layer, "error": exc.message}) from exc
        except (FileNotFoundError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if report.get("status") == "failed":
            raise HTTPException(status_code=500, detail=report)
        return report

    return app
