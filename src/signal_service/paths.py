from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    here = Path(__file__).resolve()
    for candidate in [here, *here.parents]:
        if (candidate / "pyproject.toml").exists() and (candidate / "data").exists():
            return candidate
    return Path.cwd()


def data_path(*parts: str) -> Path:
    return repo_root().joinpath("data", *parts)


def sql_path(*parts: str) -> Path:
    return repo_root().joinpath("sql", *parts)
