from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional


ENV_FILE_BASENAME = ".env"
REPO_ROOT = Path(__file__).resolve().parents[2]


def _dotenv_candidate_paths() -> list[Path]:
    paths = [Path.cwd() / ENV_FILE_BASENAME, REPO_ROOT / ENV_FILE_BASENAME]
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in paths:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _load_dotenv_file(path: Path, *, override: bool) -> bool:
    if not path.exists():
        return False
    try:
        content = path.read_text(encoding="utf-8")
    except Exception:
        return False

    for raw_line in content.splitlines():
        line = str(raw_line or "").strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = str(key or "").strip()
        if not key:
            continue
        if (not override) and key in os.environ:
            continue
        value = str(value or "").strip()
        if (value.startswith('"') and value.endswith('"')) or (
            value.startswith("'") and value.endswith("'")
        ):
            value = value[1:-1]
        os.environ[key] = value
    return True


def load_dotenv(
    paths: Optional[Iterable[Path]] = None,
    *,
    override: bool = False,
) -> list[Path]:
    loaded: list[Path] = []
    for path in paths if paths is not None else _dotenv_candidate_paths():
        if _load_dotenv_file(Path(path), override=override):
            loaded.append(Path(path))
    return loaded
