from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


def _load_sync_module():
    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "sync_to_turso.py"
    spec = importlib.util.spec_from_file_location("sync_to_turso", script_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load script: {script_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeStdin:
    def __init__(self, *, fail_on_write: bool = False) -> None:
        self._fail_on_write = bool(fail_on_write)
        self.writes: list[str] = []
        self.closed = False

    def write(self, data: str) -> int:
        if self._fail_on_write:
            raise BrokenPipeError("simulated broken pipe")
        self.writes.append(str(data))
        return len(data)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        self.closed = True


class _FakeProcess:
    def __init__(self, *, fail_on_write: bool = False, exited: bool = False) -> None:
        self.stdin = _FakeStdin(fail_on_write=fail_on_write)
        self._exited = bool(exited)
        self.returncode = 1 if exited else None

    def poll(self) -> int | None:
        return self.returncode if self._exited else None

    def wait(self, _timeout: float | None = None) -> int:
        if self.returncode is None:
            self.returncode = 0
        return int(self.returncode)

    def terminate(self) -> None:
        self._exited = True
        self.returncode = 143


class TursoShellSessionTests(unittest.TestCase):
    def test_reuses_single_process_for_multiple_execute(self) -> None:
        module = _load_sync_module()
        fake_proc = _FakeProcess()
        with patch.object(module.subprocess, "Popen", return_value=fake_proc) as popen:
            with module.TursoShellSession(
                turso_db="demo-db",
                shell_cmd=None,
                reconnect_attempts=1,
            ) as session:
                session.execute("BEGIN; SELECT 1; COMMIT;\n")
                session.execute("BEGIN; SELECT 2; COMMIT;\n")

        self.assertEqual(popen.call_count, 1)
        self.assertIn("BEGIN; SELECT 1; COMMIT;\n", fake_proc.stdin.writes)
        self.assertIn("BEGIN; SELECT 2; COMMIT;\n", fake_proc.stdin.writes)

    def test_reconnects_once_and_retries_current_sql(self) -> None:
        module = _load_sync_module()
        failed_proc = _FakeProcess(fail_on_write=True, exited=True)
        ok_proc = _FakeProcess()
        with patch.object(
            module.subprocess, "Popen", side_effect=[failed_proc, ok_proc]
        ) as popen:
            with module.TursoShellSession(
                turso_db="demo-db",
                shell_cmd=None,
                reconnect_attempts=1,
            ) as session:
                session.execute("BEGIN; INSERT INTO t VALUES (1); COMMIT;\n")

        self.assertEqual(popen.call_count, 2)
        self.assertIn(
            "BEGIN; INSERT INTO t VALUES (1); COMMIT;\n",
            ok_proc.stdin.writes,
        )


if __name__ == "__main__":
    unittest.main()
