from __future__ import annotations

import importlib.util
import os
import sqlite3
import tempfile
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


class LibsqlSessionTests(unittest.TestCase):
    def test_execute_calls_execute_and_commit(self) -> None:
        module = _load_sync_module()
        session_mod = module.session_lib

        calls: dict[str, list[str]] = {"execute": [], "commit": [], "close": []}

        class _FakeConn:
            def execute(self, sql_text: str) -> None:
                calls["execute"].append(sql_text)

            def commit(self) -> None:
                calls["commit"].append("1")

            def close(self) -> None:
                calls["close"].append("1")

        fake_conn = _FakeConn()
        connect_calls: list[tuple[object, ...]] = []

        class _FakeLibsql:
            @staticmethod
            def connect(*args, **kwargs):
                connect_calls.append((*args, kwargs))
                return fake_conn

        with patch.object(session_mod, "libsql", _FakeLibsql):
            session = session_mod.LibsqlSession(
                url="libsql://demo.turso.io",
                auth_token="t",
                timeout_sec=3,
            )
            with session:
                session.execute("SELECT 1;")
                session.execute("SELECT 2;")

        self.assertEqual(len(connect_calls), 1)
        self.assertEqual(calls["execute"], ["SELECT 1;", "SELECT 2;"])
        self.assertEqual(len(calls["commit"]), 2)
        self.assertEqual(len(calls["close"]), 1)


class MainEnvValidationTests(unittest.TestCase):
    def test_missing_env_exits_with_key_names(self) -> None:
        module = _load_sync_module()

        argv = [
            "sync_to_turso.py",
            "--db",
            "does-not-matter.sqlite3",
            "--turso-db",
            "xueqiu",
            "--full",
        ]
        with (
            patch("sys.argv", argv),
            patch.dict(os.environ, {}, clear=True),
            patch.object(module, "_load_dotenv"),
            self.assertRaises(SystemExit) as ctx,
        ):
            module.main()

        msg = str(ctx.exception)
        self.assertIn("XUEQIU_TURSO_DATABASE_URL", msg)
        self.assertIn("XUEQIU_TURSO_AUTH_TOKEN", msg)


class MainMetaUpdateTests(unittest.TestCase):
    def test_full_sync_does_not_set_meta_when_close_fails(self) -> None:
        module = _load_sync_module()
        with tempfile.NamedTemporaryFile(suffix=".sqlite3") as tmp:
            db_path = Path(tmp.name)
            conn = sqlite3.connect(str(db_path))
            conn.execute(
                """
                CREATE TABLE raw_records (
                  fetched_at_bj TEXT,
                  value TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO raw_records(fetched_at_bj, value) VALUES(?, ?)",
                ("2026-01-01T00:00:00+08:00", "v1"),
            )
            conn.commit()
            conn.close()

            class _FailCloseSession:
                def __init__(self, *_args, **_kwargs) -> None:
                    return None

                def __enter__(self) -> "_FailCloseSession":
                    return self

                def __exit__(self, exc_type, _exc, _tb) -> None:
                    self.close(raise_on_error=exc_type is None)

                def execute(self, _sql_text: str) -> None:
                    return None

                def close(self, *, raise_on_error: bool = True) -> None:
                    if raise_on_error:
                        raise RuntimeError("close failed")

            argv = [
                "sync_to_turso.py",
                "--db",
                str(db_path),
                "--turso-db",
                "demo-db",
                "--full",
                "--include",
                "raw_records",
            ]
            env = {
                "DEMO_DB_TURSO_DATABASE_URL": "libsql://demo.turso.io",
                "DEMO_DB_TURSO_AUTH_TOKEN": "t",
            }
            with (
                patch("sys.argv", argv),
                patch.dict(os.environ, env, clear=True),
                patch.object(module, "_load_dotenv"),
                patch.object(module.session_lib, "LibsqlSession", _FailCloseSession),
                patch.object(module.sync_lib, "_set_meta") as set_meta,
                self.assertRaises(RuntimeError),
            ):
                module.main()

            set_meta.assert_not_called()


if __name__ == "__main__":
    unittest.main()
