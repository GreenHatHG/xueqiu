from __future__ import annotations

from typing import Any, Protocol

try:
    import libsql  # type: ignore
except ImportError:  # pragma: no cover
    libsql = None  # type: ignore


DEFAULT_CONNECT_TIMEOUT_SEC = 60
DEFAULT_ACK_TIMEOUT_SEC = 300


class TursoSession(Protocol):
    def __enter__(self) -> "TursoSession": ...

    def __exit__(self, exc_type: Any, exc: Any, _tb: Any) -> None: ...

    def execute(self, sql_text: str) -> None: ...

    def close(self, *, raise_on_error: bool = True) -> None: ...


class LibsqlSession:
    def __init__(self, *, url: str, auth_token: str, timeout_sec: int) -> None:
        self._url = str(url or "").strip()
        self._auth_token = str(auth_token or "").strip()
        self._timeout_sec = max(1, int(timeout_sec))
        self._conn = None

    def __enter__(self) -> "LibsqlSession":
        libsql_mod = libsql
        if libsql_mod is None:
            raise RuntimeError(
                "缺少 libsql，请先安装：uv sync（或 pip install libsql）"
            )
        self._conn = libsql_mod.connect(
            self._url,
            auth_token=self._auth_token,
            timeout=float(self._timeout_sec),
        )
        return self

    def __exit__(self, exc_type, _exc, _tb) -> None:
        self.close(raise_on_error=exc_type is None)

    def execute(self, sql_text: str) -> None:
        if not sql_text.strip():
            return
        conn = self._conn
        if conn is None:
            raise RuntimeError("还没连接 Turso")
        conn.execute(sql_text)
        conn.commit()

    def close(self, *, raise_on_error: bool = True) -> None:
        conn = self._conn
        self._conn = None
        if conn is None:
            return
        try:
            conn.close()
        except Exception:
            if raise_on_error:
                raise
