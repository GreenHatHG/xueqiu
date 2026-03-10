from __future__ import annotations

from pathlib import Path

BASE_URL = "https://xueqiu.com"
BEIJING_TIMEZONE_NAME = "Asia/Shanghai"

DEFAULT_USER_DATA_DIR = Path(".playwright") / "user-data"
DEFAULT_OUTPUT_DIR = Path("data")
DEFAULT_DB_BASENAME = "xueqiu_{user_id}.sqlite3"
DEFAULT_BATCH_DB_BASENAME = "xueqiu_batch.sqlite3"
DEFAULT_BATCH_USER_COOLDOWN_SEC = 60.0

# Anti-abuse defaults: conservative on purpose.
DEFAULT_MIN_DELAY_SEC = 1.2
DEFAULT_JITTER_SEC = 0.6
DEFAULT_MAX_RETRIES = 2
DEFAULT_MAX_CONSECUTIVE_BLOCKS = 3
DEFAULT_BACKOFF_INITIAL_SEC = 2.0
DEFAULT_BACKOFF_MAX_SEC = 60.0

# Core mode defaults: "only user_id + since" and best-effort crawl.
# These are intentionally conservative enough to reduce risk, but high enough
# to make "long talks" usable without extra flags.
DEFAULT_CORE_MAX_TALK_PAGES = 200
DEFAULT_CORE_MAX_USER_COMMENTS_SCAN_PAGES = 2000

# API limits observed in UI/network.
USER_COMMENTS_PAGE_SIZE = 20
TALKS_PAGE_SIZE = 50
