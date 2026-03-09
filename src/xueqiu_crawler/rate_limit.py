from __future__ import annotations

import random
import time


class RateLimiter:
    def __init__(self, min_delay_sec: float, jitter_sec: float) -> None:
        self._min_delay_sec = float(min_delay_sec)
        self._jitter_sec = float(jitter_sec)
        self._next_allowed_ts = 0.0

    def sleep_before_next(self) -> None:
        now = time.time()
        if now < self._next_allowed_ts:
            time.sleep(self._next_allowed_ts - now)

        delay = self._min_delay_sec + random.random() * self._jitter_sec
        self._next_allowed_ts = time.time() + delay
