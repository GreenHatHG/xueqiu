from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from xueqiu_crawler.env_file import load_dotenv


class EnvFileTests(unittest.TestCase):
    def test_load_dotenv_reads_values_without_overriding_existing_env(self) -> None:
        old_existing = os.environ.get("XQ_TEST_EXISTING")
        old_new = os.environ.get("XQ_TEST_NEW")
        try:
            os.environ["XQ_TEST_EXISTING"] = "from-env"
            os.environ.pop("XQ_TEST_NEW", None)
            with tempfile.TemporaryDirectory() as temp_dir:
                env_path = Path(temp_dir) / ".env"
                env_path.write_text(
                    "\n".join(
                        [
                            "# ignored",
                            "XQ_TEST_EXISTING=from-file",
                            "export XQ_TEST_NEW='from-dotenv'",
                        ]
                    ),
                    encoding="utf-8",
                )

                loaded = load_dotenv([env_path])

            self.assertEqual(loaded, [env_path])
            self.assertEqual(os.environ.get("XQ_TEST_EXISTING"), "from-env")
            self.assertEqual(os.environ.get("XQ_TEST_NEW"), "from-dotenv")
        finally:
            if old_existing is None:
                os.environ.pop("XQ_TEST_EXISTING", None)
            else:
                os.environ["XQ_TEST_EXISTING"] = old_existing
            if old_new is None:
                os.environ.pop("XQ_TEST_NEW", None)
            else:
                os.environ["XQ_TEST_NEW"] = old_new


if __name__ == "__main__":
    unittest.main()
