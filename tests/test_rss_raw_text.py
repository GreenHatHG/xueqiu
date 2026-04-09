from __future__ import annotations

import unittest

from xueqiu_crawler.rss_server import _pick_title, _rss_raw_text, _rss_title_text
from xueqiu_crawler.storage import TALK_TEXT_SEPARATOR


class RssRawTextTests(unittest.TestCase):
    def test_rss_raw_text_splits_tail_quote_into_older_reply_line(self) -> None:
        text = (
            "Gentle_Breeze：回复@SWQing: [狗头][菜狗]会到，耐心等待吧，"
            "应该还有到70的机会，如果有足够耐心的话，"
            "//@SWQing:回复@Gentle_Breeze:会到90么"
        )

        expected = TALK_TEXT_SEPARATOR.join(
            [
                "SWQing：会到90么",
                "Gentle_Breeze：[狗头][菜狗]会到，耐心等待吧，应该还有到70的机会，如果有足够耐心的话，",
            ]
        )

        self.assertEqual(_rss_raw_text(text), expected)

    def test_rss_raw_text_does_not_duplicate_older_reply_line_when_present(
        self,
    ) -> None:
        text = TALK_TEXT_SEPARATOR.join(
            [
                "SWQing：回复@Gentle_Breeze:会到90么",
                (
                    "Gentle_Breeze：回复@SWQing: [狗头][菜狗]会到，耐心等待吧，"
                    "应该还有到70的机会，如果有足够耐心的话，"
                    "//@SWQing:回复@Gentle_Breeze:会到90么"
                ),
            ]
        )

        expected = TALK_TEXT_SEPARATOR.join(
            [
                "SWQing：会到90么",
                "Gentle_Breeze：[狗头][菜狗]会到，耐心等待吧，应该还有到70的机会，如果有足够耐心的话，",
            ]
        )

        self.assertEqual(_rss_raw_text(text), expected)

    def test_rss_title_text_keeps_current_reply_as_title_source(self) -> None:
        text = (
            "Gentle_Breeze：回复@SWQing: [狗头][菜狗]会到，耐心等待吧，"
            "应该还有到70的机会，如果有足够耐心的话，"
            "//@SWQing:回复@Gentle_Breeze:会到90么"
        )

        self.assertEqual(
            _pick_title(_rss_title_text(text)),
            "Gentle_Breeze：[狗头][菜狗]会到，耐心等待吧，应该还有到70的机会，如果有足够耐心的话，",
        )

    def test_rss_title_text_uses_last_part_for_multi_part_chain(self) -> None:
        text = TALK_TEXT_SEPARATOR.join(
            [
                "SWQing：回复@Gentle_Breeze:会到90么",
                (
                    "Gentle_Breeze：回复@SWQing: [狗头][菜狗]会到，耐心等待吧，"
                    "应该还有到70的机会，如果有足够耐心的话，"
                    "//@SWQing:回复@Gentle_Breeze:会到90么"
                ),
            ]
        )

        self.assertEqual(
            _pick_title(_rss_title_text(text)),
            "Gentle_Breeze：[狗头][菜狗]会到，耐心等待吧，应该还有到70的机会，如果有足够耐心的话，",
        )


if __name__ == "__main__":
    unittest.main()
