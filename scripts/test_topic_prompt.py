#!/usr/bin/env python3
# ruff: noqa: E402
from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import threading
import time
import warnings
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from xueqiu_crawler.text_sanitize import sanitize_xueqiu_text

MERGED_TABLE_NAME = "merged_records"
ASSERTIONS_TABLE_NAME = "assertions"
POSTS_TABLE_NAME = "posts"
TOPIC_RUN_PROGRESS_TABLE_NAME = "topic_package_run_progress"
USERNAME_COLUMN_NAME = "username"
JSON_ARRAY_EMPTY = "[]"
ASSERTION_UID_PREFIX = "xueqiu:assertion"
ASSERTION_UID_COLUMN_NAME = "assertion_uid"
EVIDENCE_REFS_FIELD_NAME = "evidence_refs"
EVIDENCE_REFS_JSON_COLUMN_NAME = "evidence_refs_json"
ENTRY_PREFIX = "entry:"
DISPLAY_SEPARATOR = "\n\n---\n\n"
HUMAN_COMMENT_LIMIT = 8
HUMAN_REPORT_TITLE = "人工核查汇总"
HUMAN_TOPIC_SEPARATOR = "\n\n" + "=" * 40 + "\n\n"
BEIJING_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
AI_DUPLICATE_TEXT_MIN_CHARS = 20
AI_DUPLICATE_TEXT_RATIO = 0.96
PROMPT_FOCUS_USERNAME_PLACEHOLDER = "__FOCUS_USERNAME__"
FORWARD_MARKER = "//@"
DEFAULT_AI_TEMPERATURE = 1.0
DEFAULT_AI_TIMEOUT_SEC = 120.0
DEFAULT_AI_RETRY_COUNT = 3
DEFAULT_AI_RETRY_BACKOFF_SEC = 2.0
DEFAULT_AI_MAX_INFLIGHT = 32
DEFAULT_AI_MODE = "responses"
AI_MODE_COMPLETION = "completion"
AI_MODE_RESPONSES = "responses"
PROMPT_VERSION = "topic-prompt-v2"
TOPIC_PACKAGE_KEY_SEPARATOR = "::"
TRACE_RESPONSE_KEYS_LIMIT = 12
TRACE_CHUNK_TYPE_LIMIT = 8
COVERAGE_DETAIL_LIMIT = 20
COVERAGE_IGNORED_SKIP_REASONS = frozenset({"skip_image_only_status"})

warnings.filterwarnings(
    "ignore",
    message=r"^Pydantic serializer warnings:",
    category=UserWarning,
    module=r"pydantic\.main",
)

PROMPT_HEADER_TEMPLATE = """你现在的角色是一个**面向理财小白的“大白话”投资翻译官**。
你的任务是从一堆大佬的日常发言中，提取他们真正在买卖什么、提示什么风险、或者有什么投资感悟，并将其结构化，以便供系统构建“抄作业雷达”和“风险看板”。

【🎯 核心作者聚焦准则 (最高优先级)】
【这是一条绝对指令，必须无条件遵守。】
1. 【唯一焦点】：你的所有提取任务，必须且只能围绕核心作者 __FOCUS_USERNAME__ 的发言进行。绝不允许为其他任何人的发言生成 items 列表中的条目。所有输出的 item 中的 speaker 字段必须是 __FOCUS_USERNAME__。
2. 【上下文工具化】：其他所有人的发言都只是背景工具。它们存在的唯一目的，就是让你搞清楚 __FOCUS_USERNAME__ 是在回答什么问题、反驳什么观点或延续什么话题。
3. 【清晰归因】：在 summary 中，如果 __FOCUS_USERNAME__ 的发言是回复，必须清晰地交代背景。例如，使用“针对网友担心XX的观点，他认为...”或“在回应网友关于XX的提问时，他指出...”这样的句式，确保观点明确归属于 __FOCUS_USERNAME__。
4. 输入 JSON 里最重要的是 `message_tree`。你必须顺着 `children` 理解父子回复关系，不能把并列节点误看成上下级回复。

【正文优先、转发降权准则（极度重要，防张冠李戴）】
1. 每个树节点都会给你 `text`，有些节点还会给 `quoted_text`：
   - `text`（说明：这个人这次自己真正说的话）才是主分析对象。
   - `quoted_text`（说明：他转发、引用、带出来的别人原话）只能当背景，不能直接当成 __FOCUS_USERNAME__ 自己说的结论。
2. `evidence_refs` 里的每个 `quote`，优先从 `text` 里截原文。只有在 `text` 为空、明显属于“纯转发表态”时，才允许从 `quoted_text` 里取证据。
3. 如果是“纯转发”或“只写了很短一句态度”，可以保留 item，但必须：
   - `relation_to_topic` 优先用 `forward`
   - 降低 `action_strength`
   - 降低 `confidence`
   - 在 `summary` 里明确写成“转发表示认同”或“转发提醒大家注意”，不能装作是他展开讲了一大段。
4. 如果 `text` 和 `quoted_text` 明显无关，按“新观点”处理：以 `text` 为准，`quoted_text` 直接当背景噪音。

【绝对禁止金融黑话规则】
小白听不懂专业术语。你的 `summary` 必须用最直白、最接地气的大白话写。
- 禁止使用：上行动能、支撑位、阻力位、筹码分布、量价齐升、估值中枢、筑底等黑话。
- 必须翻译：如果大佬原文说“该价位缺乏支撑”，你要翻译成“觉得这价格还会继续跌”；如果说“估值修复完成”，你要翻译成“价格已经不便宜了，涨得差不多了”；如果说“左侧建仓”，翻译成“趁现在大跌没人要，开始买入”。
- 只要能用人话说明白大佬是“看涨、看跌、让买、让卖、还是提示危险”，就绝不用专业词汇。

【股市语境与反讽识别（极度重要，防多空颠倒准则）】
你必须具备“老股民”的常识，绝不能单纯按字面意思理解，必须时刻警惕以下 4 种典型语境：

1. 供需逻辑的反转（买不到才是好，随便买就是坑）：
   - 通用常识里“物资丰富/随便拿”是好事，但在交易中，“随便买”、“管够”、“想要多少有多少”代表卖盘极大、毫无资金愿意接盘，这是绝对的【极度看弱/嫌弃 (view.bearish)】。
   - 反之，“买不到”、“排队”、“抢筹”、“封死”才是真正的强势看多。

2. 阴阳怪气与反讽（字面看多，实则看空）：
   - 中国股民极爱用反讽和自嘲。如果发言配有 `[捂脸]`、`[狗头]`、`[吃瓜]`、`[微笑]`、`[二哈]`、`[允悲]` 等表情，或者在暴跌时说出“太强了”、“A股永远的神”、“又给大家送钱了”、“赢麻了”。
   - 规则：这99%是亏损后的无奈、嘲讽或看戏。绝不能标记为 bullish，必须将其翻译成“无奈吐槽、觉得这股/大盘不行”。

3. 被动套牢 vs 主动看好（区分真实的 Hold）：
   - “装死”、“躺平”、“关灯吃面”、“删软件了”、“死拿到底”表示的是被套牢后毫无办法的被动无奈，属于【被迫持有】。
   - 规则：动作归为 `trade.hold`（继续拿着），但 `summary` 必须翻译为“被套牢了，无奈只能拿着”，绝不能翻译成“非常看好，决定长线持有”。

4. 事件驱动与套利底线（陈述数学事实 ≠ 看好基本面）：
   - 遇到“私有化、收购、合并、下修转股价”等特定公告时，股票通常会有一个数学上的“价格底线”或“天花板”。
   - 规则：如果大佬说“跌不到X元”、“空间有限”、“不太可能”，他往往只是在陈述一个套利区间的客观事实，并不代表他看好这家公司去“做多”。除非大佬明确喊出“有肉吃、赶紧上、我买了”，否则统统归为 `trade.watch`（先观察）或极低强度的观点，并翻译为“觉得价格到底了/算出了底价”，而不是“看好买入”。

【信息过滤、合并与标签优先级准则（极度重要，防数据库污染）】
1. **先按主话题拆，再合并同类项**：先判断一段发言里到底有几个“主话题”。如果同时聊了电力和黄金，这就是两个主话题，必须先拆成两个 item。拆完以后，如果同一个发言人在不同时间连发多条补充发言，但都在讲同一个主话题，再把它们合并为 1 个 item 总结，严禁机械拆分成多条。1 个 item 可以挂多条 `evidence_refs`，但每条证据都必须单独写 `source_kind`、`source_id`、`quote`，严禁把多条原话拼成 1 个大字符串。
2. **强制带入背景**：你可以通过树节点里的 `source_kind`、`source_id`、`speaker` 和 `children` 弄清上下文结构。如果某人是在反驳或赞同另一个人，你的 `summary` 必须交代背景，格式如：“针对楼主/网友看好某股，他反驳认为...”。绝不可剥离背景输出干瘪的结论。
3. **废话零容忍**：没有明确指向特定个股、行业、具体宏观事件的“万金油心灵鸡汤/正确的废话”（如：看不懂别买、心态最重要），直接抛弃，不要生成 item！
4. **标签优先级绝对压制**：个股/行业实操观点 > 抽象方法论。只要发言涉及对具体股票/行业的基本面判断或买卖态度，必须强制使用 `trade.*`, `view.*`, `valuation.*`, `risk.*`。绝不允许将其弱化归类为 `education.method`。
5. **一条只挂一个主话题**：每个 item 只能有一个 `topic_key`。如果一段话同时聊了两个核心主题，你应该先拆成两条 item；只有当多句话都围绕同一个 `topic_key` 时，才允许把它们合并进同一个 item。

【标签与分类体系】（完全对齐数据库）
`topic_key`（主聚合键，一条只能有一个，优先具体个股 > 行业 > 宏观/方法）：
- 个股：`stock:代码`（如 stock:600519.SH）
- 行业：`industry:名称`（如 industry:电力）
- 指数：`index:名称`（如 index:沪深300）
- 宏观：`macro:名称`（如 macro:汇率）
- 商品：`commodity:名称`（如 commodity:黄金）
- 方法：`method:名称` / 心态：`mindset:名称` / 生活：`life:名称`

`action`（核心动作分类）：
- 交易流：`trade.buy` (买入/建仓), `trade.add` (加仓), `trade.reduce` (减仓/止盈), `trade.sell` (卖出/清仓), `trade.hold` (继续拿着), `trade.watch` (先观察别动)
- 观点流：`view.bullish` (极度看好), `view.bearish` (极度看衰), `valuation.cheap` (觉得便宜), `valuation.expensive` (觉得太贵)
- 风险流：`risk.warning` (提示暴跌/拥挤/别追高等大盘或板块风险), `risk.event` (提示个别雷区)
- 经验流：`education.method` (投资纪律与方法), `education.mindset` (心态控制), `education.life` (生活哲学)

`action_strength`（语气强度 0-3）：
- 0: 顺带一提
- 1: 一般性看法或纯转发
- 2: 明确具体的动作或建议
- 3: 强烈警告、反复强调、带情绪的喊话

`relation_to_topic`（这条话和当前话题的关系）：
- `new`：这条话直接抛出一个新的核心判断、新的买卖动作，或者把话题明显带到一个新的重点上
- `follow`：这条话是在顺着上文继续解释、补充理由、回答提问
- `repeat`：这条话本质上还是之前那个观点，只是换个说法再强调一次，没有新增核心信息
- `forward`：这条话主要是在转发、引用、借别人原话表达态度；如果只是纯转发附带很短一句态度，优先用这个

判定顺序：
1. 只要主要信息来自转发或引用，优先判 `forward`
2. 不是转发时，如果带来新的核心判断或新动作，判 `new`
3. 不是新的主判断，只是在解释、接话、补充，判 `follow`
4. 如果几乎没有新增信息，只是在重复原观点，判 `repeat`

输出 JSON 格式（必须严格遵循，包含为数据库设计的数组字段）：
{
  "topic_status_id": "string",
  "topic_summary": "string (大白话总结当前话题的脉络)",
  "items": [
    {
      "speaker": "string (提取纯名字，去掉👑和楼主等修饰词)",
      "relation_to_topic": "new|follow|repeat|forward",
      "topic_key": "stock:600519.SH (主键，必须按规范填)",
      "action": "trade.buy (必须从 action 枚举中选)",
      "action_strength": 2,
      "summary": "string (极其直白的大白话概括，严禁金融黑话)",
      "evidence_refs": [
        {
          "source_kind": "status|comment|topic_post|talk_reply (从树节点中提取对应的英文)",
          "source_id": "string (从树节点中提取具体ID，如 12345)",
          "quote": "string (对应消息里的原文子串)"
        }
      ],
      "confidence": 0.0,
      "stock_codes": ["600519.SH", "000858.SZ"],
      "stock_names": ["贵州茅台", "五粮液"],
      "industries": ["白酒", "消费"],
      "commodities": ["黄金"],
      "indices": ["沪深300"]
    }
  ]
}
注意：
- 如果没有提到具体的股票、行业、商品、指数，对应字段输出空数组 `[]` 即可。
- `speaker` 必须始终是 __FOCUS_USERNAME__；如果这条话只是别人说的背景内容，不要产出 item。
- `evidence_refs` 至少要有 1 项。如果这条总结用了 2 条原话，就写 2 项；每项 `quote` 都必须是对应消息里的原样子串。
- 不要再输出顶层的 `source_kind`、`source_id`、`evidence` 旧字段。
- `topic_summary` 要用大白话总结这个话题最近在吵什么、__FOCUS_USERNAME__ 这次主要补了什么、整体偏乐观还是偏谨慎。

下面会有很多个话题块。每个话题块都提供一份 JSON 对话树，各话题之间用 `----` 分开。"""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="test_topic_prompt",
        description="Build a topic package from merged_records and print an AI-ready prompt.",
    )
    parser.add_argument(
        "--db",
        type=Path,
        required=True,
        help="SQLite path.",
    )
    parser.add_argument(
        "--list-topics",
        action="store_true",
        help="List available topic_status_id values instead of generating a prompt.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="How many topics to include. Use 0 for all topics.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="Optional output file path for the generated prompt.",
    )
    parser.add_argument(
        "--human-out",
        type=Path,
        default=None,
        help="Optional output file path for the human-readable review text.",
    )
    parser.add_argument(
        "--human-only",
        action="store_true",
        help="Print the human-readable review text instead of the AI prompt.",
    )
    parser.add_argument(
        "--call-ai",
        action="store_true",
        help="Call AI and process topics selected by --limit.",
    )
    parser.add_argument(
        "--api-type",
        default=None,
        help="AI provider type, for example: openai, gemini.",
    )
    parser.add_argument(
        "--base-url",
        default=None,
        help="AI base URL.",
    )
    parser.add_argument(
        "--api-key",
        default=None,
        help="AI API key.",
    )
    parser.add_argument(
        "--api-key-env",
        default=None,
        help="Environment variable name that stores AI API key.",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="LiteLLM model name.",
    )
    parser.add_argument(
        "--api-mode",
        default=DEFAULT_AI_MODE,
        choices=[AI_MODE_COMPLETION, AI_MODE_RESPONSES],
        help="LiteLLM API mode. Use completion for chat/completions, responses for /responses.",
    )
    parser.add_argument(
        "--ai-stream",
        action="store_true",
        help="Enable LiteLLM streaming and rebuild final text from streamed events.",
    )
    parser.add_argument(
        "--ai-out",
        type=Path,
        default=None,
        help="Optional output file path for the AI JSON result.",
    )
    parser.add_argument(
        "--trace-out",
        type=Path,
        default=None,
        help="Optional text trace file path for per-topic readable + JSON details.",
    )
    parser.add_argument(
        "--assertions-out",
        type=Path,
        default=None,
        help="Optional output file path for the normalized assertions preview.",
    )
    parser.add_argument(
        "--write-assertions",
        action="store_true",
        help="Write normalized assertions into sqlite assertions table.",
    )
    parser.add_argument(
        "--write-posts",
        action="store_true",
        help="Write normalized posts into sqlite posts table.",
    )
    parser.add_argument(
        "--force-rerun",
        action="store_true",
        help="Rerun topics even if checkpoint says completed.",
    )
    parser.add_argument(
        "--ai-timeout-sec",
        type=float,
        default=DEFAULT_AI_TIMEOUT_SEC,
        help="LiteLLM request timeout in seconds.",
    )
    parser.add_argument(
        "--ai-retries",
        type=int,
        default=DEFAULT_AI_RETRY_COUNT,
        help="How many times to retry AI call after failure.",
    )
    parser.add_argument(
        "--ai-temperature",
        type=float,
        default=DEFAULT_AI_TEMPERATURE,
        help="AI temperature. Default is 1.",
    )
    parser.add_argument(
        "--ai-reasoning-effort",
        default="xhigh",
        choices=["none", "minimal", "low", "medium", "high", "xhigh"],
        help="AI reasoning effort. GPT-5.2 highest is xhigh.",
    )
    parser.add_argument(
        "--ai-rpm",
        type=float,
        default=0.0,
        help="AI requests per minute. Use 0 to disable rate limit.",
    )
    parser.add_argument(
        "--ai-max-inflight",
        type=int,
        default=DEFAULT_AI_MAX_INFLIGHT,
        help="Max number of in-flight AI requests waiting for replies.",
    )
    return parser.parse_args()


def _resolve_db_path(raw_path: Path) -> Path:
    return Path(raw_path)


def _build_prompt_header(focus_username: str) -> str:
    return PROMPT_HEADER_TEMPLATE.replace(
        PROMPT_FOCUS_USERNAME_PLACEHOLDER,
        str(focus_username).strip(),
    )


def _build_topic_package_key(topic_status_id: str, focus_username: str) -> str:
    return f"{str(topic_status_id).strip()}{TOPIC_PACKAGE_KEY_SEPARATOR}{str(focus_username).strip()}"


def _topic_focus_username(topic_package: dict[str, Any]) -> str:
    return str(topic_package.get("focus_username") or "").strip()


def _load_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    try:
        obj = json.loads(str(value))
    except Exception:
        return {}
    return obj if isinstance(obj, dict) else {}


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(sanitize_xueqiu_text(str(value))).strip()


def _normalize_created_at(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    try:
        if raw.isdigit():
            raw_timestamp = int(raw)
            timestamp = (
                raw_timestamp / 1000.0
                if raw_timestamp > 10**12
                else float(raw_timestamp)
            )
            parsed = dt.datetime.fromtimestamp(
                timestamp, tz=dt.timezone.utc
            ).astimezone(BEIJING_TIMEZONE)
            return parsed.replace(microsecond=0).isoformat()

        parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=BEIJING_TIMEZONE)
        else:
            parsed = parsed.astimezone(BEIJING_TIMEZONE)
        return parsed.replace(microsecond=0).isoformat()
    except Exception:
        return raw


def _first_non_empty_text(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _split_csv_urls(value: Any) -> list[str]:
    raw = str(value or "").strip()
    if not raw:
        return []
    return [part.strip() for part in raw.split(",") if str(part).strip()]


def _extract_status_image_urls(raw_obj: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add_url(value: Any) -> None:
        text = str(value or "").strip()
        if not text or text in seen:
            return
        seen.add(text)
        urls.append(text)

    for key in ("firstImg", "first_img", "cover_pic"):
        add_url(raw_obj.get(key))

    for item in _split_csv_urls(raw_obj.get("pic")):
        add_url(item)

    image_info_list = raw_obj.get("image_info_list")
    if isinstance(image_info_list, list):
        for item in image_info_list:
            if not isinstance(item, dict):
                continue
            for key in (
                "url",
                "image_url",
                "original_url",
                "origin_url",
                "raw_url",
                "download_url",
                "thumbnail",
                "thumb",
                "large",
                "small",
            ):
                value = item.get(key)
                if isinstance(value, dict):
                    add_url(value.get("url"))
                else:
                    add_url(value)
    return urls


def _split_commentary_and_quoted_text(text: Any) -> tuple[str, str]:
    cleaned = _clean_text(text)
    if not cleaned:
        return "", ""
    if FORWARD_MARKER not in cleaned:
        return cleaned, ""

    commentary, quoted = cleaned.split(FORWARD_MARKER, 1)
    commentary_text = commentary.strip()
    quoted_text = f"{FORWARD_MARKER}{quoted.strip()}".strip()
    return commentary_text, quoted_text


def _user_label_from_user_obj(user_obj: Any) -> str:
    if not isinstance(user_obj, dict):
        return ""
    for key in ("screen_name", "screenName", "name", "nickname"):
        value = user_obj.get(key)
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    for key in ("id", "user_id", "uid"):
        value = user_obj.get(key)
        text = str(value).strip() if value is not None else ""
        if text:
            return text
    return ""


def _speaker_from_raw_json(raw_json: Any, _unused_fallback_user_id: Any) -> str:
    raw_obj = _load_json_obj(raw_json)
    if raw_obj:
        label = _user_label_from_user_obj(raw_obj.get("user"))
        if label:
            return label
        for key in ("screen_name", "screenName", "name"):
            value = raw_obj.get(key)
            text = str(value).strip() if value is not None else ""
            if text:
                return text
    return ""


def _split_display_lines(text: Any) -> list[str]:
    if text is None:
        return []
    parts = [str(part).strip() for part in str(text).split(DISPLAY_SEPARATOR)]
    return [part for part in parts if part]


def _collapse_inline_whitespace(value: Any) -> str:
    return " ".join(str(value or "").split())


def _strip_reply_wrappers(text: Any) -> str:
    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    cleaned = re.sub(r"^(?:回复@[^:：]+[:：]\s*)+", "", cleaned)
    cleaned = re.sub(r"\s*//@.*$", "", cleaned)
    return cleaned.strip()


def _ai_compare_text(value: Any) -> str:
    text = _collapse_inline_whitespace(_clean_text(value))
    if not text:
        return ""
    return _strip_reply_wrappers(text)


def _looks_like_same_message(left: Any, right: Any) -> bool:
    left_text = _ai_compare_text(left)
    right_text = _ai_compare_text(right)
    if not left_text or not right_text:
        return False
    if left_text == right_text:
        return True

    shorter, longer = sorted((left_text, right_text), key=len)
    if len(shorter) >= AI_DUPLICATE_TEXT_MIN_CHARS and shorter in longer:
        return True
    if len(shorter) < AI_DUPLICATE_TEXT_MIN_CHARS:
        return False
    return (
        SequenceMatcher(None, left_text, right_text).ratio() >= AI_DUPLICATE_TEXT_RATIO
    )


def _line_to_speaker_and_text(line: str) -> tuple[str, str]:
    content = str(line or "").strip()
    if not content:
        return "", ""
    if "：" not in content:
        return "", content
    speaker, body = content.split("：", 1)
    return speaker.strip(), body.strip()


def _build_root_status_hint(
    *,
    row_text: Any,
    talk_context: list[dict[str, str]],
    topic_status_id: str,
    root_status_id: str,
) -> Optional[dict[str, str]]:
    if not root_status_id or root_status_id == topic_status_id:
        return None

    lines = _split_display_lines(row_text)
    if not lines:
        return None

    chain_line_count = len(talk_context) + 1
    if len(lines) <= chain_line_count:
        return None

    root_line = lines[: len(lines) - chain_line_count][-1]
    speaker, text = _line_to_speaker_and_text(root_line)
    if not text:
        return None

    return {
        "source_kind": "status",
        "source_id": root_status_id,
        "speaker": speaker,
        "created_at": "",
        "text": text,
        "display_text": root_line,
    }


def _build_reply_parent_hint(raw_json: Any) -> Optional[dict[str, str]]:
    raw_obj = _load_json_obj(raw_json)
    if not raw_obj:
        return None

    parent_obj = raw_obj.get("reply_comment")
    if not isinstance(parent_obj, dict):
        return None

    source_id = str(parent_obj.get("id") or parent_obj.get("comment_id") or "").strip()
    if not source_id:
        return None

    text = _first_non_empty_text(parent_obj.get("text"), parent_obj.get("description"))
    if not text:
        return None

    speaker = _user_label_from_user_obj(parent_obj.get("user"))
    if not speaker:
        speaker = _user_label_from_user_obj(raw_obj.get("reply_user"))
    if not speaker:
        speaker = str(raw_obj.get("reply_screenName") or "").strip()

    return {
        "source_kind": "talk_reply",
        "source_id": source_id,
        "speaker": speaker,
        "created_at": _normalize_created_at(
            parent_obj.get("created_at_bj") or parent_obj.get("created_at")
        ),
        "text": text,
        "in_reply_to_comment_id": str(
            parent_obj.get("in_reply_to_comment_id") or ""
        ).strip(),
    }


def _pick_row_created_at(row_created_at_bj: Any, record: dict[str, Any]) -> str:
    return _normalize_created_at(record.get("created_at_bj") or row_created_at_bj)


def _message_sort_key(item: dict[str, Any]) -> tuple[str, str]:
    return (
        str(item.get("created_at") or "").strip(),
        str(item.get("source_id") or "").strip(),
    )


def _build_status_message(
    *,
    merge_key: str,
    row_created_at_bj: Any,
    row_text: Any,
    context_obj: dict[str, Any],
    payload_obj: dict[str, Any],
) -> Optional[dict[str, Any]]:
    status_payload = payload_obj.get("status")
    if not isinstance(status_payload, dict):
        return None
    record = status_payload.get("record")
    if not isinstance(record, dict):
        return None

    source_id = str(
        context_obj.get("status_id") or record.get("status_id") or ""
    ).strip()
    if not source_id:
        return None

    text = _first_non_empty_text(
        record.get("text"),
        record.get("description"),
        _load_json_obj(record.get("raw_json")).get("description"),
        _load_json_obj(record.get("raw_json")).get("text"),
    )
    if not text:
        return None

    return {
        "merge_key": merge_key,
        "source_kind": "status",
        "source_id": source_id,
        "speaker": _speaker_from_raw_json(
            record.get("raw_json"), record.get("user_id")
        ),
        "created_at": _pick_row_created_at(row_created_at_bj, record),
        "text": text,
        "display_text": str(row_text or "").strip(),
        "topic_status_id": str(context_obj.get("topic_status_id") or "").strip(),
    }


def _build_talk_context(
    payload_obj: dict[str, Any], parent_source_id: str
) -> list[dict[str, str]]:
    talk_payload = payload_obj.get("talk")
    if not isinstance(talk_payload, dict):
        return []
    clean_obj = talk_payload.get("clean")
    if not isinstance(clean_obj, dict):
        return []

    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    pages = clean_obj.get("pages")
    if not isinstance(pages, list):
        return []

    for page in pages:
        if not isinstance(page, dict):
            continue
        comments = page.get("comments")
        if not isinstance(comments, list):
            continue
        for item in comments:
            if not isinstance(item, dict):
                continue
            text = _first_non_empty_text(item.get("text"), item.get("description"))
            if not text:
                continue
            speaker = _user_label_from_user_obj(item.get("user"))
            if not speaker:
                speaker = str(item.get("user_id") or item.get("uid") or "").strip()
            key = (speaker, text)
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "source_kind": "talk_reply",
                    "source_id": str(
                        item.get("id") or item.get("comment_id") or ""
                    ).strip(),
                    "speaker": speaker,
                    "created_at": _normalize_created_at(
                        item.get("created_at_bj") or item.get("created_at")
                    ),
                    "text": text,
                }
            )
            if out[-1]["source_id"] == parent_source_id:
                out.pop()
    out.sort(key=_message_sort_key, reverse=True)
    return out


def _build_comment_message(
    *,
    merge_key: str,
    row_created_at_bj: Any,
    row_text: Any,
    context_obj: dict[str, Any],
    payload_obj: dict[str, Any],
) -> Optional[dict[str, Any]]:
    comment_payload = payload_obj.get("comment")
    if not isinstance(comment_payload, dict):
        return None
    record = comment_payload.get("record")
    if not isinstance(record, dict):
        return None

    source_id = str(
        context_obj.get("comment_id") or record.get("comment_id") or ""
    ).strip()
    if not source_id:
        return None

    text = _first_non_empty_text(
        record.get("text"),
        record.get("description"),
        _load_json_obj(record.get("raw_json")).get("description"),
        _load_json_obj(record.get("raw_json")).get("text"),
    )
    if not text:
        return None

    message: dict[str, Any] = {
        "merge_key": merge_key,
        "source_kind": "comment",
        "source_id": source_id,
        "speaker": _speaker_from_raw_json(
            record.get("raw_json"), record.get("user_id")
        ),
        "created_at": _pick_row_created_at(row_created_at_bj, record),
        "text": text,
        "display_text": str(row_text or "").strip(),
        "in_reply_to_comment_id": str(
            record.get("in_reply_to_comment_id") or ""
        ).strip(),
        "root_status_id": str(
            context_obj.get("root_status_id")
            or record.get("root_status_id")
            or record.get("root_in_reply_to_status_id")
            or ""
        ).strip(),
        "root_status_url": str(
            context_obj.get("root_status_url") or record.get("root_status_url") or ""
        ).strip(),
        "talk_context": _build_talk_context(payload_obj, source_id),
    }

    topic_status_id = str(context_obj.get("topic_status_id") or "").strip()
    message["root_status_hint"] = _build_root_status_hint(
        row_text=row_text,
        talk_context=message["talk_context"],
        topic_status_id=topic_status_id,
        root_status_id=message["root_status_id"],
    )
    message["reply_parent_hint"] = _build_reply_parent_hint(record.get("raw_json"))

    lines = _split_display_lines(row_text)
    if lines:
        topic_speaker, topic_text = _line_to_speaker_and_text(lines[0])
        if topic_text:
            message["topic_post_hint"] = {
                "source_kind": "topic_post",
                "source_id": topic_status_id,
                "speaker": topic_speaker,
                "created_at": "",
                "text": topic_text,
                "display_text": lines[0],
            }
    return message


def _dedupe_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for message in messages:
        source_id = str(message.get("source_id") or "").strip()
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        out.append(message)
    out.sort(key=_message_sort_key, reverse=True)
    return out


def _choose_topic_post(
    topic_status_id: str,
    statuses: list[dict[str, Any]],
    comments: list[dict[str, Any]],
) -> tuple[Optional[dict[str, Any]], list[dict[str, Any]]]:
    if statuses:
        preferred: Optional[dict[str, Any]] = None
        for item in statuses:
            if str(item.get("source_id") or "") == topic_status_id:
                preferred = item
                break
        if preferred is None:
            preferred = statuses[0]
        rest = [item for item in statuses if item is not preferred]
        return preferred, rest

    for comment in comments:
        hint = comment.get("topic_post_hint")
        if not isinstance(hint, dict):
            continue
        text = str(hint.get("text") or "").strip()
        if text:
            return hint, []
    return None, []


def _load_entry_rows(db_path: Path) -> list[sqlite3.Row]:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        column_names = {
            str(row["name"]).strip()
            for row in conn.execute(f"PRAGMA table_info({MERGED_TABLE_NAME})")
        }
        if USERNAME_COLUMN_NAME not in column_names:
            raise SystemExit("这个库里没有 username 这一列，先补数据再跑。")
        rows = list(
            conn.execute(
                f"""
                SELECT merge_key, username, created_at_bj, text, context_json, payload_json
                FROM {MERGED_TABLE_NAME}
                WHERE merge_key LIKE ?
                ORDER BY created_at_bj ASC, merge_key ASC
                """,
                (f"{ENTRY_PREFIX}%",),
            )
        )
    finally:
        conn.close()
    return rows


def _add_coverage_skip_reason(report: dict[str, Any], reason: str) -> None:
    reason_text = _normalize_text(reason) or "unknown"
    reason_counts = report.setdefault("skip_reason_counts", {})
    reason_counts[reason_text] = int(reason_counts.get(reason_text) or 0) + 1


def _add_coverage_skipped_entry(
    report: dict[str, Any],
    *,
    merge_key: str,
    reason: str,
    topic_status_id: str = "",
    focus_username: str = "",
) -> None:
    skipped_entries = report.setdefault("skipped_entries", [])
    skipped_entries.append(
        {
            "merge_key": _normalize_text(merge_key),
            "reason": _normalize_text(reason),
            "topic_status_id": _normalize_text(topic_status_id),
            "focus_username": _normalize_text(focus_username),
        }
    )


def _classify_status_skip_reason(
    *,
    context_obj: dict[str, Any],
    payload_obj: dict[str, Any],
) -> str:
    status_payload = payload_obj.get("status")
    if not isinstance(status_payload, dict):
        return "skip_status_payload_missing"
    record = status_payload.get("record")
    if not isinstance(record, dict):
        return "skip_status_record_missing"

    source_id = str(
        context_obj.get("status_id") or record.get("status_id") or ""
    ).strip()
    if not source_id:
        return "skip_status_missing_source_id"

    raw_obj = _load_json_obj(record.get("raw_json"))
    text = _first_non_empty_text(
        record.get("text"),
        record.get("description"),
        raw_obj.get("description"),
        raw_obj.get("text"),
    )
    if text:
        return ""
    if _extract_status_image_urls(raw_obj):
        return "skip_image_only_status"
    return "skip_invalid_status_message"


def _build_topics_from_rows(
    rows: list[sqlite3.Row],
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    report: dict[str, Any] = {
        "entry_rows_total": len(rows),
        "accepted_rows_total": 0,
        "accepted_status_rows": 0,
        "accepted_chain_rows": 0,
        "skipped_rows_total": 0,
        "skip_reason_counts": {},
        "topic_packages_total": 0,
    }

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        merge_key = str(row["merge_key"] or "").strip()
        focus_username = str(row[USERNAME_COLUMN_NAME] or "").strip()
        if not focus_username:
            _add_coverage_skip_reason(report, "skip_empty_username")
            _add_coverage_skipped_entry(
                report,
                merge_key=merge_key,
                reason="skip_empty_username",
            )
            continue
        context_obj = _load_json_obj(row["context_json"])
        payload_obj = _load_json_obj(row["payload_json"])
        topic_status_id = str(context_obj.get("topic_status_id") or "").strip()
        if not topic_status_id:
            _add_coverage_skip_reason(report, "skip_empty_topic_status_id")
            _add_coverage_skipped_entry(
                report,
                merge_key=merge_key,
                reason="skip_empty_topic_status_id",
                focus_username=focus_username,
            )
            continue
        topic_package_key = _build_topic_package_key(topic_status_id, focus_username)

        topic = grouped.setdefault(
            topic_package_key,
            {
                "topic_package_key": topic_package_key,
                "topic_status_id": topic_status_id,
                "focus_username": focus_username,
                "topic_post": None,
                "status_updates": [],
                "comments": [],
            },
        )

        entry_type = str(
            context_obj.get("entry_type") or payload_obj.get("entry_type") or ""
        ).strip()
        if entry_type == "status":
            message = _build_status_message(
                merge_key=merge_key,
                row_created_at_bj=row["created_at_bj"],
                row_text=row["text"],
                context_obj=context_obj,
                payload_obj=payload_obj,
            )
            if message is not None:
                topic["status_updates"].append(message)
                report["accepted_rows_total"] = (
                    int(report.get("accepted_rows_total") or 0) + 1
                )
                report["accepted_status_rows"] = (
                    int(report.get("accepted_status_rows") or 0) + 1
                )
            else:
                reason = (
                    _classify_status_skip_reason(
                        context_obj=context_obj,
                        payload_obj=payload_obj,
                    )
                    or "skip_invalid_status_message"
                )
                _add_coverage_skip_reason(report, reason)
                _add_coverage_skipped_entry(
                    report,
                    merge_key=merge_key,
                    reason=reason,
                    topic_status_id=topic_status_id,
                    focus_username=focus_username,
                )
        elif entry_type == "chain":
            message = _build_comment_message(
                merge_key=merge_key,
                row_created_at_bj=row["created_at_bj"],
                row_text=row["text"],
                context_obj=context_obj,
                payload_obj=payload_obj,
            )
            if message is not None:
                topic["comments"].append(message)
                report["accepted_rows_total"] = (
                    int(report.get("accepted_rows_total") or 0) + 1
                )
                report["accepted_chain_rows"] = (
                    int(report.get("accepted_chain_rows") or 0) + 1
                )
            else:
                _add_coverage_skip_reason(report, "skip_invalid_chain_message")
                _add_coverage_skipped_entry(
                    report,
                    merge_key=merge_key,
                    reason="skip_invalid_chain_message",
                    topic_status_id=topic_status_id,
                    focus_username=focus_username,
                )
        else:
            reason = f"skip_unknown_entry_type:{entry_type or 'empty'}"
            _add_coverage_skip_reason(
                report,
                reason,
            )
            _add_coverage_skipped_entry(
                report,
                merge_key=merge_key,
                reason=reason,
                topic_status_id=topic_status_id,
                focus_username=focus_username,
            )

    for topic in grouped.values():
        status_updates = _dedupe_messages(topic["status_updates"])
        comments = _dedupe_messages(topic["comments"])
        topic_post, rest_statuses = _choose_topic_post(
            str(topic.get("topic_status_id") or ""),
            status_updates,
            comments,
        )
        for comment in comments:
            comment.pop("topic_post_hint", None)
        topic["topic_post"] = topic_post
        topic["status_updates"] = rest_statuses
        topic["comments"] = comments
        latest_activity_candidates = [
            str(item.get("created_at") or "") for item in rest_statuses
        ]
        latest_activity_candidates.extend(
            str(item.get("created_at") or "") for item in comments
        )
        if topic_post:
            latest_activity_candidates.append(str(topic_post.get("created_at") or ""))
        topic["stats"] = {
            "status_count": len(rest_statuses) + (1 if topic_post else 0),
            "comment_count": len(comments),
            "talk_message_count": sum(
                len(comment.get("talk_context") or []) for comment in comments
            ),
            "latest_activity_at": max(latest_activity_candidates)
            if latest_activity_candidates
            else "",
        }
    report["skipped_rows_total"] = int(report.get("entry_rows_total") or 0) - int(
        report.get("accepted_rows_total") or 0
    )
    report["topic_packages_total"] = len(grouped)
    return grouped, report


def _ranked_topics(topics: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    ranked = list(topics.values())
    ranked.sort(
        key=lambda item: (
            str(item.get("stats", {}).get("latest_activity_at") or ""),
            int(item.get("stats", {}).get("comment_count") or 0),
            int(item.get("stats", {}).get("talk_message_count") or 0),
            str(item.get("topic_status_id") or ""),
        ),
        reverse=True,
    )
    return ranked


def _build_topic_json(topic_package: dict[str, Any]) -> str:
    return json.dumps(topic_package, ensure_ascii=False, indent=2)


def _build_internal_message_payload(
    message: dict[str, Any],
) -> Optional[dict[str, Any]]:
    text = str(message.get("text") or "").strip()
    source_kind = str(message.get("source_kind") or "").strip()
    source_id = str(message.get("source_id") or "").strip()
    if not text or not source_kind or not source_id:
        return None

    commentary_text, quoted_text = _split_commentary_and_quoted_text(text)
    return {
        "source_kind": source_kind,
        "source_id": source_id,
        "speaker": str(message.get("speaker") or "").strip(),
        "created_at": str(message.get("created_at") or "").strip(),
        "text": text,
        "commentary_text": commentary_text,
        "quoted_text": quoted_text,
    }


def _build_prompt_message_text(message: dict[str, Any]) -> str:
    commentary_text = str(message.get("commentary_text") or "").strip()
    if commentary_text:
        return _strip_reply_wrappers(commentary_text) or commentary_text

    raw_text = str(message.get("text") or "").strip()
    stripped_text = _strip_reply_wrappers(raw_text)
    if stripped_text:
        return stripped_text
    return raw_text


def _build_message_node_key(source_kind: Any, source_id: Any) -> str:
    return f"{str(source_kind or '').strip()}:{str(source_id or '').strip()}"


class _ConversationTreeNode:
    def __init__(self, payload: dict[str, Any]):
        self.key = _build_message_node_key(
            payload.get("source_kind"),
            payload.get("source_id"),
        )
        self.payload = payload
        self.sort_time = _normalize_created_at(payload.get("created_at"))
        self.children: dict[str, "_ConversationTreeNode"] = {}


def _build_conversation_tree_node(
    message: dict[str, Any],
) -> Optional[_ConversationTreeNode]:
    payload = _build_internal_message_payload(message)
    if payload is None:
        return None
    return _ConversationTreeNode(payload)


def _sorted_tree_children(node: _ConversationTreeNode) -> list[_ConversationTreeNode]:
    children = list(node.children.values())
    children.sort(
        key=lambda child: (
            child.sort_time,
            str(child.payload.get("source_kind") or "").strip(),
            str(child.payload.get("source_id") or "").strip(),
        )
    )
    return children


def _serialize_conversation_tree(node: _ConversationTreeNode) -> dict[str, Any]:
    out = dict(node.payload)
    out["children"] = [
        _serialize_conversation_tree(child) for child in _sorted_tree_children(node)
    ]
    return out


def _insert_conversation_tree_path(
    tree_root: _ConversationTreeNode,
    path_nodes: list[_ConversationTreeNode],
) -> None:
    current = tree_root
    for node in path_nodes:
        if not node.key:
            continue
        if node.key not in current.children:
            current.children[node.key] = node
        current = current.children[node.key]


def _collect_tree_status_candidates(
    topic_package: dict[str, Any],
) -> tuple[dict[str, dict[str, Any]], dict[str, int]]:
    status_candidates: dict[str, dict[str, Any]] = {}
    reply_counts: dict[str, int] = {}

    for item in topic_package.get("status_updates") or []:
        if not isinstance(item, dict):
            continue
        source_id = str(item.get("source_id") or "").strip()
        if not source_id:
            continue
        status_candidates[source_id] = item
        reply_counts.setdefault(source_id, 0)

    for comment in topic_package.get("comments") or []:
        if not isinstance(comment, dict):
            continue
        hint = comment.get("root_status_hint")
        if not isinstance(hint, dict):
            continue
        source_id = str(hint.get("source_id") or "").strip()
        if not source_id:
            continue
        reply_counts[source_id] = reply_counts.get(source_id, 0) + 1
        current = status_candidates.get(source_id)
        if current is None:
            status_candidates[source_id] = hint
            continue
        current_text = str(current.get("text") or "")
        hint_text = str(hint.get("text") or "")
        if len(hint_text) > len(current_text):
            status_candidates[source_id] = hint

    return status_candidates, reply_counts


def _matching_status_id_for_comment(
    comment: dict[str, Any],
    status_candidates: dict[str, dict[str, Any]],
    reply_counts: dict[str, int],
) -> str:
    speaker = str(comment.get("speaker") or "").strip()
    text = str(comment.get("text") or "").strip()
    source_id = str(comment.get("source_id") or "").strip()
    if not speaker or not text or not source_id:
        return ""

    for status_id, status_item in status_candidates.items():
        if status_id == source_id or reply_counts.get(status_id, 0) <= 0:
            continue
        if str(status_item.get("speaker") or "").strip() != speaker:
            continue
        if _looks_like_same_message(text, status_item.get("text")):
            return status_id
    return ""


def _collect_status_anchor_paths(
    comments: list[dict[str, Any]],
    comments_by_id: dict[str, dict[str, Any]],
    reply_parents_by_id: dict[str, dict[str, Any]],
    hidden_status_ids_by_comment_id: dict[str, str],
) -> dict[str, list[dict[str, Any]]]:
    anchor_paths: dict[str, list[dict[str, Any]]] = {}

    for comment in comments:
        comment_id = str(comment.get("source_id") or "").strip()
        status_id = hidden_status_ids_by_comment_id.get(comment_id, "")
        if not status_id:
            continue

        path = _comment_parent_path(comment, comments_by_id, reply_parents_by_id)
        current = anchor_paths.get(status_id)
        if current is None or len(path) > len(current):
            anchor_paths[status_id] = path

    return anchor_paths


def _expand_status_anchor_path(
    path: list[dict[str, Any]],
    status_anchor_paths: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    expanded = list(path)
    seen_status_ids: set[str] = set()

    while expanded:
        first = expanded[0]
        if str(first.get("source_kind") or "").strip() != "status":
            break

        status_id = str(first.get("source_id") or "").strip()
        if not status_id or status_id in seen_status_ids:
            break

        anchor_path = status_anchor_paths.get(status_id) or []
        if not anchor_path:
            break

        seen_status_ids.add(status_id)
        expanded = list(anchor_path) + expanded

    return expanded


def _resolve_comment_parent_chain(
    parent_comment_id: str,
    comments_by_id: dict[str, dict[str, Any]],
    reply_parents_by_id: dict[str, dict[str, Any]],
    root_status_hint: Optional[dict[str, Any]],
    seen: set[str],
) -> list[dict[str, Any]]:
    if not parent_comment_id:
        return [root_status_hint] if isinstance(root_status_hint, dict) else []
    if parent_comment_id in seen:
        return [root_status_hint] if isinstance(root_status_hint, dict) else []

    seen.add(parent_comment_id)

    parent_comment = comments_by_id.get(parent_comment_id)
    if isinstance(parent_comment, dict):
        chain = _comment_parent_path(
            parent_comment,
            comments_by_id,
            reply_parents_by_id,
            seen=seen,
        )
        return chain + [parent_comment]

    parent_hint = reply_parents_by_id.get(parent_comment_id)
    if not isinstance(parent_hint, dict):
        return [root_status_hint] if isinstance(root_status_hint, dict) else []

    next_parent_id = str(parent_hint.get("in_reply_to_comment_id") or "").strip()
    chain = _resolve_comment_parent_chain(
        next_parent_id,
        comments_by_id,
        reply_parents_by_id,
        root_status_hint,
        seen,
    )
    return chain + [parent_hint]


def _comment_parent_path(
    comment: dict[str, Any],
    comments_by_id: dict[str, dict[str, Any]],
    reply_parents_by_id: dict[str, dict[str, Any]],
    *,
    seen: Optional[set[str]] = None,
) -> list[dict[str, Any]]:
    if seen is None:
        seen = set()

    root_status_hint = comment.get("root_status_hint")
    talks = comment.get("talk_context") or []
    if talks:
        path: list[dict[str, Any]] = []
        if isinstance(root_status_hint, dict):
            path.append(root_status_hint)
        path.extend(item for item in reversed(talks) if isinstance(item, dict))
        return path

    parent_comment_id = str(comment.get("in_reply_to_comment_id") or "").strip()
    return _resolve_comment_parent_chain(
        parent_comment_id,
        comments_by_id,
        reply_parents_by_id,
        root_status_hint if isinstance(root_status_hint, dict) else None,
        seen,
    )


def _build_message_tree(
    topic_package: dict[str, Any],
    *,
    topic_status_id: str,
    topic_post_speaker: str,
) -> dict[str, Any]:
    topic_post = topic_package.get("topic_post")
    root_node = None
    if isinstance(topic_post, dict):
        root_node = _build_conversation_tree_node(topic_post)

    if root_node is None:
        synthetic_text = "原帖缺失"
        root_node = _ConversationTreeNode(
            {
                "source_kind": "topic_post",
                "source_id": topic_status_id or "root",
                "speaker": topic_post_speaker,
                "created_at": "",
                "text": synthetic_text,
                "commentary_text": synthetic_text,
                "quoted_text": "",
            }
        )

    status_updates = topic_package.get("status_updates")
    comments = topic_package.get("comments")
    ordered_comments = (
        sorted(
            [item for item in comments if isinstance(item, dict)],
            key=_message_sort_key,
        )
        if isinstance(comments, list)
        else []
    )
    comments_by_id = {
        str(item.get("source_id") or "").strip(): item
        for item in ordered_comments
        if str(item.get("source_id") or "").strip()
    }
    reply_parents_by_id = {
        str(hint.get("source_id") or "").strip(): hint
        for item in ordered_comments
        for hint in [item.get("reply_parent_hint")]
        if isinstance(hint, dict) and str(hint.get("source_id") or "").strip()
    }

    status_candidates, reply_counts = _collect_tree_status_candidates(topic_package)
    hidden_status_ids_by_comment_id = {
        str(comment.get("source_id") or "").strip(): _matching_status_id_for_comment(
            comment,
            status_candidates,
            reply_counts,
        )
        for comment in ordered_comments
        if str(comment.get("source_id") or "").strip()
    }
    hidden_status_ids_by_comment_id = {
        comment_id: status_id
        for comment_id, status_id in hidden_status_ids_by_comment_id.items()
        if status_id
    }
    status_anchor_paths = _collect_status_anchor_paths(
        ordered_comments,
        comments_by_id,
        reply_parents_by_id,
        hidden_status_ids_by_comment_id,
    )
    anchored_status_ids = {
        status_id
        for status_id, anchor_path in status_anchor_paths.items()
        if anchor_path
    }

    if isinstance(status_updates, list):
        for item in sorted(status_updates, key=_message_sort_key):
            if not isinstance(item, dict):
                continue
            status_id = str(item.get("source_id") or "").strip()
            if not status_id or status_id in anchored_status_ids:
                continue
            node = _build_conversation_tree_node(item)
            if node is not None:
                _insert_conversation_tree_path(root_node, [node])

    for status_id in sorted(anchored_status_ids):
        anchor_path = _expand_status_anchor_path(
            status_anchor_paths.get(status_id) or [],
            status_anchor_paths,
        )
        status_item = status_candidates.get(status_id)
        if not anchor_path or not isinstance(status_item, dict):
            continue

        path_nodes: list[_ConversationTreeNode] = []
        for item in anchor_path:
            node = _build_conversation_tree_node(item)
            if node is not None:
                path_nodes.append(node)

        status_node = _build_conversation_tree_node(status_item)
        if status_node is None:
            continue
        path_nodes.append(status_node)
        _insert_conversation_tree_path(root_node, path_nodes)

    visible_comments = [
        comment
        for comment in ordered_comments
        if str(comment.get("source_id") or "").strip()
        not in hidden_status_ids_by_comment_id
    ]

    for comment in visible_comments:
        parent_path = _comment_parent_path(
            comment,
            comments_by_id,
            reply_parents_by_id,
        )
        parent_path = _expand_status_anchor_path(parent_path, status_anchor_paths)
        comment_path_nodes: list[_ConversationTreeNode] = []
        for item in parent_path:
            node = _build_conversation_tree_node(item)
            if node is not None:
                comment_path_nodes.append(node)

        comment_node = _build_conversation_tree_node(comment)
        if comment_node is not None:
            comment_path_nodes.append(comment_node)
        if comment_path_nodes:
            _insert_conversation_tree_path(root_node, comment_path_nodes)

    return _serialize_conversation_tree(root_node)


def _serialize_prompt_message_tree(node: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "source_kind": str(node.get("source_kind") or "").strip(),
        "source_id": str(node.get("source_id") or "").strip(),
        "speaker": str(node.get("speaker") or "").strip(),
        "text": _build_prompt_message_text(node),
    }

    quoted_text = str(node.get("quoted_text") or "").strip()
    if quoted_text:
        out["quoted_text"] = quoted_text

    raw_children = node.get("children")
    children = raw_children if isinstance(raw_children, list) else []
    serialized_children = [
        _serialize_prompt_message_tree(child)
        for child in children
        if isinstance(child, dict)
    ]
    if serialized_children:
        out["children"] = serialized_children
    return out


def _build_message_lookup_from_tree(
    message_tree: dict[str, Any],
) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}

    def walk(node: dict[str, Any], path: list[dict[str, Any]]) -> None:
        source_kind = _normalize_text(node.get("source_kind"))
        source_id = _normalize_text(node.get("source_id"))
        node_payload = {key: value for key, value in node.items() if key != "children"}
        current_path = list(path)
        current_path.append(node_payload)
        if source_kind and source_id and (source_kind, source_id) not in lookup:
            lookup[(source_kind, source_id)] = {
                **node_payload,
                "root_path": current_path,
            }

        raw_children = node.get("children")
        children = raw_children if isinstance(raw_children, list) else []
        for child in children:
            if isinstance(child, dict):
                walk(child, current_path)

    walk(message_tree, [])
    return lookup


def _build_topic_runtime_context(
    topic_package: dict[str, Any], focus_username: Optional[str] = None
) -> dict[str, Any]:
    topic_status_id = str(topic_package.get("topic_status_id") or "").strip()
    resolved_focus_username = str(
        focus_username or _topic_focus_username(topic_package)
    ).strip()
    topic_post = topic_package.get("topic_post")
    topic_post_speaker = ""
    if isinstance(topic_post, dict):
        topic_post_speaker = str(topic_post.get("speaker") or "").strip()

    message_tree = _build_message_tree(
        topic_package,
        topic_status_id=topic_status_id,
        topic_post_speaker=topic_post_speaker,
    )
    ai_topic_package = {
        "topic_status_id": topic_status_id,
        "focus_username": resolved_focus_username,
        "message_tree": _serialize_prompt_message_tree(message_tree),
    }
    return {
        "topic_status_id": topic_status_id,
        "focus_username": resolved_focus_username,
        "message_tree": message_tree,
        "message_lookup": _build_message_lookup_from_tree(message_tree),
        "ai_topic_package": ai_topic_package,
    }


class _AICallError(RuntimeError):
    def __init__(self, message: str, *, trace: Optional[dict[str, Any]] = None):
        super().__init__(message)
        self.trace = trace or {}


def _build_ai_topic_package(
    topic_package: dict[str, Any], focus_username: Optional[str] = None
) -> dict[str, Any]:
    runtime_context = _build_topic_runtime_context(topic_package, focus_username)
    return dict(runtime_context.get("ai_topic_package") or {})


def _human_meta_text(*parts: Any) -> str:
    values = [str(part or "").strip() for part in parts]
    return " | ".join(value for value in values if value)


def _human_message_lines(
    title: str,
    *,
    speaker: Any,
    created_at: Any,
    source_id: Any,
    display_text: Any,
    body: Any,
) -> list[str]:
    speaker_text = str(speaker or "").strip() or "未知用户"
    meta = _human_meta_text(speaker_text, created_at, source_id)

    # 彻底与数据库原样展示保持一致
    final_text = str(display_text or "").strip()
    if not final_text:
        final_text = str(body or "没内容").strip()

    return [f"【{title}】 {meta}", final_text]


def _human_collection_block(
    title: str,
    item_blocks: list[str],
    *,
    hidden_count: int,
    hidden_label: str,
) -> str:
    parts = [f"--- {title} ---"]
    if item_blocks:
        parts.extend(item_blocks)
        if hidden_count > 0:
            parts.append(f"\n（还有 {hidden_count} 条{hidden_label}没展开）")
    else:
        parts.append("没有")
    return "\n\n".join(parts)


def _human_topic_post_block(topic_post: Optional[dict[str, Any]]) -> str:
    if not isinstance(topic_post, dict):
        return "主帖\n  没拿到"
    return "\n".join(
        _human_message_lines(
            "主帖",
            speaker=topic_post.get("speaker"),
            created_at=topic_post.get("created_at"),
            source_id=topic_post.get("source_id"),
            display_text=topic_post.get("display_text"),
            body=topic_post.get("text"),
        )
    )


def _human_comment_block(comment: dict[str, Any], index: int) -> str:
    return "\n".join(
        _human_message_lines(
            f"第 {index} 条",
            speaker=comment.get("speaker"),
            created_at=comment.get("created_at"),
            source_id=comment.get("source_id"),
            display_text=comment.get("display_text"),
            body=comment.get("text"),
        )
    )


def _human_status_update_block(item: dict[str, Any], index: int) -> str:
    return "\n".join(
        _human_message_lines(
            f"第 {index} 条",
            speaker=item.get("speaker"),
            created_at=item.get("created_at"),
            source_id=item.get("source_id"),
            display_text=item.get("display_text"),
            body=item.get("text"),
        )
    )


def _build_human_topic_block(topic: dict[str, Any], index: int) -> str:
    topic_status_id = str(topic.get("topic_status_id") or "").strip()
    focus_username = _topic_focus_username(topic) or "未知用户"
    stats = topic.get("stats") or {}
    latest_activity_at = str(stats.get("latest_activity_at") or "").strip()
    comment_count = int(stats.get("comment_count") or 0)
    talk_message_count = int(stats.get("talk_message_count") or 0)
    status_updates = topic.get("status_updates") or []
    comments = topic.get("comments") or []
    if not isinstance(status_updates, list):
        status_updates = []
    if not isinstance(comments, list):
        comments = []

    summary_lines = [
        f"[{index}] 话题 {topic_status_id}",
        f"处理作者：{focus_username}",
        f"最近活跃：{latest_activity_at or '未知'}",
        f"数量：独立发言 {len(status_updates)} 条 | 评论 {comment_count} 条 | 对话 {talk_message_count} 条",
    ]
    sections = [
        "\n".join(summary_lines),
        _human_topic_post_block(topic.get("topic_post")),
    ]

    status_blocks = []
    if isinstance(status_updates, list):
        for status_index, item in enumerate(
            status_updates[:HUMAN_COMMENT_LIMIT], start=1
        ):
            status_blocks.append(_human_status_update_block(item, status_index))
    sections.append(
        _human_collection_block(
            "独立发言",
            status_blocks,
            hidden_count=max(0, len(status_updates) - HUMAN_COMMENT_LIMIT),
            hidden_label="独立发言",
        )
    )

    comment_blocks = []
    if isinstance(comments, list):
        for comment_index, item in enumerate(comments[:HUMAN_COMMENT_LIMIT], start=1):
            comment_blocks.append(_human_comment_block(item, comment_index))
    sections.append(
        _human_collection_block(
            "评论",
            comment_blocks,
            hidden_count=max(0, len(comments) - HUMAN_COMMENT_LIMIT),
            hidden_label="评论",
        )
    )

    return "\n\n".join(sections)


def _build_human_report(topic_packages: list[dict[str, Any]]) -> str:
    topic_blocks = [
        _build_human_topic_block(topic, index)
        for index, topic in enumerate(topic_packages, start=1)
    ]
    if not topic_blocks:
        return HUMAN_REPORT_TITLE
    return f"{HUMAN_REPORT_TITLE}\n\n{HUMAN_TOPIC_SEPARATOR.join(topic_blocks)}"


def _build_combined_output(topic_packages: list[dict[str, Any]]) -> str:
    sections: list[str] = []
    for index, topic in enumerate(topic_packages, start=1):
        focus_username = _topic_focus_username(topic)
        topic_json = _build_single_topic_json(topic)
        human_block = _build_human_topic_block(topic, index)
        sections.append(
            "\n\n".join(
                [
                    _build_prompt_header(focus_username),
                    "\n".join(
                        [
                            f"话题块 {index}",
                            "JSON：",
                            "```json",
                            topic_json,
                            "```",
                            "人工核查（纯数据库 text 拼接形态）：",
                            human_block,
                        ]
                    ),
                ]
            )
        )
    return "\n\n----\n\n".join(sections) + "\n"


def _build_single_topic_output(
    topic_package: dict[str, Any],
    *,
    index: int = 1,
    ai_topic_package: Optional[dict[str, Any]] = None,
) -> str:
    focus_username = _topic_focus_username(topic_package)
    topic_json = _build_single_topic_json(
        topic_package,
        ai_topic_package=ai_topic_package,
    )
    human_block = _build_human_topic_block(topic_package, index)
    return (
        "\n\n".join(
            [
                _build_prompt_header(focus_username),
                "\n".join(
                    [
                        f"话题块 {index}",
                        "JSON：",
                        "```json",
                        topic_json,
                        "```",
                        "人工核查（纯数据库 text 拼接形态）：",
                        human_block,
                    ]
                ),
            ]
        )
        + "\n"
    )


def _build_single_topic_json(
    topic_package: dict[str, Any],
    *,
    ai_topic_package: Optional[dict[str, Any]] = None,
) -> str:
    if ai_topic_package is None:
        focus_username = _topic_focus_username(topic_package)
        ai_topic_package = _build_ai_topic_package(topic_package, focus_username)
    return _build_topic_json(ai_topic_package)


def _resolve_model_name(raw_model: Optional[str]) -> str:
    model_name = str(raw_model or "").strip()
    if not model_name:
        raise SystemExit("要调 AI 时必须提供模型名。请传 --model。")
    return model_name


def _resolve_api_config(args: argparse.Namespace) -> dict[str, str]:
    api_type = str(args.api_type or "").strip()
    if not api_type:
        raise SystemExit("要调 AI 时必须提供 --api-type。比如 openai 或 gemini。")

    api_mode = str(args.api_mode or DEFAULT_AI_MODE).strip()
    model_name = _resolve_model_name(args.model)
    base_url = str(args.base_url or "").strip()

    direct_api_key = str(args.api_key or "").strip()
    api_key_env = str(args.api_key_env or "").strip()
    env_api_key = str(os.getenv(api_key_env) or "").strip() if api_key_env else ""
    api_key = direct_api_key or env_api_key
    if not api_key:
        raise SystemExit(
            "要调 AI 时必须提供 --api-key，或者传 --api-key-env 指向已有环境变量。"
        )

    return {
        "api_type": api_type,
        "api_mode": api_mode,
        "ai_stream": "1" if bool(args.ai_stream) else "",
        "model_name": model_name,
        "base_url": base_url,
        "api_key": api_key,
        "api_key_env": api_key_env,
    }


def _response_attr(obj: Any, key: str) -> Any:
    if isinstance(obj, dict):
        return obj.get(key)
    return getattr(obj, key, None)


def _extract_text_from_response_content(content: Any) -> str:
    parts: list[str] = []

    def walk(node: Any) -> None:
        if node is None:
            return
        if isinstance(node, str):
            text = node.strip()
            if text:
                parts.append(text)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)
            return

        text_value = _response_attr(node, "text")
        if isinstance(text_value, str):
            text = text_value.strip()
            if text:
                parts.append(text)

        content_value = _response_attr(node, "content")
        if content_value is not None and content_value is not node:
            walk(content_value)

    walk(content)
    return "\n".join(parts).strip()


def _extract_ai_text(response: Any, *, _seen_ids: Optional[set[int]] = None) -> str:
    if _seen_ids is None:
        _seen_ids = set()
    current_id = id(response)
    if current_id in _seen_ids:
        return ""
    _seen_ids.add(current_id)

    output_text = _response_attr(response, "output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    nested_response = _response_attr(response, "response")
    if nested_response is not None and nested_response is not response:
        nested_text = _extract_ai_text(nested_response, _seen_ids=_seen_ids)
        if nested_text:
            return nested_text

    output = _response_attr(response, "output")
    extracted_output_text = _extract_text_from_response_content(output)
    if extracted_output_text:
        return extracted_output_text

    choices = getattr(response, "choices", None)
    if choices and len(choices) > 0:
        first_choice = choices[0]
        message = getattr(first_choice, "message", None)
        if message is not None:
            content = getattr(message, "content", None)
            extracted_choice_text = _extract_text_from_response_content(content)
            if extracted_choice_text:
                return extracted_choice_text

    if isinstance(response, dict):
        raw_choices = response.get("choices")
        if isinstance(raw_choices, list) and raw_choices:
            first_choice = raw_choices[0]
            if isinstance(first_choice, dict):
                message = first_choice.get("message")
                if isinstance(message, dict):
                    extracted_choice_text = _extract_text_from_response_content(
                        message.get("content")
                    )
                    if extracted_choice_text:
                        return extracted_choice_text
    return ""


def _resolve_litellm_model_name(model_name: str, api_type: str, base_url: str) -> str:
    resolved_model_name = str(model_name or "").strip()
    if not resolved_model_name:
        return ""
    if "/" in resolved_model_name:
        return resolved_model_name
    if str(api_type or "").strip() == "openai" and str(base_url or "").strip():
        return f"openai/{resolved_model_name}"
    return resolved_model_name


def _mask_secret(value: Any) -> str:
    secret = str(value or "").strip()
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}***{secret[-4:]}"


def _compact_timestamp() -> str:
    return time.strftime("%Y%m%dT%H%M%S", time.localtime())


def _trace_id_part(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "na"
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"[^\w\u4e00-\u9fff\-]+", "-", text)
    text = re.sub(r"-{2,}", "-", text).strip("-_")
    return text or "na"


def _build_trace_id(
    *,
    topic_status_id: str,
    focus_username: str,
    attempt: int,
    api_config: dict[str, str],
    stamp: str,
) -> str:
    api_mode = _trace_id_part(api_config.get("api_mode"))
    stream_mode = (
        "stream" if bool(_normalize_text(api_config.get("ai_stream"))) else "sync"
    )
    return "::".join(
        [
            _trace_id_part(topic_status_id),
            _trace_id_part(focus_username),
            f"try{int(attempt):02d}",
            api_mode,
            stream_mode,
            _trace_id_part(stamp),
        ]
    )


def _to_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): _to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_jsonable(item) for item in value]

    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        for kwargs in (
            {"mode": "json", "warnings": False},
            {"mode": "json"},
            {},
        ):
            try:
                with warnings.catch_warnings():
                    warnings.filterwarnings(
                        "ignore",
                        message=r".*Pydantic serializer warnings:.*",
                        category=UserWarning,
                        module=r"pydantic\.main",
                    )
                    return _to_jsonable(model_dump(**kwargs))
            except TypeError:
                continue
            except Exception:
                break

    dict_fn = getattr(value, "dict", None)
    if callable(dict_fn):
        try:
            return _to_jsonable(dict_fn())
        except Exception:
            pass

    return str(value)


def _sanitize_request_payload(payload: dict[str, Any]) -> dict[str, Any]:
    sanitized = _to_jsonable(payload)
    if not isinstance(sanitized, dict):
        return {"raw": sanitized}
    if "api_key" in sanitized:
        sanitized["api_key"] = _mask_secret(sanitized.get("api_key"))
    return sanitized


def _summarize_trace_chunk_types(chunks: Any) -> dict[str, int]:
    if not isinstance(chunks, list):
        return {}
    counts: dict[str, int] = {}
    for chunk in chunks:
        if not isinstance(chunk, dict):
            continue
        chunk_type = _normalize_text(chunk.get("type"))
        if not chunk_type:
            continue
        counts[chunk_type] = counts.get(chunk_type, 0) + 1
    return dict(
        sorted(
            counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:TRACE_CHUNK_TYPE_LIMIT]
    )


def _summarize_response_snapshot(snapshot: Any) -> dict[str, Any]:
    if isinstance(snapshot, dict):
        if bool(snapshot.get("stream_mode")):
            chunks = snapshot.get("chunks")
            out: dict[str, Any] = {
                "stream_mode": True,
                "chunk_count": len(chunks) if isinstance(chunks, list) else 0,
            }
            chunk_type_counts = _summarize_trace_chunk_types(chunks)
            if chunk_type_counts:
                out["chunk_type_counts"] = chunk_type_counts
            if snapshot.get("rebuilt_response") not in (None, "", {}, []):
                out["has_rebuilt_response"] = True
            return out

        keys = [str(key).strip() for key in snapshot.keys() if str(key).strip()]
        return {
            "stream_mode": False,
            "key_count": len(keys),
            "top_level_keys": keys[:TRACE_RESPONSE_KEYS_LIMIT],
        }

    if isinstance(snapshot, list):
        return {
            "snapshot_type": "list",
            "item_count": len(snapshot),
        }

    if snapshot in (None, ""):
        return {}

    return {"snapshot_type": type(snapshot).__name__}


def _append_trace_record(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(_format_trace_record(record))
        fh.write("\n")


def _append_trace_note(path: Path, title: str, lines: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write("=" * 100)
        fh.write("\n")
        fh.write(str(title).strip())
        fh.write("\n")
        for line in lines:
            fh.write(str(line or "").rstrip())
            fh.write("\n")
        fh.write("=" * 100)
        fh.write("\n\n")


def _extract_stream_text_delta(chunk: Any) -> str:
    choices = _response_attr(chunk, "choices")
    if isinstance(choices, list) and choices:
        first_choice = choices[0]
        delta = _response_attr(first_choice, "delta")
        content = _response_attr(delta, "content")
        if isinstance(content, str) and content:
            return content

    event_type = _normalize_text(_response_attr(chunk, "type"))
    delta = _response_attr(chunk, "delta")
    if "delta" in event_type:
        if isinstance(delta, str) and delta:
            return delta
        delta_text = _response_attr(delta, "text")
        if isinstance(delta_text, str) and delta_text:
            return delta_text
        delta_content = _response_attr(delta, "content")
        if isinstance(delta_content, str) and delta_content:
            return delta_content
    return ""


def _collect_streamed_ai_text(
    stream_response: Any,
    *,
    api_mode: str,
) -> dict[str, Any]:
    chunks: list[Any] = []
    chunk_snapshots: list[Any] = []
    text_parts: list[str] = []

    for chunk in stream_response:
        chunks.append(chunk)
        chunk_snapshots.append(_to_jsonable(chunk))
        text_delta = _extract_stream_text_delta(chunk)
        if text_delta:
            text_parts.append(text_delta)

    streamed_text = "".join(text_parts).strip()
    if streamed_text:
        return {
            "raw_text": streamed_text,
            "response_snapshot": {
                "stream_mode": True,
                "chunks": chunk_snapshots,
            },
        }

    if api_mode == AI_MODE_COMPLETION and chunks:
        try:
            import litellm

            rebuilt = litellm.stream_chunk_builder(chunks)
            rebuilt_text = _extract_ai_text(rebuilt)
            if rebuilt_text:
                return {
                    "raw_text": rebuilt_text,
                    "response_snapshot": {
                        "stream_mode": True,
                        "chunks": chunk_snapshots,
                        "rebuilt_response": _to_jsonable(rebuilt),
                    },
                }
        except Exception:
            pass

    for chunk in reversed(chunks):
        chunk_text = _extract_ai_text(chunk)
        if chunk_text:
            return {
                "raw_text": chunk_text,
                "response_snapshot": {
                    "stream_mode": True,
                    "chunks": chunk_snapshots,
                },
            }
    return {
        "raw_text": "",
        "response_snapshot": {
            "stream_mode": True,
            "chunks": chunk_snapshots,
        },
    }


def _strip_json_fence(text: str) -> str:
    stripped = str(text or "").strip()
    if stripped.startswith("```json"):
        stripped = stripped[len("```json") :].strip()
    elif stripped.startswith("```"):
        stripped = stripped[len("```") :].strip()
    if stripped.endswith("```"):
        stripped = stripped[:-3].strip()
    return stripped


def _parse_ai_json_text(text: str) -> dict[str, Any]:
    cleaned = _strip_json_fence(text)
    if not cleaned:
        raise ValueError("AI 没返回正文。")
    try:
        obj = json.loads(cleaned)
    except Exception as exc:
        raise ValueError(f"AI 返回的不是合法 JSON：{exc}") from exc
    if not isinstance(obj, dict):
        raise ValueError("AI 返回的 JSON 顶层不是对象。")
    return obj


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _now_log_time() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _log(message: str) -> None:
    print(f"[{_now_log_time()}] {message}", file=sys.stderr)


def _wait_for_ai_rpm_slot(
    rpm: float,
    limiter_state: dict[str, float],
    limiter_lock: Optional[threading.Lock] = None,
) -> None:
    rpm_value = float(rpm)
    if rpm_value <= 0:
        return

    def wait_once() -> None:
        min_interval_sec = 60.0 / rpm_value
        now = time.time()
        next_allowed_at = float(limiter_state.get("next_allowed_at") or 0.0)
        if next_allowed_at > now:
            sleep_sec = next_allowed_at - now
            time.sleep(sleep_sec)
            now = time.time()
        limiter_state["next_allowed_at"] = now + min_interval_sec

    if limiter_lock is None:
        wait_once()
        return

    with limiter_lock:
        wait_once()


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = str(item or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _normalize_confidence(value: Any) -> float:
    try:
        num = float(value)
    except Exception:
        return 0.0
    if num < 0:
        return 0.0
    if num > 1:
        return 1.0
    return round(num, 4)


def _normalize_action_strength(value: Any) -> int:
    try:
        num = int(value)
    except Exception:
        return 0
    return max(0, min(3, num))


def _build_post_uid(source_kind: str, source_id: str) -> str:
    return f"xueqiu:{source_kind}:{source_id}"


def _build_assertion_uid(topic_status_id: str, author: str, item_index: int) -> str:
    return "::".join(
        [
            ASSERTION_UID_PREFIX,
            _trace_id_part(topic_status_id),
            _trace_id_part(author),
            f"{int(item_index):03d}",
        ]
    )


def _normalize_evidence_refs(item: dict[str, Any]) -> list[dict[str, str]]:
    refs_raw = item.get(EVIDENCE_REFS_FIELD_NAME)
    refs: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    if isinstance(refs_raw, list):
        for row in refs_raw:
            if not isinstance(row, dict):
                continue
            ref = {
                "source_kind": _normalize_text(row.get("source_kind")),
                "source_id": _normalize_text(row.get("source_id")),
                "quote": _normalize_text(row.get("quote")),
            }
            ref_key = (ref["source_kind"], ref["source_id"], ref["quote"])
            if ref_key in seen:
                continue
            seen.add(ref_key)
            refs.append(ref)
    if refs:
        return refs

    fallback_ref = {
        "source_kind": _normalize_text(item.get("source_kind")),
        "source_id": _normalize_text(item.get("source_id")),
        "quote": _normalize_text(item.get("evidence")),
    }
    if any(fallback_ref.values()):
        return [fallback_ref]
    return []


def _primary_evidence_ref(evidence_refs: list[dict[str, str]]) -> dict[str, str]:
    return evidence_refs[0] if evidence_refs else {}


def _assertion_post_refs(
    row: dict[str, Any],
    *,
    message_lookup: dict[tuple[str, str], dict[str, Any]],
    focus_username: str,
) -> list[dict[str, Any]]:
    post_refs: list[dict[str, Any]] = []
    seen_post_uids: set[str] = set()
    focus_username_text = _normalize_text(focus_username)
    for ref in _normalize_evidence_refs(row):
        source_kind = _normalize_text(ref.get("source_kind"))
        source_id = _normalize_text(ref.get("source_id"))
        if not source_kind or not source_id:
            continue
        message = message_lookup.get((source_kind, source_id), {})
        if _normalize_text(message.get("speaker")) != focus_username_text:
            continue
        post_uid = _build_post_uid(source_kind, source_id)
        if post_uid in seen_post_uids:
            continue
        seen_post_uids.add(post_uid)
        post_refs.append(
            {
                "post_uid": post_uid,
                "source_kind": source_kind,
                "source_id": source_id,
                "quote": _normalize_text(ref.get("quote")),
                "message": message,
            }
        )
    return post_refs


def _build_assertions_preview(
    *,
    topic_package: dict[str, Any],
    ai_result: dict[str, Any],
    message_lookup: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    topic_status_id = _normalize_text(
        ai_result.get("topic_status_id")
    ) or _normalize_text(topic_package.get("topic_status_id"))
    items = ai_result.get("items")
    if not isinstance(items, list):
        return []

    post_counters: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    for item_index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        evidence_refs = _normalize_evidence_refs(item)
        primary_ref = _primary_evidence_ref(evidence_refs)
        source_kind = _normalize_text(primary_ref.get("source_kind"))
        source_id = _normalize_text(primary_ref.get("source_id"))
        post_uid = _build_post_uid(source_kind or "unknown", source_id or "unknown")
        post_counters[post_uid] = post_counters.get(post_uid, 0) + 1
        idx = post_counters[post_uid]

        stock_codes = _normalize_string_list(item.get("stock_codes"))
        stock_names = _normalize_string_list(item.get("stock_names"))
        industries = _normalize_string_list(item.get("industries"))
        commodities = _normalize_string_list(item.get("commodities"))
        indices = _normalize_string_list(item.get("indices"))
        message_context = message_lookup.get((source_kind, source_id), {})
        author = _normalize_text(item.get("speaker")) or _normalize_text(
            message_context.get("speaker")
        )

        rows.append(
            {
                "assertion_uid": _build_assertion_uid(
                    topic_status_id, author or "unknown", item_index
                ),
                "post_uid": post_uid,
                "idx": idx,
                "topic_status_id": topic_status_id,
                "source_kind": source_kind,
                "source_id": source_id,
                "author": author,
                "created_at": _normalize_text(message_context.get("created_at")),
                "relation_to_topic": _normalize_text(item.get("relation_to_topic")),
                "topic_key": _normalize_text(item.get("topic_key")),
                "action": _normalize_text(item.get("action")),
                "action_strength": _normalize_action_strength(
                    item.get("action_strength")
                ),
                "summary": _normalize_text(item.get("summary")),
                "evidence": _normalize_text(primary_ref.get("quote")),
                EVIDENCE_REFS_FIELD_NAME: evidence_refs,
                EVIDENCE_REFS_JSON_COLUMN_NAME: json.dumps(
                    evidence_refs, ensure_ascii=False
                ),
                "confidence": _normalize_confidence(item.get("confidence")),
                "stock_codes_json": json.dumps(stock_codes, ensure_ascii=False),
                "stock_names_json": json.dumps(stock_names, ensure_ascii=False),
                "industries_json": json.dumps(industries, ensure_ascii=False),
                "commodities_json": json.dumps(commodities, ensure_ascii=False),
                "indices_json": json.dumps(indices, ensure_ascii=False),
            }
        )
    return rows


def _source_url_from_message(message: dict[str, Any]) -> str:
    source_kind = _normalize_text(message.get("source_kind"))
    source_id = _normalize_text(message.get("source_id"))
    if not source_kind or not source_id:
        return ""
    if source_kind in {"status", "topic_post"}:
        return f"https://xueqiu.com/S/{source_id}"
    return ""


def _message_text_for_evidence(message: dict[str, Any]) -> str:
    commentary_text = _normalize_text(message.get("commentary_text"))
    if commentary_text:
        return commentary_text
    return _normalize_text(message.get("text"))


def _message_text_for_post_context(message: dict[str, Any]) -> str:
    text = _build_prompt_message_text(message)
    if not text:
        text = _normalize_text(message.get("text"))
    if not text:
        text = _normalize_text(message.get("display_text"))
    speaker = _normalize_text(message.get("speaker"))
    if speaker and text:
        return f"{speaker}：{text}"
    return text


def _build_post_raw_text(message: dict[str, Any]) -> str:
    raw_path = message.get("root_path")
    path_nodes = raw_path if isinstance(raw_path, list) else [message]
    parts: list[str] = []
    seen_nodes: set[tuple[str, str]] = set()
    for node in path_nodes:
        if not isinstance(node, dict):
            continue
        node_key = (
            _normalize_text(node.get("source_kind")),
            _normalize_text(node.get("source_id")),
        )
        if node_key in seen_nodes:
            continue
        if any(node_key):
            seen_nodes.add(node_key)
        text = _message_text_for_post_context(node)
        if text:
            parts.append(text)
    if parts:
        return DISPLAY_SEPARATOR.join(parts)
    return _normalize_text(message.get("text"))


def _validate_assertions_preview(
    *,
    focus_username: str,
    rows: list[dict[str, Any]],
    message_lookup: dict[tuple[str, str], dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    valid_rows: list[dict[str, Any]] = []
    invalid_rows: list[dict[str, Any]] = []
    focus_username_text = _normalize_text(focus_username)

    for row in rows:
        errors: list[str] = []
        topic_key = _normalize_text(row.get("topic_key"))
        action = _normalize_text(row.get("action"))
        author = _normalize_text(row.get("author"))
        evidence_refs = _normalize_evidence_refs(row)

        if not topic_key:
            errors.append("topic_key 为空")
        if not action:
            errors.append("action 为空")
        if author != focus_username_text:
            errors.append("author 不是核心作者")
        if not evidence_refs:
            errors.append("evidence_refs 为空")

        for ref_index, ref in enumerate(evidence_refs, start=1):
            ref_label = f"evidence_refs[{ref_index}]"
            source_kind = _normalize_text(ref.get("source_kind"))
            source_id = _normalize_text(ref.get("source_id"))
            quote = _normalize_text(ref.get("quote"))
            if not source_kind:
                errors.append(f"{ref_label}.source_kind 为空")
            if not source_id:
                errors.append(f"{ref_label}.source_id 为空")
            if not quote:
                errors.append(f"{ref_label}.quote 为空")
            if not source_kind or not source_id:
                continue

            message = message_lookup.get((source_kind, source_id), {})
            message_speaker = _normalize_text(message.get("speaker"))
            if message_speaker != focus_username_text:
                errors.append(f"{ref_label} 对应原消息 speaker 不是核心作者")
            if not quote:
                continue

            message_text = _message_text_for_evidence(message)
            if not message_text:
                errors.append(f"{ref_label} 没找到对应原文，无法校验 quote")
            elif quote not in message_text:
                errors.append(f"{ref_label}.quote 不在对应消息正文里")

        if errors:
            invalid_rows.append(
                {
                    **row,
                    "validation_errors": errors,
                }
            )
        else:
            valid_rows.append(row)

    return valid_rows, invalid_rows


def _build_posts_preview(
    *,
    topic_package: dict[str, Any],
    ai_result: dict[str, Any],
    assertions_preview: list[dict[str, Any]],
    valid_assertions: list[dict[str, Any]],
    invalid_assertions: list[dict[str, Any]],
    model_name: str,
    message_lookup: dict[tuple[str, str], dict[str, Any]],
) -> list[dict[str, Any]]:
    topic_status_id = _normalize_text(
        ai_result.get("topic_status_id")
    ) or _normalize_text(topic_package.get("topic_status_id"))
    processed_at = time.strftime("%Y-%m-%dT%H:%M:%S%z", time.localtime())

    rows: list[dict[str, Any]] = []
    rows_by_post_uid: dict[str, dict[str, Any]] = {}
    focus_username_text = _normalize_text(
        (topic_package.get("focus_username") if isinstance(topic_package, dict) else "")
        or ""
    )
    valid_assertion_uids = {
        _normalize_text(row.get(ASSERTION_UID_COLUMN_NAME)) for row in valid_assertions
    }
    invalid_errors_by_assertion_uid = {
        _normalize_text(row.get(ASSERTION_UID_COLUMN_NAME)): [
            _normalize_text(error)
            for error in row.get("validation_errors", [])
            if _normalize_text(error)
        ]
        for row in invalid_assertions
    }
    invalid_errors_by_post_uid: dict[str, list[str]] = {}
    status_priority = {"irrelevant": 0, "error": 1, "relevant": 2}

    for item in assertions_preview:
        if not isinstance(item, dict):
            continue
        if _normalize_text(item.get("author")) != focus_username_text:
            continue
        assertion_uid = _normalize_text(item.get(ASSERTION_UID_COLUMN_NAME))
        status = "irrelevant"
        if assertion_uid in valid_assertion_uids:
            status = "relevant"
        elif assertion_uid in invalid_errors_by_assertion_uid:
            status = "error"

        for post_ref in _assertion_post_refs(
            item,
            message_lookup=message_lookup,
            focus_username=focus_username_text,
        ):
            post_uid = _normalize_text(post_ref.get("post_uid"))
            source_id = _normalize_text(post_ref.get("source_id"))
            message_value = post_ref.get("message")
            message: dict[str, Any] = (
                message_value if isinstance(message_value, dict) else {}
            )
            current_row = rows_by_post_uid.get(post_uid)
            candidate_priority = status_priority.get(status, 0)
            if current_row is None:
                current_row = {
                    "post_uid": post_uid,
                    "platform": "xueqiu",
                    "platform_post_id": source_id,
                    "author": _normalize_text(message.get("speaker"))
                    or focus_username_text,
                    "created_at": _normalize_text(message.get("created_at")),
                    "url": _source_url_from_message(message),
                    "raw_text": _build_post_raw_text(message),
                    "topic_status_id": topic_status_id,
                    "status": status,
                    "invest_score": 1.0 if status == "relevant" else 0.0,
                    "processed_at": processed_at,
                    "attempts": 1,
                    "last_error": "",
                    "model": model_name,
                    "prompt_version": PROMPT_VERSION,
                }
                rows_by_post_uid[post_uid] = current_row
            elif candidate_priority > status_priority.get(
                str(current_row.get("status") or ""), 0
            ):
                current_row["status"] = status
                current_row["invest_score"] = 1.0 if status == "relevant" else 0.0

            if status == "error":
                error_bucket = invalid_errors_by_post_uid.setdefault(post_uid, [])
                for error in invalid_errors_by_assertion_uid.get(assertion_uid, []):
                    if error not in error_bucket:
                        error_bucket.append(error)

    for post_uid, row in rows_by_post_uid.items():
        if _normalize_text(row.get("status")) == "error":
            row["last_error"] = "; ".join(invalid_errors_by_post_uid.get(post_uid, []))
        rows.append(row)
    return rows


def _ensure_assertions_table(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {ASSERTIONS_TABLE_NAME} (
              post_uid TEXT NOT NULL,
              idx INTEGER NOT NULL,
              {ASSERTION_UID_COLUMN_NAME} TEXT NOT NULL DEFAULT '',
              topic_status_id TEXT NOT NULL DEFAULT '',
              source_kind TEXT NOT NULL DEFAULT '',
              source_id TEXT NOT NULL DEFAULT '',
              author TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT '',
              relation_to_topic TEXT NOT NULL DEFAULT '',
              topic_key TEXT NOT NULL DEFAULT '',
              action TEXT NOT NULL DEFAULT '',
              action_strength INTEGER NOT NULL DEFAULT 0,
              summary TEXT NOT NULL DEFAULT '',
              evidence TEXT NOT NULL DEFAULT '',
              {EVIDENCE_REFS_JSON_COLUMN_NAME} TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              confidence REAL NOT NULL DEFAULT 0,
              stock_codes_json TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              stock_names_json TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              industries_json TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              commodities_json TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              indices_json TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}',
              PRIMARY KEY (post_uid, idx)
            )
            """
        )
        column_names = {
            str(row[1]).strip()
            for row in conn.execute(f"PRAGMA table_info({ASSERTIONS_TABLE_NAME})")
        }
        if ASSERTION_UID_COLUMN_NAME not in column_names:
            conn.execute(
                f"ALTER TABLE {ASSERTIONS_TABLE_NAME} ADD COLUMN {ASSERTION_UID_COLUMN_NAME} TEXT NOT NULL DEFAULT ''"
            )
        if EVIDENCE_REFS_JSON_COLUMN_NAME not in column_names:
            conn.execute(
                f"ALTER TABLE {ASSERTIONS_TABLE_NAME} ADD COLUMN {EVIDENCE_REFS_JSON_COLUMN_NAME} TEXT NOT NULL DEFAULT '{JSON_ARRAY_EMPTY}'"
            )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{ASSERTIONS_TABLE_NAME}_topic_key_created_at
            ON {ASSERTIONS_TABLE_NAME}(topic_key, created_at)
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{ASSERTIONS_TABLE_NAME}_author_created_at
            ON {ASSERTIONS_TABLE_NAME}(author, created_at)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _upsert_assertions(db_path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executemany(
            f"""
            INSERT INTO {ASSERTIONS_TABLE_NAME} (
              post_uid,
              idx,
              {ASSERTION_UID_COLUMN_NAME},
              topic_status_id,
              source_kind,
              source_id,
              author,
              created_at,
              relation_to_topic,
              topic_key,
              action,
              action_strength,
              summary,
              evidence,
              {EVIDENCE_REFS_JSON_COLUMN_NAME},
              confidence,
              stock_codes_json,
              stock_names_json,
              industries_json,
              commodities_json,
              indices_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(post_uid, idx) DO UPDATE SET
              {ASSERTION_UID_COLUMN_NAME} = excluded.{ASSERTION_UID_COLUMN_NAME},
              topic_status_id = excluded.topic_status_id,
              source_kind = excluded.source_kind,
              source_id = excluded.source_id,
              author = excluded.author,
              created_at = excluded.created_at,
              relation_to_topic = excluded.relation_to_topic,
              topic_key = excluded.topic_key,
              action = excluded.action,
              action_strength = excluded.action_strength,
              summary = excluded.summary,
              evidence = excluded.evidence,
              {EVIDENCE_REFS_JSON_COLUMN_NAME} = excluded.{EVIDENCE_REFS_JSON_COLUMN_NAME},
              confidence = excluded.confidence,
              stock_codes_json = excluded.stock_codes_json,
              stock_names_json = excluded.stock_names_json,
              industries_json = excluded.industries_json,
              commodities_json = excluded.commodities_json,
              indices_json = excluded.indices_json
            """,
            [
                (
                    str(row.get("post_uid") or ""),
                    int(row.get("idx") or 0),
                    str(row.get(ASSERTION_UID_COLUMN_NAME) or ""),
                    str(row.get("topic_status_id") or ""),
                    str(row.get("source_kind") or ""),
                    str(row.get("source_id") or ""),
                    str(row.get("author") or ""),
                    str(row.get("created_at") or ""),
                    str(row.get("relation_to_topic") or ""),
                    str(row.get("topic_key") or ""),
                    str(row.get("action") or ""),
                    int(row.get("action_strength") or 0),
                    str(row.get("summary") or ""),
                    str(row.get("evidence") or ""),
                    str(row.get(EVIDENCE_REFS_JSON_COLUMN_NAME) or JSON_ARRAY_EMPTY),
                    float(row.get("confidence") or 0),
                    str(row.get("stock_codes_json") or JSON_ARRAY_EMPTY),
                    str(row.get("stock_names_json") or JSON_ARRAY_EMPTY),
                    str(row.get("industries_json") or JSON_ARRAY_EMPTY),
                    str(row.get("commodities_json") or JSON_ARRAY_EMPTY),
                    str(row.get("indices_json") or JSON_ARRAY_EMPTY),
                )
                for row in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _delete_assertions_by_topic(
    db_path: Path, topic_status_id: str, focus_username: str
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            f"DELETE FROM {ASSERTIONS_TABLE_NAME} WHERE topic_status_id = ? AND author = ?",
            (str(topic_status_id or ""), str(focus_username or "")),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def _ensure_posts_table(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {POSTS_TABLE_NAME} (
              post_uid TEXT PRIMARY KEY,
              platform TEXT NOT NULL DEFAULT '',
              platform_post_id TEXT NOT NULL DEFAULT '',
              author TEXT NOT NULL DEFAULT '',
              created_at TEXT NOT NULL DEFAULT '',
              url TEXT NOT NULL DEFAULT '',
              raw_text TEXT NOT NULL DEFAULT '',
              topic_status_id TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT '',
              invest_score REAL NOT NULL DEFAULT 0,
              processed_at TEXT NOT NULL DEFAULT '',
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '',
              model TEXT NOT NULL DEFAULT '',
              prompt_version TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{POSTS_TABLE_NAME}_status_created_at
            ON {POSTS_TABLE_NAME}(status, created_at)
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{POSTS_TABLE_NAME}_topic_status_id
            ON {POSTS_TABLE_NAME}(topic_status_id)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _upsert_posts(db_path: Path, rows: list[dict[str, Any]]) -> int:
    if not rows:
        return 0
    conn = sqlite3.connect(str(db_path))
    try:
        conn.executemany(
            f"""
            INSERT INTO {POSTS_TABLE_NAME} (
              post_uid,
              platform,
              platform_post_id,
              author,
              created_at,
              url,
              raw_text,
              topic_status_id,
              status,
              invest_score,
              processed_at,
              attempts,
              last_error,
              model,
              prompt_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(post_uid) DO UPDATE SET
              platform = excluded.platform,
              platform_post_id = excluded.platform_post_id,
              author = excluded.author,
              created_at = excluded.created_at,
              url = excluded.url,
              raw_text = excluded.raw_text,
              topic_status_id = excluded.topic_status_id,
              status = excluded.status,
              invest_score = excluded.invest_score,
              processed_at = excluded.processed_at,
              attempts = excluded.attempts,
              last_error = excluded.last_error,
              model = excluded.model,
              prompt_version = excluded.prompt_version
            """,
            [
                (
                    str(row.get("post_uid") or ""),
                    str(row.get("platform") or ""),
                    str(row.get("platform_post_id") or ""),
                    str(row.get("author") or ""),
                    str(row.get("created_at") or ""),
                    str(row.get("url") or ""),
                    str(row.get("raw_text") or ""),
                    str(row.get("topic_status_id") or ""),
                    str(row.get("status") or ""),
                    float(row.get("invest_score") or 0),
                    str(row.get("processed_at") or ""),
                    int(row.get("attempts") or 0),
                    str(row.get("last_error") or ""),
                    str(row.get("model") or ""),
                    str(row.get("prompt_version") or ""),
                )
                for row in rows
            ],
        )
        conn.commit()
        return len(rows)
    finally:
        conn.close()


def _delete_posts_by_topic(
    db_path: Path, topic_status_id: str, focus_username: str
) -> int:
    conn = sqlite3.connect(str(db_path))
    try:
        cursor = conn.execute(
            f"DELETE FROM {POSTS_TABLE_NAME} WHERE topic_status_id = ? AND author = ?",
            (str(topic_status_id or ""), str(focus_username or "")),
        )
        conn.commit()
        return int(cursor.rowcount or 0)
    finally:
        conn.close()


def _ensure_topic_run_progress_table(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {TOPIC_RUN_PROGRESS_TABLE_NAME} (
              topic_package_key TEXT PRIMARY KEY,
              topic_status_id TEXT NOT NULL DEFAULT '',
              focus_username TEXT NOT NULL DEFAULT '',
              status TEXT NOT NULL DEFAULT '',
              attempts INTEGER NOT NULL DEFAULT 0,
              last_error TEXT NOT NULL DEFAULT '',
              updated_at TEXT NOT NULL DEFAULT '',
              prompt_version TEXT NOT NULL DEFAULT ''
            )
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TOPIC_RUN_PROGRESS_TABLE_NAME}_status_updated_at
            ON {TOPIC_RUN_PROGRESS_TABLE_NAME}(status, updated_at)
            """
        )
        conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TOPIC_RUN_PROGRESS_TABLE_NAME}_topic_focus
            ON {TOPIC_RUN_PROGRESS_TABLE_NAME}(topic_status_id, focus_username)
            """
        )
        conn.commit()
    finally:
        conn.close()


def _get_topic_progress_map(db_path: Path) -> dict[str, dict[str, Any]]:
    _ensure_topic_run_progress_table(db_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = list(
            conn.execute(
                f"""
                SELECT topic_package_key, topic_status_id, focus_username, status, attempts, last_error, updated_at, prompt_version
                FROM {TOPIC_RUN_PROGRESS_TABLE_NAME}
                """
            )
        )
    finally:
        conn.close()

    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        topic_package_key = str(row["topic_package_key"] or "").strip()
        if not topic_package_key:
            continue
        out[topic_package_key] = {
            "topic_status_id": str(row["topic_status_id"] or "").strip(),
            "focus_username": str(row["focus_username"] or "").strip(),
            "status": str(row["status"] or "").strip(),
            "attempts": int(row["attempts"] or 0),
            "last_error": str(row["last_error"] or "").strip(),
            "updated_at": str(row["updated_at"] or "").strip(),
            "prompt_version": str(row["prompt_version"] or "").strip(),
        }
    return out


def _upsert_topic_progress(
    db_path: Path,
    *,
    topic_package_key: str,
    topic_status_id: str,
    focus_username: str,
    status: str,
    attempts: int,
    last_error: str,
) -> None:
    _ensure_topic_run_progress_table(db_path)
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            f"""
            INSERT INTO {TOPIC_RUN_PROGRESS_TABLE_NAME} (
              topic_package_key, topic_status_id, focus_username, status, attempts, last_error, updated_at, prompt_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(topic_package_key) DO UPDATE SET
              topic_status_id = excluded.topic_status_id,
              focus_username = excluded.focus_username,
              status = excluded.status,
              attempts = excluded.attempts,
              last_error = excluded.last_error,
              updated_at = excluded.updated_at,
              prompt_version = excluded.prompt_version
            """,
            (
                str(topic_package_key or ""),
                str(topic_status_id or ""),
                str(focus_username or ""),
                str(status or ""),
                int(attempts),
                str(last_error or ""),
                _now_log_time(),
                PROMPT_VERSION,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _call_ai_for_topic(
    *,
    prompt: str,
    api_type: str,
    api_mode: str,
    ai_stream: bool,
    model_name: str,
    base_url: str,
    api_key: str,
    timeout_sec: float,
    retry_count: int,
    temperature: float,
    reasoning_effort: str,
    retry_log_label: str = "",
) -> dict[str, Any]:
    try:
        import litellm
    except Exception as exc:
        raise SystemExit(
            "当前环境没法导入 litellm。先装好 berriai/litellm 再跑 --call-ai。"
        ) from exc

    setattr(litellm, "suppress_debug_info", True)
    setattr(litellm, "set_verbose", False)

    last_error: Optional[Exception] = None
    retries = max(0, int(retry_count))
    backoff_sec = DEFAULT_AI_RETRY_BACKOFF_SEC
    request_model_name = _resolve_litellm_model_name(model_name, api_type, base_url)
    attempt_traces: list[dict[str, Any]] = []
    for attempt in range(retries + 1):
        call_kwargs: dict[str, Any] = {}
        request_payload: dict[str, Any] = {}
        response_snapshot: Any = None
        raw_text = ""
        try:
            if api_mode == AI_MODE_RESPONSES:
                responses_fn = getattr(litellm, "responses", None)
                if not callable(responses_fn):
                    import importlib.metadata

                    try:
                        litellm_version = importlib.metadata.version("litellm")
                    except Exception:
                        litellm_version = "unknown"
                    raise SystemExit(
                        "当前 LiteLLM 不支持 `litellm.responses(...)`。"
                        f" 现在装的是 {litellm_version}，请先升级到支持 responses API 的版本。"
                    )

                call_kwargs = {
                    "model": request_model_name,
                    "input": prompt,
                    "temperature": float(temperature),
                    "timeout": float(timeout_sec),
                    "api_key": api_key,
                    "reasoning_effort": str(reasoning_effort),
                    "stream": bool(ai_stream),
                }
                if api_type:
                    call_kwargs["custom_llm_provider"] = api_type
                if base_url:
                    call_kwargs["api_base"] = base_url

                request_payload = _sanitize_request_payload(call_kwargs)
                response = responses_fn(
                    **call_kwargs,
                )
            else:
                completion_fn = getattr(litellm, "completion", None)
                if not callable(completion_fn):
                    raise SystemExit("当前 LiteLLM 不支持 `litellm.completion(...)`。")

                call_kwargs = {
                    "model": request_model_name,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": float(temperature),
                    "timeout": float(timeout_sec),
                    "api_key": api_key,
                    "reasoning_effort": str(reasoning_effort),
                    "stream": bool(ai_stream),
                }
                if api_type:
                    call_kwargs["custom_llm_provider"] = api_type
                if base_url:
                    call_kwargs["api_base"] = base_url
                    call_kwargs["base_url"] = base_url

                request_payload = _sanitize_request_payload(call_kwargs)
                response = completion_fn(
                    **call_kwargs,
                )
            if ai_stream:
                stream_result = _collect_streamed_ai_text(response, api_mode=api_mode)
                raw_text = str(stream_result.get("raw_text") or "")
                response_snapshot = stream_result.get("response_snapshot")
            else:
                raw_text = _extract_ai_text(response)
                response_snapshot = _to_jsonable(response)
            parsed = _parse_ai_json_text(raw_text)
            attempt_traces.append(
                {
                    "attempt": attempt + 1,
                    "request_payload": request_payload,
                    "response_summary": _summarize_response_snapshot(response_snapshot),
                    "raw_ai_text_len": len(raw_text),
                    "error": "",
                }
            )
            return {
                "ai_result": parsed,
                "raw_ai_text": raw_text,
                "call_trace": {
                    "api_mode": api_mode,
                    "api_type": api_type,
                    "model": request_model_name,
                    "stream": bool(ai_stream),
                    "retries": retries,
                    "attempts": attempt_traces,
                },
            }
        except Exception as exc:
            last_error = exc
            attempt_trace = {
                "attempt": attempt + 1,
                "request_payload": request_payload,
                "response_summary": _summarize_response_snapshot(response_snapshot),
                "raw_ai_text_len": len(raw_text),
                "error": str(exc),
            }
            if raw_text:
                attempt_trace["raw_ai_text"] = raw_text
            attempt_traces.append(attempt_trace)
            if attempt >= retries:
                break
            next_wait_sec = float(backoff_sec)
            total_attempts = retries + 1
            label = _normalize_text(retry_log_label)
            prefix = f"[ai-retry][{label}]" if label else "[ai-retry]"
            _log(
                f"{prefix} 内部请求第 {attempt + 1}/{total_attempts} 次失败：{exc}；{next_wait_sec:.1f}s 后重试。"
            )
            time.sleep(backoff_sec)
            backoff_sec *= 2

    assert last_error is not None
    raise _AICallError(
        f"AI 调用最终失败：{last_error}",
        trace={
            "api_mode": api_mode,
            "api_type": api_type,
            "model": request_model_name,
            "stream": bool(ai_stream),
            "retries": retries,
            "attempts": attempt_traces,
        },
    )


def _list_topics(topics: dict[str, dict[str, Any]], limit: int) -> str:
    rows: list[dict[str, Any]] = []
    for topic in _ranked_topics(topics):
        topic_status_id = str(topic.get("topic_status_id") or "")
        topic_post = topic.get("topic_post") or {}
        text = str(topic_post.get("text") or "").strip()
        rows.append(
            {
                "topic_package_key": str(topic.get("topic_package_key") or ""),
                "topic_status_id": topic_status_id,
                "focus_username": _topic_focus_username(topic),
                "status_count": int(topic.get("stats", {}).get("status_count") or 0),
                "comment_count": int(topic.get("stats", {}).get("comment_count") or 0),
                "talk_message_count": int(
                    topic.get("stats", {}).get("talk_message_count") or 0
                ),
                "latest_activity_at": str(
                    topic.get("stats", {}).get("latest_activity_at") or ""
                ),
                "topic_post_speaker": str(topic_post.get("speaker") or ""),
                "topic_post_text_head": text[:80],
            }
        )

    if int(limit) <= 0:
        return json.dumps(rows, ensure_ascii=False, indent=2)
    return json.dumps(rows[: int(limit)], ensure_ascii=False, indent=2)


def _select_topics(
    topics: dict[str, dict[str, Any]], limit: int
) -> list[dict[str, Any]]:
    ranked = _ranked_topics(topics)
    if int(limit) <= 0:
        return ranked
    return ranked[: int(limit)]


def _count_topic_message_nodes(topic: dict[str, Any]) -> int:
    runtime_context = _build_topic_runtime_context(topic, _topic_focus_username(topic))
    message_lookup = runtime_context.get("message_lookup") or {}
    return len(message_lookup) if isinstance(message_lookup, dict) else 0


def _build_coverage_preflight_report(
    *,
    coverage_base: dict[str, Any],
    topics: dict[str, dict[str, Any]],
    selected_topics: list[dict[str, Any]],
    selected_limit: int,
    progress_map: Optional[dict[str, dict[str, Any]]] = None,
    force_rerun: bool = False,
) -> dict[str, Any]:
    report = dict(coverage_base)
    report["selected_limit"] = int(selected_limit)
    report["selected_topic_packages"] = len(selected_topics)
    report["selected_message_nodes_total"] = sum(
        _count_topic_message_nodes(topic) for topic in selected_topics
    )

    completed_topic_packages = 0
    progress_lookup = progress_map if isinstance(progress_map, dict) else {}
    if not force_rerun:
        for topic in selected_topics:
            topic_package_key = _normalize_text(topic.get("topic_package_key"))
            progress = progress_lookup.get(topic_package_key, {})
            if _normalize_text(progress.get("status")) == "completed":
                completed_topic_packages += 1

    report["already_completed_topic_packages"] = completed_topic_packages
    report["this_run_topic_packages"] = max(
        0, len(selected_topics) - completed_topic_packages
    )
    report["is_full_selection"] = int(selected_limit) <= 0
    skipped_entries = [
        item for item in report.get("skipped_entries", []) if isinstance(item, dict)
    ]
    report["ignored_skipped_entries"] = [
        item
        for item in skipped_entries
        if _normalize_text(item.get("reason")) in COVERAGE_IGNORED_SKIP_REASONS
    ]
    report["blocking_skipped_entries"] = [
        item
        for item in skipped_entries
        if _normalize_text(item.get("reason")) not in COVERAGE_IGNORED_SKIP_REASONS
    ]
    report["ignored_skipped_rows_total"] = len(report["ignored_skipped_entries"])
    report["blocking_skipped_rows_total"] = len(report["blocking_skipped_entries"])
    report["coverage_ok"] = len(report["blocking_skipped_entries"]) <= 0
    return report


def _coverage_preflight_lines(report: dict[str, Any]) -> list[str]:
    entry_rows_total = int(report.get("entry_rows_total") or 0)
    accepted_rows_total = int(report.get("accepted_rows_total") or 0)
    skipped_rows_total = int(report.get("skipped_rows_total") or 0)
    accepted_status_rows = int(report.get("accepted_status_rows") or 0)
    accepted_chain_rows = int(report.get("accepted_chain_rows") or 0)
    topic_packages_total = int(report.get("topic_packages_total") or 0)
    selected_topic_packages = int(report.get("selected_topic_packages") or 0)
    selected_limit = int(report.get("selected_limit") or 0)
    selected_message_nodes_total = int(report.get("selected_message_nodes_total") or 0)
    already_completed_topic_packages = int(
        report.get("already_completed_topic_packages") or 0
    )
    this_run_topic_packages = int(report.get("this_run_topic_packages") or 0)
    ignored_skipped_rows_total = int(report.get("ignored_skipped_rows_total") or 0)
    blocking_skipped_rows_total = int(report.get("blocking_skipped_rows_total") or 0)

    limit_text = "0(全量)" if selected_limit <= 0 else str(selected_limit)
    lines = [
        (
            "[coverage] "
            f"entry总行={entry_rows_total} "
            f"可入包行={accepted_rows_total} "
            f"(status={accepted_status_rows}, chain={accepted_chain_rows}) "
            f"跳过行={skipped_rows_total} "
            f"全部话题包={topic_packages_total} "
            f"本次选中={selected_topic_packages} "
            f"limit={limit_text} "
            f"本次AI树节点={selected_message_nodes_total}"
        ),
        (
            "[coverage] "
            f"已完成可跳过={already_completed_topic_packages} "
            f"本次实际会调用AI={this_run_topic_packages}"
        ),
        (
            "[coverage] "
            f"可忽略跳过={ignored_skipped_rows_total} "
            f"阻塞坏行={blocking_skipped_rows_total}"
        ),
    ]

    if not bool(report.get("is_full_selection")):
        lines.append(
            "[coverage] 当前不是全量选择；如果要覆盖全部话题包，请用 --limit 0。"
        )

    reason_counts = report.get("skip_reason_counts") or {}
    if isinstance(reason_counts, dict) and reason_counts:
        reason_text = "；".join(
            f"{_normalize_text(reason)}={int(count or 0)}"
            for reason, count in sorted(
                reason_counts.items(),
                key=lambda item: (-int(item[1] or 0), str(item[0])),
            )
        )
        lines.append(f"[coverage] 跳过原因：{reason_text}")
    else:
        lines.append("[coverage] 跳过原因：无")

    skipped_entries = report.get("skipped_entries") or []
    if isinstance(skipped_entries, list) and skipped_entries:
        parts: list[str] = []
        for item in skipped_entries[:COVERAGE_DETAIL_LIMIT]:
            if not isinstance(item, dict):
                continue
            merge_key = _normalize_text(item.get("merge_key")) or "unknown"
            reason = _normalize_text(item.get("reason")) or "unknown"
            parts.append(f"{merge_key}({reason})")
        detail_line = "；".join(parts) if parts else "无"
        omitted = len(skipped_entries) - len(parts)
        if omitted > 0:
            detail_line = f"{detail_line}；...还有{omitted}条"
        lines.append(f"[coverage] 跳过明细：{detail_line}")
    return lines


def _summarize_topic_for_log(topic: dict[str, Any]) -> str:
    topic_status_id = _normalize_text(topic.get("topic_status_id"))
    focus_username = _topic_focus_username(topic) or "未知用户"
    stats = topic.get("stats") or {}
    return (
        f"topic={topic_status_id} "
        f"作者={focus_username} "
        f"独立发言={int(stats.get('status_count') or 0)} "
        f"评论={int(stats.get('comment_count') or 0)} "
        f"对话={int(stats.get('talk_message_count') or 0)}"
    )


def _summarize_invalid_reasons(invalid_assertions: list[dict[str, Any]]) -> str:
    reason_counts: dict[str, int] = {}
    for row in invalid_assertions:
        errors = row.get("validation_errors")
        if not isinstance(errors, list):
            continue
        for error in errors:
            reason = _normalize_text(error)
            if not reason:
                continue
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

    if not reason_counts:
        return "无"

    parts = [
        f"{reason}x{count}"
        for reason, count in sorted(
            reason_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[:3]
    ]
    return "；".join(parts)


def _build_trace_record(
    *,
    trace_id: str,
    topic: dict[str, Any],
    attempt: int,
    prompt: str,
    ai_topic_package: dict[str, Any],
    api_config: dict[str, str],
    ai_trace: Optional[dict[str, Any]] = None,
    ai_result: Optional[dict[str, Any]] = None,
    raw_ai_text: str = "",
    assertions_preview: Optional[list[dict[str, Any]]] = None,
    valid_assertions: Optional[list[dict[str, Any]]] = None,
    invalid_assertions: Optional[list[dict[str, Any]]] = None,
    posts_preview: Optional[list[dict[str, Any]]] = None,
    human_report: str = "",
    error: str = "",
) -> dict[str, Any]:
    invalid_rows = invalid_assertions or []
    return {
        "trace_type": "topic_attempt",
        "trace_id": trace_id,
        "captured_at": _now_log_time(),
        "topic_package_key": _normalize_text(topic.get("topic_package_key")),
        "topic_status_id": _normalize_text(topic.get("topic_status_id")),
        "focus_username": _topic_focus_username(topic),
        "attempt": int(attempt),
        "api_config": {
            "api_type": _normalize_text(api_config.get("api_type")),
            "api_mode": _normalize_text(api_config.get("api_mode")),
            "ai_stream": bool(_normalize_text(api_config.get("ai_stream"))),
            "model_name": _normalize_text(api_config.get("model_name")),
            "base_url": _normalize_text(api_config.get("base_url")),
            "api_key": _mask_secret(api_config.get("api_key")),
        },
        "prompt": prompt,
        "ai_topic_package": _to_jsonable(ai_topic_package),
        "ai_trace": _to_jsonable(ai_trace or {}),
        "raw_ai_text": raw_ai_text,
        "ai_result": _to_jsonable(ai_result or {}),
        "assertions_preview": _to_jsonable(assertions_preview or []),
        "valid_assertions": _to_jsonable(valid_assertions or []),
        "invalid_assertions": _to_jsonable(invalid_rows),
        "invalid_reason_summary": _summarize_invalid_reasons(invalid_rows),
        "posts_preview": _to_jsonable(posts_preview or []),
        "human_report": human_report,
        "error": error,
    }


def _trace_section(title: str, body: str) -> str:
    content = str(body or "").strip()
    if not content:
        content = "（空）"
    return "\n".join([f"## {title}", content])


def _build_trace_display_ai_trace(
    ai_trace: dict[str, Any],
    *,
    include_detail: bool,
) -> dict[str, Any]:
    trace_obj = _to_jsonable(ai_trace)
    if not isinstance(trace_obj, dict):
        return {"raw": trace_obj}

    attempts = trace_obj.get("attempts")
    if isinstance(attempts, list):
        cleaned_attempts: list[dict[str, Any]] = []
        for item in attempts:
            row = item if isinstance(item, dict) else {"raw": item}
            request_payload = row.get("request_payload")
            if isinstance(request_payload, dict):
                payload = dict(request_payload)
                if "input" in payload:
                    payload["input"] = "（省略：见 Prompt / AI Topic Package JSON）"
                if "messages" in payload:
                    payload["messages"] = "（省略：见 Prompt / AI Topic Package JSON）"
                row = dict(row)
                row["request_payload"] = payload
            response_snapshot = row.get("response_snapshot")
            if response_snapshot is not None:
                row = dict(row)
                row.pop("response_snapshot", None)
                row["response_summary"] = _summarize_response_snapshot(
                    response_snapshot
                )
            if not include_detail and "raw_ai_text" in row:
                row = dict(row)
                row.pop("raw_ai_text", None)
            cleaned_attempts.append(row)
        trace_obj = dict(trace_obj)
        trace_obj["attempts"] = cleaned_attempts

    return trace_obj


def _format_trace_record(record: dict[str, Any]) -> str:
    trace_id = _normalize_text(record.get("trace_id"))
    captured_at = _normalize_text(record.get("captured_at"))
    topic_status_id = _normalize_text(record.get("topic_status_id"))
    focus_username = _normalize_text(record.get("focus_username"))
    invalid_reason_summary = (
        _normalize_text(record.get("invalid_reason_summary")) or "无"
    )
    error = _normalize_text(record.get("error"))
    status_text = "error" if error else "ok"
    prompt = _normalize_text(record.get("prompt"))
    raw_ai_text = _normalize_text(record.get("raw_ai_text"))
    has_invalid_assertions = bool(record.get("invalid_assertions") or [])
    show_detail = bool(error or has_invalid_assertions)
    api_config = json.dumps(
        record.get("api_config") or {}, ensure_ascii=False, indent=2
    )
    ai_topic_package = json.dumps(
        record.get("ai_topic_package") or {},
        ensure_ascii=False,
        indent=2,
    )
    ai_trace = json.dumps(
        _build_trace_display_ai_trace(
            record.get("ai_trace") or {},
            include_detail=show_detail,
        ),
        ensure_ascii=False,
        indent=2,
    )
    ai_result = json.dumps(record.get("ai_result") or {}, ensure_ascii=False, indent=2)
    assertions_preview = json.dumps(
        record.get("assertions_preview") or [],
        ensure_ascii=False,
        indent=2,
    )
    invalid_assertions = json.dumps(
        record.get("invalid_assertions") or [],
        ensure_ascii=False,
        indent=2,
    )

    header_lines = [
        "=" * 100,
        f"TRACE {trace_id}",
        f"captured_at={captured_at}",
        f"status={status_text}",
        f"topic_status_id={topic_status_id}",
        f"focus_username={focus_username}",
        f"attempt={int(record.get('attempt') or 0)}",
        f"invalid_reason_summary={invalid_reason_summary}",
    ]
    if error:
        header_lines.append(f"error={error}")

    sections = [
        "\n".join(header_lines),
        _trace_section("AI Topic Package JSON", ai_topic_package),
        _trace_section("API Config", api_config),
        _trace_section("AI Trace", ai_trace),
        _trace_section("AI Result JSON", ai_result),
    ]
    if show_detail and prompt:
        sections.insert(1, _trace_section("Prompt", prompt))
    if show_detail and raw_ai_text:
        sections.append(_trace_section("Raw AI Text", raw_ai_text))
    if show_detail:
        sections.append(_trace_section("Assertions Preview JSON", assertions_preview))
    if has_invalid_assertions:
        sections.append(_trace_section("Invalid Assertions JSON", invalid_assertions))
    sections.extend(
        [
            "=" * 100,
        ]
    )
    return "\n\n".join(sections)


def _process_single_topic(
    *,
    topic: dict[str, Any],
    db_path: Path,
    args: argparse.Namespace,
    api_config: dict[str, str],
    retry_log_label: str = "",
) -> dict[str, Any]:
    focus_username = _topic_focus_username(topic)
    runtime_context = _build_topic_runtime_context(topic, focus_username)
    ai_topic_package = dict(runtime_context.get("ai_topic_package") or {})
    message_lookup = dict(runtime_context.get("message_lookup") or {})
    prompt = _build_single_topic_output(
        topic,
        index=1,
        ai_topic_package=ai_topic_package,
    )
    model_name = str(api_config.get("model_name") or "").strip()
    ai_call_result = _call_ai_for_topic(
        prompt=prompt,
        api_type=str(api_config.get("api_type") or ""),
        api_mode=str(api_config.get("api_mode") or DEFAULT_AI_MODE),
        ai_stream=bool(str(api_config.get("ai_stream") or "").strip()),
        model_name=model_name,
        base_url=str(api_config.get("base_url") or ""),
        api_key=str(api_config.get("api_key") or ""),
        timeout_sec=float(args.ai_timeout_sec),
        retry_count=int(args.ai_retries),
        temperature=float(args.ai_temperature),
        reasoning_effort=str(args.ai_reasoning_effort),
        retry_log_label=retry_log_label,
    )
    ai_result = dict(ai_call_result.get("ai_result") or {})
    raw_ai_text = str(ai_call_result.get("raw_ai_text") or "")
    ai_trace = dict(ai_call_result.get("call_trace") or {})

    assertions_preview = _build_assertions_preview(
        topic_package=topic,
        ai_result=ai_result,
        message_lookup=message_lookup,
    )
    valid_assertions, invalid_assertions = _validate_assertions_preview(
        focus_username=focus_username,
        rows=assertions_preview,
        message_lookup=message_lookup,
    )
    posts_preview = _build_posts_preview(
        topic_package=topic,
        ai_result=ai_result,
        assertions_preview=assertions_preview,
        valid_assertions=valid_assertions,
        invalid_assertions=invalid_assertions,
        model_name=model_name,
        message_lookup=message_lookup,
    )

    if not raw_ai_text.strip():
        raise RuntimeError("AI 调用了，但没拿到文本结果。")

    return {
        "topic_package_key": _normalize_text(topic.get("topic_package_key")),
        "topic_status_id": _normalize_text(topic.get("topic_status_id")),
        "focus_username": focus_username,
        "ai_result": ai_result,
        "raw_ai_text": raw_ai_text,
        "posts_preview": posts_preview,
        "assertions_preview": assertions_preview,
        "valid_assertions": valid_assertions,
        "invalid_assertions": invalid_assertions,
        "ai_trace": ai_trace,
        "ai_topic_package": ai_topic_package,
        "prompt": prompt,
        "human_report": _build_human_topic_block(topic, 1),
    }


def main() -> int:
    args = _parse_args()
    db_path = _resolve_db_path(args.db)
    if not db_path.exists():
        raise SystemExit(f"sqlite 文件不存在: {db_path}")

    entry_rows = _load_entry_rows(db_path)
    topics, coverage_base = _build_topics_from_rows(entry_rows)
    if args.list_topics:
        print(_list_topics(topics, int(args.limit)))
        return 0

    selected_topics = _select_topics(topics, int(args.limit))
    if not selected_topics:
        raise SystemExit("库里没有可用话题。")

    if args.call_ai:
        progress_map = _get_topic_progress_map(db_path)
        trace_out_path = Path(args.trace_out) if args.trace_out is not None else None
        coverage_report = _build_coverage_preflight_report(
            coverage_base=coverage_base,
            topics=topics,
            selected_topics=selected_topics,
            selected_limit=int(args.limit),
            progress_map=progress_map,
            force_rerun=bool(args.force_rerun),
        )
        coverage_lines = _coverage_preflight_lines(coverage_report)
        for line in coverage_lines:
            _log(line)
        if trace_out_path is not None:
            _append_trace_note(
                trace_out_path,
                "COVERAGE CHECK",
                [
                    f"captured_at={_now_log_time()}",
                    *coverage_lines,
                ],
            )
        if not bool(coverage_report.get("coverage_ok")):
            _log(
                "覆盖预检失败：发现 entry 行没能进 topic package，先修这些问题再跑 AI。"
            )
            return 2

        api_config = _resolve_api_config(args)
        configured_max_inflight = max(1, int(args.ai_max_inflight))
        ai_limiter_state = {"next_allowed_at": 0.0}
        ai_limiter_lock = threading.Lock()
        if trace_out_path is not None:
            _append_trace_note(
                trace_out_path,
                "BATCH START",
                [
                    f"captured_at={_now_log_time()}",
                    f"requested_topics={len(selected_topics)}",
                    f"api_type={_normalize_text(api_config.get('api_type'))}",
                    f"api_mode={_normalize_text(api_config.get('api_mode'))}",
                    f"ai_stream={bool(_normalize_text(api_config.get('ai_stream')))}",
                    f"model_name={_normalize_text(api_config.get('model_name'))}",
                    f"base_url={_normalize_text(api_config.get('base_url'))}",
                    f"ai_max_inflight={configured_max_inflight}",
                    f"trace_file={trace_out_path}",
                ],
            )
        _log(
            f"本次共拿到 {len(selected_topics)} 个话题，准备开始处理。ai_max_inflight={configured_max_inflight}"
        )
        if args.out is not None:
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(
                "\n\n====\n\n".join(
                    _build_single_topic_output(topic, index=index)
                    for index, topic in enumerate(selected_topics, start=1)
                ),
                encoding="utf-8",
            )

        batch_results_by_index: dict[int, dict[str, Any]] = {}
        failures: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        pending_tasks: list[dict[str, Any]] = []
        for index, topic in enumerate(selected_topics, start=1):
            topic_status_id = _normalize_text(topic.get("topic_status_id"))
            focus_username = _topic_focus_username(topic)
            topic_package_key = _normalize_text(topic.get("topic_package_key"))
            progress = progress_map.get(topic_package_key, {})
            if (not args.force_rerun) and str(
                progress.get("status") or ""
            ).strip() == "completed":
                skipped.append(
                    {
                        "topic_package_key": topic_package_key,
                        "topic_status_id": topic_status_id,
                        "focus_username": focus_username,
                        "reason": "already_completed",
                        "updated_at": str(progress.get("updated_at") or ""),
                    }
                )
                _log(
                    f"[topic-skip] {index}/{len(selected_topics)} 跳过处理包 {topic_package_key}，因为之前已经完成。"
                )
                if trace_out_path is not None:
                    _append_trace_note(
                        trace_out_path,
                        f"TOPIC SKIP {topic_package_key}",
                        [
                            f"captured_at={_now_log_time()}",
                            f"topic_status_id={topic_status_id}",
                            f"focus_username={focus_username}",
                            "reason=already_completed",
                            f"updated_at={str(progress.get('updated_at') or '')}",
                        ],
                    )
                continue

            attempts = int(progress.get("attempts") or 0) + 1
            trace_stamp = _compact_timestamp()
            trace_id = _build_trace_id(
                topic_status_id=topic_status_id,
                focus_username=focus_username,
                attempt=attempts,
                api_config=api_config,
                stamp=trace_stamp,
            )
            pending_tasks.append(
                {
                    "index": index,
                    "topic": topic,
                    "topic_status_id": topic_status_id,
                    "focus_username": focus_username,
                    "topic_package_key": topic_package_key,
                    "attempts": attempts,
                    "trace_id": trace_id,
                }
            )

        max_inflight = max(1, min(configured_max_inflight, len(pending_tasks) or 1))
        future_to_task: dict[concurrent.futures.Future, dict[str, Any]] = {}
        pending_index = 0
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_inflight
        ) as executor:
            while pending_index < len(pending_tasks) or future_to_task:
                while (
                    pending_index < len(pending_tasks)
                    and len(future_to_task) < max_inflight
                ):
                    task_meta = pending_tasks[pending_index]
                    pending_index += 1
                    _wait_for_ai_rpm_slot(
                        float(args.ai_rpm), ai_limiter_state, ai_limiter_lock
                    )
                    _upsert_topic_progress(
                        db_path,
                        topic_package_key=str(task_meta["topic_package_key"]),
                        topic_status_id=str(task_meta["topic_status_id"]),
                        focus_username=str(task_meta["focus_username"]),
                        status="processing",
                        attempts=int(task_meta["attempts"]),
                        last_error="",
                    )
                    _log(
                        f"[topic-start][{task_meta['trace_id']}] {task_meta['index']}/{len(selected_topics)} 开始处理，第 {task_meta['attempts']} 次尝试。{_summarize_topic_for_log(task_meta['topic'])}"
                    )
                    if trace_out_path is not None:
                        _append_trace_note(
                            trace_out_path,
                            f"TOPIC START {task_meta['trace_id']}",
                            [
                                f"captured_at={_now_log_time()}",
                                f"queue_index={task_meta['index']}/{len(selected_topics)}",
                                f"topic_package_key={task_meta['topic_package_key']}",
                                f"topic_status_id={task_meta['topic_status_id']}",
                                f"focus_username={task_meta['focus_username']}",
                                f"attempt={task_meta['attempts']}",
                                _summarize_topic_for_log(task_meta["topic"]),
                            ],
                        )
                    task_meta["started_at"] = time.time()
                    future = executor.submit(
                        _process_single_topic,
                        topic=task_meta["topic"],
                        db_path=db_path,
                        args=args,
                        api_config=api_config,
                        retry_log_label=str(task_meta["trace_id"]),
                    )
                    future_to_task[future] = task_meta

                if not future_to_task:
                    continue

                done, _ = concurrent.futures.wait(
                    future_to_task,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    task_meta = future_to_task.pop(future)
                    topic = task_meta["topic"]
                    topic_status_id = str(task_meta["topic_status_id"])
                    focus_username = str(task_meta["focus_username"])
                    topic_package_key = str(task_meta["topic_package_key"])
                    attempts = int(task_meta["attempts"])
                    trace_id = str(task_meta["trace_id"])
                    topic_started_at = float(task_meta.get("started_at") or time.time())

                    try:
                        result = future.result()
                        deleted_posts = 0
                        deleted_assertions = 0
                        posts_written = 0
                        assertions_written = 0
                        if args.write_posts:
                            _ensure_posts_table(db_path)
                            deleted_posts = _delete_posts_by_topic(
                                db_path,
                                topic_status_id,
                                focus_username,
                            )
                            posts_written = _upsert_posts(
                                db_path,
                                list(result.get("posts_preview") or []),
                            )
                        if args.write_assertions:
                            _ensure_assertions_table(db_path)
                            deleted_assertions = _delete_assertions_by_topic(
                                db_path,
                                topic_status_id,
                                focus_username,
                            )
                            assertions_written = _upsert_assertions(
                                db_path,
                                list(result.get("valid_assertions") or []),
                            )

                        result["deleted_posts"] = deleted_posts
                        result["deleted_assertions"] = deleted_assertions
                        result["posts_written"] = posts_written
                        result["assertions_written"] = assertions_written
                        batch_results_by_index[int(task_meta["index"])] = result

                        _upsert_topic_progress(
                            db_path,
                            topic_package_key=topic_package_key,
                            topic_status_id=topic_status_id,
                            focus_username=focus_username,
                            status="completed",
                            attempts=attempts,
                            last_error="",
                        )
                        elapsed_sec = time.time() - topic_started_at
                        ai_items = (
                            result.get("ai_result", {}).get("items")
                            if isinstance(result.get("ai_result"), dict)
                            else []
                        )
                        ai_item_count = (
                            len(ai_items) if isinstance(ai_items, list) else 0
                        )
                        valid_count = len(result.get("valid_assertions") or [])
                        invalid_count = len(result.get("invalid_assertions") or [])
                        invalid_reason_summary = _summarize_invalid_reasons(
                            result.get("invalid_assertions") or []
                        )
                        if trace_out_path is not None:
                            _append_trace_record(
                                trace_out_path,
                                _build_trace_record(
                                    trace_id=trace_id,
                                    topic=topic,
                                    attempt=attempts,
                                    prompt=str(result.get("prompt") or ""),
                                    ai_topic_package=dict(
                                        result.get("ai_topic_package") or {}
                                    ),
                                    api_config=api_config,
                                    ai_trace=dict(result.get("ai_trace") or {}),
                                    ai_result=dict(result.get("ai_result") or {}),
                                    raw_ai_text=str(result.get("raw_ai_text") or ""),
                                    assertions_preview=list(
                                        result.get("assertions_preview") or []
                                    ),
                                    valid_assertions=list(
                                        result.get("valid_assertions") or []
                                    ),
                                    invalid_assertions=list(
                                        result.get("invalid_assertions") or []
                                    ),
                                    posts_preview=list(
                                        result.get("posts_preview") or []
                                    ),
                                    human_report=str(result.get("human_report") or ""),
                                ),
                            )
                        _log(
                            f"[topic-done][{trace_id}] 话题 {topic_status_id} 完成，耗时={elapsed_sec:.1f}s，AI抽取={ai_item_count} 条，通过={valid_count} 条，拦下={invalid_count} 条，主要原因={invalid_reason_summary}，删除旧 posts={deleted_posts}，删除旧 assertions={deleted_assertions}，写入 posts={posts_written}，写入 assertions={assertions_written}。"
                        )
                    except Exception as exc:
                        if trace_out_path is not None:
                            runtime_context = _build_topic_runtime_context(
                                topic, focus_username
                            )
                            ai_topic_package = dict(
                                runtime_context.get("ai_topic_package") or {}
                            )
                            prompt = _build_single_topic_output(
                                topic,
                                index=1,
                                ai_topic_package=ai_topic_package,
                            )
                            error_trace = dict(getattr(exc, "trace", {}) or {})
                            _append_trace_record(
                                trace_out_path,
                                _build_trace_record(
                                    trace_id=trace_id,
                                    topic=topic,
                                    attempt=attempts,
                                    prompt=prompt,
                                    ai_topic_package=ai_topic_package,
                                    api_config=api_config,
                                    ai_trace=error_trace,
                                    human_report=_build_human_topic_block(topic, 1),
                                    error=str(exc),
                                ),
                            )
                        failures.append(
                            {
                                "topic_package_key": topic_package_key,
                                "topic_status_id": topic_status_id,
                                "focus_username": focus_username,
                                "error": str(exc),
                            }
                        )
                        _upsert_topic_progress(
                            db_path,
                            topic_package_key=topic_package_key,
                            topic_status_id=topic_status_id,
                            focus_username=focus_username,
                            status="error",
                            attempts=attempts,
                            last_error=str(exc),
                        )
                        _log(
                            f"[topic-error][{trace_id}] 话题 {topic_status_id} 处理失败：{exc}"
                        )

        batch_results = [
            batch_results_by_index[idx] for idx in sorted(batch_results_by_index)
        ]

        if args.ai_out is not None:
            ai_out_path = Path(args.ai_out)
            ai_out_path.parent.mkdir(parents=True, exist_ok=True)
            ai_out_path.write_text(
                json.dumps(
                    [item.get("ai_result") for item in batch_results],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        if args.assertions_out is not None:
            assertions_out_path = Path(args.assertions_out)
            assertions_out_path.parent.mkdir(parents=True, exist_ok=True)
            assertions_out_path.write_text(
                json.dumps(
                    [
                        assertion
                        for item in batch_results
                        for assertion in item.get("assertions_preview", [])
                    ],
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
        if args.human_out is not None:
            human_out_path = Path(args.human_out)
            human_out_path.parent.mkdir(parents=True, exist_ok=True)
            human_out_path.write_text(
                "\n\n"
                + HUMAN_TOPIC_SEPARATOR.join(
                    str(item.get("human_report") or "") for item in batch_results
                ),
                encoding="utf-8",
            )

        _log(
            f"[batch-done] 请求话题={len(selected_topics)}，成功处理={len(batch_results)}，失败={len(failures)}，跳过={len(skipped)}，写入 posts={sum(int(item.get('posts_written') or 0) for item in batch_results)}，写入 assertions={sum(int(item.get('assertions_written') or 0) for item in batch_results)}。"
        )
        if trace_out_path is not None:
            _append_trace_note(
                trace_out_path,
                "BATCH DONE",
                [
                    f"captured_at={_now_log_time()}",
                    f"requested_topics={len(selected_topics)}",
                    f"processed_topics={len(batch_results)}",
                    f"failed_topics={len(failures)}",
                    f"skipped_topics={len(skipped)}",
                    f"posts_written={sum(int(item.get('posts_written') or 0) for item in batch_results)}",
                    f"assertions_written={sum(int(item.get('assertions_written') or 0) for item in batch_results)}",
                ],
            )

        print(
            json.dumps(
                {
                    "summary": {
                        "requested_topics": len(selected_topics),
                        "processed_topics": len(batch_results),
                        "failed_topics": len(failures),
                        "skipped_topics": len(skipped),
                        "posts_written": sum(
                            int(item.get("posts_written") or 0)
                            for item in batch_results
                        ),
                        "assertions_written": sum(
                            int(item.get("assertions_written") or 0)
                            for item in batch_results
                        ),
                        "deleted_posts": sum(
                            int(item.get("deleted_posts") or 0)
                            for item in batch_results
                        ),
                        "deleted_assertions": sum(
                            int(item.get("deleted_assertions") or 0)
                            for item in batch_results
                        ),
                    },
                    "results": [
                        {
                            "topic_package_key": item.get("topic_package_key"),
                            "topic_status_id": item.get("topic_status_id"),
                            "focus_username": item.get("focus_username"),
                            "posts_preview": item.get("posts_preview"),
                            "assertions_preview": item.get("assertions_preview"),
                            "valid_assertions": item.get("valid_assertions"),
                            "invalid_assertions": item.get("invalid_assertions"),
                            "deleted_posts": item.get("deleted_posts"),
                            "deleted_assertions": item.get("deleted_assertions"),
                            "posts_written": item.get("posts_written"),
                            "assertions_written": item.get("assertions_written"),
                        }
                        for item in batch_results
                    ],
                    "skipped": skipped,
                    "failures": failures,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    human_report = _build_human_report(selected_topics)
    combined_output = _build_combined_output(selected_topics)
    if args.out is not None:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(combined_output, encoding="utf-8")

    if args.human_out is not None:
        human_out_path = Path(args.human_out)
        human_out_path.parent.mkdir(parents=True, exist_ok=True)
        human_out_path.write_text(human_report, encoding="utf-8")

    if args.human_only:
        print(human_report)
    else:
        print(combined_output, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
