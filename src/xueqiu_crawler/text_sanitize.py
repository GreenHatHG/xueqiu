from __future__ import annotations

import html as html_lib
import re
from typing import Any, Optional


_RE_A_OPEN = re.compile(r"<a\b[^>]*>", re.IGNORECASE)
_RE_A_CLOSE = re.compile(r"</a\s*>", re.IGNORECASE)
_RE_BR = re.compile(r"<br\s*/?>", re.IGNORECASE)
_RE_IMG = re.compile(r"<img\b[^>]*>", re.IGNORECASE)

# Extract src from an <img ...> tag; accept single/double quotes or no quotes.
_RE_IMG_SRC = re.compile(
    r"""\bsrc\s*=\s*(?P<q>["']?)(?P<src>[^"'\s>]+)(?P=q)""", re.IGNORECASE
)
_RE_IMG_ALT = re.compile(
    r"""\balt\s*=\s*(?P<q>["']?)(?P<alt>[^"'\s>]*)(?P=q)""", re.IGNORECASE
)
_RE_IMG_TITLE = re.compile(
    r"""\btitle\s*=\s*(?P<q>["']?)(?P<title>[^"'\s>]*)(?P=q)""", re.IGNORECASE
)

# Xueqiu common reply/forward wrappers, used by "raw_text"-style outputs.
_RE_REPLY_WRAPPER_PREFIX = re.compile(r"^(?:回复@[^:：]+[:：]\s*)+")
_RE_FORWARD_SUFFIX = re.compile(r"\s*//@.*$")


def _is_xueqiu_emoji_src(src: str) -> bool:
    s = (src or "").strip().lower()
    if not s:
        return False
    # Common patterns observed in this repo's data dumps.
    # Examples:
    # - //assets.imedao.com/ugc/images/face/emoji_33_face.png?v=1
    # - //assets.imedao.com/ugc/images/face/emoji_32.png?v=1
    if "/ugc/images/face/emoji" in s or "/images/face/emoji" in s:
        return True
    if "imedao.com" in s and ("face/emoji" in s or "emoji_" in s):
        return True
    return False


def _extract_img_alt_or_title(tag: str) -> str:
    alt_m = _RE_IMG_ALT.search(tag or "")
    if alt_m:
        alt = str(alt_m.group("alt") or "").strip()
        if alt:
            return alt
    title_m = _RE_IMG_TITLE.search(tag or "")
    if title_m:
        title = str(title_m.group("title") or "").strip()
        if title:
            return title
    return ""


def sanitize_xueqiu_text(text: Any) -> Optional[str]:
    """
    Minimal HTML cleanup for readability (and stable SQL querying):

    - Strip <a ...> and </a> but keep inner text (e.g. @user, $SYMBOL$).
    - Convert <br/> / <br> to newline.
    - Replace emoji <img ...> (assets.imedao.com/ugc/images/face/emoji_*.png) with alt/title text, keep other <img>.

    Returns:
    - None if input is None
    - Original string if input is not a string (stringified)
    - Cleaned string otherwise
    """

    if text is None:
        return None
    if not isinstance(text, str):
        text = str(text)

    # Unescape first so that entity-encoded tags like '&lt;a ...&gt;' can be removed too.
    s = html_lib.unescape(str(text))
    s = _RE_BR.sub("\n", s)
    s = _RE_A_OPEN.sub("", s)
    s = _RE_A_CLOSE.sub("", s)

    def _img_repl(m: re.Match[str]) -> str:
        tag = m.group(0) or ""
        src_m = _RE_IMG_SRC.search(tag)
        src = src_m.group("src") if src_m else ""
        if _is_xueqiu_emoji_src(src):
            return _extract_img_alt_or_title(tag)
        return tag

    s = _RE_IMG.sub(_img_repl, s)
    return s


def strip_reply_wrappers(text: Any) -> str:
    """
    Remove Xueqiu reply/forward wrappers for "raw_text"-style plain text:

    - Prefix: "回复@xxx:" / "回复@xxx：" (can repeat)
    - Suffix: " //@..." (forward marker and quoted chain)
    """

    cleaned = str(text or "").strip()
    if not cleaned:
        return ""
    cleaned = _RE_REPLY_WRAPPER_PREFIX.sub("", cleaned)
    cleaned = _RE_FORWARD_SUFFIX.sub("", cleaned)
    return cleaned.strip()
