# 雪球用户内容爬取（Playwright 登录态）

这个项目的核心思路是：**不做接口签名逆向**，所有取数都尽量留在已登录浏览器会话内完成。当前实现不是单一路径，而是按数据类型做了收敛：`timeline` 固定走“浏览主页 + UI 网络拦截”；`comments` 在默认 Playwright 会话下仍走 UI 拦截，但在 `--cdp` 连接你本机正常 Chrome 时，会优先改为在该浏览器会话里直接请求 `/statuses/user/comments.json` 并按 `max_id` 游标翻页。若目标 JSON 完全拿不到，则降级为保存 HTML 留痕。更完整的接口与数据流转记录见 `docs/数据流转与抓取说明.md`。

## 1. 安装

推荐使用 `uv`（本仓库已包含 `uv.lock`）：

1）同步依赖并创建虚拟环境：

`uv sync`

2）安装 Playwright 浏览器（首次需要）：

`uv run python -m playwright install chromium`

## 2. 首次登录（只需要一次）

本项目默认使用 Playwright 的 **persistent context**，把登录态保存在 `./.playwright/user-data/`。第一次运行会打开一个可见浏览器窗口；你在窗口里手动登录雪球一次，后续运行会自动复用登录态，不需要重复登录。

如果你看到“等待登录/可能出现验证码”的提示，说明当前登录态不可用：请在打开的浏览器窗口完成登录（或处理验证码），程序会继续执行。

注意：即使你“看起来已登录”，自动化浏览器也可能被风控导致 JSON 接口不可用；此时程序会提示并继续尝试抓取，若持续失败可参考下方的反自动化处理建议。

## 3. 运行示例（核心默认：只需 user_id + 截止时间）

你最常用的场景只需要两件事：博主自己发的内容（时间线 statuses）和博主在全站的所有回复（comments），并且为每条回复抓取“查看对话”（talks）以还原上下文。为此 CLI 默认改为 `core` 模式：你只需要提供 `--user-id` 与 `--since`（截止时间），其余都按程序默认“能抓就尽量抓”，并在遇到更早内容时自动停止回溯。

最小可用（先跑通流程，再逐步把 since 往更早挪）：

`uv run xq-crawl --user-id 9650668145 --since 2026-03-06`

补充说明：当前 `timeline` 会全自动打开用户主页并通过滚动/翻页触发页面自身请求，再在浏览器侧拦截 `/v4/statuses/user_timeline.json` 的响应落盘；`comments` 在默认会话下仍采用类似方式，但如果你用了 `--cdp` 连接本机正常 Chrome，则会优先在该浏览器会话内直接请求 `/statuses/user/comments.json?user_id=...&size=...&max_id=...`。若在一定窗口内完全拿不到目标 JSON，会在 `data/html/{user_id}/` 写入 HTML 快照并提示你已降级留痕（避免静默失败）。core 模式默认也会尽量补齐每条回复的“查看对话”（talks）；如果你只要主干数据，可加 `--no-talks`。

如果你遇到风控导致抓取或 talks 补齐失败，优先使用 `--cdp` 连接你本机正常 Chrome Profile（更接近日常浏览，尤其对 `comments` 的 direct fetch 更稳）。

说明：

- `--since 2026-03-06` 表示按 `Asia/Shanghai` 的本地日期，从最新向历史回溯抓取 **>= 2026-03-06** 的内容；如果你想精确到时刻，也可以用 ISO 8601（例如 `2026-03-06T00:00:00+08:00`）。
- 你担心的“回复页只有问答片段、不知道挂在哪条博文下”，会通过 SQLite 的 `comments` 表里的 `root_status_url/root_status_id/root_status_target` 直接解决：每条回复都会带可还原定位信息。
- “查看对话”可能很长。程序会把 talks 的聚合结果写入 SQLite 的 `talks` 表（`raw_json` 字段保存原始 JSON 文本），并支持你重复运行来继续补齐（例如提高 `--max-talk-pages` 后可继续把同一条对话链补全）。
- 如果你遇到“为保证正常访问请验证/验证失败，请刷新重试”，请在打开的 UI 标签页里完成验证码/挑战（必要时刷新雪球首页）。为避免反复刷新触发更严风控，程序会暂停重试并提示你“完成验证后按回车继续”。

如果打开网页后出现 `alichlgref` / `md5__1038` 参数的无限跳转（疑似风控/反自动化挑战），先试：

`uv run xq-crawl --user-id 9650668145 --since 2026-03-06 --reduce-automation-fingerprint`

必要时也可以换一个全新的登录态目录重新登录一次（避免旧目录被风控状态污染）：

`uv run xq-crawl --user-id 9650668145 --user-data-dir .playwright/user-data-2 --since 2026-03-06 --reduce-automation-fingerprint`

如果你确信已登录，并且不想被“登录/风控探测”阻塞流程，可用（不推荐）：

`uv run xq-crawl --user-id 9650668145 --skip-login-check --since 2026-03-06`

输出默认写到 `data/`：

- `data/xueqiu_{user_id}.sqlite3`：唯一主输出（SQLite 数据库，最终只保留可直接阅读的展示记录；每一行要么是单独原博文，要么是一整条评论链，原始来源数据保存在 `payload_json` 中）
- `data/html/{user_id}/`：当未能拦截到目标 JSON 响应时，降级保存 HTML 快照（用于诊断风控/页面结构变化）

查看结果时，建议优先直接看表里的 `text` 字段；如果后续需要追溯这条记录来自哪条 status/comment/talk，再看同一行里的 `payload_json`。

## 4. 反爬/账号安全（建议不要改得太激进）

程序默认启用：

- 单页面串行请求（不并发）
- 请求间最小延迟 + 随机抖动
- 429/403/返回 HTML（挑战页）视为风控信号：指数退避重试，达到阈值自动停止并落盘断点
- SQLite 内置缓存表（同一 DB 文件中）减少重复访问

你可以用这些参数更保守或更激进，但不建议把延迟降得太低：

- `--min-delay 1.2 --jitter 0.6`
- `--max-retries 2 --max-consecutive-blocks 3`

## 5. 可选：连接你正在调试的 Chrome（CDP attach）

如果你希望“就是同一个调试浏览器继续跑”，可以让爬虫直接连接一个已启动的 Chrome（需要你手动以 remote debugging port 启动）。

这也是目前最接近“用本地正常浏览器”的方式：你用自己的 Chrome + 自己的 Profile 登录并通过风控挑战，爬虫只负责连接到这个会话继续抓取。当前代码下，`timeline` 在 CDP 模式中仍然保持 UI 拦截；`comments` 则会优先在同一浏览器会话里直接请求接口并按 `max_id` 翻页。

更详细的操作步骤与注意事项见：`docs/使用本地Chrome(CDP)操作指南.md`。

示例（请自行根据系统调整 Chrome 启动方式与参数）：

- 启动 Chrome 时加 `--remote-debugging-port=9222`（macOS 还必须加一个非默认的 `--user-data-dir`，否则不会真正监听端口）
- 然后运行：

`xq-crawl --user-id 9650668145 --cdp http://127.0.0.1:9222 --since 2026-03-06`

注意：CDP attach 会受到你当前 Chrome Profile、扩展、锁文件等影响，稳定性与可控性通常不如默认的 persistent context；除非你明确需要“同一个调试窗口”，否则建议用默认方式。

如果你想先单独验证当前 Chrome 会话里 direct fetch 是否稳定，再决定是否用 `--cdp` 跑主流程，可以执行：

`./.venv/bin/python scripts/test_cdp_fetch.py --cdp http://127.0.0.1:9222 --user-id 9650668145`

脚本会分别验证：

- timeline 同参重复请求是否稳定；
- timeline 跨页是否真的变化；
- comments 首批请求与 `next_max_id` 后续批次是否真的变化；
- 是否中途出现 HTML / 疑似挑战页。
