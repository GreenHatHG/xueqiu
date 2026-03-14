# 雪球用户内容爬取

这个项目现在只走一条主路：

- 先准备一份“基础浏览器资料目录”
- 第一次在这个目录里手动登录一次雪球
- 后面每抓一个用户，程序都会先复制一份新的资料目录
- 再用这份新目录起一个新的 Chrome 去抓

这样做的目的很直接：前面某个用户把浏览器会话跑脏了，也别污染后面的用户。

## 1. 安装

推荐用 `uv`：

1）同步依赖：

`uv sync`

2）首次装 Playwright 浏览器：

`uv run python -m playwright install chromium`

## 2. 基础浏览器资料目录

默认基础目录是 `./.playwright/user-data/`。

第一次运行时，程序会先用这个目录起一个 Chrome。如果还没登录，你就在打开的窗口里手动登录雪球一次。后面再跑，就继续拿这份目录当“母本”。

重点是：

- 这份目录不是直接给所有用户共用跑到尾
- 它只是“母本”
- 真正抓每个用户时，程序都会先复制一份新的目录再起新的浏览器

如果你怀疑这份母本已经脏了，最简单的办法就是换个新目录重新登录一次，比如：

`uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06 --user-data-dir .playwright/user-data-2`

## 3. 运行方式

现在只收 `--user-list-file`，不再收 `--user-id`。

就算你只想跑一个用户，也是在文件里放一行，比如：

```text
9650668145
```

然后执行：

`uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06`

如果你要跑多个用户，文件就一行一个：

```text
9650668145
1234567890
```

常用例子：

- 最小可用：
  `uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06`
- 增量更新（抓取同一批用户的最新内容；首次跑新用户仍需要 --since 初始化）：
  `uv run xq-crawl --user-list-file data/user_ids.txt --incremental`
- Browserless incremental (HTTP, no Playwright/Chrome):
  - Requires `XUEQIU_COOKIE` (full Cookie header value)
  - Fetches only **1 page** for timeline + comments each run, but still backfills talks/detail best-effort
  - Example:
    - `export XUEQIU_COOKIE='xq_a_token=...; u=...; ...'`
    - `uv run xq-crawl --mode incremental_http --user-list-file data/user_ids.txt`
  - Note: `--since` is required only for `--mode core` (browser backfill).
- RSS 服务（按 user_id 实时出前几条）：
  - 先说一句：这个服务会用 SQLite 当缓存；TTL 内只读库，TTL 过了会先跑一次增量抓取再返回 RSS。
  - 环境变量：
    - `XUEQIU_COOKIE`：必须（抓取用；TTL 内如果不触发抓取，可以暂时不需要）
    - `XQ_RSS_KEY`：必须（访问 RSS 时要带 `key`）
    - `XQ_RSS_TTL_SEC`：缓存秒数（默认 300）
    - `XQ_RSS_DB_PATH`：SQLite 路径（默认 `data/xueqiu_batch.sqlite3`）
  - 本地跑：
    - `export XUEQIU_COOKIE='xq_a_token=...; u=...; ...'`
    - `export XQ_RSS_KEY='your_key'`
    - `export XQ_RSS_TTL_SEC=300`
    - `uv run xq-rss --host 0.0.0.0 --port 8000`
    - 打开：`http://127.0.0.1:8000/u/123456789?limit=20&key=your_key`
  - Docker 跑（不安装 Playwright/浏览器依赖）：
    - 构建：`docker build -f docker/xq-rss/Dockerfile -t xq-rss .`
    - 运行（把 SQLite 挂到 volume，重启不丢）：
      - `docker run --rm -p 8000:8000 -e XUEQIU_COOKIE='...' -e XQ_RSS_KEY='your_key' -e XQ_RSS_TTL_SEC=300 -v xq_data:/app/data xq-rss`
  - 失败行为：
    - TTL 过期且抓取失败 → 直接返回 HTTP 502（方便监控）
- 指定统一数据库：
  `uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06 --db data/my_batch.sqlite3`
- 调大两个用户之间的等待：
  `uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06 --user-cooldown-sec 90`
- 跳过登录检查（不推荐）：
  `uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06 --skip-login-check`
- 出现 `alichlgref` / `md5__1038` 这种跳转时：
  `uv run xq-crawl --user-list-file data/user_ids.txt --since 2026-03-06 --reduce-automation-fingerprint`

说明：

- `--since 2026-03-06` 表示按 `Asia/Shanghai` 往回抓，只保留 `>= 2026-03-06` 的内容
- `timeline` 还是走主页浏览 + 页面拦截，但现在会记住已经跑过的批次，停掉后会先快进再继续
- `comments` 会优先在当前浏览器会话里直接拿数据；如果重试后还是不对，或者根本没拿到回复 JSON，会留 HTML 快照
- `talks` 默认会尽量补

### Incremental mode (`--incremental`)

- A per-user checkpoint (watermark) is stored in SQLite table `crawl_checkpoints`.
- Newly crawled `status/comment/talk` rows are written into `raw_records` (de-duped by `merge_key`).
- `merged_records` is treated as a **derived reading table** containing only `entry:*` rows:
  - It is rebuilt from `raw_records` **only when** the current run wrote any new raw rows.
  - If a run finds no new content, it will **not** rewrite `merged_records`.
- Legacy (non-incremental) mode also follows the same **raw-first** storage pipeline:
  - Write `status/comment/talk` into `raw_records` first
  - Rebuild `entry:*` into `merged_records` from `raw_records`
- For a user with no checkpoint and no `raw_records` history, you must still provide `--since` once.

### Repair: fix truncated original status text ([detail])

This repairs “truncated original/quoted status preview” (first display line endswith `...` / `…`) by:

- launching a logged-in browser session
- rebuilding `entry:*` rows in `merged_records` from `raw_records`
- best-effort re-fetching the original status detail page during rebuild ([detail])

`uv run python scripts/repair_truncated_details.py --db data/xueqiu_batch.sqlite3`

## 4. 每个用户的新浏览器

程序现在会这样跑：

1）先检查基础浏览器资料目录能不能登录
2）开始抓某个用户前，先复制一份新的资料目录
3）用这份新目录起一个新的 Chrome
4）抓完这个用户就关掉这个 Chrome

临时目录默认放在：

`data/browser_profiles/<时间戳>/`

规则是：

- 抓成功：临时目录会尽量删掉
- 抓失败：临时目录先保留，日志里会把路径打出来，方便你查

## 5. 输出

默认输出在 `data/`：

- `data/xueqiu_batch.sqlite3`：默认统一数据库
- `data/html/{user_id}/`：拿不到目标 JSON 时保存的网页快照
- `data/browser_profiles/<时间戳>/`：本次运行时每个用户的临时浏览器目录

看结果时，先看表里的 `text` 字段就行；要追原始内容，再看同一行的 `payload_json`。

但要注意：

- `text` 现在不是“全清洗后的纯文本”
- 目前只会顺手处理一小部分 HTML：去掉 `<a ...>` 包裹但保留里面的字，把 `<br>` 变成换行，把雪球表情那种 `<img ...>` 换成对应文字
- 像 `<p>`、`<strong>`、`<figure>`、大部分普通 `<img>` 这类标签，现在还可能保留在 `text` 里
- 所以后面如果你要拿 `text` 去做搜索、分析、喂模型，最好再做一层你自己的正文清洗

## 6. 风控和稳定性

程序默认是偏保守的：

- 单线程顺着抓
- 每次请求有最小等待和随机抖动
- 两个用户之间还会额外等一会儿
- 某个用户如果已经明显出问题，默认会停掉后面的用户

如果你看到“请手动验证”之类的话，就去打开的浏览器窗口里把验证处理完，再回来按回车继续。

# 免责声明 / Disclaimer

## 中文版本

本项目是一个开源软件，仅供学习和研究目的使用。使用者在使用本软件时，必须遵守所在国家/地区的所有相关法律法规。

项目作者及贡献者明确声明：

1. 本项目仅用于技术学习和研究目的，不得用于任何违法或不道德的活动。
2. 使用者对本软件的使用行为承担全部责任，包括但不限于任何修改、分发或商业应用。
3. 项目作者及贡献者不对因使用本软件而导致的任何直接、间接、附带或特殊的损害或损失承担责任，即使已被告知可能发生此类损害。
4. 如果您的使用行为违反了所在司法管辖区的法律，请立即停止使用并删除本软件。
5. 本项目按"现状"提供，不提供任何形式的担保，包括但不限于适销性、特定用途适用性和非侵权性担保。

本项目采用 MIT 许可证发布。根据该许可证，您可以自由使用、复制、修改、分发本软件，但必须保留原始版权声明和本免责声明。

项目作者保留随时更改本免责声明的权利，恕不另行通知。使用本软件即表示您同意受本免责声明条款的约束。

## English Version

This project is open source software provided for learning and research purposes only. Users must comply with all relevant laws and regulations in their jurisdiction when using this software.

The project owner and contributors explicitly state:

1. This project is for technical learning and research purposes only and must not be used for any illegal or unethical activities.
2. Users assume full responsibility for their use of the software, including but not limited to any modifications, distributions, or commercial applications.
3. The project owner and contributors are not liable for any direct, indirect, incidental, or special damages or losses resulting from the use of this software, even if advised of the possibility of such damages.
4. If your use violates the laws of your jurisdiction, please stop using and delete this software immediately.
5. This project is provided "as is" without warranty of any kind, either express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, or non-infringement.

This project is released under the MIT License. Under this license, you are free to use, copy, modify, and distribute this software, but you must retain the original copyright notice and this disclaimer.

The project owner reserves the right to change this disclaimer at any time without notice. Your use of the software indicates your acceptance of the terms of this disclaimer.
