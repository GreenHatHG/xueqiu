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
