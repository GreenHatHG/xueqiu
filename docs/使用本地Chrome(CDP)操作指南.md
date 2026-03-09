# 使用本地“正常 Chrome”运行爬虫（CDP attach）操作指南

当 Playwright 自己拉起的浏览器反复触发雪球风控（例如出现 `alichlgref` / `md5__1038` 无限跳转）时，最稳妥的方案通常是：**用你本地的正常 Chrome（你日常用的那个）完成登录与挑战**，然后让爬虫通过 CDP（Chrome DevTools Protocol）连接到这个浏览器继续抓取。当前代码下，CDP 模式并不是“全部都 direct fetch”：`timeline` 仍然保持 UI 页面驱动与网络拦截，`comments` 则优先在该浏览器会话里直接请求 `/statuses/user/comments.json` 并按 `max_id` 游标翻页。

这条路的优点是更接近真实用户环境（同一套 Profile、扩展、字体、指纹等），缺点是需要你手动以 remote debugging port 启动 Chrome，且要注意 Profile 锁文件与安全边界。

## 1. 核心要求与风险

1）必须用带 `--remote-debugging-port` 的方式启动 Chrome，否则 Playwright 无法附加。
2）**macOS 上 DevTools 远程调试要求非默认 data directory**：必须额外指定 `--user-data-dir`，否则 Chrome 会提示 `DevTools remote debugging requires a non-default data directory` 并且不会真正监听端口。
3）如果你要复用“已有登录态”，确实可以尝试直接指向你日常 Chrome 的用户数据目录（见下文“复用已有 Profile（高风险）”）。但这会带来 Profile 锁文件与潜在数据损坏风险：**必须先完全退出所有 Chrome**，并建议优先采用“克隆 Profile（推荐折中）”。
3）remote debugging port 建议只绑定本机回环地址（`127.0.0.1`），不要暴露到局域网。

## 2. macOS 启动命令（推荐）

先完全退出所有 Chrome（⌘Q），然后在终端执行：

`"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --user-data-dir "$HOME/.xq-cdp-chrome"`

如果你日常使用的不是默认 Profile，可以加：

`--profile-directory="Profile 1"`

注意：是否需要 `--profile-directory` 取决于你的实际 Chrome 配置。一般不填就是 `Default`。

## 2.1 复用已有 Profile（高风险，不推荐但可尝试）

如果你的目标是“不要重新登录”，你可以让 CDP Chrome 直接使用你日常 Chrome 的用户数据目录（这通常能复用 cookie/登录态）。但请务必注意：**必须先完全退出所有 Chrome（⌘Q）**，否则会因为锁文件导致异常甚至污染 Profile。

先在日常 Chrome 地址栏打开 `chrome://version`，找到 `Profile Path`。你会看到类似：

- `.../Library/Application Support/Google/Chrome/Default`
- 或 `.../Library/Application Support/Google/Chrome/Profile 1`

那么 `--user-data-dir` 应该指向其上一级目录（即 `.../Google/Chrome`），并配合 `--profile-directory` 指向具体 profile：

`"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --user-data-dir "$HOME/Library/Application Support/Google/Chrome" --profile-directory="Default"`

如果你看到仍然拒绝监听端口、或出现大量 profile 错误日志，立刻停止并改用“克隆 Profile”（下一节）。

## 2.2 克隆 Profile（推荐折中：不影响日常、通常免二次登录）

这是更推荐的折中方案：把你的日常用户数据目录复制到一个“专用抓取目录”，然后用这个目录启动 CDP Chrome。因为复制的是同一台机器同一用户下的数据，cookie/会话信息通常能直接复用，从而避免重新登录；同时也避免了对日常 Profile 的写入风险。

做法是：

1）完全退出所有 Chrome（⌘Q）。
2）把你的用户数据目录复制到 `~/.xq-cdp-chrome`（如果目录很大，复制会花时间）。
3）用 `--user-data-dir "$HOME/.xq-cdp-chrome"` 启动 CDP Chrome。

复制方式你可以自行选择（`cp -R` / `ditto` / `rsync -a` 均可）。复制完成后，一般只需在这个专用目录里登录一次（很多情况下甚至不需要重新登录）。

## 3. Windows 启动命令（示例）

先关闭所有 Chrome，然后在 `cmd` 或 PowerShell 执行（路径按实际安装位置调整）：

`"C:\Program Files\Google\Chrome\Application\chrome.exe" --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --user-data-dir "%USERPROFILE%\.xq-cdp-chrome"`

## 4. Linux 启动命令（示例）

`google-chrome --remote-debugging-address=127.0.0.1 --remote-debugging-port=9222 --user-data-dir "$HOME/.xq-cdp-chrome"`

## 5. 连接并运行爬虫

Chrome 启动后，你可以在这个窗口里手动打开雪球并确保处于“可正常浏览”的状态（若出现验证码/挑战，先处理完）。

然后在项目目录执行：

`uv run xq-crawl --user-id 9650668145 --cdp http://127.0.0.1:9222 --since 2026-03-06`

如果你不想被“登录/风控探测”阻塞，可加（不推荐，但在 CDP 模式下有时更符合直觉）：

`--skip-login-check`

如果你想先确认“当前这套 Chrome 会话里 direct fetch 到底稳不稳”，推荐先跑独立探针脚本，而不是直接跑完整爬虫：

`./.venv/bin/python scripts/test_cdp_fetch.py --cdp http://127.0.0.1:9222 --user-id 9650668145`

如果你只想重点验证 comments 深分页，可用：

`./.venv/bin/python scripts/test_cdp_fetch.py --cdp http://127.0.0.1:9222 --user-id 9650668145 --comments-only --comment-size 10 --comments-max-pages 20 --delay 1.5`

## 6. 常见问题排查

### Q1：还是提示接口不可用，但我在浏览器里能看页面

这通常表示“页面可见”但“接口请求仍被风控/挑战拦截”。建议：

1）先在同一个 Chrome 窗口里刷新雪球首页，确认不会再跳转挑战页。
2）先跑一次独立探针脚本，看当前会话下到底是 `timeline`、`comments` 还是两者都能稳定返回 JSON。
3）降低抓取强度（提高 `--min-delay` / `--jitter`，并把 `--since` 先设得更近；必要时再用其它模式时再调低 `--max-*pages`）。
4）确认小范围稳定后，再扩大 `--since` 或页数上限。

补充：如果你遇到“为保证您的正常访问请验证 / 验证失败请刷新重试 / 需要滑动验证”的页面提示，优先在这个 CDP Chrome 的正常雪球页面里把验证过掉，而不是反复刷新 JSON 接口地址。当前代码会尽量复用你已经打开的雪球标签页承接人工处理，以避免把挑战页强行导航到 API URL 上。

### Q2：端口 9222 被占用

换一个端口，比如 9223，并在 `--cdp` 里保持一致：

- 启动 Chrome：`--remote-debugging-port=9223`
- 运行爬虫：`--cdp http://127.0.0.1:9223`

### Q3：我不想关闭正在使用的 Chrome

如果不关闭 Chrome，你很难安全地复用同一套 Profile。更推荐的做法是：使用上面示例里的 `--user-data-dir "$HOME/.xq-cdp-chrome"` 作为“专用抓取 Profile 目录”，它与日常 Chrome Profile 隔离，风险最小；你只需要在这个专用目录里登录雪球一次即可。

### Q4：如何确认 Chrome 真的在监听 CDP 端口？

在另一个终端执行：

`curl -s http://127.0.0.1:9222/json/version`

能返回包含 `webSocketDebuggerUrl` 的 JSON，才表示端口可用。
