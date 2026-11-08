# issue-agent 部署与运维指南（共享基础设施实例，Gitea + opencode 自动修码）

> 共享代码源在 `~/.dotfiles/.agent/issue-agent/`；每个项目写一份 `.issue-agent.json` 后用 `install.sh <project-path>` 一条命令接入。按本文配置即得：
> **新 issue → ~1s 认领 → opencode 在 `issue/N-fix` 分支修码推送 → 在 issue 评论里与你多轮对话 → 推送后自动 `/review-loop` 评审（Trivial 修复自动续推）**；范围外 issue 自动拒收；评论「暂停/恢复」交接；TUI 随时接管；人工合并仍是终审。

## 1. 架构一览

```
Gitea (自建, docker)                         本机 (systemd user service)
┌────────────────┐   webhook (HMAC-SHA256)  ┌─────────────────────────────┐
│ issue 事件 ────┼───────────1s────────────▶│ listener :<port> (任意POST路径)│
│ issue_comment ─┼───────────1s────────────▶│        ↓ 签名验证后入队      │
└───────▲────────┘                          │                              │
        │                                   │ worker 串行回合（保障层）：  │
        │                                   │  claim→🤖确认→fetch基线ref→  │
        │ REST API (Authorization: token)   │  opencode run（cwd=主仓）─── │
        │  评论回发/建hook/读issue           │  ─▶ agent 自建/自清 worktree，│
        │                                   │     提交 issue/N-fix 分支    │
        │                                   │  →daemon检测新提交→统一push→ │
        │                                   │  回复→落账（主仓零接触监察） │
        └───────────────────────────────────┴─────────────────────────────┘
                                            兜底：每 600s reconcile 全量对账
                                            （webhook 全挂也不断服务，仅降速）
```

**文件清单**：`daemon.py http.py fsm.py api.py runner.py gitop.py orchestrator.py state.py config.py PROTOCOL.md install.sh uninstall.sh README.md DEPLOY.md tests/`

## 2. 前置条件

| 端 | 要求 |
|---|---|
| 本机 | python3 ≥3.10、git、**opencode CLI 已登录**、systemd 用户会话、与 Gitea 服务器**双向可达**（同网段最佳） |
| 服务器 | Gitea ≥1.16（实测 1.21.11）、**管理员改一次 app.ini**（见 §3） |

## 3. 服务器端配置（关键且一次性，最容易踩坑）

Gitea 1.16+ 默认**禁止 webhook 投递到私网地址**（SSRF 防护），不改则投递全部 `deny`，本机只能靠 10 分钟轮询。

编辑 Gitea 的 `app.ini`（docker 部署通常在宿主机挂载目录，如 `/data34t/dev_data/gitea_config/app.ini`，容器内路径 `/etc/gitea/app.ini`）：

```ini
[webhook]
ALLOWED_HOST_LIST = private
```

⚠️ **键名是 `ALLOWED_HOST_LIST`，不是 `ALLOWED_HOSTS`**——写错会被静默忽略且 Gitea 照常启动（本项目实际踩过）。

```bash
docker restart gitea && curl -s http://<gitea>/api/v1/version   # 等恢复
# 验证不再 deny：
docker logs gitea --since 5m 2>&1 | grep -i deny   # 应无新 deny
```

**本实例的现成证据**：`ALLOWED_HOST_LIST` 是 Gitea 服务器**主机级**一次性配置（与回调端口无关——换端口不影响其生效）。本服务器上已有一个实例的 webhook 持续成功投递到本机，即证明该配置已就位，**本节无需再动服务器**。判断依据只看已有实例的投递记录，不从 git remote 等任何旁路推断。

## 4. 本实例配置五步

### ① PROTOCOL.md 的项目身份（已完成）
`PROTOCOL.template.md` 第 0 条由 daemon 在启动时用实例 config（identity/out_of_scope）渲染；若项目定位变化，改项目 `.issue-agent.json` 的 identity/out_of_scope 再重启服务即可。

### ② 建 API token（Gitea 网页）
右上角头像 → Settings → Applications → Generate New Token，勾选 `write:repository`、`write:issue`、`read:user`（或全选）。注意：本实例与另一个已存实例**共用同一 token**，吊销会同时停掉两边（见 §10）。

### ③ 开 linger（守护进程在登出后存活）
```bash
loginctl enable-linger "$USER"        # Ubuntu 22.04 实测免 sudo
loginctl show-user "$USER" -p Linger  # 期望 Linger=yes
```

### ④ 安装（两种方式）
```bash
cd ~/.dotfiles/.agent/issue-agent
bash install.sh                        # 交互式：依次问 API地址/PAT/仓库路径/回调URL
# 或非交互（CI/e2e 用）：
ISSUE_AGENT_SERVICE_NAME=<service>-issue-agent \
ISSUE_AGENT_PORT=<port> \
ISSUE_AGENT_DEFAULT_BRANCH=<default-branch> \
ISSUE_AGENT_API_BASE=http://<gitea>:3000 \
ISSUE_AGENT_CALLBACK_URL=http://<本机对Gitea那个网段的IP>:<port> \
bash install.sh
```
PAT 经环境变量 `ISSUE_AGENT_PAT` 传入或交互输入（不回显），绝不上命令行参数。

安装器自动完成：**canonical 路径钉定（离线，最先执行——非 `.issue-agent.json` 的 canonical_path 一律拒绝）** → PAT 校验（并记下 Gitea 用户名）→ 端口预检 → linger 检查 → 部署快照到 `~/.local/lib/` → 写 0600 配置 → 注册 webhook（按 URL 幂等 upsert）→ 保护分支检查（仅告警）→ 起 systemd 服务并验证。

### ⑤ 验证
```bash
systemctl --user is-active <service>-issue-agent     # active
# 建一个测试 issue → 期望 ~1s 内 journal 出现 claim：
journalctl --user -u <service>-issue-agent -f        # 看 "claim: issue#N -> pending"
# 全链路冒烟（可选，约 8 分钟）：
ISSUE_AGENT_API_BASE=... ISSUE_AGENT_PAT=... bash tests/e2e_smoke.sh
# 拒收路径冒烟（可选）：
bash tests/e2e_reject.sh
```

## 5. 日常使用

| 动作 | 方式 |
|---|---|
| 提需求 | 正常建 issue，~1s 收到「🤖 已接手」，~1min 收到修码汇报（根因/改动/验证三段）+ `issue/N-fix` 分支 |
| bot 修复评审 | 推送后自动调用 `/review-loop`（真实命令+模块矩阵；显式范围 `agent-remote/<基线>..issue/N-fix`；证据持久化到 `<repo>/.omo/evidence/<service>-issue-agent/review-<iid>-<ts>/`；Trivial 内修复、超界上报；推送仍由 daemon 统一执行）；评审失败只评论提示人工复查，不阻塞 issue；人工合并仍是终审 |
| 追问/改需求 | 直接在 issue 里评论（同一 issue 续用同一会话，见 §9） |
| 范围外 issue | agent 回复「范围外」三段说明，不动代码；补充说明后可重新触发 |
| 人工接管 | issue 评论整行「暂停」→ `cd <project-path> && opencode`（会话根＝主仓）按 `issue#N` 名进入续跑 → 整行「恢复」交还 |
| 零改动回合（拒收/无需修码） | daemon 不推送、不动任何分支；遗留的空 `issue/N-fix` 分支可在主仓随手 `git branch -d` |
| 升级 agent 代码 | 改 `scripts/issue-agent/` → **确认在默认分支上**合入 → 重跑 `install.sh`（自动重启加载新快照；升级后务必跑一次 §4⑤验证） |
| 卸载 | `bash uninstall.sh`（保留配置与账本；重装不重放历史 issue，见 §10） |

## 6. 故障排查（实战踩坑速查表）

| 症状 | 根因 | 处置 |
|---|---|---|
| 新 issue 最长 10 分钟才响应 | webhook 被 Gitea `ALLOWED_HOST_LIST=external` 默认拦（docker logs 可见 `deny 'IP'`） | §3 改 `private` 并重启容器；轮询兜底不断服 |
| 已配置仍 deny，日志提示 `check your webhook.ALLOWED_HOST_LIST` | 键名写成 `ALLOWED_HOSTS`（被静默忽略） | 改为 `ALLOWED_HOST_LIST` |
| Gitea 投递记录 Response=0 且日志无 deny | 网络层：服务器→本机不通 | 服务器上 `curl http://<本机IP>:<port>/`（预期任何 HTTP 码即通）；同网段一般无此问题 |
| 回调地址是另一个网段的 IP | 双网卡机器：默认路由网卡 ≠ 与 Gitea 同网段网卡 | install 已用 `ip route get <gitea主机>` 选源 IP；手工场景用 `ISSUE_AGENT_CALLBACK_URL` 指定 |
| agent 永不处理、账本显示 adopted | 存量 issue 被首装基线收编（设计如此，只处理安装后的新 issue） | 正常；或删 `~/.local/state/<service>-issue-agent/state.json` 重做基线 |
| 回合一直 defer 直至 blocked | 基线 fetch 失败（网络/PAT/Gitea 不可达）或 opencode 超时 | 查 journal 的 `deferred` 原因行；确认 Gitea 可达、PAT 有效；连续 7 次失败会自动 blocked 并评论求助；重量级验证回合可调大 config 的 `opencode_timeout`（默认 1800s） |
| journal 反复出现 `CRITICAL: ... modified the main working copy` | 本机用户就在钉定检出里开发，正常 WIP 即触发 tracked_dirty 监察 | **仅日志、无动作**，不必处理；只有确认是 agent 回合造成的改动才需要追查 |
| 长脚本跑一半静默失败、curl 全空 | 凭据头文件被外部清理（沙箱 TMPDIR 场景） | 已迁至 XDG_RUNTIME_DIR 且自愈；勿从自动清理 tmp 的环境跑长脚本 |
| 「暂停」后仍有回复 | 暂停**不中断在途回合**（设计），回合收尾的回复会到达 | 先等在途回合结束（账本回 active）再计静默窗口 |
| 把服务端当 GitLab 调（`/api/v4`、`PRIVATE-TOKEN`、`oauth2:`） | 它其实是 **Gitea**（`/api/v1`、`Authorization: token`、`user:token@`） | 确认 `curl <地址>/api/v1/version` 返回版本号 |
| 投递到达但被 404（Gitea 投递历史 Response=404，或本地自测也 404） | 快照代码与默认分支不一致（曾从滞留分支部署后回退），或 hook secret 与 config 不一致 | ① 本地签名自测（见 §7）；② 确认默认分支含修复后重跑 install；③ 重跑 install 会用 config secret 重 PATCH hook，自然对齐 |
| 修复已提交但线上行为没变 | 部署链断裂：合到别的分支 / 服务未重启 / 快照未更新 | 逐环验证：`git branch --show-current`（合并前必查！）→ install 输出「服务已重启以加载更新后的快照」 → §4⑤ 验证 |
| Gitea 队列疑似卡死（触发 test-hook 无投递记录） | docker logs 对成功/4xx 均不记 error，易误判 | 查权威源：hook_task 表（sqlite 在 data/gitea.db，查 is_delivered/is_succeed）+ 网页端「Recent Deliveries」；必要时 docker restart gitea 冲队列 |
| 安装器报「仓库路径不在钉定目录」并退出码 1 | 从其它检出（如 worktree）跑了 install，或 `ISSUE_AGENT_REPO` 指错 | 必须在 `.issue-agent.json` 钉定的 canonical_path 目录上安装；该检查离线执行，先于一切网络/PAT 步骤 |
| 服务起不来，journal 有 `ConfigError: ... repo_path` | config 的 `repo_path` 非钉定目录（如手工改过 config） | daemon 加载时拒绝非钉定路径；修正 config 或重跑 install |

## 7. 签名链自测（排障神器）

疑 webhook 被拒时，先用一条命令区分「监听器问题」还是「Gitea 侧问题」：

```bash
SECRET=$(python3 -c 'import json,os; print(json.load(open(os.path.expanduser("~/.config/<service>-issue-agent/config.json")))["secret_token"])')
BODY='{"probe":1}'
SIG=$(python3 -c "import hmac,hashlib; print(hmac.new('$SECRET'.encode(),'$BODY'.encode(),hashlib.sha256).hexdigest())")
curl -s -o /dev/null -w '%{http_code}\n' -X POST -H "X-Gitea-Signature: $SIG" -d "$BODY" http://127.0.0.1:<port>/
# 200 = 监听器+secret 健康，问题在 Gitea 侧；404 = 本地代码/secret 有问题
```

## 8. 安全注记

- PAT 只存 `~/.config/<service>-issue-agent/config.json`（0600）；脚本日志/错误信息一律脱敏（`***`）
- webhook 回调为明文 HTTP（局域网按设计接受）：密钥可被同网段嗅探——如需收紧，前置 HTTPS 反向代理并把回调改为 https
- **强烈建议在 Gitea 仓库 Settings → Branches 保护默认分支**（安装器每次都会告警提醒）
- agent 的推送仅 `issue/N-fix` 分支且由脚本执行（LLM 不掌推送权）；协议层另禁 push/reset/stash/动默认分支——但内层 agent 权限全开，防线以协议+分支保护为主，如实评估；task 8 起推送后另有独立 `review#` 会话跑真实 `/review-loop` 复查 bot 提交，该评审同样无推送权（Trivial 修复由 daemon 检测后统一再推送），证据落主仓 `.omo/evidence/`
- 卸载不会删配置与账本（重装不重放）；彻底清除需手动删 `~/.config/<service>-issue-agent/` 与 `~/.local/state/<service>-issue-agent/`

## 9. 重启恢复（设计为零操作）

**电脑/服务器重启后无需任何手动操作**，自动恢复链：

| 环节 | 靠什么活着 |
|---|---|
| 本机守护进程 | `loginctl enable-linger`（已开）+ unit 已 enabled → 开机自启，无需登录 |
| 快照/配置/账本/token | 全在磁盘（`~/.local/lib`、`~/.config`、`~/.local/state`） |
| Gitea 容器 | docker `restart=always`（服务器重启自拉） |
| webhook 注册/ALLOWED_HOST_LIST | 服务器持久化，不随重启丢失 |
| 开机竞态（网络晚于守护进程） | reconcile 600s 兜底补齐，webhook 网络就绪后自动恢复秒级 |

**会话连续性**：同一 issue 的评论续用同一 opencode 会话（session_id 落账本，重启/重装均不丢）。例外仅两个：会话失效 → 自动开新会话并在 issue 补一条说明评论；账本整体丢失 → journal 记 warning，下条评论以新会话开始。

**重启后 30 秒健康检查卡**：

```bash
systemctl --user is-active <service>-issue-agent            # active
journalctl --user -u <service>-issue-agent -b | tail -3     # 有 "daemon: ready"
# 随手在某 issue 评论一句 → journalctl 里几秒内出现 claim 即全通
```

**唯一需要动手的例外**：本机 IP 变了（DHCP 重新分配）→ webhook 回调地址失效，自动降级为 10 分钟轮询（功能不断）；跑一次 `install.sh` 重注册新 IP 即恢复秒级。

**删除项目后重新 clone 恢复服务**：项目删除→服务自动注销（自取消）后，`~/.config`（token 等）与 `~/.local/state`（账本）**仍在磁盘**——重新 clone 后进入 `scripts/issue-agent/` 跑一次 `install.sh` 即完全恢复：快照/unit/webhook 重建、secret 复用、**历史 issue 不重放**；删除期间新提的 issue 会被当作新任务正常处理（合理：它们从未被处理过）。注意：重装路径必须仍是钉定目录，否则被 canonical 检查拒绝。

## 10. 回滚（卸载 / e2e 失败 / 令牌吊销）

- **卸载**：`bash uninstall.sh` 停服并 disable unit、API 删 webhook、删部署快照；**保留** `~/.config/<service>-issue-agent/`（配置含 PAT）与 `~/.local/state/<service>-issue-agent/`（会话账本）——因此重装**不会重放历史 issue**。要彻底抹除再手动删这两个目录。
- **e2e/启动失败的退出路径**：install 末端启动验证失败时自动回滚——删除刚注册的 webhook、移除 unit，**保留配置**；修好根因（端口、PAT、网络）后重跑 `install.sh` 即可，不会留下半成品状态。
- **令牌吊销恢复**：Gitea 侧吊销 token 后本实例立即全线 401（且共用 token 的另一个实例同停）。恢复：Gitea 网页新建 token → 带新 `ISSUE_AGENT_PAT` 重跑 `install.sh`（幂等更新 config 并重 PATCH webhook）。

## 11. 多项目/多实例引入清单

把 `scripts/issue-agent/` **整目录**复制进新项目仓库后：

- **每项目必改（唯一内容性修改）**：PROTOCOL.md §0 的项目身份——写明新项目是什么、哪些算范围外，拒收判断随之适配。本实例已写为 <项目>。
- **分支**：基线**固定为 `<default-branch>`**（本仓唯一基线分支；历史上 install.sh 曾用 `origin/HEAD` 自动探测，因本仓 `origin/HEAD` 指向 `origin/master` 会在无 env 重装时静默回落到 master，该探测已删除）。`ISSUE_AGENT_DEFAULT_BRANCH` 环境变量仍可覆盖（写入 config.json，git 全链路生效），供测试或刻意换基线使用。
- **同机多实例**：每项目用独立实例名与端口安装，config/snapshot/unit/账本按实例完全隔离。**实例参数**：服务名 `<service>-issue-agent`、端口 `<port>`、基线 `<default-branch>`：

```bash
ISSUE_AGENT_SERVICE_NAME=<service>-issue-agent \
ISSUE_AGENT_PORT=<port> \
ISSUE_AGENT_DEFAULT_BRANCH=<default-branch> \
bash install.sh        # 卸载/排障带同名变量：uninstall.sh、journalctl --user -u <service>-issue-agent
```

  另注意：本实例的 install 会把 `SERVICE_NAME` 指向的既有 config 做 repo_path 校验——指向别处的既有配置会被拒绝覆盖（防误伤另一个实例）。
- **同一 Gitea 服务器**：§3 的 `ALLOWED_HOST_LIST` 是服务器级一次性配置，全实例共享，无需重复改（本服务器已有实例成功投递即证据，见 §3）。
- **worktree**：由 agent 按其全局 AGENTS.md 自理（每回合现场建/清，见 §12）；遗留的 `ISSUE_AGENT_WORKTREE`/`ISSUE_AGENT_WORKTREE_PATH` 环境变量与 config 的 `worktree_path` 字段被静默忽略（deprecated）。
- **复制什么**：`scripts/issue-agent/` 整目录（文件清单见 §1）。

## 12. worktree 职责划分（agent 自理）

- **agent 自理**：opencode 回合内按其全局 AGENTS.md 的 worktree 铁律在主仓同级父目录现场创建/清理 worktree；分支命名 `issue/N-fix`，基于 daemon 每回合拉取的基线 `refs/remotes/agent-remote/<default-branch>`；完成后删除 worktree、保留分支（含提交）。
- **daemon 只做保证**：回合前 `fetch_base`（仅更新 agent-remote 跟踪 ref——不 checkout、不碰主仓工作副本）→ 回合后检测 `issue/N-fix` 新提交则推送（主仓推送与分支被 worktree 检出不冲突，refs 全局共享）→ 主仓 tracked-dirty 监察（仅记 CRITICAL 日志，不自动处理——本机用户就在钉定检出开发，正常 WIP 即触发，见 §6）。
- **无常驻 worktree**：安装/卸载不再创建或删除任何 worktree；卸载只做 best-effort `git worktree prune`。启动期自动清扫：daemon 启动时在 prune 之后、监听端口之前，按四重谓词安全收走空闲残留——同级 `<项目>__wt-*` 命名＋检出 `issue/N-fix` 分支＋无进程 cwd 占用（删前复探）＋工作树干净（git 自带脏检查，无 `--force`）。脏/占用残留不被清扫，journal 记 `startup: worktree kept (dirty/busy): <路径>`，仍需手工处理（`git worktree remove --force` 仅限未跟踪构建产物；出现 M/D 跟踪改动先人工判断）。
  - 限度：① 运行期被杀会话（超时/重启）的残留要等 daemon 下次重启的清扫才收；② 启动清扫与人工 TUI 回合建 worktree 存在秒级竞态窗口（删前复探缓解、非零）；③ 超时击杀且含未跟踪门禁产物（build.log/build.exit/ctest.log）的 worktree 因「干净才删」决策不被启动清扫，需手工清理。
- **回退**：无。旧的 `ISSUE_AGENT_WORKTREE=0` 直作模式已随本架构移除（该变量现为无效输入）。

---
*生成于 <项目> 项目落地实战：13 任务计划、双轮对抗评审、两轮全链路 e2e、拒收验证与 webhook 秒级调通，全过程证据见 `.omo/evidence/gitlab-issue-agent/`（27+ 份）。*
