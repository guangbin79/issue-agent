# issue-agent（共享基础设施）

Gitea issue → 自动拉起 opencode 在 `issue/<number>-fix` 分支修复 → 分析/进展/结果回评 issue → 推送前含知识/经验沉淀（知识随 fix 分支单独提交，经验草稿入全局库待人工审核）→ 推送后自动调用 `/review-loop` 评审 bot 提交。不建 PR、不关 issue、不动默认分支；人工合并仍是终审。

本目录是**共享代码源**（源自单项目实例的参数化提取）：每个项目写一份 `.issue-agent.json`（5 参数：canonical_path / default_branch / identity / out_of_scope / port）+ 一条 `install.sh <project-path>` 命令即完成接入；安全修复改本目录一处、各项目 restart 后生效。

## 安装

项目接入（在项目根写 `.issue-agent.json` 后）：

```bash
bash ~/issue-agent/install.sh /path/to/<project>
```

非交互（e2e/自动化；环境变量覆盖全部输入，缺哪项提示哪项）。完整环境组合示例：

```bash
ISSUE_AGENT_SERVICE_NAME=<project>-issue-agent \
ISSUE_AGENT_PORT=<port> \
ISSUE_AGENT_DEFAULT_BRANCH=<default-branch> \
ISSUE_AGENT_API_BASE=http://<gitea-host>:3000 \
ISSUE_AGENT_CALLBACK_URL=http://<本机IP>:<port> \
bash ~/issue-agent/install.sh /path/to/<project>
```

`ISSUE_AGENT_PAT` 经环境变量传入或在交互提示下输入（不回显），**绝不出现在命令行参数上**——脚本只以静默方式读取并写入 0600 配置。

`ISSUE_AGENT_API_BASE` 是 Gitea 的 web 地址（不含 `/api/v1`，客户端自行拼接）。

**创建 Token**：Gitea 右上角头像 → Settings → Applications → Generate New Token（勾选 `issue` 与 `repository` 相关权限），复制生成的 token 作为 `ISSUE_AGENT_PAT`。

**钉定检查在最前**：canonical 路径钉定与 origin 归属校验在任何网络探测或 PAT 步骤**之前**完成——指向非钉定目录的安装会在完全离线的状态下被拒绝（退出码 1，报期望/实际路径），不会发出任何网络请求。

TLS 可选覆盖（明文 HTTP 的 Gitea 无需以下配置；仅适用于 https 部署的自签名证书场景）：

```bash
# 写入 config（Python 侧 daemon）并强制开启证书校验
ISSUE_AGENT_SSL_VERIFY=true bash scripts/issue-agent/install.sh
# 安装/卸载脚本 curl 侧：指定 CA bundle 后全部 curl 转为 --cacert 完整校验
ISSUE_AGENT_CA_BUNDLE=/path/to/ca.pem bash scripts/issue-agent/install.sh
# 运行期覆盖（不重写 config）：unit 加 Environment 后 restart 即可生效
```

安装器流程：**canonical 路径钉定 + origin 归属校验（离线，最先执行）** → 探测 API base（`/api/v1/version` 须返回 200——Gitea 版本接口无需认证——否则中止）→ 校验 PAT（`GET /api/v1/user`，同时捕获 `login` 写入 config 的 `gitea_user`，用于构造 `https://{gitea_user}:{PAT}@host/{owner}/{repo}.git` 推送 URL；PAT 经 0600 头文件以 `Authorization: token` 传给 curl，绝不上 argv）→ 预检监听端口（占用则退出码 2；服务已 active 时跳过该项——active 守护进程本就持有端口，重跑更新不会误判）与 linger（未启用会打印精确 sudo 命令并中止）→ 部署快照到 `~/.local/lib/<service>/` → 写 `~/.config/<service>/config.json`（0600，含 PAT、`gitea_user`、`project`=owner/repo、service_name/canonical_path/identity/out_of_scope；基线分支取 `.issue-agent.json` 的 default_branch，可用 `ISSUE_AGENT_DEFAULT_BRANCH` 覆盖）→ 装 systemd user unit → 按回调 URL upsert webhook（Gitea `type=gitea`，仅 `issues`+`issue_comment` 事件，`config.url` 匹配幂等）→ branch_protections 保护检查（空/403/404 均告警不中止）→ 触发 test-hook 验证可达（仅告警）→ 启动并验证（失败自动回滚并删除 webhook，保留配置；服务原本 active 则 restart 以加载新快照）。**幂等**：重复运行更新而不堆叠（webhook 复用同一 URL，secret 复用）。

## 卸载

```bash
bash scripts/issue-agent/uninstall.sh
```

停服、删 unit、API 删 webhook、删快照；保留 `~/.config/<service>/`（配置）与 `~/.local/state/<service>/`（会话账本）——因此重装不会重放历史 issue。

## TUI 接管

```bash
cd <project-path> && opencode
```

（会话根＝主仓。worktree 由 agent 按其全局 AGENTS.md 规则自理——daemon 仅保证基线 fetch、分支推送与主仓零改动监察；每回合现场建/清，无常驻 worktree。）

在会话列表按标题 `issue#<number>: ...` 选中对应会话，即可进入同一上下文人工接管。

## 暂停 / 恢复

在 issue 里发**整行**评论（仅整行命令生效，行中包含不触发；暂停不中断在途回合）：

```text
暂停
恢复
```

- **有界并行**：`config.json` 加 `"max_parallel": N`（默认 1＝串行；合法域 1..4）后 restart 生效——N 个 worker 消费同一队列，任意时刻 in-flight 回合 ≤N，同一 issue 的多事件仍严格串行（按 issue 锁）。机器负载与模型并发随 N 线性放大，N=2 起步够解排队痛点。

## agora 模式

`execution_mode` 逐实例显式启用的新执行路径（默认 `legacy`＝上文全部行为）：
daemon 不再本地拉起 opencode worker，而是经**持久语义 Router** 分析 issue、
以 `external_branch` 任务派发给已部署的 agora 服务，由 agora 的 worker env
修码＋门体验证，issue-agent 只负责把验证过的 commit 精确推到开发分支。
依赖、启用与试点步骤见 DEPLOY.md「agora 模式部署」。

- **依赖**：同机已部署的 agorad 实例（F94–F103 交付栈，含 external_branch
  交付、client_key 幂等建单与多实例隔离）。凭据面：issue-agent 以 root 牌
  为 `issue-agent:<service_name>` 身份签发/取回 dispatcher 牌（一次性，进程
  内缓存；root 牌仅此用途，不落日志与配置）；Gitea PAT 仍只留在 issue-agent。
- **启用/回滚**：`.issue-agent.json` 加 `agora` 节（或等价 `ISSUE_AGENT_*`
  环境变量）后重跑 install.sh 即启用：

  ```json
  "agora": {"execution_mode": "agora", "router_model": "<providerID/modelID>",
            "agora_home": "<AGORA_HOME 绝对路径>", "agora_poll_interval": 15}
  ```

  回滚＝把 `execution_mode` 切回 `legacy` 重跑（或直接改 config 后
  restart）——**agora 故障绝不自动降级双跑**：服务不可达时 issue 保留
  `agora_pending` 待派发状态并按轮询重试，恢复后继续；绝不偷偷起本地
  legacy worker（防超时后实际已建单造成双 worker）。不启用则安装面一个
  agora 键都不写，legacy 部署 config 零变化。
- **交付语义（external_branch）**：agora 门体验证（env verify＋评审锚）后
  任务驻留 `done + awaiting_external`，绝不推 origin、绝不动默认分支；
  issue-agent 持 PAT 把在案 commit 以 `<sha>:refs/heads/issue/<iid>-fix`
  精确推送（非 force、只此一 ref），成功后 `task_deliver(delivered)` 收口；
  远端已前推（non_ff）时 issue-agent 取实际远端 tip 经 `task_deliver(non_ff,
  base=...)` 换基，agora 重排重验出新 handoff 后二次推送（≤3 次总尝试）。
  默认分支仍只能由**人工 PR** 合并——机器人从不持有合并权。
- **行为变化（相对 legacy）**：
  - **持久 Router 替代旧 worker session 连续性**：每回合一任务、新 worker
    会话（上下文干净无历史腐化）；跨轮记忆经 issue Thread＋worker 报告末行
    「坑位：」提示传递（读码发现的隐含认知写给下轮，随报告回评上墙）。
    legacy `session_id` 字段不被 agora 路径改写。
  - **评审前移恒一次**：worker 必须在 done 前完成 review-loop 锚（agora 六
    闸受理即复验），issue-agent 不再跑推送后 `_run_review_phase`——同一条
    修复链不会再有第二次评审；`review_enabled=false` 只影响 legacy。
  - **启动清扫隔离**：agora 模式跳过启动期全局 `git worktree prune`/评审
    worktree 清扫——仓下 worktree 归 agora 服务（env）与其它实例所有，全局
    清扫违反「多实例只清自身环境」；legacy 行为不变。
  - **心跳/汇报口径**：心跳评论显示 agora 任务 state/delivery.status；终态
    后 worker 结构化报告原样回评（含坑位行），报告缺失只发事实型说明。
  - **暂停/关单语义**：暂停不取消在飞任务（monitor 照常完成交付）；关单＝
    终态暂停——不调 task_cancel，在飞任务跑完并照常外部交付，重开恢复路由。


## 本实例绑定与已知限制

- **canonical 路径钉定**：本实例只接受 `.issue-agent.json` 钉定的 canonical_path 仓库路径。
  - 安装期：指向任何其它目录（含本仓的其它 worktree 检出）都会在完全离线的状态下被拒绝——退出码 1，报错带期望/实际路径，不发出任何网络请求；服务名默认从 canonical_path 末段目录名小写化派生（+ `-issue-agent`）。
  - 运行期：daemon 加载 config 时若发现 `repo_path` 非钉定目录，直接抛 `ConfigError`，systemd unit 进入 failed，不处理任何 issue。
  - 防误伤：若把 `ISSUE_AGENT_SERVICE_NAME` 显式指向另一个已存实例的服务名，而其已有 config 的 `repo_path` 指向别处，安装器拒绝覆盖（保护另一个实例不被顶掉）。
- **会话连续性**：同一 issue 下的所有评论默认**续用同一会话**（session_id 持久化在账本 `~/.local/state/<service>/` 中，daemon 重启、卸载/重装均不丢）。仅两个例外，均自动说明：
  - 会话失效（如 opencode 侧会话被清理）：自动开启新会话，并在 issue 里补一条「原会话已失效，已自动开启新会话继续处理。」说明评论。
  - 账本整体丢失：journal 记 warning，下一条评论以全新会话开始。
- **tracked_dirty 噪音**：每回合后 daemon 会检查主工作副本是否有 tracked 改动，有则记 **CRITICAL** 日志。本机用户就在钉定检出里日常开发，正常 WIP 就会触发这条日志——**仅记录、无任何动作**，看到不必紧张。
- **暂停/恢复粒度**：`暂停` / `恢复` 必须是**整行**评论才生效（行内提到不触发），按 issue 独立生效；暂停不中断在途回合，回合收尾的回复仍会到达。
- **孤儿 worktree 清理**：agent 的 worktree 按回合现场建/清，正常情况下无残留；运行期被杀会话（回合超时/重启）可能留下残留。daemon 启动时自动清扫：在 `git worktree prune` 之后、监听端口之前，按四重谓词安全收走空闲残留——同级 `<project>__wt-*` 命名＋检出 `issue/N-fix` 分支＋无进程 cwd 占用（删前复探）＋工作树干净（git 自带脏检查，无 `--force`）。
  - 脏/占用残留不被清扫，journal 记 `startup: worktree kept (dirty/busy): <路径>`，仍需手工处理（`git worktree remove --force` 仅限未跟踪构建产物；出现 M/D 跟踪改动先人工判断）。
  - 限度：① 运行期被杀会话的残留要等 daemon 下次重启的清扫才收；② 启动清扫与人工 TUI 回合建 worktree 存在秒级竞态窗口（删前复探缓解、非零）；③ 超时击杀且含未跟踪门禁产物（build.log/build.exit/ctest.log）的 worktree 因「干净才删」决策不被启动清扫，需手工清理。

- **bot 修复自动评审**：修复推送后 daemon 自动开全新 `review#<iid>` 会话调用真实 `/review-loop` 命令（模块矩阵由子进程用户级配置解析）；评审范围显式为 `refs/remotes/agent-remote/<基线>..issue/<iid>-fix`，不用 `.last-reviewed` 推导；证据持久化到主仓 `.omo/evidence/<service>/review-<iid>-<ts>/`（评审 worktree 是临时的）；Trivial 界内（≤1 文件且 ≤20 行）发现由评审提交修复、超界只上报；推送仍由 daemon 统一执行（评审无推送权）。评审超时/崩溃不阻塞 issue，仅补一条人工复查建议评论；`review_enabled=false` 可关、`review_timeout` 可调。人工合并仍是终审。
- **评审路径分工（模块 headless lane 判据）**：本实例的 bot 评审为 **prompt 直调 /review-loop 形态**——这是 review-loop 模块 headless lane 分工判据认可的并行形态（守护进程自带编排：跨分支评审范围/临时 worktree/推送管制，不适合包装器模型）。无自持编排的 headless `opencode run` 调用者应改用 dotfiles 包装器 `~/.dotfiles/.agent/review-loop/run-with-review.sh`（dotfiles master @3259b4b 起）。
- **opencode_timeout 调节**：config.json 的 `opencode_timeout` 字段（默认 1800 秒）是单回合超时。重量级验证回合（如全量构建）若超时，调大该值后 restart 服务即可（评审会话超时用同级的 `review_timeout`，默认 3600 秒）。
- **长任务心跳评论**：回合运行超过 `heartbeat_interval`（config.json，默认 600 秒，`0`＝关）仍未结束时，daemon 主动在 issue 里发一条「仍在进行：已运行 X 分钟，当前正在：\<最新工具/文本活动\>」，此后每满一个 interval 再发一条（评论唯一出口是心跳线程，频率硬上限＝每 interval 一条/回合；活动摘自 opencode NDJSON 流的最新工具调用/文本，取不到时只报时长）。fix 回合与评审回合均生效；回合结束（完成/defer/blocked/超时）心跳自动停止。
- **PAT 共命运**：本实例复用与另一个已存实例相同的 Gitea token——在 Gitea 侧吊销该 token 会同时停掉两个实例。
- **bot 沉淀纪律**（协议步骤 3：修复提交后、删 worktree 前，agent 在 worktree 内做知识 ingest——知识改动以 `docs(issue-agent):` 单独提交随 `issue/<iid>-fix` 分支走评审与人工合并；经验按 agent-experience capture 写草稿入全局库 `~/.dotfiles/.agent-experiences/`，project 字段为 owner/repo，人工审核后 confirmed）：
  - 轻验证口径：仅 pushed 回合；knowledge 检查为累计口径——基线未前移期间多轮续修任一轮写入即 ok，人工合并后基线前移，以本轮新增为准；经验检查按 issue#<iid> 词边界＋project 双门；缺失仅 journal `capture:` warning 不阻塞；Trivial/豁免回合通常 missing（log-only 例外为 ok），warning 属预期噪音而非违规信号。

## 常见问题

- **API base 探测失败**：`/api/v1/version` 未返回 200——确认填的是 Gitea web 地址（形如 `http://host[:port]`，不带 `/api/v1` 后缀）后重跑安装。
- **自签名证书**（https Gitea）：客户端按 `ssl_verify=false` 跳过校验（安装器默认写入，属已接受的设计）；`curl` 报证书错误（exit 60）属预期。可用 `ISSUE_AGENT_SSL_VERIFY=true` / `ISSUE_AGENT_CA_BUNDLE=...` 开启完整校验（见上文安装节）。
- **明文 HTTP 回调**：默认回调 `http://<局域网IP>:<port>` 为按设计接受的方案，webhook 密钥理论上可被局域网嗅探伪造；安装器会打印警告，生产环境建议前置 HTTPS 反向代理后改用 https:// 回调。
- **查看日志**：`journalctl --user -u <service>-issue-agent -f`。
- **回调不可达**：检查本机防火墙对监听端口的放行（安装器会尝试触发 Gitea test-hook 验证）。
- **issue 与本仓库无关时会怎样**：agent 会以「范围外」开头回复并说明建议去向，不动代码；补充说明后可重新触发处理。
