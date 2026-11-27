# 命令作者域闸与 Router 冲突消解硬规则（ag-464a1b）

- 溯源：2026-11-27 HXNativeApp issue#393 r2 复盘。①ag-7391cb 的 Router 生成规格教「cherry-pick/变基方式引入」→ worker 照做 → external_branch 交付 non-FF(diverged) 失败；②fsm 命令路径无作者域，任何账号整行「重试」即触发重派。
- 生效：commit 202c212c（main）；部署＝各项目 install.sh <path>＋restart（快照拷贝，不是引用——2026-11-27 两个 daemon 均已重装验证）。

## 命令作者域闸（fsm.py）

- 整行命令（重试/解决冲突/重派/取消任务/取消/暂停/恢复/重发汇报）命中后校验 comment.user.login：
  - 允许＝issue 提出人（issue.user.login，缺省白名单；轮询通道经 orchestrator 注入）＋config 新键 commanders（logins 列表，缺省空）
  - bot 自身（gitea_user）一律拒——bot 永不是 commander
  - 非白名单 → ignore，journal reason 含 non-commander（排查命令被无视先查这里）
- 批准 <hex> 豁免：作者域仍走 approve 执行器的 approvers 白名单（ag-72646a 机制不变）。
- 排查面：命令没生效时看 journal 三因——非整行（含其他文字）/非白名单作者/bot 自身。

## Router 冲突消解硬规则（ROUTER.template.md）

- 冲突消解/分支同步类任务的 goal/规格必须写明 merge 嫁接法：双亲 merge 提交（父1＝远端 fix tip、父2＝集成分支 tip）、禁 rebase/禁历史重写/禁 cherry-pick 重放、交付提交须为远端目标分支 tip 后代、推送前 git merge-base --is-ancestor 自检。
- 关联经验：HXNativeApp 经验库 20261127-124236-81ed（external_branch 交付 rebase 必翻车）。
