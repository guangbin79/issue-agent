# SDD ledger — plan: /home/guangbin/.opencode/plan/2026-11-12-issue-agent-agora-integration.md

Setup: worktree issue-agent__wt-feat-agora-lane__11131042 (branch feat/agora-external-lane, base d7ef14f); spec=2026-11-12-issue-agent-agora-design.md（含 2026-11-13 坑位提示增补）。
Dependency: 生产 agorad 载荷已核验含 task_deliver(F101)/client_key(F95)——真实联调依赖绿。

Pre-flight:
- T1→T3: AgoraClient.create_task 签名缺 after 参数，spec §9.4 要求任务链保留连续性 — Ruling: Task 1 增可选 after: str|None=None 透传，Task 3 续修轮传入上轮 tid — cost if wrong: 后续补参数一次小改。
- T4 内部: plan Interfaces 写 push_verified_commit -> None，但 monitor 状态机需要 ok/non_ff/failed 分类（Step 1 测试注入 push_result） — Ruling: 返回 str 状态 "ok"|"non_ff"|"failed" — cost if wrong: 调用方改判异常。
- T2→T3 decide 签名一致；T1→T5 post/check_mail 签名一致；账本字段 T3=spec §9.6 六字段一致。

Todos: T1..T6（见计划）。

Pivot (2026-11-13): agora 写闸（F1 enforce）拦本会话直改 worktree → 执行模式从 executing-plans inline 切换为 agora 派发执行（dispatcher=本会话；worker 在任务 env 持凭据写码）。worktree issue-agent__wt-feat-agora-lane__11131042 已移除（未提交测试草稿一并废弃——worker 按计划 TDD 重写）；branch feat/agora-external-lane 已删。计划 2026-11-13 更新已核对：关单＝终态暂停（不调 task_cancel，在飞跑完照常交付确认，close 后不建新任务、重开恢复）；坑位提示约束已入 Global Constraints/Task2/Task5。Pre-flight rulings 随各任务 goal 下发。
Task 1: dispatched ag-bc6af3 (2026-11-13, model glm-5.3, verify=pytest full, client_key issue-agent-agora-integration:T1)
Task 1: worker done c5801b0469dae4df4250b760c4e06bec70e5f086（首轮全量 512 passed=477 基线+35 新增；config 四字段/agora_client 三类异常两段凭据/daemon 接线/orchestrator inert 参数/三份测试）→ in_gate，等门体 verify+评审锚+集成。
Task 1: stalled 升级处置=放任——取证：串行门被 ag-549dac 占用（ASAN build verify，预算 2700s，子进程 PID 1191787 实跑、env 活跃），ag-bc6af3 为队列第 3 位（ag-6e0f68 在前）。health 对 done-排队态的固有误报，非门死锁。watchdog 7200s 续挂。
Task 1: complete — merged@c5801b0 closed（513 passed＝477+36；评审环 2 轮修 4 finding）。T1 坑位已注入 T2 goal：①http.py shadow→测试文件需 stdlib-http preload shim；②计划 git add 清单漏 orchestrator.py（按交付物清单 add）。
Task 2: dispatched ag-72de93（after ag-bc6af3，model glm-5.3，verify=全量 pytest，client_key issue-agent-agora-integration:T2）
Task 2: worker done 40aa58c73b87534748d61acd5a105a39e27c065d（结题三段式汇报已交：根因/改动文件清单/验证…截断待门后取全文）→ in_gate。
Task 2: complete — merged@40aa58c closed（556 passed＝513+43；OCR 评审环四轮 5→3→1→0 全修）。T2 坑位三条已注入 T3 goal（共享 RLock 可重入/available_models 调用点取/urllib shim）。
Task 3: dispatched ag-c4d1e5（after ag-72de93，model glm-5.3，verify=全量 pytest，client_key issue-agent-agora-integration:T3）
Task 3: worker done 1c403ccafbcc9c2ebcd5389ee35544d03147c75c（3 实现提交＋评审环 Trivial 修复 1c403cc；全量 577 passed＝556+21）→ in_gate。
Task 3: complete — merged@1c403cc closed（580 passed＝556+24；评审 2 轮＋独立复核）。移交 finding：F3 容量竞态定点移交 T4；F5 心跳归 T5。T3 坑位五条已注入 T4 goal（锁序/零网络骨架/F3 原子 reserve/delivery_commit 首写方/FakeAgoraClient wire 形状/端口撞绑）。
Task 4: dispatched ag-8f3133（after ag-c4d1e5，model glm-5.3，verify=全量 pytest，client_key issue-agent-agora-integration:T4）
Task 4: worker done 72ce3ebdb02253d4462e4145adfae54c5fafc77d（链 1c403cc..72ce3eb 三提交：主体/评审修复/终闸修复）→ in_gate。
Task 4: 门体 verify 红→worker 自愈——根因 state._atomic_write 共享单 .tmp 暂存名并发互噬（os.replace FileNotFoundError）；修复 a335f9b 重报 done。门将重跑。
Task 4: complete — merged@a335f9b closed（606 passed＝580+26；门体 verify 红一次自愈：_atomic_write 并发暂存互噬）。终闸移交 B1 critical（awaiting_external 窗口新评论覆盖 tid→孤儿化/死锁）裁定随 T5 根修（强制项）。T4 坑位五条已注入 T5 goal。
Ruling: T4-B1 死锁根修归 T5 持续对话门控——main 虽带缺陷但 execution_mode 默认 legacy、无生产实例启用，零实暴露；T5 合并前不推广启用。
Task 5: dispatched ag-da8fec（after ag-8f3133，model glm-5.3，verify=全量 pytest，client_key issue-agent-agora-integration:T5）
Task 5: complete — merged@0fc7f3df closed（635 passed＝606+29；三分册终闸全过；T4-B1 死锁根修落地：持续对话门控/终态暂停关单/坑位行回评/mailbox 原子管线/重启恢复）。T5 坑位五条已注入 T6 goal。
Task 6: dispatched ag-13291f（after ag-da8fec，model glm-5.3，verify=全量 pytest timeout 900，client_key issue-agent-agora-integration:T6；真实 agorad e2e＋install/文档）
Task 6: watchdog 7200s 超时（exit 1 非事故）→取证 stale_candidate（last_seen 95min/env 71min）但服务 busy 闸拒重派（oc 回合挂起边缘态）→处置：task_nudge 自检注入成功（nudged=true）＋agora watch 3600s 重挂；仍死则 force-cancel＋T6b 重建（换 client_key，防幂等命中死单）。
Task 6: nudge 探针命中——worker 在轨（长评审回合，stale 误报）。进度：四交付物落地 f872253（daemon 清扫隔离/两锚例、install 四配置项、真实 agorad e2e 四场景全绿、README/DEPLOY）；验收 pytest 642=635 零回归、install.sh 231 ok；评审 R1 八项修 @7b08563，R2 六项收敛中→锚更新后 done。dispatcher 回执已发（msg 5648）。
Task 6: watch 3600s 二次超时→取证 healthy：busy_round＋env 9s 前活跃（R2 六项修复落地中，深潜长尾非挂死）。处置＝放任＋watch 3600s 三挂。事件尾含 dispatch_rejected（首次重派被拒留痕）与 worker_nudge（探针命中留痕）。
Task 6: worker done 4fed439e113f69871ff27187847dcb4de299baf9（7 提交 f872253→4fed439；R1 八项＋R2 六项全修＋e2e 回评竞态根治 wait_until 收敛）→ in_gate。
Task 6: complete — merged@4fed439 closed（642 passed=635 零回归＋e2e 4×8 连跑稳定；install.sh 263 ok；ocr 3 轮 16+ findings 全修＋三分册终闸 A/B/C 零 blocker）。
FINAL: 六单全链 merged（d7ef14f..4fed439＝24 提交，24 文件 +6708/-44）；测试进度 477→513→556→580→606→635→642；坑位链 T1(2)→T2(3)→T3(5)→T4(5)→T5(5)→T6(4) 逐单注入。受控试点＝人步骤（DEPLOY §13）。
PILOT (2026-11-13/14): hxnativeapp 切换 agora 模式成功。三单闭环：#191 测试单(flash/3987a32)、#185 积压真实单(k3.1-pro/bbd99ef)、热修 ag-6fa87c(0bc0ba3, install 清单补 ROUTER.template.md——试点首启抓到的真缺陷)。默认分支零接触；回滚面 legacy 一键。运维坑位：env 不继承 .review-loop 锚（worker 结题 note 在案）。
2026-11-14 晨间事故：用户 Gitea 合 PR（默认支 +36）→ #178/181/182 留言「解决冲突」→ 三变基轮任务；sweep 10:41 落地变基链、external 交付独立产 handoff → #182 双路径撞车 rebase_conflict；#181/178 vacuous blocked。处置：三单取消（已验证无内容丢失：range-diff 代码/知识页全等，log.md 双方 468 条等量）。用户 issue-agent 仓 push 问题＝本地滞后于 agora 自动交付（纯 ff rebase 解决，主干 25 提交线性零合并）。
Fix: dispatched ag-8d6a70（双路径交付单写者原则＋worker 契约加固＋根因实证）。
Fix: complete — ag-8d6a70 merged@a2153c2 closed（650=642+8；根因实证：env worktree 共享 ref 库→worker 推进 issue/N-fix→sweep 落地→与 external 交付双写撞车；修复：sweep 让位+契约硬禁区+cancelled/blocked 面板回评+文案纠偏）。
Ruling (用户裁定 2026-11-14): blocked 处置=方案 C——先人工（回评机制即观察仪器），vacuous-blocked 若复现成常态再立窄域自动取消单（方案 A）；不做全量自动化（B 否）。
2026-11-14 午后：titannavi 切换 agora（verify=bash ./build_linux.sh 评审先例；重启打断的 #334 安全 requeue 且已入 agora 车道）；两实例快照均=main a2153c2。
新需求双单：ag-723860（issue-agent health_digest.py 巡检脚本）＋ag-71ce44（dotfiles cron-dispatch 08:30 任务槽＋notify-owner 每日邮，接口契约互钉：rc 0/1 定主题前缀）。
