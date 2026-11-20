# 双机认领协议 v2（ag-947f91）

## 是什么
两台机共用同一 Gitea 事件源时的不双吃方案：纯轮询（webhook 全退役）＋粘住式认领（认领评论 id 最小者胜，无过期无抢占）＋唯一防御让位。

## 契约要点
- **认领评论面＝唯一跨机所有权凭据**：`<!--claim:machine=<8hex>-->` 嵌入认领评论（同时带实例 note marker＋🤖 前缀保 fsm 过滤不打环）；本地账本 `owner{machine, claim_comment_id}`＋`claim_state(none/candidate/owned/yielded)` 只是它的缓存与崩溃恢复面。
- **认领单入口**：发认领评论只允许 `_scan_once → _claim_new_issue` 一条路径；reconcile（`_reconcile_ownership`）纯对账（让位/认输/candidate 复算），绝不发认领评论。
- **存量 adopted issue 不重放**：初装基线快照的静默存量永不自动开工；被人类新评论唤醒的存量走认领面接管（`_fresh_human_notes` 探测＋wake_notes 直通接管回合），否则双机都会经重放车道双吃。
- **满槽不认领**：`free = max_parallel − max(triggered 需求, monitor 占用)`；认领评论一旦发出即粘住终身——满槽发出＝永久占坑饿死他机。
- **candidate 崩溃恢复**：裁决窗内崩溃＝账本留 candidate；下轮 reconcile 复算裁决（评论是真源），幂等。

## 坑（终闸判例）
- **让位守卫必须盖满所有派发/推送车道**：defer-retry（`_handle_requeue`）、回合收尾推送、评审后补推、sweep 欠账补推、评审延迟补推、close-push——漏任何一条＝让位后复活双跑/双推。单点修法：`_yielded(iid)` 读检助手＋各车道守卫。
- **裁决窗与 reconcile 竞态**：60s 窗内 reconcile 的 candidate 恢复会先派发（置 pending）；窗后 `_arbitrate` 必须查 `status=="pending"` 闩，否则双派发。
- **空 machine_id 陷阱**：ownership 对账里 `m != self.machine_id` 对空 id 恒真——自己的旧认领全成「他机」→批量误让位＋误取消在飞任务。空 id 时 reconcile/scan 同口径早退。
- **敌意认领评论**：machine 标记必须限长（≤16 hex）＋hex 字符集校验，否则超长 body 进账本＝每次 save_state 全量重写的持久化 DoS。
- **machine_id 复用要过格式门**：手改坏值（非 8-hex）会让本机认领评论对他机 `claim_machine` 失明（当作无主重新认领）→双吃。install.sh 复用前正则校验。

## 孤儿恢复 runbook
死机重启→triggered 自续（本地账本）；永久死→人在 Gitea 删其认领评论→他机下轮扫描视为无主（yielded 条目重新可认领，复核把门）。
