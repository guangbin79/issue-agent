# verify 机械失败自愈回路（ag-52d0d3）

## 是什么
agorad 判 `verify_infra_failed`（signal 类 rc，判例 ag-aa8798 rc=137）/`verify_dead`（rc 126/127）后任务**驻留 done** 并以 kind=result mail 通知派发方；issue-agent 侧 monitor 的主处理分支——消费该 mail → board 核对 → 延迟复查 → 自动 reverify。此前无消费分支，死锁 13 小时靠人工 root reverify 解锁。

## 契约要点
- **mail 识别＝文案特征单源**，锚定 agorad core/gate.py `_verify_infra`/`_verify_dead` 下发文案：「verify 机械失败」/「verify 命令自身死」——**勿按 rc 猜**（服务端改文案才动这里）。
- **board 核对三条件**（触发登记前置）：`state==done ∧ 非 in_gate ∧ delivery.status≠awaiting_external`。awaiting_external 驻留是设计内稳态（处置动词是 task_deliver），in_gate＝他方已处置。
- **延迟复查 ≥5 分钟**（`_REVERIFY_DELAY_S=300`）：防与派发方/服务端兜底抢跑；落地为 sidecar `state.json+".agora-reverify.json"` 登记＋下轮 `_reverify_sweep` 复查（poll 线程，禁内存 sleep）。
- **每任务自动重排上限 2 次**：计数与登记全在 sidecar（daemon 重启不丢）；超限走 issue 报警评论（`_comment_once` mark=`reverify_exhausted` 幂等）。
- **reverify 走 dispatcher 牌**（`AgoraClient.reverify` → `/reverify`，同 task_deliver 通道）；服务端仅 done 非 in_gate 可重排，worker 角色永拒。
- **瞬态≠毒**：登记写失败抛 `_MailDeferred` 留 pending 重放；sweep 中 board/reverify 瞬断保留登记下轮重试；确定性拒绝（AgoraRejected）才消费登记。

## 坑（评审环判例）
- **sidecar 坏档必须行级矫正**（`_reverify_load`）：非 dict 行丢弃、count 坏值按预算尽口径（下次 mail 走报警面）、recheck_at 非数值丢弃——否则毒行 TypeError 逐轮饿死整轮 sweep，或 `int(count)` ValueError 把 verify 机械失败 mail 当毒消息吞掉（回到死锁原案）。同 `_stuck_store_load`（ag-72646a）纪律。
- **预算先行固化**：reverify 成功后先独立落盘 count 增量再消费 recheck_at——消费写失败不得回退已耗预算（上限可被第 3 次突破）。
- **盘上残登记自愈**：消费写失败留下到期 recheck_at 时，下轮 sweep 重读 board，已 in_gate 即「他方已处置」消费掉——服务端幂等口径兜底，不会双排。

代码指纹: _reverify_load, _reverify_sweep, _verify_requeue_register, _verify_infra_stuck, AgoraClient.reverify, .agora-reverify.json, agora_monitor.py, agora_client.py
