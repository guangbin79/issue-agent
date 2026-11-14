"""agora 任务监视器（计划 Task 3 骨架＋Task 4 外部交付回路＋Task 5 终态
汇报/持续评论/重启恢复）——单后台线程轮询本实例登记的活动任务（设计
§9.2/§6.2/§7.4/§9.3-§9.5）。

Task 4：poll 按 board(task) 的 state/delivery 驱动外部交付状态机——done＋
awaiting_external → push_verified_commit 精确推送 → task_deliver；non_ff →
fetch 实际远端 tip，先过变基轮护栏（ag-6da714，legacy 协议 §4 移植）：
旧链零丢失且本地分支即在案 handoff → force-with-lease 重写落地
（align-rewrite，journal 一行），否则携 base 轮转 handoff（同一任务循环，
不做本地 rebase）；blocked/failed → 按实况记 push_failed/blocked
pre-delivery 不自动重派不弹回 worker（ag-8d6a70：未进交付段的 blocked
从未推送，不误报 push_failed(None)）。

Task 5：
- 汇报管线：check_mail 整批先原子落盘 state_path+".agora-mail.json" 再
  按 tid 分发处理（服务端「拉取即送达无 ack」——读后崩溃重放未处理
  批次，处理完成才标记）；worker note 持久化为报告
  （state_path+".agora-reports.json"），delivered/closed 后回评 Gitea
  （原样保留报告末行「坑位：」行），缺报告只发事实型降级评论；worker
  question 经 answer 回调（orchestrator 在 ledger_lock 内交 Router）后
  post(answer)；worker answer 回评 issue 线程。
- board 任务态落账（agora_status 首写方＝本模块；以 board 为准，不以
  worker 自述为准）；cancelled 只报取消事实不归因质量失败，blocked+
  delivery.failed 回评具体 push_failed。
- 心跳：agora 回合心跳显示 board state/delivery.status（legacy 心跳在
  orchestrator runner 面不变）。
- 重启恢复：start() 从账本恢复全部未终态 agora_task_id（幂等 register，
  终态以账本 agora_status 头段为准，未同步按未终态恢复——poll 首轮以
  board 对账收口）。

活动名额（max_active，接线 config.max_parallel）为原子 check-and-reserve
（F3）；_run_turn 返回不释放——释放权只在本状态机（终态）与回合失败出口。
agora 任务自身的 origin push 只经 push_verified_commit（精确 SHA、绝不动
默认支）；唯一 force 例外＝变基轮护栏判定零丢失后的 force-with-lease
落地（daemon 侧双条件：本地分支即在案 handoff＋range-diff 零丢失，
worker 永无 force 权）；force 落地被确定性拒绝（受保护分支/pre-receive）
时按轮转预算路由：转 non_ff 轮转（≤3 有界）或 deliver(failed,
force_rejected) 终局，绝不无限重试（ag-cad1d6 High-1）；agora 不推
issue 仓远端——外部交付权威在 issue-agent。
"""

import json
import os
import re
import threading
import time
from pathlib import Path

import gitop
import state
from agora_client import AgoraProtocolError, AgoraRejected, AgoraUnavailable

_HEX40_OK = re.compile(r"[0-9a-f]{40}")

# board 主状态（task 行 state）：这三个对本监视器是终态（名额可释放）
_TERMINAL_STATES = ("merged", "closed", "cancelled")

# non_ff 轮转预算（服务端契约 task_deliver）：≤3 次总尝试，第 4 次起
# retry_exhausted 终局拒——attempt 已达 3 再收到确定性 force 拒绝时不再
# 轮转，直接 deliver(failed, force_rejected) 终局（ag-cad1d6 High-1）
_MAX_DELIVERY_ATTEMPTS = 3


class _MailDeferred(RuntimeError):
    """瞬态投递失败（answer post 瞬断／sidecar 写 OSError）：本轮停止
    批次推进，消息留 pending 下一轮 poll 重放重试——确定性失败才按毒
    消息消费＋journal（终闸 B-2/B-3：瞬态≠毒，不吞 worker 汇报）。"""


class AgoraMonitor:
    """本实例活动 agora 任务的登记簿＋轮询线程＋外部交付状态机。"""

    def __init__(self, *, client, journal=None, max_active=1):
        self._client = client
        self._journal = journal if journal is not None else (
            lambda message: None)
        self._max_active = max(1, int(max_active))
        self._guard = threading.Lock()
        self._active = {}  # iid(int) -> tid(str|None)：一 issue 一活动任务
        self._stop = threading.Event()
        self._thread = None
        # 外部交付上下文（Task 4）：orchestrator 接线注入——daemon 构造面
        # 不变（T3 wiring 锁定），ledger_lock 与回合侧账本写共用一把。
        self._repo_path = None
        self._remote_url = ""
        self._ssl_verify = True
        self._state_path = None
        self._ledger_lock = None
        # 会话面（Task 5，orchestrator 经 attach_conversation 接线）：
        # post_note=issue 评论通道；answer_question=worker 提问答复回调
        # （orchestrator 保证先 ledger_lock 再 Router 的锁序）；心跳间隔
        # 取自 orchestrator.heartbeat_interval（0＝关）。
        self._post_note = None
        self._answer_question = None
        self._hb_interval = 0
        self._hb = {}  # iid -> {"start","last","status"}（_guard 保护）
        self._questions_this_drain = 0  # 每轮 drain 的 LLM 答复预算（B-4）
        # ag-1c7315（评审 R1）：处置路径（worker 线程）与轮询（poll 线程）
        # 首次出现跨线程并发面——reports sidecar 写者互斥＋per-issue 对账串行
        self._store_lock = threading.Lock()
        self._reconcile_locks = {}

    # ---- 登记簿 ----

    def attach_delivery(self, *, repo_path, remote_url, ssl_verify=True,
                        state_path, ledger_lock):
        """交付上下文接线（orchestrator __init__ 调用）：git 交付原语与
        issue 账本。ledger_lock 与回合侧共用——delivery_commit 落账与
        orchestrator/router 的账本写互斥。"""
        self._repo_path = repo_path
        self._remote_url = remote_url
        self._ssl_verify = ssl_verify
        self._state_path = state_path
        self._ledger_lock = ledger_lock

    def attach_conversation(self, *, post_note, answer_question,
                            heartbeat_interval=0):
        """会话面接线（orchestrator __init__ 调用，daemon 构造面不变）：
        issue 评论通道、worker 提问答复回调与心跳间隔。answer_question
        由 orchestrator 保证锁序（先 ledger_lock 再 Router，T3 锁序铁律）。
        monitor 轮询线程内同步调用答复回调——worker 提问属低频事件，
        阻塞轮询可接受（简单性优先）。"""
        self._post_note = post_note
        self._answer_question = answer_question
        self._hb_interval = max(0, int(heartbeat_interval))

    def active_task(self, iid):
        """持续对话门控（计划 Task 5，T4-B1 根修）：登记在案且未释放的
        tid。名额释放点＝终态/交付收口，故「在案」即「非终态」（含 done+
        awaiting_external 交付窗口）。"""
        with self._guard:
            return self._active.get(int(iid))

    def report_for(self, iid):
        """该 issue 最新持久化 worker 报告（重发汇报复用：零派工零推送）。"""
        store = self._reports_load()
        keyed = [(r.get("note_id") or 0, r) for r in
                 store.get("reports", {}).values()
                 if r.get("iid") == int(iid) and r.get("note")]
        if not keyed:
            return None
        return max(keyed, key=lambda item: item[0])[1]["note"]

    def reserve(self, iid) -> bool:
        """原子 check-and-reserve（F3）：容量内占位（tid=None 待 register
        转正），满员 False；同 iid 重入＝复用己占名额（中断恢复路径不走
        第二个槽）。修复 has_capacity check-then-act 下 max_parallel≥2
        并发回合双双突破 max_active 的竞态。"""
        with self._guard:
            key = int(iid)
            if key in self._active:
                return True
            if len(self._active) >= self._max_active:
                return False
            self._active[key] = None
            return True

    def register(self, iid, tid) -> None:
        """幂等登记：同 iid 重登记覆盖为最新 tid（回合链推进时旧任务
        已终态让位）；容量检查是派发方 create 前的职责，登记只反映事实。"""
        with self._guard:
            key = int(iid)
            self._active[key] = str(tid)
            now = time.monotonic()
            self._hb[key] = {"start": now, "last": now, "status": ""}

    def release(self, iid, tid=None) -> None:
        """compare-and-release（评审环 ocr #5）：任务终态或回合失败出口
        归还活动名额（幂等）。tid=None 时只清本 issue 的 reserve 占位
        （回合失败出口专用——登记已转正则不碰）；携 tid 时仅当登记仍属
        该任务才释放（快照轮询网络窗内回合链 register 已让位给更新任务
        时，旧任务的迟到 release 不得逐掉新登记）。"""
        with self._guard:
            key = int(iid)
            if key not in self._active:
                return
            current = self._active[key]
            if tid is None:
                if current is not None:
                    return  # 已转正登记不属占位释放权
            elif current != tid:
                return  # 登记已让位给更新任务
            del self._active[key]
            self._hb.pop(key, None)  # 心跳随名额 lifecycle 收口

    def tasks(self) -> list:
        """[(iid, tid|None)] 快照（升序）；tid=None 是 reserve 占位（回合
        在途）。测试与 Task 5 恢复面共用。"""
        with self._guard:
            return sorted(self._active.items())

    def _iid_reconcile_lock(self, iid):
        """per-issue 处置↔对账串行锁（ag-1c7315 B-F2）：面板「重试」的
        register 与 poll 线程的 board 读→release 互斥——迟到的 blocked
        收口不得删掉重派后的同 tid 新登记（compare-and-release 只挡异
        tid）。"""
        with self._guard:
            key = int(iid)
            lock = self._reconcile_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._reconcile_locks[key] = lock
            return lock

    # ---- 轮询与交付状态机 ----

    def poll_once(self):
        """一轮状态 reconciliation：先排干 mailbox（汇报管线），再逐登记
        任务对账 board，最后心跳。单任务异常只 journal，不拖垮同轮其他
        任务（线程跨异常存活）。"""
        try:
            self._drain_mail()
        except Exception as exc:  # 汇报面异常不阻断交付对账
            self._journal(f"agora-monitor: mail drain failed: {exc!r}")
        for iid, tid in self.tasks():
            if tid is None:  # reserve 占位（回合在途，tid 未定）
                continue
            try:
                self._reconcile_task(iid, tid)
            except (AgoraUnavailable, AgoraProtocolError) as exc:
                # agora 不可达/协议错乱：保留现状，下一轮天然重试
                self._journal(f"agora-monitor: issue#{iid} task {tid} agora "
                              f"unreachable, retry next poll: {exc}")
            except gitop.GitopError as exc:
                # git 传输病：保留 awaiting_external 有界重试（poll 轮次
                # 天然退避），不 deliver 不释放
                self._journal(f"agora-monitor: issue#{iid} task {tid} git "
                              f"transport error, retry next poll: {exc}")
            except Exception as exc:  # 防单任务病拖垮整轮
                self._journal(f"agora-monitor: issue#{iid} task {tid} poll "
                              f"failed: {exc!r}")
        self._heartbeat_tick()

    def _reconcile_task(self, iid, tid):
        """ag-1c7315：整段对账持 per-issue 串行锁（见 _iid_reconcile_lock）
        ——board 读到 release 原子于处置路径。"""
        with self._iid_reconcile_lock(iid):
            self._reconcile_task_inner(iid, tid)

    def _reconcile_task_inner(self, iid, tid):
        task = self._board_task(tid)
        task_state = task.get("state")
        delivery = task.get("delivery") or {}
        status = self._status_text(task_state, delivery)
        self._sync_agora_status(iid, status)
        with self._guard:  # 心跳显示最新 board 态（Task 5）
            if iid in self._hb:
                self._hb[iid]["status"] = status
        if task_state in _TERMINAL_STATES:
            self._journal(f"agora-monitor: issue#{iid} task {tid} "
                          f"{task_state}; slot released")
            self._finish_terminal(iid, tid, task_state, delivery)
            return
        if task_state == "blocked" or delivery.get("status") == "failed":
            # 交付失败终局（rebase_conflict/push_failed/retry_exhausted…，
            # 设计 §7.4/§8.2）：不自动重派、不弹回 worker，人工跟进（回评
            # 具体错误）。未进交付段就 blocked 的任务从未尝试过推送——按
            # 实况记 blocked pre-delivery，不误报 push_failed(None)（ag-8d6a70：
            # 2026-11-14 #178/#181 vacuous-done 三连拒文案）
            if delivery.get("status") == "failed":
                self._journal(
                    f"agora-monitor: issue#{iid} task {tid} push_failed "
                    f"({delivery.get('error')}); blocked, manual follow-up, "
                    "no auto-redispatch")
            else:
                self._journal(
                    f"agora-monitor: issue#{iid} task {tid} blocked "
                    "pre-delivery (no push attempted); manual follow-up, "
                    "no auto-redispatch")
            self._finish_blocked(iid, tid, delivery)
            return
        if task_state == "done":
            if delivery.get("status") == "awaiting_external":
                handoff = delivery.get("commit")
                if not isinstance(handoff, str) \
                        or not _HEX40_OK.fullmatch(handoff):
                    # 确定性协议违约（服务端 hex40 校验使 deliver 无法
                    # ack）——本地终局释放，不重派、不无限 journal 占名额
                    self._journal(
                        f"agora-monitor: issue#{iid} task {tid} malformed "
                        f"handoff commit {handoff!r}; terminal release")
                    self.release(iid, tid)
                    return
                self._deliver_handoff(iid, tid, handoff, delivery)
            elif delivery.get("status") == "delivered":
                # delivered ack 后收口读竞态/瞬断的恢复面（评审环 ocr #4）：
                # 本次 board 读即确认——补记 delivery_commit、回评汇报并释放
                commit = delivery.get("commit")
                if isinstance(commit, str) and _HEX40_OK.fullmatch(commit):
                    self._record_delivery_commit(iid, commit)
                self._report_comment(iid, tid, commit)
                self.release(iid, tid)
        # 其余（pending/integrating 与未到 done 的运行态）：本轮无动作

    # ---- 终态汇报（Task 5）----

    @staticmethod
    def _status_text(task_state, delivery) -> str:
        """board 行 -> 账本 agora_status：done 带交付态后缀（人在账本能
        区分交付窗口与已交付）；终态不带（cancelled 不混入交付叙事）。"""
        status = task_state if isinstance(task_state, str) else ""
        dstatus = delivery.get("status") if isinstance(delivery, dict) else None
        if dstatus and task_state not in _TERMINAL_STATES:
            status = f"{status}/{dstatus}"
        return status

    def _sync_agora_status(self, iid, status):
        """board 任务态落账（state.agora_status 首写方＝本模块）：以 board
        为准，不以 worker 自述为准；值未变零写。"""
        if self._ledger_lock is None or self._state_path is None \
                or not status:
            return
        with self._ledger_lock:
            ledger = state.load_state(self._state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None or entry.get("agora_status") == status:
                return
            entry["agora_status"] = status
            state.save_state(self._state_path, ledger)

    def _finish_terminal(self, iid, tid, task_state, delivery):
        """终态收口：merged/closed＝交付成功面（回评 worker 报告；缺报告
        走事实型降级），cancelled＝只报取消事实不归因质量失败（2026-11-13
        关单/取消语义）。终态评论经 finalized 标记只发一次。"""
        if task_state == "cancelled":
            self._comment_once(iid, tid,
                               f"agora 任务 {tid} 已取消（board 口径，非质量"
                               "结论）；issue 待人工跟进。",
                               mark="cancelled")
        else:
            self._report_comment(iid, tid, delivery.get("commit"))
        self.release(iid, tid)

    def _finish_blocked(self, iid, tid, delivery):
        """blocked 收口（delivery.failed＝push 类失败回评具体错误）＋处置
        提示行（ag-1c7315：面板上的人知道有「重试／取消任务」这双手）。"""
        if delivery.get("status") == "failed":
            text = (f"agora 任务 {tid} 外部交付 push_failed"
                    f"（{delivery.get('error') or 'unknown'}）")
        else:
            text = f"agora 任务 {tid} 被 blocked（board 口径）"
        self._comment_once(
            iid, tid,
            text + "，需要人工处理；不自动重派。"
            "\n处置：评论『重试』重派，或『取消任务』归档。",
            mark="blocked")
        self.release(iid, tid)

    def redispatched(self, iid, tid):
        """面板「重试」指令的 monitor 收位（ag-1c7315）：重新登记（relay
        与交付轮询恢复）＋账本头段离开 blocked（关掉一个 poll 窗口内的
        二次「重试」重派竞态），并清该 tid 旧轮的终局评论标记与报告
        （重派是新一轮：再 blocked／终态照常回评，旧轮报告不得冒充新轮
        汇报）；清标与登记同入对账串行锁（评审 R2：迟到的 blocked/终态
        release 不得删掉本登记，等待锁的 poll 对账也不得读到旧轮残留）。
        note_id 保留为跨轮重放水位——旧轮 pending 邮件（mid≤水位）不得
        借重置冒充新轮报告，新轮 note 的 mid 单调更高照常入库。"""
        with self._iid_reconcile_lock(iid):
            self.register(iid, tid)
            self._sync_agora_status(iid, "dispatched")
            if self._state_path is None:
                return
            with self._store_lock:  # B-F1：与 poll 线程报告入库互斥
                store = self._reports_load()
                entry = store.get("reports", {}).get(tid)
                if entry is not None:
                    entry["sent_marks"] = []
                    entry["finalized"] = False
                    entry["note"] = None  # note_id 保留＝跨轮重放水位
                    state.write_json_atomic(self._reports_path(), store)

    def announce_cancelled(self, iid, tid):
        """面板「取消任务」指令的事实回评（ag-1c7315）：mark=cancelled 与
        result-mail 收口回评同 mark 幂等——面板恰好一条取消事实，后到的
        邮件回评零动作；取消 API 已成功＝board 权威，账本同步终态。"""
        self._sync_agora_status(iid, "cancelled")
        self._comment_once(
            iid, tid,
            f"agora 任务 {tid} 已按面板「取消任务」指令取消；issue 待人工"
            "跟进。",
            mark="cancelled")

    def _report_comment(self, iid, tid, commit):
        """交付成功面回评：报告已先持久化（mail 处理时入库）；原样保留
        报告末行「坑位：」提示（下轮 Router 上下文依赖该行存在于 issue
        线程）。缺报告只发事实型降级评论＋journal warning——不虚构验证
        结论、不阻塞已确认交付（重发汇报可后续补发）。"""
        entry = self._reports_load().get("reports", {}).get(tid) or {}
        report = entry.get("note")
        hint = ""
        if isinstance(commit, str) and commit:
            hint = f"（commit {commit[:12]}）"
        if report:
            text = (f"agora 任务 {tid} 已交付{hint}，worker 最终汇报："
                    f"\n\n{report}")
        else:
            self._journal(f"agora-monitor: issue#{iid} task {tid} report "
                          "missing after delivery; posting factual note only")
            text = (f"agora 任务 {tid} 已交付{hint}，worker 未提供结构化"
                    "汇报（可另发「重发汇报」索取）。")
        self._comment_once(iid, tid, text)

    def _comment_once(self, iid, tid, text, mark="final"):
        """终态评论按 mark 只发一次：final＝交付/终局汇报，blocked＝求助，
        cancelled＝取消事实（ag-8d6a70：blocked 收口后再被 root 取消是
        不同事实，各评一次，单布尔 finalized 会把取消事实吞掉）。finalized
        先落盘再评论（崩溃窗宁可少一条评论也不重复刷屏；报告已持久化可查）。
        评论通道未接线＝零动作。ag-1c7315（B-F1）：store 段持写者互斥锁
        ——处置 worker 线程（announce_cancelled）与本 poll 线程路径并发。"""
        if self._state_path is None:
            return
        with self._store_lock:
            store = self._reports_load()
            entry = store.setdefault("reports", {}).setdefault(
                tid, {"iid": int(iid), "note": None, "note_id": -1,
                      "finalized": False, "sent_marks": []})
            sent = list(entry.get("sent_marks") or [])
            # 旧档案（布尔 finalized）迁移：True＝final 评论已发，其余 mark 照常
            if entry.get("finalized") and "final" not in sent:
                sent.append("final")
            if mark in sent:
                return
            entry["iid"] = int(iid)
            sent.append(mark)
            entry["sent_marks"] = sent
            entry["finalized"] = mark == "final" or bool(entry.get("finalized"))
            state.write_json_atomic(self._reports_path(), store)
        self._comment(iid, text)

    def _comment(self, iid, text):
        """best-effort issue 评论：瞬断/异常只 journal（交付事实不因评论
        通道病被阻塞；报告已持久化、可重发）。"""
        if self._post_note is None:
            return
        try:
            if not self._post_note(iid, text):
                self._journal(f"agora-monitor: issue#{iid} comment failed "
                              "transiently")
        except Exception as exc:
            self._journal(f"agora-monitor: issue#{iid} comment failed: "
                          f"{exc!r}")

    def _deliver_handoff(self, iid, tid, handoff, delivery):
        """*delivery*＝board 在案交付快照（force 拒绝时的轮转预算
        attempt 判定材料，ag-cad1d6）。"""
        if self._repo_path is None:
            self._journal("agora-monitor: delivery context not attached; "
                          "cannot push")
            return
        # 旧 commit 防线（评审环 ocr #3 裁定）：board handoff 是权威，推送
        # 只认它；账本 delivery_commit 跨回合不清理，若在此比对跳过会把
        # 回合2 起永久卡死（max_active=1 全监视器停摆）。落账侧防线＝
        # 写前 board 核对＋服务端 stale 403＋monitor 单线程串行落账。
        result = gitop.push_verified_commit(
            self._repo_path, iid, handoff, self._remote_url, self._ssl_verify)
        if result == "ok":
            self._ack(iid, tid, "delivered", handoff)
        elif result == "non_ff":
            # 远端已前进：取实际远端 tip，先过变基轮护栏（ag-6da714，
            # legacy 协议 §4 移植）——安全则 force-with-lease 重写落地；
            # 不安全/漂移才携 base 轮转 handoff（同一任务循环，等门体
            # 换基重验出新 handoff 再推，不做本地 rebase）
            base = gitop.fetch_fix_branch_tip(
                self._repo_path, iid, self._remote_url, self._ssl_verify)
            try:
                landed = self._land_align_rewrite(iid, handoff, base)
            except gitop.ForcePushRejected as exc:
                # ag-cad1d6 High-1：确定性 force 拒绝（受保护分支禁
                # force/pre-receive/服务器拒绝改写）——重试同一改写永不
                # 可能成功，绝不按传输病无限重试占名额。内容未落地（被
                # 拒＝ref 未动）且护栏双条件已过（走到 force 的前提）→
                # 有轮转预算：转 non_ff（既有 ≤3 有界，轮转出的新
                # handoff 以 fast-forward 落地，无需 force）；预算尽：
                # deliver(failed, force_rejected) 终局。
                if (delivery or {}).get("attempt", 0) >= _MAX_DELIVERY_ATTEMPTS:
                    self._journal(
                        f"agora-monitor: issue#{iid} force landing rejected "
                        f"as deterministic ({exc}); rotation budget "
                        f"exhausted (attempt="
                        f"{(delivery or {}).get('attempt')}), reporting "
                        "failed(force_rejected)")
                    self._ack(iid, tid, "failed", handoff,
                              reason="force_rejected")
                else:
                    self._journal(
                        f"agora-monitor: issue#{iid} force landing rejected "
                        f"as deterministic ({exc}); rotating handoff "
                        "(bounded, next handoff lands as fast-forward)")
                    self._ack(iid, tid, "non_ff", handoff, base=base)
                return
            if landed:
                self._ack(iid, tid, "delivered", handoff)
            else:
                self._ack(iid, tid, "non_ff", handoff, base=base)
        else:  # failed：确定性失败，重试同 commit 永不可能成功——终局上报
            self._journal(f"agora-monitor: issue#{iid} deterministic push "
                          f"failure for {handoff[:12]}; reporting failed")
            self._ack(iid, tid, "failed", handoff, reason="push_failed")

    def _land_align_rewrite(self, iid, handoff, base):
        """变基轮护栏（ag-6da714，legacy 协议 §4 移植）：判定把远端旧链
        （base）重写到 handoff 是否零丢失——本地 issue/<iid>-fix 必须就是
        在案 handoff（force 只推已验证的精确 commit，不带分支漂移后的
        内容出门），且 rebase_safe_to_force 证明旧链提交在新链全有
        range-diff 对应。双条件全过 → 精确 sha refspec（handoff 本身，
        ag-852c8f：推送的即验证过的内容，探测→push 窗口对协议外写手
        免疫）＋force-with-lease 锚定 base 落地并记 align-rewrite 一行；
        任一不过 → False（维持 non_ff 轮转现状链，绝不 force）。落地走
        push_force_classified（ag-cad1d6）：确定性 force 拒绝抛
        ForcePushRejected 由调用方分类路由（轮转/终局），传输病/未命中
        形态向上抛——poll 层按传输病有界重试。"""
        branch = gitop.fix_branch(iid)
        if gitop.local_branch_tip(self._repo_path, branch) != handoff:
            self._journal(f"agora-monitor: issue#{iid} local {branch} "
                          "drifted off handoff; rotating, no force")
            return False
        ok, reason = gitop.rebase_safe_to_force(
            self._repo_path, branch, base, handoff)
        if not ok:
            self._journal(f"agora-monitor: issue#{iid} rewrite unsafe "
                          f"({reason}); rotating handoff, no force")
            return False
        # ag-852c8f（High-2）：精确 sha refspec——推 handoff 本身而非分支
        # 当前位置，探测→push 窗口内协议外写手移动共享分支无法改变出门
        # 内容（推送的即验证过的）；写手动远端则 lease（期望值＝探测时
        # 远端 tip base）失配，push 失败进分类路径，绝不产假 ack 成功
        gitop.push_force_classified(
            self._repo_path, self._remote_url,
            f"{handoff}:refs/heads/{branch}",
            self._ssl_verify, force_with_lease=f"{branch}:{base}")
        self._journal(f"agora-monitor: issue#{iid} align-rewrite landed "
                      f"via force-with-lease ({handoff[:12]} over "
                      f"{base[:12]})")
        return True

    def _ack(self, iid, tid, outcome, commit, base=None, reason=""):
        """task_deliver 三态确认；ack 被拒/超时先重读 board 对账（可能已
        收口或已换 handoff），不盲目重复交付。"""
        attempted = commit if outcome == "delivered" else None
        try:
            self._client.deliver(tid, outcome, commit, base=base,
                                 reason=reason)
        except AgoraRejected as exc:
            # 服务端 403 契约：stale/late 确认（在案 handoff 已换）或状态
            # 已不可确认——零状态改写，按最新 board 事实处置
            self._journal(f"agora-monitor: issue#{iid} deliver rejected "
                          f"({exc}); reconciling with board")
            self._reconcile_after_ack(iid, tid, attempted)
            return
        except (AgoraUnavailable, AgoraProtocolError) as exc:
            self._journal(f"agora-monitor: issue#{iid} deliver ack failed "
                          f"({exc}); reconciling with board")
            self._reconcile_after_ack(iid, tid, attempted)
            return
        if outcome == "delivered":
            self._confirm_delivered(iid, tid, commit)
        elif outcome == "failed":
            self.release(iid, tid)
        # non_ff：等门体重排重验出新 handoff（同一任务循环），名额保持占用

    def _confirm_delivered(self, iid, tid, commit):
        """delivered ack 成功后的收口核对：写账本前重读 board（旧 commit
        永不覆盖新交付），一致或已收口才落 delivery_commit、回评汇报并
        释放名额。"""
        task = self._board_task(tid)
        delivery = task.get("delivery") or {}
        if task.get("state") in _TERMINAL_STATES \
                or delivery.get("status") == "delivered":
            if delivery.get("commit") in (None, commit):
                self._record_delivery_commit(iid, commit)
            # ack 成功即服务端收口事实（board 权威，此处免再读一轮）；
            # board 快照若已给出终态则照抄（ocr R1 #4：merged 不得记成
            # closed），报告 commit 提示以 board 在案为准。
            board_state = task.get("state")
            status = f"{board_state}/delivered" \
                if board_state in _TERMINAL_STATES else "closed/delivered"
            self._sync_agora_status(iid, status)
            self._report_comment(iid, tid, delivery.get("commit") or commit)
            self.release(iid, tid)
            return
        self._journal(f"agora-monitor: issue#{iid} board moved after "
                      f"delivered ack ({task.get('state')}/"
                      f"{delivery.get('status')}); delivery_commit not "
                      "recorded")

    def _reconcile_after_ack(self, iid, tid, attempted=None):
        """ack 被拒/超时后的对账：以重读的 board 为准处置，本轮绝不重复
        交付（未收口的 awaiting_external 留给下一轮 poll 天然重试）。"""
        task = self._board_task(tid)
        task_state = task.get("state")
        delivery = task.get("delivery") or {}
        self._sync_agora_status(iid, self._status_text(task_state, delivery))
        if task_state in _TERMINAL_STATES:
            if attempted and delivery.get("commit") in (None, attempted):
                self._record_delivery_commit(iid, attempted)
            self._finish_terminal(iid, tid, task_state, delivery)
            return
        if task_state == "blocked" or delivery.get("status") == "failed":
            # 同 _reconcile_task 的 blocked 分支口径：未进交付段按实况记
            # blocked pre-delivery，只有 delivery.failed 才是 push_failed
            if delivery.get("status") == "failed":
                self._journal(f"agora-monitor: issue#{iid} push_failed "
                              f"({delivery.get('error')}); blocked, manual "
                              "follow-up, no auto-redispatch")
            else:
                self._journal(f"agora-monitor: issue#{iid} task {tid} "
                              "blocked pre-delivery (no push attempted); "
                              "manual follow-up, no auto-redispatch")
            self._finish_blocked(iid, tid, delivery)
            return
        if delivery.get("status") == "delivered" and attempted \
                and delivery.get("commit") == attempted:
            self._record_delivery_commit(iid, attempted)
            # ocr R1 #3：board 确认交付必须回评报告——release 后无人再
            # 轮询，静默释放会让汇报丢到重启。
            self._report_comment(iid, tid, attempted)
            self.release(iid, tid)
            return
        # 仍 awaiting_external（同 commit＝未处理，异 commit＝已轮转）或
        # pending/integrating：下一轮 poll 重试
        self._journal(f"agora-monitor: issue#{iid} board shows "
                      f"{task_state}/{delivery.get('status')} after ack "
                      "failure; retry next poll")

    # ---- board / 账本 ----

    def _board_task(self, tid) -> dict:
        board = self._client.board(tid)
        task = board.get("task") if isinstance(board, dict) else None
        if not isinstance(task, dict):
            raise AgoraProtocolError(
                f"/board -> task row missing for {tid}")
        return task

    def _record_delivery_commit(self, iid, commit):
        """delivery_commit 落账（首个写方＝本模块；agora_status 归 Task 5）。
        调用方保证已按 board 核对过本次 commit。"""
        if self._ledger_lock is None or self._state_path is None:
            return
        with self._ledger_lock:
            ledger = state.load_state(self._state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None:
                self._journal(f"agora-monitor: issue#{iid} missing from "
                              "ledger; delivery_commit not recorded")
                return
            if entry.get("delivery_commit") == commit:
                return
            entry["delivery_commit"] = commit
            state.save_state(self._state_path, ledger)

    # ---- mailbox 汇报管线（Task 5）----

    def _mail_path(self):
        return os.fspath(self._state_path) + ".agora-mail.json"

    def _reports_path(self):
        return os.fspath(self._state_path) + ".agora-reports.json"

    def _reports_load(self) -> dict:
        try:
            return json.loads(
                Path(self._reports_path()).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {"reports": {}}

    def _drain_mail(self):
        """check_mail 汇报管线：服务端「拉取即送达（无 ack）」——整批先
        原子落盘再按 tid 分发处理；pending 逐条推进（处理完成才标记）；
        重启先重放未处理批次（读后崩溃不丢消息、不吞其他任务消息）。"""
        if self._state_path is None:
            return
        self._questions_this_drain = 0  # 每轮至多一个 LLM 答复（终闸 B-4）
        replay = self._mail_pending()
        if replay:
            self._journal(f"agora-monitor: replaying {len(replay)} "
                          "unprocessed mail message(s)")
            self._process_batch(replay)
        try:
            batch = self._client.check_mail()
        except (AgoraUnavailable, AgoraRejected, AgoraProtocolError) as exc:
            self._journal(f"agora-monitor: check_mail failed, retry next "
                          f"poll: {exc}")
            return
        if batch:
            state.write_json_atomic(self._mail_path(),
                                    {"pending": list(batch)})
            self._process_batch(batch)

    def _mail_pending(self) -> list:
        try:
            raw = json.loads(
                Path(self._mail_path()).read_text(encoding="utf-8"))
        except OSError:
            return []
        except ValueError:
            self._journal("agora-monitor: mail store unreadable; "
                          "replay batch dropped")
            return []
        pending = raw.get("pending") if isinstance(raw, dict) else None
        if not isinstance(pending, list):
            return []
        return [m for m in pending if isinstance(m, dict)]

    def _process_batch(self, messages):
        """逐条处理并逐条推进 pending 标记：瞬态失败（_MailDeferred）停
        止推进留待重放；确定性异常只 journal 不吞批次（毒消息不得无限
        重放，也不得拖垮同批其他任务的消息）。每轮 drain 至多答复一个
        question（终闸 B-4：封顶 monitor 线程持 ledger_lock 跨 LLM 的
        停摆窗，其余顺延下一轮）。"""
        remaining = list(messages)
        for message in messages:
            if isinstance(message, dict) \
                    and message.get("kind") == "question" \
                    and self._questions_this_drain >= 1:
                return  # 本轮答复预算已用：其余 question 留 pending 重放
            if not isinstance(message, dict):
                # ocr R1 #5：非对象毒元素在此丢弃——except 处理器自身的
                # message.get 再抛会中断整批一轮
                self._journal(f"agora-monitor: non-object mail message "
                              f"dropped: {message!r}")
            else:
                try:
                    self._process_message(message)
                except _MailDeferred as exc:
                    self._journal(f"agora-monitor: mail deferred, batch "
                                  f"replays next poll: {exc}")
                    return  # 不推进 remaining：本条＋后续都留 pending
                except Exception as exc:
                    self._journal(f"agora-monitor: mail message "
                                  f"{message.get('id')!r} processing failed: "
                                  f"{exc!r}")
                else:
                    if message.get("kind") == "question":
                        self._questions_this_drain += 1
            remaining = remaining[1:]
            state.write_json_atomic(self._mail_path(),
                                    {"pending": remaining})

    def _process_message(self, message):
        tid = message.get("task")
        kind = message.get("kind")
        body = message.get("body")
        if not isinstance(tid, str) or not isinstance(kind, str) \
                or not isinstance(body, str):
            self._journal(f"agora-monitor: malformed mail message "
                          f"{message.get('id')!r} dropped")
            return
        iid = self._iid_for_task(tid)
        if iid is None:
            # ag-ee0c29 缺口 2（#182）：账本无属主（tid 已被新单覆盖的孤儿
            # 窗口）不得无声 dropped——warning 含 tid/kind，并按报告库历史
            # tid 给归属提示，供人工追认。
            self._journal(f"agora-monitor: warning: mail for unknown task "
                          f"{tid} (kind={kind}) dropped; "
                          f"{self._mail_owner_hint(tid)}")
            return
        if kind == "note":
            self._store_report_note(iid, tid, message.get("id"), body)
        elif kind == "question":
            self._handle_worker_question(iid, tid, body)
        elif kind == "answer":
            self._comment(iid, f"worker 答复（任务 {tid}）：\n{body}")
        elif kind == "result":
            self._handle_result_mail(iid, tid, body)
        else:
            # done/verify 等状态类消息：board 是唯一权威（不以
            # worker 自述为准），只留痕
            self._journal(f"agora-monitor: mail kind={kind} for {tid} "
                          "noted (board is authoritative)")

    def _iid_for_task(self, tid):
        """tid -> iid：先查登记簿（活动任务），再按账本登记过的
        agora_task_id 反查（名额已释放的终态收口窗口）。"""
        with self._guard:
            for iid, active in self._active.items():
                if active == tid:
                    return iid
        if self._ledger_lock is None or self._state_path is None:
            return None
        with self._ledger_lock:
            ledger = state.load_state(self._state_path, warn=self._journal)
            for key, entry in ledger["issues"].items():
                if entry.get("agora_task_id") == tid:
                    return int(key)
        return None

    def _mail_owner_hint(self, tid) -> str:
        """unknown-task 邮件归属提示（ag-ee0c29）：按报告库历史 tid 反查
        可能属主 iid——账本只留在录 tid，被新单覆盖后唯一线索是历史报告。"""
        record = self._reports_load().get("reports", {}).get(str(tid)) or {}
        iid = record.get("iid")
        if iid is not None:
            return f"possibly issue#{iid} (historical task); manual follow-up"
        return "no historical attribution; manual follow-up"

    def _store_report_note(self, iid, tid, mid, body):
        """worker note 持久化为报告候选（最新 note_id 胜；重放旧批次不
        回退）。终态回评与重发汇报共用此库。ag-1c7315（B-F1）：写段持
        写者互斥锁——与处置侧 redispatched 清标互斥。"""
        with self._store_lock:
            store = self._reports_load()
            reports = store.setdefault("reports", {})
            current = reports.get(tid) or {}
            current_id = current.get("note_id")
            if current_id is not None and mid is not None \
                    and int(mid) <= int(current_id):
                return  # 已有更新的报告（或同条重放）
            reports[tid] = {"iid": int(iid), "note": body,
                            "note_id": int(mid) if mid is not None else -1,
                            "finalized": bool(current.get("finalized")),
                            "sent_marks": list(current.get("sent_marks") or [])}
            try:
                state.write_json_atomic(self._reports_path(), store)
            except OSError as exc:
                # 终闸 B-3：落盘瞬态失败≠毒消息——留 pending 下一轮重放，
                # 不让报告永久丢失成降级评论
                raise _MailDeferred(f"reports store write failed: {exc}") from exc

    def _handle_worker_question(self, iid, tid, body):
        """worker 提问 → 答复回调（orchestrator 在 ledger_lock 内交
        Router，T3 锁序）→ post(answer)。回调故障给事实型兜底答复，不
        虚构、不静默吞问题（worker 不能无限等待）。"""
        if self._answer_question is None:
            self._journal(f"agora-monitor: worker question for {tid} "
                          "dropped (no conversation face)")
            return
        answer = None
        try:
            answer = self._answer_question(iid, tid, body)
        except Exception as exc:
            self._journal(f"agora-monitor: answer callback failed for "
                          f"{tid}: {exc!r}")
        if not answer:
            answer = (f"自动化答复暂不可用，需要人工在 Gitea issue#{iid} "
                      f"跟进。worker 原问题（任务 {tid}）：{body}")
        try:
            self._client.post(tid, "answer", answer)
        except (AgoraUnavailable, AgoraProtocolError) as exc:
            # 终闸 B-2：瞬断不吞——消息留 pending 下一轮重放重试（worker
            # 提问无服务端重投通道，丢了就是永久等待）
            raise _MailDeferred(f"answer post transient for {tid}: {exc}") \
                from exc
        except AgoraRejected as exc:
            self._journal(f"agora-monitor: answer post rejected for {tid}, "
                          f"dropped (deterministic): {exc}")

    def _handle_result_mail(self, iid, tid, body):
        """生命周期迁移回传（agorad notify_transition：终态与 blocked 实时
        kind=result 直投 dispatcher）。blocked/failed 释放名额后的迁移
        （如 root 取消 blocked 任务）轮询面已不可见——本邮件是唯一信号
        （ag-8d6a70：2026-11-14 三单取消后面板停在求助/失败旧评论）。board
        为权威：重读 board 确认终态才回评终局文案（不以邮件正文自述为准；
        _comment_once 按 mark 幂等，轮询路径已回评过则零动作）。ag-1c7315
        （评审 R2）：board 读→release 整段持 per-issue 对账串行锁——迟到的
        终态收口不得删掉「重试」刚重登记的同 tid 名额（B-F2 邮件面）。"""
        self._journal(f"agora-monitor: result mail for {tid}: {body}")
        with self._iid_reconcile_lock(iid):
            self._handle_result_mail_inner(iid, tid, body)

    def _handle_result_mail_inner(self, iid, tid, body):
        try:
            task = self._board_task(tid)
        except AgoraRejected as exc:
            # 确定性 4xx（任务已不在服务面）：毒邮件口径，消费不重放
            self._journal(f"agora-monitor: result mail reconcile rejected "
                          f"for {tid}, dropped (deterministic): {exc}")
            return
        except (AgoraUnavailable, AgoraProtocolError) as exc:
            # 瞬断≠毒：留 pending 下一轮重放（取消事实不因一次网络病丢失）
            raise _MailDeferred(f"result mail board reconcile transient "
                                f"for {tid}: {exc}") from exc
        task_state = task.get("state")
        delivery = task.get("delivery") or {}
        if task_state not in _TERMINAL_STATES:
            return  # 非 terminal（如 blocked 迁移本身）：轮询面已覆盖
        try:
            self._sync_agora_status(iid, self._status_text(task_state,
                                                           delivery))
            self._finish_terminal(iid, tid, task_state, delivery)
        except OSError as exc:
            # 终闸 B-3 同口径：落盘瞬态≠毒消息——留 pending 重放（按 mark
            # 幂等，重投不刷屏），取消事实不因一次磁盘病丢失
            raise _MailDeferred(f"result mail finalize store failed for "
                                f"{tid}: {exc}") from exc

    # ---- 心跳（Task 5：agora 回合报 board 态；legacy 心跳不变）----

    def _heartbeat_tick(self, now=None):
        """每个 poll 轮次收尾扫一次：间隔已到的活动任务发一条心跳评论
        （显示 board state/delivery.status）。零注册零网络。"""
        if not self._hb_interval or self._post_note is None:
            return
        now = time.monotonic() if now is None else now
        with self._guard:
            due = []
            for iid, hb in self._hb.items():
                if not hb["status"] or now - hb["last"] < self._hb_interval:
                    continue
                hb["last"] = now
                minutes = int((now - hb["start"]) // 60)
                due.append((iid, f"仍在进行：已运行 {minutes} 分钟，"
                                 f"agora 任务 {self._active.get(iid)} "
                                 f"状态 {hb['status']}"))
        for iid, text in due:
            self._comment(iid, text)

    # ---- 线程生命周期 ----

    def start(self) -> None:
        if self._thread is not None:
            return
        self._recover_from_ledger()
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="issue-agent-agora-monitor", daemon=True)
        self._thread.start()

    def _recover_from_ledger(self):
        """daemon 启动恢复（计划 Task 5）：从账本恢复全部未终态
        agora_task_id（幂等 register，重复 register 覆盖无害）。终态判定
        以账本 agora_status 头段为准；从未同步过的按未终态恢复（安全
        方向——poll 首轮以 board 对账收口多余登记）。恢复本身零网络。"""
        if self._state_path is None or self._ledger_lock is None:
            return
        with self._ledger_lock:
            ledger = state.load_state(self._state_path, warn=self._journal)
        recovered = []
        for key, entry in ledger["issues"].items():
            tid = entry.get("agora_task_id")
            if not tid:
                continue
            head = (entry.get("agora_status") or "").split("/", 1)[0]
            if head in _TERMINAL_STATES:
                continue
            self.register(key, tid)
            recovered.append(f"issue#{key}={tid}")
        if recovered:
            self._journal("agora-monitor: recovered from ledger: "
                          + ", ".join(recovered))

    def stop(self) -> None:
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None:
            thread.join(timeout=5)

    def _loop(self):
        """按客户端轮询间隔空转 poll_once；线程永不死（异常只 journal）。"""
        while not self._stop.wait(self._client.poll_interval()):
            try:
                self.poll_once()
            except Exception as exc:  # 监视线程必须跨异常存活
                self._journal(f"agora-monitor: poll failed: {exc!r}")
