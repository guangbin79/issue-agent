"""agora 任务监视器（计划 Task 3 骨架＋Task 4 外部交付回路）——单后台线程
轮询本实例登记的活动任务（设计 §9.2/§6.2/§7.4）。

Task 4 接通网络面：poll_once 只处理本实例登记任务（iid→tid），按
board(task) 的 state/delivery 驱动外部交付状态机——done＋
awaiting_external → push_verified_commit 精确推送 → task_deliver；
non_ff → fetch 实际远端 tip 轮转 handoff（同一任务循环，不做本地
rebase）；blocked/failed → 记 push_failed 不自动重派不弹回 worker（汇报
归 Task 5）。每次写账本前重读 board 对账，旧 commit 永不覆盖新交付；
stale/late 交付确认按服务端 403 契约（AgoraRejected）处理，绝不盲目
重复交付。delivery_commit 的首个写方＝本模块（agora_status 归 Task 5）。

活动名额（max_active，接线 config.max_parallel）改原子 check-and-reserve
（F3，修 check-then-act 竞态：max_parallel≥2 并发回合不得双双突破
max_active）：回合侧 reserve 占位、register 转正、终态 release 归还；
_run_turn 返回不释放——释放权只在本状态机（终态）与回合失败出口。

agora 任务自身的 origin push 只经 push_verified_commit（精确 SHA、绝不
force、绝不动默认支）；agora 不推 issue 仓远端——外部交付权威在
issue-agent。
"""

import re
import threading

import gitop
import state
from agora_client import AgoraProtocolError, AgoraRejected, AgoraUnavailable

_HEX40_OK = re.compile(r"[0-9a-f]{40}")

# board 主状态（task 行 state）：这三个对本监视器是终态（名额可释放）
_TERMINAL_STATES = ("merged", "closed", "cancelled")


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
            self._active[int(iid)] = str(tid)

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

    def tasks(self) -> list:
        """[(iid, tid|None)] 快照（升序）；tid=None 是 reserve 占位（回合
        在途）。测试与 Task 5 恢复面共用。"""
        with self._guard:
            return sorted(self._active.items())

    # ---- 轮询与交付状态机 ----

    def poll_once(self):
        """一轮状态 reconciliation：只处理本实例登记的任务；单任务异常
        只 journal，不拖垮同轮其他任务（线程跨异常存活）。"""
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

    def _reconcile_task(self, iid, tid):
        task = self._board_task(tid)
        task_state = task.get("state")
        delivery = task.get("delivery") or {}
        if task_state in _TERMINAL_STATES:
            self._journal(f"agora-monitor: issue#{iid} task {tid} "
                          f"{task_state}; slot released")
            self.release(iid, tid)
            return
        if task_state == "blocked" or delivery.get("status") == "failed":
            # 交付失败终局（rebase_conflict/push_failed/retry_exhausted…，
            # 设计 §7.4/§8.2）：记 push_failed 不自动重派、不弹回 worker；
            # 人工跟进，汇报归 Task 5
            self._journal(
                f"agora-monitor: issue#{iid} task {tid} push_failed "
                f"({delivery.get('error')}); blocked, manual follow-up, "
                "no auto-redispatch")
            self.release(iid, tid)
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
                self._deliver_handoff(iid, tid, handoff)
            elif delivery.get("status") == "delivered":
                # delivered ack 后收口读竞态/瞬断的恢复面（评审环 ocr #4）：
                # 本次 board 读即确认——补记 delivery_commit 并释放
                commit = delivery.get("commit")
                if isinstance(commit, str) and _HEX40_OK.fullmatch(commit):
                    self._record_delivery_commit(iid, commit)
                self.release(iid, tid)
        # 其余（pending/integrating 与未到 done 的运行态）：本轮无动作

    def _deliver_handoff(self, iid, tid, handoff):
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
            # 远端已前进：取实际 tip 轮转 handoff——同一任务循环，等门体
            # 换基重验出新 handoff 再推，不做本地 rebase
            base = gitop.fetch_fix_branch_tip(
                self._repo_path, iid, self._remote_url, self._ssl_verify)
            self._ack(iid, tid, "non_ff", handoff, base=base)
        else:  # failed：确定性失败，重试同 commit 永不可能成功——终局上报
            self._journal(f"agora-monitor: issue#{iid} deterministic push "
                          f"failure for {handoff[:12]}; reporting failed")
            self._ack(iid, tid, "failed", handoff, reason="push_failed")

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
        永不覆盖新交付），一致或已收口才落 delivery_commit 并释放名额。"""
        task = self._board_task(tid)
        delivery = task.get("delivery") or {}
        if task.get("state") in _TERMINAL_STATES \
                or delivery.get("status") == "delivered":
            if delivery.get("commit") in (None, commit):
                self._record_delivery_commit(iid, commit)
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
        if task_state in _TERMINAL_STATES:
            if attempted and delivery.get("commit") in (None, attempted):
                self._record_delivery_commit(iid, attempted)
            self.release(iid, tid)
            return
        if task_state == "blocked" or delivery.get("status") == "failed":
            self._journal(f"agora-monitor: issue#{iid} push_failed "
                          f"({delivery.get('error')}); blocked, manual "
                          "follow-up, no auto-redispatch")
            self.release(iid, tid)
            return
        if delivery.get("status") == "delivered" and attempted \
                and delivery.get("commit") == attempted:
            self._record_delivery_commit(iid, attempted)
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

    # ---- 线程生命周期 ----

    def start(self) -> None:
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="issue-agent-agora-monitor", daemon=True)
        self._thread.start()

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
