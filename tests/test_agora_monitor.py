"""AgoraMonitor 测试（计划 Task 3 骨架＋Task 4 外部交付回路）。

Task 3 面：登记簿幂等/容量/释放、start/stop 轮询生命周期。
Task 4 面：poll_once 外部交付状态机——board(task) 驱动 awaiting_external
→push_verified_commit 精确推送→task_deliver；non_ff 轮转 handoff；blocked/
failed 记 push_failed 不自动重派；ack 超时/被拒先 board 对账不盲目重发；
F3 原子 check-and-reserve。终态汇报归 Task 5。

FakeAgoraClient 镜像真实 wire 形状（坑位④）：board(tid) 返回单任务 detail
（task 行含 state/delivery）；deliver 按服务端 F101 契约——stale commit 拒
（AgoraRejected）、delivered→closed、failed→blocked、non_ff→pending 轮转。

Run: python3 -m pytest tests/test_agora_monitor.py -q
"""

import json
import threading
import time

import pytest

import gitop
import state
from agora_client import AgoraRejected, AgoraUnavailable
from agora_monitor import AgoraMonitor


class FakeClient:
    """最小客户端替身：只有轮询间隔可问（构造与轮询零网络）。"""

    def __init__(self, interval=15):
        self.interval = interval
        self.calls = []

    def poll_interval(self):
        self.calls.append("poll_interval")
        return float(self.interval)


def make_monitor(max_active=1, interval=15, journal=None):
    return AgoraMonitor(client=FakeClient(interval), journal=journal,
                        max_active=max_active)


def wait_until(condition, timeout=3.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return condition()


# --- 登记簿 -------------------------------------------------------------------


def test_register_is_idempotent_per_issue_and_tracks_latest_tid():
    """register 幂等：同 iid 重登记覆盖为最新 tid（一轮只跟踪一个任务，
    回合链推进时旧 tid 自然让位），重复登记不产生第二行。"""
    monitor = make_monitor()
    monitor.register(7, "ag-123")
    monitor.register(7, "ag-123")
    assert monitor.tasks() == [(7, "ag-123")]
    monitor.register(7, "ag-456")
    assert monitor.tasks() == [(7, "ag-456")]


def test_reserve_occupies_slot_until_terminal_release():
    """活动名额到任务终态才释放：reserve 占名额（占位 tid=None）、release
    归还；满员原子拒绝；同 iid 重入＝复用己占名额（中断恢复路径）。"""
    monitor = make_monitor(max_active=2)
    assert monitor.reserve(7)
    assert monitor.reserve(8)
    assert not monitor.reserve(9), "满员 reserve 必须拒绝"
    assert monitor.tasks() == [(7, None), (8, None)], "占位登记，tid 待 register"
    assert monitor.reserve(7), "同 iid 重入复用己占名额"
    monitor.register(7, "ag-1")  # 占位转正
    monitor.release(7, "ag-1")  # 任务到终态（compare-and-release）
    assert monitor.reserve(9)
    monitor.release(9)  # 占位式释放（回合失败出口）＋幂等
    monitor.release(9)


def test_reserve_is_atomic_two_concurrent_turns_one_slot():
    """F3：check-then-act 竞态修复——max_parallel≥2 并发回合恰一个通过。"""
    for _ in range(20):  # 竞态窗口极小，重复放大确定性
        monitor = AgoraMonitor(client=FakeClient(), max_active=1)
        barrier = threading.Barrier(2)
        outcomes = []

        def contender(iid):
            barrier.wait()
            outcomes.append(monitor.reserve(iid))

        threads = [threading.Thread(target=contender, args=(iid,))
                   for iid in (7, 8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert outcomes.count(True) == 1, outcomes


# --- poll_once -----------------------------------------------------------------


def test_poll_once_without_registered_tasks_is_zero_network():
    """poll 只处理本实例登记任务：登记簿空转零客户端调用。"""
    monitor = make_monitor()
    assert monitor.poll_once() is None
    assert monitor._client.calls == []


def test_poll_once_skips_reserved_placeholder_without_tid():
    """reserve 占位（回合在途、tid 未 register）不触网络——无 tid 可查。"""
    monitor = make_monitor()
    assert monitor.reserve(7)
    monitor.poll_once()
    assert monitor._client.calls == []


# --- start/stop 生命周期 --------------------------------------------------------


class CountingMonitor(AgoraMonitor):
    polled = 0

    def poll_once(self):
        self.polled += 1


def test_start_runs_background_polling_and_stop_joins():
    monitor = CountingMonitor(client=FakeClient(interval=1),
                              max_active=1)
    monitor.start()
    try:
        assert wait_until(lambda: monitor.polled >= 1), "轮询线程必须真的在转"
    finally:
        monitor.stop()
    polled_at_stop = monitor.polled
    time.sleep(0.2)
    assert monitor.polled == polled_at_stop, "stop 后线程必须退出"
    monitor.start()  # 重启安全
    monitor.stop()
    assert monitor._thread is None


def test_poll_thread_survives_exceptions_and_journals():
    journal = []

    class BoomMonitor(AgoraMonitor):
        def poll_once(self):
            raise RuntimeError("boom")

    monitor = BoomMonitor(client=FakeClient(interval=1), journal=journal.append,
                          max_active=1)
    monitor.start()
    try:
        assert wait_until(lambda: any("boom" in m for m in journal)), \
            "异常必须进 journal 而非杀死线程"
        first = len(journal)
        assert wait_until(lambda: len(journal) > first), \
            "线程必须跨异常继续轮询"
    finally:
        monitor.stop()


# --- Task 4 外部交付回路 ---------------------------------------------------------


TID = "ag-77"
PAT_URL = "https://oauth2:SECRETPAT123@gitea.invalid/group/repo.git"


def hex40(ch):
    return ch * 40


class FakeAgoraClient:
    """交付面替身（镜像真实 wire 形状）：board 单任务 detail；deliver 按
    服务端契约——stale/late commit 拒（AgoraRejected）、delivered→
    delivery.delivered＋closed、failed→delivery.failed＋blocked、non_ff→
    pending 轮转（gate_requeue 模拟门体按 base 重排重验出新 handoff）。

    ack_mode：""＝正常；"lost"＝服务端已处理但回包丢失（先落账再抛
    AgoraUnavailable）；"down"＝未处理即抛（纯超时）。"""

    def __init__(self, tid=TID, commit=None):
        commit = commit or hex40("a")
        self.tasks = {tid: {
            "state": "done",
            "delivery": {"mode": "external_branch",
                         "status": "awaiting_external", "commit": commit,
                         "base": None, "attempt": 0, "error": None,
                         "target_branch": "issue/7-fix",
                         "local_status": "pending"}}}
        self.board_calls = []
        self.deliver_calls = []
        self.ack_mode = ""

    def board(self, tid):
        self.board_calls.append(tid)
        return {"task": json.loads(json.dumps(self.tasks[tid])),
                "events": [], "done_body": None, "classification": "ok"}

    def deliver(self, tid, outcome, commit, base=None, reason=""):
        self.deliver_calls.append({"task": tid, "outcome": outcome,
                                   "commit": commit, "base": base,
                                   "reason": reason})
        t = self.tasks[tid]
        d = t["delivery"]
        if self.ack_mode == "down":
            raise AgoraUnavailable("deliver timeout, unprocessed (fake)")
        if t["state"] != "done" or d.get("status") != "awaiting_external":
            raise AgoraRejected(
                f"任务 {tid} 当前 {t['state']}/{d.get('status')}"
                "——仅 done＋awaiting_external 可确认交付")
        if commit != d.get("commit"):
            raise AgoraRejected(
                f"过期 commit：在案 {str(d.get('commit'))[:12]}，"
                f"收到 {str(commit)[:12]}——stale/late 确认拒绝")
        if outcome == "delivered":
            d.update(status="delivered", local_status="not_required")
            t["state"] = "closed"
        elif outcome == "failed":
            d.update(status="failed",
                     error=reason or "external_delivery_failed")
            t["state"] = "blocked"
        else:  # non_ff：携 base 轮转，attempt+1，等门体重排重验
            d.update(status="pending", base=base, commit=None, error=None,
                     attempt=d.get("attempt", 0) + 1)
        if self.ack_mode == "lost":  # 服务端已提交，回包丢失
            raise AgoraUnavailable("deliver ack lost after commit (fake)")
        return {"ok": True, "outcome": outcome}

    def gate_requeue(self, tid, new_commit):
        """模拟门体按 base 重排重验完成：pending→awaiting_external(新 commit)。"""
        d = self.tasks[tid]["delivery"]
        assert d["status"] == "pending"
        d["status"] = "awaiting_external"
        d["commit"] = new_commit


class FakeGitDelivery:
    """gitop 交付原语替身＋legacy 推送面间谍：agora 交付只准走
    push_verified_commit/fetch_fix_branch_tip，legacy push 族调用必须为零。"""

    def __init__(self, monkeypatch, push_results=None, push_error=None,
                 fetch_tip=None):
        self.push_calls = []
        self.fetch_calls = []
        self.legacy_calls = []
        self.push_results = push_results or {}  # commit -> "ok"|"non_ff"|"failed"
        self.push_error = push_error
        self.fetch_tip = fetch_tip or hex40("f")

        def legacy(_name):
            def spy(*args, **kwargs):
                self.legacy_calls.append(_name)
            return spy

        for name in ("push_fix_branch", "push_fix_branch_resilient", "push"):
            monkeypatch.setattr(gitop, name, legacy(name))
        monkeypatch.setattr(gitop, "push_verified_commit",
                            self.fake_push_verified)
        monkeypatch.setattr(gitop, "fetch_fix_branch_tip", self.fake_fetch_tip)

    def fake_push_verified(self, repo, iid, commit, remote_url,
                           ssl_verify=True):
        self.push_calls.append({"repo": repo, "iid": iid, "commit": commit,
                                "remote_url": remote_url,
                                "ssl_verify": ssl_verify})
        if self.push_error is not None:
            raise self.push_error
        return self.push_results.get(commit, "ok")

    def fake_fetch_tip(self, repo, iid, remote_url, ssl_verify=True):
        self.fetch_calls.append({"repo": repo, "iid": iid,
                                 "remote_url": remote_url,
                                 "ssl_verify": ssl_verify})
        return self.fetch_tip


def delivery_monitor(tmp_path, monkeypatch, client, fake_git=None,
                     max_active=2):
    """接线完毕的 monitor＋issue 账本：iid=7 已登记 tid。"""
    if fake_git is None:
        fake_git = FakeGitDelivery(monkeypatch)
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {"7": {"status": "active"}}}))
    journal = []
    monitor = AgoraMonitor(client=client, journal=journal.append,
                           max_active=max_active)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url=PAT_URL, ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())
    tid = next(iter(client.tasks))
    monitor.register(7, tid)
    return monitor, state_path, journal


def entry(state_path, iid=7):
    return state.load_state(state_path)["issues"][str(iid)]


def test_awaiting_external_pushes_exact_commit_only_to_issue_branch(
        tmp_path, monkeypatch):
    """计划锚例：done＋awaiting_external → 只把在案 commit 精确推给
    issue/<iid>-fix（refspec 形态与默认分支不动由 test_gitop 锁定），
    delivered ack.commit==delivery_commit，且 delivery_commit 落账、名额
    释放、legacy push 族调用为零。"""
    delivery_commit = hex40("a")
    client = FakeAgoraClient(commit=delivery_commit)
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    assert fake_git.push_calls == [
        {"repo": monitor._repo_path, "iid": 7, "commit": delivery_commit,
         "remote_url": PAT_URL, "ssl_verify": True}]
    delivered_ack = client.deliver_calls[0]
    assert delivered_ack["outcome"] == "delivered"
    assert delivered_ack["commit"] == delivery_commit
    assert entry(state_path)["delivery_commit"] == delivery_commit
    assert monitor.tasks() == [], "delivered 终局释放名额"
    assert fake_git.legacy_calls == [], "agora origin push 调用为零"
    assert not any("SECRETPAT123" in m for m in journal), "PAT 不进日志"


def test_non_ff_rotates_delivery_commit_and_rejects_stale_ack(
        tmp_path, monkeypatch):
    """计划锚例：non_ff → fetch 实际远端 tip → task_deliver(non_ff,
    commit=当前 handoff, base=tip)；门体换基重验出新 handoff 后二次推送
    commit==新 delivery commit；旧 commit 的 deliver 被拒（stale 契约）。"""
    old = hex40("a")
    fetched_remote_tip = hex40("b")
    new = hex40("c")
    client = FakeAgoraClient(commit=old)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={old: "non_ff", new: "ok"},
        fetch_tip=fetched_remote_tip)
    monitor, state_path, _ = delivery_monitor(tmp_path, monkeypatch, client,
                                              fake_git)

    monitor.poll_once()
    non_ff_ack = client.deliver_calls[0]
    assert non_ff_ack["outcome"] == "non_ff"
    assert non_ff_ack["base"] == fetched_remote_tip
    assert non_ff_ack["commit"] == old
    assert entry(state_path)["delivery_commit"] is None, "未交付不落账"
    assert monitor.tasks() == [(7, TID)], "non_ff 轮转中名额保持占用"

    client.gate_requeue(TID, new)  # 门体按 base 重排重验出新 handoff
    monitor.poll_once()
    assert fake_git.push_calls[1]["commit"] == new, "二次推送新 delivery commit"
    assert client.deliver_calls[1] == {
        "task": TID, "outcome": "delivered", "commit": new,
        "base": None, "reason": ""}
    assert entry(state_path)["delivery_commit"] == new

    with pytest.raises(AgoraRejected):
        client.deliver(TID, "delivered", old)  # 旧 commit 迟到确认被拒


def test_blocked_delivery_records_push_failed_without_redispatch(
        tmp_path, monkeypatch):
    """计划 L281：rebase_conflict/blocked 记 push_failed，不自动重派、
    不推送、不 deliver——名额释放，人工跟进（汇报归 Task 5）。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    client.tasks[TID]["delivery"].update(status="failed",
                                         error="rebase_conflict")
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, _, journal = delivery_monitor(tmp_path, monkeypatch, client,
                                           fake_git)

    monitor.poll_once()

    assert fake_git.push_calls == [] and client.deliver_calls == []
    assert monitor.tasks() == [], "blocked 终局释放名额"
    assert any("push_failed" in m and "rebase_conflict" in m
               for m in journal), journal


def test_deterministic_push_failure_delivers_failed_outcome(
        tmp_path, monkeypatch):
    """其他 push 错误（确定性失败）回传 task_deliver(outcome=failed,
    reason 机器可读) 终局：不自动重派、不弹回 worker；delivery_commit 不落账。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    fake_git = FakeGitDelivery(monkeypatch, push_results={commit: "failed"})
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client, fake_git)

    monitor.poll_once()

    assert client.deliver_calls[0]["outcome"] == "failed"
    assert client.deliver_calls[0]["reason"], "reason 必须机器可读非空"
    assert client.tasks[TID]["state"] == "blocked"
    assert entry(state_path)["delivery_commit"] is None
    assert monitor.tasks() == []
    assert not any("SECRETPAT123" in m for m in journal)


def test_push_transport_error_keeps_awaiting_external_for_retry(
        tmp_path, monkeypatch):
    """网络病保留 awaiting_external 有界重试（poll 轮次天然退避）：git
    传输错误不 deliver、不释放；下一轮推送成功照常交付。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    fake_git = FakeGitDelivery(
        monkeypatch, push_error=gitop.GitopError(
            "push failed: fatal: unable to access '***' (fake)"))
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client, fake_git)

    monitor.poll_once()
    assert client.deliver_calls == []
    assert client.tasks[TID]["delivery"]["status"] == "awaiting_external"
    assert monitor.tasks() == [(7, TID)], "网络病不释放名额"

    fake_git.push_error = None
    monitor.poll_once()
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert entry(state_path)["delivery_commit"] == commit
    assert monitor.tasks() == []
    assert not any("SECRETPAT123" in m for m in journal)


def test_deliver_ack_lost_reconciles_via_board_without_repeat_delivery(
        tmp_path, monkeypatch):
    """push 成功但 deliver ack 超时（服务端已处理、回包丢失）：先重读
    board 对账——已收口则落账释放，绝不盲目重复交付。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    client.ack_mode = "lost"
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, _ = delivery_monitor(tmp_path, monkeypatch, client,
                                              fake_git)

    monitor.poll_once()

    assert len(client.deliver_calls) == 1, "对账后不得重复 deliver"
    assert entry(state_path)["delivery_commit"] == commit
    assert monitor.tasks() == []


def test_deliver_ack_unprocessed_keeps_task_for_next_poll(
        tmp_path, monkeypatch):
    """ack 超时且服务端未处理：board 仍 awaiting_external 同 commit——本
    轮不重发，下一轮 poll 天然重试并交付。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    client.ack_mode = "down"
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, _ = delivery_monitor(tmp_path, monkeypatch, client,
                                              fake_git)

    monitor.poll_once()
    assert len(client.deliver_calls) == 1, "ack 失败当轮不得立即重发"
    assert entry(state_path)["delivery_commit"] is None
    assert monitor.tasks() == [(7, TID)]

    client.ack_mode = ""
    monitor.poll_once()
    assert len(client.deliver_calls) == 2
    assert entry(state_path)["delivery_commit"] == commit
    assert monitor.tasks() == []


def test_stale_deliver_rejection_journals_and_rechecks_board(
        tmp_path, monkeypatch):
    """stale 拒绝按服务端 403 契约处理：deliver 抛 AgoraRejected → 记
    journal＋board 重读对账（本测试构造「已轮转到新 handoff」），本轮不
    再动作，下一轮按新 handoff 推送。"""
    old = hex40("a")
    new = hex40("c")
    client = FakeAgoraClient(commit=old)
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client, fake_git)
    # deliver 一进门就被拒（服务端已换成新 handoff 的竞态模拟）
    original_deliver = client.deliver

    def rejecting_deliver(tid, outcome, commit, base=None, reason=""):
        client.deliver_calls.append({"task": tid, "outcome": outcome,
                                     "commit": commit, "base": base,
                                     "reason": reason})
        raise AgoraRejected("过期 commit (fake race)")

    client.deliver = rejecting_deliver
    monitor.poll_once()
    assert len(client.deliver_calls) == 1
    assert any("过期 commit" in m or "rejected" in m for m in journal), journal

    # board 已轮转到新 handoff：恢复 deliver，下一轮按新 handoff 交付
    client.deliver = original_deliver
    client.tasks[TID]["delivery"]["commit"] = new
    monitor.poll_once()
    assert fake_git.push_calls[-1]["commit"] == new
    assert entry(state_path)["delivery_commit"] == new


def test_release_for_superseded_task_never_evicts_newer_registration():
    """评审环 ocr #5：快照轮询的网络窗内回合链 register 覆盖后，旧任务
    迟到的终态 release 不得逐掉新登记（否则新任务失联＋名额账坏）；单参
    release 只清 reserve 占位，不碰已转正登记。"""
    monitor = make_monitor(max_active=2)
    monitor.register(7, "ag-old")
    monitor.register(7, "ag-new")  # 回合链推进：覆盖为最新 tid
    monitor.release(7, "ag-old")   # 旧任务终态（迟到的快照 release）
    assert monitor.tasks() == [(7, "ag-new")]
    monitor.release(7)             # 占位式 release：已转正登记不受碰
    assert monitor.tasks() == [(7, "ag-new")]
    monitor.release(7, "ag-new")
    assert monitor.tasks() == []
    monitor.release(7, "ag-new")   # 幂等
    assert monitor.tasks() == []


def test_cross_round_recorded_delivery_does_not_block_new_task(
        tmp_path, monkeypatch):
    """评审环 ocr #3：回合1 的 delivery_commit 在案时，回合2 任务（哪怕
    同一 issue 同一账本行）照常交付——「旧 handoff 跳过」闸会把
    max_active=1 的监视器永久卡死（recorded 跨回合不清理、新 handoff 必
    不同）；board handoff 是权威，旧 commit 防线＝服务端 stale 403＋写前
    board 核对。"""
    handoff = hex40("a")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, _ = delivery_monitor(tmp_path, monkeypatch, client,
                                              fake_git)
    with monitor._ledger_lock:
        ledger = state.load_state(state_path)
        issue_row = ledger["issues"]["7"]
        issue_row["delivery_commit"] = hex40("5")  # 回合1 已交付的旧 commit
        issue_row["agora_task_id"] = TID
        state.save_state(state_path, ledger)

    monitor.poll_once()

    assert fake_git.push_calls, "跨回合在案记录不得阻断新交付"
    assert client.deliver_calls[0]["commit"] == handoff
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == []


def test_done_delivered_state_records_and_releases(tmp_path, monkeypatch):
    """评审环 ocr #4：delivered ack 后收口读竞态/瞬断的恢复面——board 已
    done+delivered（非 awaiting_external）时补记 delivery_commit 并释放
    名额，交付事实不因一次收口读失败而丢失（Task 5 消费账本）。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    client.tasks[TID]["delivery"].update(status="delivered",
                                         local_status="not_required")
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, _ = delivery_monitor(tmp_path, monkeypatch, client,
                                              fake_git)

    monitor.poll_once()

    assert fake_git.push_calls == [] and client.deliver_calls == []
    assert entry(state_path)["delivery_commit"] == commit
    assert monitor.tasks() == []


def test_malformed_handoff_is_terminal_local_release(
        tmp_path, monkeypatch):
    """评审环 ocr #6：done+awaiting_external+坏 commit 是确定性协议违约
    ——服务端 hex40 校验使 deliver 无法 ack；本地终局释放（不重派、不无
    限 journal 占名额）。"""
    client = FakeAgoraClient(commit="not-a-sha")
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, _, journal = delivery_monitor(tmp_path, monkeypatch, client,
                                           fake_git)

    monitor.poll_once()

    assert fake_git.push_calls == [] and client.deliver_calls == []
    assert monitor.tasks() == [], "确定性协议违约必须终局释放名额"
    assert any("malformed" in m for m in journal), journal
