"""AgoraMonitor 测试（计划 Task 3 骨架＋Task 4 外部交付回路）。

Task 3 面：登记簿幂等/容量/释放、start/stop 轮询生命周期。
Task 4 面：poll_once 外部交付状态机——board(task) 驱动 awaiting_external
→push_verified_commit 精确推送→task_deliver；non_ff 轮转 handoff；变基轮
护栏（ag-6da714，legacy §4 移植）：旧链零丢失且本地分支即在案 handoff →
force-with-lease 重写落地（align-rewrite），否则不 force 维持轮转；blocked/
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
from pathlib import Path

import pytest

import gitop
import state
from agora_client import AgoraRejected, AgoraUnavailable
import agora_monitor
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
    AgoraUnavailable）；"down"＝未处理即抛（纯超时）。

    Task 5 面：check_mail 一次性吐出 mail 队列（镜像「拉取即送达」——
    返回即清空）；post 记账（answer 直投）；create_task/cancel 记账
    （重启恢复必须零 create，关单语义必须零 cancel）。"""

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
        self.mail = []        # 待投递消息（wire 形状：id/ts/task/kind/from/to/body）
        self.posts = []       # post() 记账
        self.post_failures = []  # post 瞬断注入（未送达不记账）
        self.board_task = {"state": "running"}  # relay 回落前的 board 重读面
        self.create_calls = []   # create_task 记账（恢复面断言零建单）
        self.cancel_calls = []   # task_cancel 记账
        self.task_updates = []   # task_update 记账（ag-fb9e5a 提案执行回路）
        self.task_update_error = None
        self.check_mail_calls = 0
        # ag-da7410 C：僵态阶梯面——task_nudge/dispatch 记账
        self.nudges = []
        self.dispatch_calls = []

    def check_mail(self):
        self.check_mail_calls += 1
        got, self.mail = self.mail, []
        return got

    def poll_interval(self):
        return 15.0

    def board(self, tid):
        return {"task": json.loads(json.dumps(self.board_task)),
                "events": []}

    def post(self, tid, kind, body):
        if self.post_failures:
            raise self.post_failures.pop(0)
        self.posts.append({"task": tid, "kind": kind, "body": body})

    def create_task(self, **kwargs):
        self.create_calls.append(kwargs)
        return "ag-new"

    def task_cancel(self, tid):
        self.cancel_calls.append(tid)

    def task_update(self, tid, verify_cmd):
        self.task_updates.append({"task": tid, "verify_cmd": verify_cmd})
        if self.task_update_error is not None:
            raise self.task_update_error

    def task_nudge(self, tid, body):
        self.nudges.append({"task": tid, "body": body})
        return {"nudged": True}

    def dispatch(self, tid):
        self.dispatch_calls.append(tid)

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
    push_verified_commit/fetch_fix_branch_tip，legacy push 族调用必须为零
    （无 lease 的裸 push 同记违约）；变基轮护栏面（ag-6da714）＝
    rebase_safe_to_force/local_branch_tip/force 落地 push_force_classified
    （ag-cad1d6：落地失败分类走 gitop 层词表，此处只注入结果）。
    缺省 rebase_safe 不安全 → non_ff 维持轮转现状链（既有轮转测试形态）；
    逃生门/护栏前提面（ag-f63b80/ag-6d7ab4 判例）＝trees_identical/
    is_ancestor/point_branch 三替身，缺省 False＝不触发拨正与等价逃生，
    既有全部测试行为不变。"""

    def __init__(self, monkeypatch, push_results=None, push_error=None,
                 fetch_tip=None, rebase_safe=None, branch_tip=None,
                 force_push_error=None, is_ancestor=False,
                 trees_identical=False):
        self.push_calls = []
        self.fetch_calls = []
        self.legacy_calls = []
        self.rebase_calls = []
        self.tip_calls = []
        self.force_push_calls = []
        self.ff_calls = []
        self.tree_eq_calls = []
        self.push_results = push_results or {}  # commit -> "ok"|"non_ff"|"failed"
        self.push_error = push_error
        self.fetch_tip = fetch_tip or hex40("f")
        self.rebase_safe = rebase_safe or (False, "fake: old-chain commit "
                                            "lost in rewrite")
        self.branch_tip = branch_tip  # None -> 分支缺失（漂移形态）
        self.force_push_error = force_push_error
        self.is_ancestor = is_ancestor
        self.trees_identical = trees_identical

        def legacy(_name):
            def spy(*args, **kwargs):
                self.legacy_calls.append(_name)
            return spy

        for name in ("push_fix_branch", "push_fix_branch_resilient"):
            monkeypatch.setattr(gitop, name, legacy(name))
        monkeypatch.setattr(gitop, "push", self.fake_push)
        monkeypatch.setattr(gitop, "push_force_classified",
                            self.fake_force_push)
        monkeypatch.setattr(gitop, "push_verified_commit",
                            self.fake_push_verified)
        monkeypatch.setattr(gitop, "fetch_fix_branch_tip", self.fake_fetch_tip)
        monkeypatch.setattr(gitop, "rebase_safe_to_force",
                            self.fake_rebase_safe)
        monkeypatch.setattr(gitop, "local_branch_tip", self.fake_local_tip)
        monkeypatch.setattr(gitop, "is_ancestor", self.fake_is_ancestor)
        monkeypatch.setattr(gitop, "trees_identical",
                            self.fake_trees_identical)
        monkeypatch.setattr(gitop, "point_branch", self.fake_point_branch)

    def fake_push(self, repo, remote_url, refspec, ssl_verify=True,
                  force_with_lease=""):
        # ag-cad1d6：变基轮落地改走 push_force_classified——push 的任何
        # 调用（含带 lease 形态）都记 legacy 违约
        self.legacy_calls.append("push")

    def fake_force_push(self, repo, remote_url, refspec, ssl_verify=True,
                        force_with_lease=""):
        self.force_push_calls.append(
            {"repo": repo, "remote_url": remote_url, "refspec": refspec,
             "ssl_verify": ssl_verify,
             "force_with_lease": force_with_lease})
        if self.force_push_error is not None:
            raise self.force_push_error

    def fake_rebase_safe(self, repo, branch, remote_tip, local_tip):
        self.rebase_calls.append({"branch": branch, "remote_tip": remote_tip,
                                  "local_tip": local_tip})
        return self.rebase_safe

    def fake_local_tip(self, repo, branch):
        self.tip_calls.append({"repo": repo, "branch": branch})
        return self.branch_tip or ""

    def fake_is_ancestor(self, repo, ancestor, descendant):
        return self.is_ancestor

    def fake_trees_identical(self, repo, sha_a, sha_b):
        self.tree_eq_calls.append({"repo": repo, "sha_a": sha_a,
                                   "sha_b": sha_b})
        return self.trees_identical

    def fake_point_branch(self, repo, branch, to_sha):
        self.ff_calls.append({"branch": branch, "to": to_sha})
        self.branch_tip = to_sha  # 拨正即移位：后续 local_branch_tip 读到新位

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
    """接线完毕的 monitor＋issue 账本：iid=7 已登记 tid（账本同步在案
    agora_task_id——生产面派发时必先落账，释放后 mail 反查依赖它）。"""
    if fake_git is None:
        fake_git = FakeGitDelivery(monkeypatch)
    state_path = tmp_path / "state.json"
    tid = next(iter(client.tasks))
    state_path.write_text(json.dumps({"issues": {"7": {
        "status": "active", "agora_task_id": tid}}}))
    journal = []
    monitor = AgoraMonitor(client=client, journal=journal.append,
                           max_active=max_active)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url=PAT_URL, ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())
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
        fetch_tip=fetched_remote_tip, branch_tip=old)
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


# --- 变基轮护栏（ag-6da714，legacy §4 移植） -------------------------------------


def test_non_ff_safe_rewrite_lands_via_force_with_lease(tmp_path, monkeypatch):
    """ag-6da714 变基轮形态：远端旧链提交在新链全有对应（range-diff 零
    丢失）且本地 issue/<iid>-fix 就是在案 handoff → force-with-lease 重写
    落地（lease 锚定观察到的远端 tip）＋delivered 照常收口＋journal 记
    align-rewrite 一行；无 lease 的 legacy push 族调用仍为零。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=handoff)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    # ag-852c8f（High-2）：refspec 必须是精确 sha 形态——推的即探测时
    # 验证过的 handoff 内容，探测→push 窗口内本地分支被协议外写手移动
    # 也无法改变出门内容；lease 期望值＝探测时远端 tip（base）
    assert fake_git.force_push_calls == [{
        "repo": monitor._repo_path, "remote_url": PAT_URL,
        "refspec": f"{handoff}:refs/heads/issue/7-fix", "ssl_verify": True,
        "force_with_lease": f"issue/7-fix:{base}"}]
    assert fake_git.rebase_calls[0] == {
        "branch": "issue/7-fix", "remote_tip": base, "local_tip": handoff}
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert client.deliver_calls[0]["commit"] == handoff
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == [], "align-rewrite 落地即收口释放名额"
    assert any("align-rewrite" in m for m in journal), journal
    assert fake_git.legacy_calls == []
    assert not any("SECRETPAT123" in m for m in journal)


def test_non_ff_true_divergence_rotates_without_force(tmp_path, monkeypatch):
    """ag-6da714 真分叉形态：远端旧链有新链缺失提交（range-diff 孤儿）→
    不 force，维持 non_ff 换基轮转现状链：携 base 轮转、名额保持占用、
    delivery_commit 不落账。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(False, "old-chain commit lost in rewrite: 1: abc < -: ---"),
        branch_tip=handoff)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    assert fake_git.force_push_calls == []
    ack = client.deliver_calls[0]
    assert ack["outcome"] == "non_ff"
    assert ack["base"] == base
    assert entry(state_path)["delivery_commit"] is None
    assert monitor.tasks() == [(7, TID)], "真分叉维持轮转，名额不释放"
    assert any("no force" in m for m in journal), journal


def test_non_ff_branch_drift_off_handoff_rotates_without_force(
        tmp_path, monkeypatch):
    """护栏第二条件：range-diff 判过但本地分支不在在案 handoff 上（force
    只许推已验证的精确 commit，绝不能把分支漂移后的内容带出门）→ 不
    force，走 non_ff 轮转。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=hex40("z"))
    monitor, _, journal = delivery_monitor(tmp_path, monkeypatch, client,
                                           fake_git)

    monitor.poll_once()

    assert fake_git.force_push_calls == []
    assert fake_git.rebase_calls == [], "分支已漂移，无需再跑 range-diff"
    assert client.deliver_calls[0]["outcome"] == "non_ff"
    assert client.deliver_calls[0]["base"] == base
    assert monitor.tasks() == [(7, TID)]
    assert any("drifted" in m for m in journal), journal


# --- 内容等价逃生门＋护栏前提拨正（ag-f63b80/ag-6d7ab4 判例三修） ---------------


def test_non_ff_tree_identical_remote_escapes_to_delivered(
        tmp_path, monkeypatch):
    """内容等价逃生门：远端 tip 已含验证过的 handoff 内容（树逐字节
    一致、仅哈希不同——记账失败搁浅形态）→ 交付事实成立，直接
    delivered 收口：不 force、不轮转、不跑 range-diff。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        branch_tip=handoff, trees_identical=True)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    assert fake_git.tree_eq_calls[0]["sha_a"] == handoff
    assert fake_git.tree_eq_calls[0]["sha_b"] == base
    assert fake_git.force_push_calls == []
    assert fake_git.rebase_calls == []
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert client.deliver_calls[0]["commit"] == handoff
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == [], "树等价即已交付，释放名额"
    assert any("树等价免重写/免轮转" in m for m in journal), journal
    assert fake_git.legacy_calls == []


def test_non_ff_tree_identical_with_stale_local_retains_handoff(
        tmp_path, monkeypatch):
    """搁浅全形态（ag-f63b80/ag-6d7ab4）：本地落后 handoff＋远端树等价
    → 拨正 FF 先把 handoff 保留到本地支（delivery_commit/汇报/重派
    上下文引用的 sha 恒可解析，绝不落 gc-eligible 位），再逃生门
    delivered 收口（ocr 轮2 #1，ag-a5472a）。"""
    stale = hex40("s")
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        branch_tip=stale, is_ancestor=True, trees_identical=True)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    assert fake_git.ff_calls == [{"branch": "issue/7-fix", "to": handoff}], \
        "逃生门前先 FF 保留 handoff"
    assert fake_git.force_push_calls == []
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert client.deliver_calls[0]["commit"] == handoff
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == []
    assert any("树等价免重写/免轮转" in m for m in journal), journal
    assert any("fast-forwarded" in m for m in journal), journal


def test_awaiting_external_fastforwards_stale_local_to_handoff(
        tmp_path, monkeypatch):
    """护栏前提拨正：本地 issue/<iid>-fix 落后在案 handoff 之后（外部
    force-update 搁浅形态）→ non_ff 路径先 FF 到 handoff，让
    _land_align_rewrite 首闸（local==handoff）在 external_branch 流程
    可达（判例实测 range-diff 15/15 配对零孤儿，护栏一旦可达即可安全
    force-with-lease 落地）；push 照常、delivered 收口。
    偏差注记：规格原表述为「push_verified_commit 之前」拨正，但 happy
    path 交付锚例（tests/test_orch.py 零裸 git 操作）要求 ok 路径零
    本地探测——拨正只服务护栏可达性，故落位 non_ff 分支（ag-a5472a
    报告注明）。"""
    stale = hex40("s")
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=stale, is_ancestor=True)
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()

    assert fake_git.ff_calls == [{"branch": "issue/7-fix", "to": handoff}]
    assert fake_git.push_calls[0]["commit"] == handoff, "push 照常不受拨正影响"
    assert fake_git.force_push_calls, "护栏可达后安全落地"
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == []
    assert any("fast-forwarded" in m for m in journal), journal


def test_force_push_transport_error_keeps_awaiting_external_for_retry(
        tmp_path, monkeypatch):
    """ag-6da714：force 推送本身撞网络病 → 不 deliver 不释放
    （awaiting_external 有界重试，poll 轮次天然退避——与普通推送同语义）；
    恢复后同 handoff 重走护栏落地 delivered。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=handoff,
        force_push_error=gitop.GitopError(
            "push issue/7-fix:issue/7-fix failed: fatal: unable to access"))
    monitor, state_path, journal = delivery_monitor(
        tmp_path, monkeypatch, client, fake_git)

    monitor.poll_once()
    assert client.deliver_calls == []
    assert client.tasks[TID]["delivery"]["status"] == "awaiting_external"
    assert monitor.tasks() == [(7, TID)], "网络病不释放名额"
    assert any("transport error" in m for m in journal), journal

    fake_git.force_push_error = None
    monitor.poll_once()
    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert entry(state_path)["delivery_commit"] == handoff
    assert monitor.tasks() == []
    assert not any("SECRETPAT123" in m for m in journal)


# --- force 落地失败分类（ag-cad1d6 High-1：恢复有界） -----------------------------


def test_force_landing_deterministic_rejection_rotates_bounded(
        tmp_path, monkeypatch):
    """High-1：护栏双条件过后的 force 落地被确定性拒绝（受保护分支禁
    force/pre-receive/服务器拒绝改写）——重试同改写永不可能成功：分类
    命中 → 转 non_ff 轮转（deliver 必须发出、服务端 attempt 前进，既有
    ≤3 有界），绝不按传输病无限重试占名额。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=handoff,
        force_push_error=gitop.ForcePushRejected(
            "push issue/7-fix:issue/7-fix failed: remote: error: GH006: "
            "Protected branch update failed\n ! [remote rejected] "
            "issue/7-fix -> issue/7-fix (pre-receive hook declined)"))
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client, fake_git)

    monitor.poll_once()

    ack = client.deliver_calls[0]  # deliver 必须发出：绝不无限重试
    assert ack["outcome"] == "non_ff"
    assert ack["base"] == base
    assert ack["commit"] == handoff
    assert client.tasks[TID]["delivery"]["attempt"] == 1, "服务端 attempt 前进"
    assert monitor.tasks() == [(7, TID)], "轮转中名额保持占用"
    assert entry(state_path)["delivery_commit"] is None
    assert any("force landing rejected" in m and "rotating" in m
               for m in journal), journal
    assert not any("SECRETPAT123" in m for m in journal)


def test_force_landing_rejection_budget_exhausted_reports_failed(
        tmp_path, monkeypatch):
    """High-1 终局臂：轮转预算已尽（attempt≥3，第 4 次 non_ff 服务端
    retry_exhausted 终局拒）时的确定性 force 拒绝 → 直接 deliver(failed,
    force_rejected) 终局：blocked＋名额释放，不弹回 worker。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    client.tasks[TID]["delivery"]["attempt"] = 3
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=handoff,
        force_push_error=gitop.ForcePushRejected(
            "push issue/7-fix:issue/7-fix failed: remote: error: denying "
            "non-fast-forward refs/heads/issue/7-fix"))
    monitor, _, journal = delivery_monitor(tmp_path, monkeypatch, client,
                                           fake_git)

    monitor.poll_once()

    ack = client.deliver_calls[0]
    assert ack["outcome"] == "failed"
    assert ack["reason"] == "force_rejected"
    assert client.tasks[TID]["state"] == "blocked"
    assert client.tasks[TID]["delivery"]["error"] == "force_rejected"
    assert monitor.tasks() == [], "终局释放名额"
    assert any("force_rejected" in m and "exhausted" in m for m in journal), \
        journal


def test_force_landing_unclassified_failure_keeps_awaiting_external(
        tmp_path, monkeypatch):
    """词表未命中两态：非 force-拒绝形态（未知远端拒绝/传输病）维持
    现状——不 deliver 不释放，awaiting_external 按轮询节奏有界重试
    （poll 层传输病通道）。"""
    handoff = hex40("a")
    base = hex40("b")
    client = FakeAgoraClient(commit=handoff)
    fake_git = FakeGitDelivery(
        monkeypatch, push_results={handoff: "non_ff"}, fetch_tip=base,
        rebase_safe=(True, "range-diff: no old-chain commit lost"),
        branch_tip=handoff,
        force_push_error=gitop.PushFailed(
            "push issue/7-fix:issue/7-fix failed: ! [remote rejected] "
            "issue/7-fix -> issue/7-fix (funny unknown reason)"))
    monitor, _, journal = delivery_monitor(tmp_path, monkeypatch, client,
                                           fake_git)

    monitor.poll_once()

    assert client.deliver_calls == []
    assert client.tasks[TID]["delivery"]["status"] == "awaiting_external"
    assert monitor.tasks() == [(7, TID)]
    assert any("transport error" in m for m in journal), journal


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


# --- Task 5: 终态汇报 / 持续评论 / mailbox 管线 / 重启恢复 / 心跳 -----------------


def mail_msg(mid, tid=TID, kind="note", body="进度正常"):
    """check_mail wire 形状（agorad store：frontmatter 字符串值＋id:int＋body）。"""
    return {"id": mid, "ts": f"{1700000000.0 + mid:.6f}", "task": tid,
            "kind": kind, "from": "ses_worker_1",
            "to": "issue-agent:agora-test", "body": body}


class ConversationFace:
    """会话面间谍：post_note 记账（可校验调用时报告已先持久化）、
    answer_question 记录问题并返回脚本化答复。"""

    def __init__(self, answer_text="答复正文", answer_error=None):
        self.posts = []       # (iid, text)
        self.answers = []     # (iid, tid, body)
        self.answer_text = answer_text
        self.answer_error = answer_error
        self.note_hook = None  # callable(iid) 在每次 post_note 前调用

    def post_note(self, iid, text):
        if self.note_hook is not None:
            self.note_hook(iid)
        self.posts.append((int(iid), text))
        return True

    def answer_question(self, iid, tid, body):
        self.answers.append((int(iid), tid, body))
        if self.answer_error is not None:
            raise self.answer_error
        return self.answer_text


def conversation_monitor(tmp_path, monkeypatch, client, face=None,
                         heartbeat_interval=0):
    """接线完毕的 monitor＋issue 账本＋会话面（Task 5）。"""
    if face is None:
        face = ConversationFace()
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client)
    monitor.attach_conversation(post_note=face.post_note,
                                answer_question=face.answer_question,
                                heartbeat_interval=heartbeat_interval)
    return monitor, state_path, journal, face


def reports_path_of(state_path):
    return str(state_path) + ".agora-reports.json"


def mail_path_of(state_path):
    return str(state_path) + ".agora-mail.json"


def test_delivered_report_comment_preserves_pitfalls_line(
        tmp_path, monkeypatch):
    """计划 L345：交付成功后从 mailbox 取最新 worker note 回评 Gitea，
    原样保留报告末行「坑位：」提示（下轮 Router 上下文依赖）；先持久化
    报告再回评；agora_status 落账以 board 为准。"""
    client = FakeAgoraClient()
    report = "根因：竞态窗口未关\n改动：agora_monitor.py\n坑位：tests/test_old.py 是替身勿信"
    client.mail = [mail_msg(1, kind="note", body="中期进度"),
                   mail_msg(2, kind="note", body=report)]
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    # 回评发生时报告必须已先持久化（先持久化再回评）
    face.note_hook = lambda iid: None  # placeholder，下方重设断言

    report_file_seen = []

    def hook(iid):
        report_file_seen.append(
            report in json.loads(
                Path(reports_path_of(state_path)).read_text(encoding="utf-8")
            )["reports"][TID]["note"])

    face.note_hook = hook
    monitor.poll_once()

    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert report_file_seen == [True], "回评前报告必须已持久化"
    assert any(report in text for _, text in face.posts), face.posts
    assert entry(state_path)["agora_status"] == "closed/delivered"
    assert entry(state_path)["delivery_commit"] == hex40("a")
    assert monitor.tasks() == []

    # 终态评论只发一次：重启后再轮询不得重复回评
    monitor.register(7, TID)
    monitor.poll_once()
    assert len(face.posts) == 1


def test_missing_report_posts_factual_comment_only(tmp_path, monkeypatch):
    """计划 L345：worker 未留结构化报告 → 只发事实型降级评论（任务 tid/
    commit 已交付、未提供结构化汇报）＋journal warning；不虚构验证结论、
    不阻塞已确认交付。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert client.deliver_calls[0]["outcome"] == "delivered"
    assert len(face.posts) == 1
    text = face.posts[0][1]
    assert TID in text and hex40("a")[:12] in text
    assert "未提供结构化汇报" in text
    assert "坑位" not in text, "缺报告不得虚构内容"
    assert any("report" in m and ("missing" in m or "缺失" in m)
               for m in journal), journal
    assert entry(state_path)["delivery_commit"] == hex40("a"), "交付不被阻塞"
    assert monitor.tasks() == []


def test_worker_question_answered_via_callback_and_posted_as_answer(
        tmp_path, monkeypatch):
    """计划 L345：worker question 交答复回调（orchestrator 在 ledger_lock
    内交 Router）后 post(kind=answer) 回 worker。ag-da7410 B：答案正文固定
    追加续作指令行——答案送达≠回合终局，防「回应即收工」陷阱。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(3, kind="question", body="测试 X 可以删吗？")]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client,
        face=ConversationFace(answer_text="可以删，它是替身"))

    monitor.poll_once()

    assert face.answers == [(7, TID, "测试 X 可以删吗？")]
    assert len(client.posts) == 1
    posted = client.posts[0]
    assert posted["kind"] == "answer"
    assert posted["body"].startswith("可以删，它是替身")
    assert "答案送达后必须继续任务直至 post(done) 或 post(kind=question)" \
        in posted["body"], "答案必须携带续作契约行"
    assert "不得以回应答案作为回合终局" in posted["body"]


def test_answer_relay_fallback_also_carries_continue_contract(
        tmp_path, monkeypatch):
    """ag-da7410 B：Router 故障走事实型兜底答复时，续作指令行同样必须在
    场——兜底不是豁免，worker 同样不能以收到答复收工。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(3, kind="question", body="测试 X 可以删吗？")]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client,
        face=ConversationFace(answer_error=RuntimeError("router down")))

    monitor.poll_once()

    assert len(client.posts) == 1
    body = client.posts[0]["body"]
    assert "自动化答复暂不可用" in body
    assert "不得以回应答案作为回合终局" in body


PROPOSAL_CMD = "HX_TIDY_BASE=0f7b145e ./container/build.sh check"
PROPOSAL_BODY = (
    "【verify 失败根因＋处置请求】改动域基点取错被基线存量提交污染。\n"
    f"修法（已验证）：verify_cmd 改为 `{PROPOSAL_CMD}`。\n"
    "注意：勿改用 --tidy-full。")


def test_verify_proposal_question_registers_cmd_and_answers(
        tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1 断言1：含「verify_cmd 改为 X」诉求的 worker
    question → 以 daemon 建单身份 task_update(tid, verify_cmd=X) ≥1 次，
    回复 answer 含登记成功结果（不经 Router 文字答复）。"""
    client = FakeAgoraClient()
    client.tasks[TID]["verify"] = {"cmd": "./container/build.sh check",
                                   "state": "registered"}
    client.mail = [mail_msg(5, kind="question", body=PROPOSAL_BODY)]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert client.task_updates == [{"task": TID, "verify_cmd": PROPOSAL_CMD}]
    assert face.answers == [], "提案走执行回路，不交 Router"
    assert len(client.posts) == 1
    posted = client.posts[0]
    assert posted["kind"] == "answer"
    assert PROPOSAL_CMD in posted["body"]
    assert "已按提案登记" in posted["body"]
    assert "答案送达后必须继续任务直至 post(done) 或 post(kind=question)" \
        in posted["body"], "提案回执同样携带续作契约行"


def test_plain_question_never_calls_task_update(tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1 断言2：普通技术提问（无修改诉求）→ task_update
    零调用，照旧 Router 文字答复。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(3, kind="question", body="测试 X 可以删吗？")]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client,
        face=ConversationFace(answer_text="可以删，它是替身"))

    monitor.poll_once()

    assert client.task_updates == []
    assert face.answers == [(7, TID, "测试 X 可以删吗？")]
    assert client.posts[0]["body"].startswith("可以删，它是替身")


def test_verify_proposal_idempotent_when_cmd_already_registered(
        tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1 幂等：任务页 verify.cmd 已等于提案命令 → 不重复
    登记（task_update 零调用），answer 回执现状。"""
    client = FakeAgoraClient()
    client.tasks[TID]["verify"] = {"cmd": PROPOSAL_CMD,
                                   "state": "registered"}
    client.mail = [mail_msg(6, kind="question", body=PROPOSAL_BODY)]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert client.task_updates == []
    assert len(client.posts) == 1
    assert "已是提案命令" in client.posts[0]["body"]
    assert "无需重复登记" in client.posts[0]["body"]


def test_verify_proposal_rejected_reports_reason(tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1 回执：task_update 被确定性拒（AgoraRejected）→
    不重放不吞，answer 携失败原因与现状转人工。"""
    client = FakeAgoraClient()
    client.tasks[TID]["verify"] = {"cmd": "./container/build.sh check",
                                   "state": "registered"}
    client.task_update_error = AgoraRejected(
        "/task_update -> HTTP 403: dispatcher 不得登记（fake）")
    client.mail = [mail_msg(7, kind="question", body=PROPOSAL_BODY)]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert len(client.task_updates) == 1, "拒绝也要有执行尝试在案"
    assert len(client.posts) == 1
    body = client.posts[0]["body"]
    assert "登记被拒" in body and "403" in body
    assert "./container/build.sh check" in body, "回执必须带未生效的现状命令"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == [], "确定性拒绝不重放"


def test_verify_proposal_adoption_gate_only_allows_prefix_pin(
        tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1 采纳门（评审环收紧）：worker 提 `true` 自绿门或
    非「现状＋NAME=<sha> 前缀」形态 → 不登记（task_update 零调用），
    answer 事实型回执未采纳原因＋现状保留。"""
    client = FakeAgoraClient()
    client.tasks[TID]["verify"] = {"cmd": "./container/build.sh check",
                                   "state": "registered"}
    client.mail = [
        mail_msg(8, kind="question", body="verify_cmd 改为 `true` 好了"),
        mail_msg(9, kind="question",
                 body="verify_cmd 里可以写 = 号吗？例如 `HX_TIDY_BASE=x "
                      "./check.sh`"),
        mail_msg(10, kind="question", body=PROPOSAL_BODY),
    ]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()  # 每轮至多答复一个 question（终闸 B-4）

    assert client.task_updates == [], "非判例形态一律不得登记"
    assert len(client.posts) == 1
    body = client.posts[0]["body"]
    assert "未采纳" in body and "./container/build.sh check" in body
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] != [], "其余 question 留 pending 下一轮"


def test_verify_proposal_non_dict_verify_field_answers_not_swallows(
        tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1（B 册 warning＋Round2 high）：board verify 载荷
    契约外形态（非 dict／dict 内 cmd 非 str）不得让异常逃逸成静默吞问
    ——零答复不重放正是本缺陷要修的僵死形态；必须照常给事实型 answer。"""
    for bad in ("registered-string", {"cmd": 1234567, "state": "registered"}):
        client = FakeAgoraClient()
        client.tasks[TID]["verify"] = bad
        client.mail = [mail_msg(11, kind="question", body=PROPOSAL_BODY)]
        monitor, state_path, _, face = conversation_monitor(
            tmp_path, monkeypatch, client)

        monitor.poll_once()

        assert client.task_updates == []
        assert len(client.posts) == 1, "必须答复，不得静默消费"
        assert "未采纳" in client.posts[0]["body"]
        assert json.loads(
            Path(mail_path_of(state_path)).read_text(encoding="utf-8")
        )["pending"] == []


def test_verify_proposal_terminal_task_not_executed(tmp_path, monkeypatch):
    """ag-fb9e5a 缺陷1（C 册 suggestion）：终态任务的提案不执行——
    僵尸 worker 对已终态任务的改写诉求只回事实型回执，零 task_update。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "closed"
    client.tasks[TID]["verify"] = {"cmd": "./container/build.sh check",
                                   "state": "registered"}
    client.mail = [mail_msg(12, kind="question", body=PROPOSAL_BODY)]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert client.task_updates == []
    assert len(client.posts) == 1
    assert "已终态" in client.posts[0]["body"]


def test_worker_answer_relayed_to_issue_thread(tmp_path, monkeypatch):
    """直通闭环：worker 对转发问题的答复（kind=answer）回评到 Gitea issue。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(4, kind="answer", body="已改用方案 B，验证全绿")]
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert any("已改用方案 B" in text for _, text in face.posts), face.posts


def test_check_mail_crash_after_read_replays_without_loss(
        tmp_path, monkeypatch):
    """计划 L345：check_mail 读取后、处理前崩溃 → 重启先重放未处理批次
    （读后崩溃不丢消息），处理完成才标记（pending 清空）。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    crashed = [mail_msg(1, kind="note", body="崩溃前的进度 note"),
               mail_msg(2, kind="question", body="崩溃前的问题？")]
    # 模拟崩溃现场：整批已原子落盘、未处理
    state.write_json_atomic(mail_path_of(state_path), {"pending": crashed})

    monitor.poll_once()  # 新收件为空：先重放未处理批次

    assert any("崩溃前的问题？" in body for _, _, body in face.answers)
    reports = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8"))
    assert reports["reports"][TID]["note"] == "崩溃前的进度 note"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


def test_mail_processing_failure_does_not_swallow_other_messages(
        tmp_path, monkeypatch):
    """计划 L345：单条消息处理失败只 journal 不吞批次——同批其他任务的
    消息照常处理，pending 照常推进。"""
    client = FakeAgoraClient()
    face = ConversationFace(answer_error=RuntimeError("router down (fake)"))
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client, face=face)
    client.mail = [mail_msg(1, kind="question", body="会失败的问题"),
                   mail_msg(2, kind="note", body="不受影响的汇报 note")]

    monitor.poll_once()

    assert any("router down" in m for m in journal), journal
    reports = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8"))
    assert reports["reports"][TID]["note"] == "不受影响的汇报 note"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


def test_restart_resumes_awaiting_external_without_duplicate_task(
        tmp_path, monkeypatch):
    """计划锚例：daemon 启动从账本恢复未终态 agora_task_id（幂等
    register），不建新任务；恢复后交付状态机照常收口。"""
    client = FakeAgoraClient()
    fake_git = FakeGitDelivery(monkeypatch)
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {"7": {
        "status": "active", "agora_task_id": TID,
        "agora_status": "awaiting_external"}}}))
    monitor = AgoraMonitor(client=client, journal=[].append, max_active=1)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url=PAT_URL, ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())

    monitor.start()  # daemon 启动（orchestrator.start → monitor.start）
    try:
        assert monitor.tasks() == [(7, TID)]
        assert client.create_calls == [], "恢复只登记，绝不建新任务"
        assert client.check_mail_calls == 0, "恢复本身零网络副作用"
    finally:
        monitor.stop()

    monitor.poll_once()
    assert entry(state_path)["delivery_commit"] == hex40("a")
    assert monitor.tasks() == []


def test_restart_skips_terminal_and_unknown_status_ledger_tasks(
        tmp_path, monkeypatch):
    """终态（merged/closed/cancelled）账本任务不恢复；从未同步过
    agora_status 的在账 tid 按未终态恢复（安全方向）。"""
    client = FakeAgoraClient()
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {
        "7": {"status": "active", "agora_task_id": "ag-closed",
              "agora_status": "closed/delivered"},
        "8": {"status": "active", "agora_task_id": "ag-unsynced",
              "agora_status": None}}}))
    monitor = AgoraMonitor(client=client, journal=[].append, max_active=4)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url=PAT_URL, ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())
    monitor.start()
    try:
        assert monitor.tasks() == [(8, "ag-unsynced")]
    finally:
        monitor.stop()


def test_cancelled_posts_factual_comment_without_quality_blame(
        tmp_path, monkeypatch):
    """计划 L345：board cancelled 与 Gitea 状态一致——只报取消事实，不归因
    质量失败；agora_status 落账；不调 task_cancel（只陈述，不动作）。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "cancelled"
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert monitor.tasks() == []
    assert len(face.posts) == 1
    assert "取消" in face.posts[0][1]
    assert "失败" not in face.posts[0][1], "cancelled 不归因质量失败"
    assert entry(state_path)["agora_status"] == "cancelled"
    assert client.cancel_calls == []
    assert client.deliver_calls == []


def test_blocked_delivery_failure_comment_carries_push_failed_detail(
        tmp_path, monkeypatch):
    """计划 L345：blocked＋delivery.status=failed 回评具体 push_failed 错误
    （设计 §7.4），人工跟进；名额释放。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    client.tasks[TID]["delivery"].update(status="failed",
                                         error="push_failed: connection reset")
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert monitor.tasks() == []
    assert len(face.posts) == 1
    assert "push_failed" in face.posts[0][1]
    assert "connection reset" in face.posts[0][1]
    assert entry(state_path)["agora_status"].startswith("blocked")


def test_heartbeat_reports_agora_board_state(tmp_path, monkeypatch):
    """计划 L345：心跳评论显示 board state/delivery.status（legacy 心跳
    不变——由 test_orch 既有用例锁定）；间隔未到零评论。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "running"
    client.tasks[TID]["delivery"] = None
    monitor, state_path, _, face = conversation_monitor(
        tmp_path, monkeypatch, client, heartbeat_interval=30)

    monitor.poll_once()  # board 读到 running：状态同步，间隔未到零评论
    assert face.posts == []
    assert entry(state_path)["agora_status"] == "running"

    with monitor._guard:  # 回拨时钟模拟间隔已到
        monitor._hb[7]["last"] -= 60
    monitor.poll_once()
    assert len(face.posts) == 1
    assert TID in face.posts[0][1] and "running" in face.posts[0][1]

    monitor.poll_once()  # 间隔未到：不重复刷屏
    assert len(face.posts) == 1


# --- agora review round 1 fixes (ocr 035ba80a) ----------------------------------


def test_ack_rejected_board_delivered_still_posts_report(
        tmp_path, monkeypatch):
    """ocr R1 #3：deliver ack 被拒但对账见 done+delivered（同 commit）——
    记 delivery_commit 之外必须照常回评报告，不得静默释放（释放后无人
    再轮询，汇报会丢到重启）。"""
    commit = hex40("a")
    client = FakeAgoraClient(commit=commit)
    original_deliver = client.deliver

    def flip_and_reject(tid, outcome, c, base=None, reason=""):
        client.deliver_calls.append({"task": tid, "outcome": outcome,
                                     "commit": c, "base": base,
                                     "reason": reason})
        # 服务端已按本次 ack 收口，但回包被拒（stale 竞态模拟）
        client.tasks[tid]["delivery"].update(status="delivered",
                                             local_status="not_required")
        raise AgoraRejected("stale (fake)")

    client.deliver = flip_and_reject
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert entry(state_path)["delivery_commit"] == commit
    assert monitor.tasks() == []
    assert len(face.posts) == 1, "对账确认交付后必须回评报告"
    assert TID in face.posts[0][1]
    # 重放（重启后误登记）不得重复回评
    monitor.register(7, TID)
    client.deliver = original_deliver
    monitor.poll_once()
    assert len(face.posts) == 1


def test_poison_non_object_mail_message_does_not_break_batch(
        tmp_path, monkeypatch):
    """ocr R1 #5：批次混入非 dict 毒元素——丢弃该元素并继续处理其余
    消息，pending 照常排空（处理器不得因毒元素自身再抛错中断整批）。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    client.mail = ["not-an-object",
                   mail_msg(9, kind="note", body="毒元素后的正常汇报")]

    monitor.poll_once()

    assert client.check_mail_calls == 1
    reports = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8"))
    assert reports["reports"][TID]["note"] == "毒元素后的正常汇报"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []
    assert any("non-object" in m or "not-an-object" in m for m in journal), \
        journal


def test_unknown_task_mail_warns_with_attribution_hint(tmp_path, monkeypatch):
    """ag-ee0c29 缺口2（#182 锚例）：账本无属主的任务邮件（账本 tid 已被
    新单覆盖的孤儿窗口）不得无声 dropped——journal warning 含 tid/kind，
    且按报告库历史 tid 给出归属提示（可能属主 issue#N）。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    old = "ag-2b98a7"  # 不在注册表、不在账本（tid 已被新单覆盖）
    state.write_json_atomic(
        reports_path_of(state_path),
        {"reports": {old: {"iid": 7, "note": "开工",
                           "note_id": 1, "finalized": False,
                           "sent_marks": []}}})
    client.mail = [mail_msg(21, tid=old, kind="question", body="还在吗？")]

    monitor.poll_once()

    warnings = [m for m in journal if "unknown task" in m and old in m]
    assert warnings, journal
    assert any("question" in m for m in warnings), "warning 必须含 kind"
    assert any("warning" in m for m in warnings), "不得无声 dropped"
    assert any("issue#7" in m for m in warnings), "归属提示命名可能属主"


# --- 终闸三分册修复（B-1/B-2/B-3/B-4） -------------------------------------------


def test_answer_post_transient_defers_for_replay(tmp_path, monkeypatch):
    """终闸 B-2：answer post 瞬断——消息留 pending 下一轮重放重试，绝不
    吞问题（worker 无限等待）也不重复投递已成功答复。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(5, kind="question", body="瞬断窗口的提问")]
    client.post_failures.append(AgoraUnavailable("post down (fake)"))
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert client.posts == [], "未送达不得记账"
    pending = json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8"))["pending"]
    assert len(pending) == 1, "瞬断的消息必须留待重放"

    client.post_failures.clear()
    monitor.poll_once()
    answers = [p for p in client.posts if p["kind"] == "answer"]
    assert len(answers) == 1, "重放恰好补投一次"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


def test_reports_store_write_failure_defers_note(tmp_path, monkeypatch):
    """终闸 B-3/A-1：reports 落盘瞬态 OSError≠毒消息——消息留 pending
    重放，恢复后报告照常入库（不降级成事实型评论）。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(6, kind="note", body="瞬断窗口的汇报正文")]
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    real_write = state.write_json_atomic
    failures = [OSError("ENOSPC (fake)")]

    def flaky_write(path, payload):
        if str(path).endswith(".agora-reports.json") and failures:
            raise failures.pop(0)
        return real_write(path, payload)

    monkeypatch.setattr(state, "write_json_atomic", flaky_write)

    monitor.poll_once()
    pending = json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8"))["pending"]
    assert len(pending) == 1, "写失败的消息必须留待重放"

    monitor.poll_once()  # 写恢复正常
    reports = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8"))
    assert reports["reports"][TID]["note"] == "瞬断窗口的汇报正文"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


def test_multiple_questions_capped_one_per_drain(tmp_path, monkeypatch):
    """终闸 B-4：每轮 drain 至多答复一个 question——封顶 monitor 线程
    持 ledger_lock 跨 LLM 的停摆窗；其余顺延下一轮（15s 级延迟）。"""
    client = FakeAgoraClient()
    client.mail = [mail_msg(7, kind="question", body="第一问"),
                   mail_msg(8, kind="question", body="第二问")]
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()
    answers = [p for p in client.posts if p["kind"] == "answer"]
    assert len(answers) == 1, "每轮至多一个 LLM 答复"
    pending = json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8"))["pending"]
    assert len(pending) == 1, "第二问顺延"

    monitor.poll_once()
    answers = [p for p in client.posts if p["kind"] == "answer"]
    assert len(answers) == 2, "下一轮补答"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


# --- ag-8d6a70：blocked pre-delivery 文案＋取消事实回评缺口 -----------------------


def test_blocked_pre_delivery_journals_actual_state_not_push_failed(
        tmp_path, monkeypatch):
    """ag-8d6a70：未进交付段（无 delivery.failed）的 blocked 任务从未尝试
    推送——journal 按实况记 blocked pre-delivery，不得误报 push_failed(None)
    （2026-11-14 #178/#181 vacuous-done 三连拒文案）。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    fake_git = FakeGitDelivery(monkeypatch)
    monitor, state_path, journal = delivery_monitor(tmp_path, monkeypatch,
                                                    client, fake_git)

    monitor.poll_once()

    assert fake_git.push_calls == [] and client.deliver_calls == []
    assert monitor.tasks() == [], "blocked 终局释放名额"
    assert any("blocked pre-delivery" in m for m in journal), journal
    assert not any("push_failed" in m for m in journal), \
        ("从未推送不得记 push_failed", journal)


def test_result_mail_cancel_after_blocked_release_comments_issue(
        tmp_path, monkeypatch):
    """ag-8d6a70 追加项（5）：blocked 释放名额后 root 取消——轮询面已不可
    见，服务端 notify_transition 的 kind=result 邮件是唯一信号：重读 board
    确认 cancelled 后回评取消事实（panel 收口；幂等重放不刷屏）。
    今晨三单（ag-97fa3e/ag-48372f/ag-f0bc27）取消后面板停在求助/失败
    旧评论的正是此缺口。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    # 第一轮：blocked（未进交付段）→ 求助评论＋释放名额，面板停在旧评论
    client.tasks[TID]["state"] = "blocked"
    monitor.poll_once()
    assert monitor.tasks() == []
    assert len(face.posts) == 1, "blocked 事实评论"

    # root 侧 task_cancel（服务端迁移）；monitor 已释放名额，仅剩邮件信号
    client.tasks[TID]["state"] = "cancelled"
    client.mail = [mail_msg(9, kind="result", body=f"任务 {TID} cancelled")]
    monitor.poll_once()

    assert monitor.tasks() == []
    assert len(face.posts) == 2, "取消事实必须回评收口"
    assert "取消" in face.posts[1][1]
    assert "失败" not in face.posts[1][1], "取消不归因质量失败"
    assert entry(state_path)["agora_status"] == "cancelled", \
        "账本终态同步（轮询不再覆盖）"

    # 重放同一封 result 邮件不得重复回评（幂等）
    client.mail = [mail_msg(10, kind="result", body=f"任务 {TID} cancelled")]
    monitor.poll_once()
    assert len(face.posts) == 2


def test_result_mail_board_unreachable_defers_for_replay(
        tmp_path, monkeypatch):
    """ag-8d6a70：result 邮件对账读 board 瞬断——留 pending 下一轮重放
    （同 B-2 纪律：瞬态≠毒，取消事实不因一次网络病丢失）。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    client.tasks[TID]["state"] = "cancelled"
    client.mail = [mail_msg(11, kind="result", body=f"任务 {TID} cancelled")]
    original_board = client.board
    board_down = {"down": True}

    def flaky_board(tid=None):
        if board_down["down"]:
            raise AgoraUnavailable("board down (fake)")
        return original_board(tid)

    monkeypatch.setattr(client, "board", flaky_board)
    monitor.poll_once()
    pending = json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8"))["pending"]
    assert len(pending) == 1, "瞬断的 result 邮件必须留待重放"
    assert face.posts == []

    board_down["down"] = False
    monitor.poll_once()
    assert len(face.posts) == 1, "恢复后取消事实照常回评"
    assert json.loads(
        Path(mail_path_of(state_path)).read_text(encoding="utf-8")
    )["pending"] == []


# --- ag-1c7315：面板评论指令处置（处置提示行＋重派/取消收位） ---------------------


def test_blocked_comment_carries_disposal_hint(tmp_path, monkeypatch):
    """ag-1c7315：blocked 收口评论追加处置提示行——面板上的人知道有
    「重试／取消任务」这双手。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()

    assert len(face.posts) == 1
    text = face.posts[0][1]
    assert "重试" in text and "取消任务" in text, text


def test_redispatched_rearms_slot_and_clears_round_marks(
        tmp_path, monkeypatch):
    """ag-1c7315：redispatched＝面板「重试」的 monitor 收位——重新登记
    名额（relay/交付轮询恢复）并清旧轮 sent_marks：重派后新一轮再
    blocked 必须照常回评求助，不被旧轮标记吞掉。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)

    monitor.poll_once()  # 第一轮 blocked：求助评论＋释放名额
    assert len(face.posts) == 1 and monitor.tasks() == []

    client.tasks[TID]["state"] = "running"
    monitor.redispatched(7, TID)
    assert monitor.tasks() == [(7, TID)], "重派后名额恢复"

    client.tasks[TID]["state"] = "blocked"
    monitor.poll_once()
    assert len(face.posts) == 2, "新一轮 blocked 必须再次回评"


def test_announce_cancelled_posts_fact_once_and_dedups_result_mail(
        tmp_path, monkeypatch):
    """ag-1c7315：announce_cancelled＝面板「取消任务」的事实回评——与
    result-mail 收口回评同 mark 幂等：面板恰好一条取消事实，邮件重放
    不刷屏；账本同步终态。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    monitor.poll_once()  # blocked 求助评论（posts=1）

    client.tasks[TID]["state"] = "cancelled"
    monitor.announce_cancelled(7, TID)
    assert len(face.posts) == 2 and "取消" in face.posts[1][1]

    client.mail = [mail_msg(12, kind="result", body=f"任务 {TID} cancelled")]
    monitor.poll_once()
    assert len(face.posts) == 2, "result-mail 收口回评被 mark 幂等吞并"
    assert entry(state_path)["agora_status"] == "cancelled"


def test_redispatch_during_blocked_finalize_keeps_registration(
        tmp_path, monkeypatch):
    """B 册 F2（ag-1c7315 评审）：poll 线程 blocked 收口的回评窗内面板
    「重试」重登记——迟到的 release（同 tid 过 compare-and-release 防线）
    不得删掉新登记，否则任务在飞却无人轮询/relay（交付断头）。"""
    client = FakeAgoraClient()
    client.tasks[TID]["state"] = "blocked"
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    in_comment = threading.Event()
    release_comment = threading.Event()

    def gated_hook(iid):  # 卡住 blocked 回评 POST：拉长 poll 的收口窗口
        in_comment.set()
        assert release_comment.wait(timeout=5)

    face.note_hook = gated_hook
    poll_thread = threading.Thread(target=monitor.poll_once)
    poll_thread.start()
    assert in_comment.wait(timeout=5), "poll 必须已进入 blocked 回评"
    redis_thread = threading.Thread(target=monitor.redispatched,
                                    args=(7, TID))
    redis_thread.start()
    redis_thread.join(timeout=2.0)  # 无串行锁版（修复前）此刻已完成
    release_comment.set()           # 放行回评 → poll 走迟到 release 收口
    poll_thread.join(timeout=5.0)
    redis_thread.join(timeout=5.0)
    assert monitor.tasks() == [(7, TID)], \
        "迟到的 blocked release 不得删掉重派新登记"


def test_redispatched_clears_stale_round_report(tmp_path, monkeypatch):
    """ag-1c7315 R1（o-3）：重派轮次重置清旧轮报告但保留 note_id 重放
    水位——旧轮 blocked 报告不得冒充新轮「worker 最终汇报」或被「重发
    汇报」回放；重放的旧轮 pending 邮件（mid≤水位）亦不得再入库（R2）。"""
    client = FakeAgoraClient()
    monitor, state_path, journal, face = conversation_monitor(
        tmp_path, monkeypatch, client)
    state.write_json_atomic(reports_path_of(state_path), {
        "reports": {TID: {"iid": 7, "note": "旧轮根因分析", "note_id": 5,
                          "finalized": False, "sent_marks": []}}})

    monitor.redispatched(7, TID)

    store_entry = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8")
    )["reports"][TID]
    assert store_entry["note"] is None, "旧轮报告随轮次重置清空"
    assert store_entry["note_id"] == 5, "note_id 保留＝跨轮重放水位"
    assert monitor.report_for(7) is None, "重发汇报不得回放旧轮报告"

    monitor._store_report_note(7, TID, 5, "旧轮 pending 邮件重放")
    replayed = json.loads(
        Path(reports_path_of(state_path)).read_text(encoding="utf-8")
    )["reports"][TID]
    assert replayed["note"] is None, "重放的旧轮邮件（mid≤水位）不得入库"


# --- ag-da7410 C: 僵态升级阶梯（nudge×2 → 重派恰一次 → blocked） ----------------


def stalls_path_of(state_path):
    return str(state_path) + ".agora-stalls.json"


def stall_client(state="running", deliverable="external_branch",
                 age_s=4000.0, last_seen=None):
    """僵态测试客户端：board 行＝running＋交付面＋created/last_seen 口径。"""
    client = FakeAgoraClient()
    client.tasks[TID] = {
        "state": state, "deliverable": deliverable, "delivery": {},
        "created": time.time() - age_s, "last_seen": last_seen}
    return client


def stall_monitor(tmp_path, client, face=None, stall_timeout=1800):
    """接线完毕的僵态阶梯 monitor（iid=7 已登记，账本在案）。"""
    if face is None:
        face = ConversationFace()
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {"7": {
        "status": "active", "agora_task_id": TID}}}))
    journal = []
    monitor = AgoraMonitor(client=client, journal=journal.append,
                           max_active=1, stall_timeout=stall_timeout)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url=PAT_URL, ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())
    monitor.attach_conversation(post_note=face.post_note,
                                answer_question=face.answer_question,
                                heartbeat_interval=0)
    monitor.register(7, TID)
    return monitor, state_path, journal, face


def age_stall_window(state_path, tid=TID, age_s=2000.0):
    """把阶梯 epoch 人为拨老一个窗口（模拟重派后零活动的时间流逝）。"""
    from agora_monitor import _stall_load, _stall_save  # 内部面直调（测试缝）
    row = _stall_load(state_path)
    row[tid]["epoch"] = time.time() - age_s
    _stall_save(state_path, row)


def test_stall_ladder_nudges_twice_redispatches_once_then_blocks(
        tmp_path, monkeypatch):
    """ag-da7410 C 三档逐级：零活动超阈值 → task_nudge 强指令×2（间隔同
    阈值）→ 自动重派恰一次（journal 留痕）→ 重派后仍零活动 → blocked
    面板提示＋释放名额；此后不再升级。"""
    client = stall_client()
    monitor, state_path, journal, face = stall_monitor(tmp_path, client)

    monitor.poll_once()
    assert len(client.nudges) == 1, "第一档：零活动超阈值即 nudge 强指令"
    assert client.nudges[0]["body"] == agora_monitor.STALL_NUDGE_TEXT
    monitor.poll_once()
    assert len(client.nudges) == 1, "窗口未满：nudge 档间隔同阈值（终闸 R1#1）"

    age_stall_window(state_path)  # 模拟间隔同阈值的时间流逝
    monitor.poll_once()
    assert len(client.nudges) == 2, "第二档：间隔同阈值，nudge 第二次"

    age_stall_window(state_path)
    monitor.poll_once()  # 两轮 nudge 无效 → 自动重派恰一次
    assert client.dispatch_calls == [TID], "第三档：自动重派恰一次"
    assert client.nudges == client.nudges[:2], "重派后不再 nudge"
    assert any("auto-redispatch" in m for m in journal), journal
    assert monitor.tasks() == [(7, TID)], "重派后名额继续占用"

    monitor.poll_once()  # 重派窗口未满：零动作
    assert client.dispatch_calls == [TID]

    age_stall_window(state_path)  # 模拟重派后时间流逝满一个窗口
    monitor.poll_once()  # 重派后仍零活动 → blocked＋面板提示＋释放
    assert monitor.tasks() == [], "终档释放名额"
    assert len(face.posts) == 1
    assert "人工" in face.posts[0][1], "面板必须给人处置提示"

    monitor.poll_once()  # 终档后不再升级
    assert client.dispatch_calls == [TID], "重派全程恰一次"
    assert len(client.nudges) == 2
    assert len(face.posts) == 1


def test_stall_ladder_skips_fresh_running_and_non_target_tasks(
        tmp_path, monkeypatch):
    """ag-da7410 C 阈值外/非目标不触发：活动新鲜、非 running 态、非
    external/out_of_band 交付面、stall_timeout=0 关闸——零 nudge 零重派。"""
    fresh = stall_client(age_s=100.0)
    monitor, _, _, _ = stall_monitor(tmp_path, fresh)
    monitor.poll_once()
    assert fresh.nudges == [] and fresh.dispatch_calls == []

    done = stall_client(state="done")
    monitor, _, _, _ = stall_monitor(tmp_path, done)
    monitor.poll_once()
    assert done.nudges == [] and done.dispatch_calls == []

    internal = stall_client(deliverable=None)
    monitor, _, _, _ = stall_monitor(tmp_path, internal)
    monitor.poll_once()
    assert internal.nudges == [] and internal.dispatch_calls == []

    off = stall_client()
    monitor, _, _, _ = stall_monitor(tmp_path, off, stall_timeout=0)
    monitor.poll_once()
    assert off.nudges == [] and off.dispatch_calls == []


def test_stall_ladder_covers_out_of_band_tasks(tmp_path, monkeypatch):
    """ag-da7410 C：out_of_band（需勘察的验证/解答类）同样受阶梯保护。"""
    client = stall_client(deliverable="out_of_band")
    monitor, _, _, _ = stall_monitor(tmp_path, client)
    monitor.poll_once()
    assert len(client.nudges) == 1


def test_stall_ladder_resets_when_worker_activity_resumes(
        tmp_path, monkeypatch):
    """ag-da7410 C：板面活动前移（last_seen 刷新）＝worker 复活——阶梯
    整体复位；再僵时从第一档重来（不是接着旧计数）。"""
    client = stall_client()
    monitor, _, journal, _ = stall_monitor(tmp_path, client)
    monitor.poll_once()
    assert len(client.nudges) == 1

    client.tasks[TID]["last_seen"] = time.time() - 100  # worker 复活
    monitor.poll_once()
    assert len(client.nudges) == 1, "活动恢复窗口内零动作"

    client.tasks[TID]["last_seen"] = time.time() - 2000  # 再僵满一个窗口
    monitor.poll_once()
    assert len(client.nudges) == 2
    assert client.dispatch_calls == [], "复位后从第一档重来，不直跳重派"
    assert sum("nudge 1/2" in m for m in journal) >= 2, \
        "复位后重走第一档（两次 nudge 1/2）"


def test_stall_ladder_state_survives_restart(tmp_path, monkeypatch):
    """ag-da7410 C：阶梯档位持久 sidecar——重启后不重走已耗档位（重派
    恰一次跨重启成立）。"""
    client_a = stall_client()
    monitor_a, state_path, journal_a, _ = stall_monitor(tmp_path, client_a)
    monitor_a.poll_once()
    assert len(client_a.nudges) == 1

    age_stall_window(state_path)  # nudge 档间隔同阈值：窗口流逝后才进第二档
    client_b = stall_client()  # 重启：同 state_path、新客户端新 monitor
    monitor_b, _, journal_b, _ = stall_monitor(tmp_path, client_b)
    monitor_b.poll_once()
    assert len(client_b.nudges) == 1
    assert any("nudge 2/2" in m for m in journal_b), \
        "重启必须从第二档续走，不得重放第一档"


def test_stall_ladder_rearms_after_panel_retry(tmp_path, monkeypatch):
    """ag-da7410 终闸 R1#2：终档 exhausted 后面板「重试」（redispatched
    收位）必须重武装阶梯——新 worker 再沉默时照常 nudge×2→重派→blocked，
    exhausted 残行不得让监控永久 no-op（名额泄漏）。"""
    client = stall_client()
    monitor, state_path, journal, face = stall_monitor(tmp_path, client)
    for _ in range(2):
        monitor.poll_once()
        age_stall_window(state_path)
    monitor.poll_once()  # 自动重派
    age_stall_window(state_path)
    monitor.poll_once()  # 终档 blocked＋释放
    assert monitor.tasks() == []
    assert agora_monitor._stall_load(state_path)[TID]["exhausted"] is True

    monitor.redispatched(7, TID)  # 面板「重试」收位（orchestrator 委托）
    assert monitor.tasks() == [(7, TID)], "重派后重新登记"
    row = agora_monitor._stall_load(state_path)[TID]
    assert row["exhausted"] is False and row["nudges"] == 0, \
        "阶梯行随轮次复位（重武装）"

    age_stall_window(state_path)
    monitor.poll_once()  # 新轮第一档 nudge
    assert len(client.nudges) == 3, "重武装后阶梯照常从第一档走"


# --- ag-da7410 D: 重派下行注入 handoff 上下文 -----------------------------------


def test_stall_redispatch_injects_handoff_context_when_commit_recorded(
        tmp_path, monkeypatch):
    """ag-da7410 D 有 handoff 态：C 档自动重派时账本 delivery_commit 在案
    → 下行 note 注入「前手 handoff 已在案（sha 短）…勿从头重新推导」。"""
    client = stall_client()
    monitor, state_path, journal, face = stall_monitor(tmp_path, client)
    st = state.load_state(state_path)
    st["issues"]["7"]["delivery_commit"] = hex40("a")
    state.save_state(state_path, st)

    for _ in range(2):  # nudge×2（档间窗口经 age_stall_window 流逝）
        monitor.poll_once()
        age_stall_window(state_path)
    monitor.poll_once()  # 自动重派

    assert client.dispatch_calls == [TID]
    notes = [p for p in client.posts if p["kind"] == "note"]
    assert len(notes) == 1, "重派上下文恰注入一次"
    body = notes[0]["body"]
    assert "前手 handoff 已在案" in body
    assert hex40("a")[:12] in body, "sha 短格式（12 位）"
    assert "勿从头重新推导" in body


def test_stall_redispatch_without_handoff_injects_nothing(tmp_path,
                                                          monkeypatch):
    """ag-da7410 D 无 handoff 态：账本无 delivery_commit（前手从未交付）
    → 重派零注入，不打扰新 worker。"""
    client = stall_client()
    monitor, state_path, journal, face = stall_monitor(tmp_path, client)

    for _ in range(2):
        monitor.poll_once()
        age_stall_window(state_path)
    monitor.poll_once()  # 自动重派

    assert client.dispatch_calls == [TID]
    assert client.posts == [], "无 handoff 在案不得注入"


def test_panel_redispatch_injection_reads_ledger_commit(tmp_path,
                                                        monkeypatch):
    """ag-da7410 D：inject_handoff_context 独立面（面板「重试」路径由
    orchestrator 委托）——有/无 delivery_commit 的两态直测。"""
    client = stall_client()
    monitor, state_path, _, _ = stall_monitor(tmp_path, client)

    monitor.inject_handoff_context(7, TID)
    assert client.posts == [], "无 delivery_commit：零动作"

    st = state.load_state(state_path)
    st["issues"]["7"]["delivery_commit"] = hex40("b")
    state.save_state(state_path, st)
    monitor.inject_handoff_context(7, TID)
    assert len(client.posts) == 1
    assert client.posts[0]["kind"] == "note"
    assert hex40("b")[:12] in client.posts[0]["body"]
