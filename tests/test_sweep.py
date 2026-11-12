"""Tests for the orchestrator's 欠账巡检 (debt sweep) thread + the
blocked-path reply store (task ag-3741cb, defects A/B).

Fake/stub style of test_orch.py: fake api, fake listener, and monkeypatched
gitop seams (push_fix_branch_resilient / remote_branch_tip / _run). The
sweep store is REAL — a tmp file beside the state path. Orchestrators here
are never started; _sweep_once is invoked directly, so no threads.

Run: python3 -m pytest tests/test_sweep.py -q
"""

import queue as queue_mod
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

PKG_DIR = Path(__file__).resolve().parent.parent

# same stdlib-http shadow dance as test_orch.py (see the comment there)
sys.path[:] = [p for p in sys.path if p not in ("", str(PKG_DIR))]
import http.client  # noqa: E402,F401
import urllib.error  # noqa: E402,F401
import urllib.request  # noqa: E402,F401
sys.path.insert(0, str(PKG_DIR))

import api  # noqa: E402
import daemon  # noqa: E402
import gitop  # noqa: E402
import orchestrator  # noqa: E402
import runner  # noqa: E402
import state  # noqa: E402

SHA_A = "a" * 40
SHA_B = "b" * 40


class FakeApi:
    def __init__(self, states=None, fail_note_once=False, comments=None):
        self.states = dict(states or {})  # {iid: state, "__transient__" = raise}
        self.comments = dict(comments or {})  # {iid: [api comment dicts]} (probe wall)
        self.posts = []                    # (iid, body)
        self.fail_note_once = fail_note_once

    def build_note_body(self, text):
        return f"\U0001f916 {text}\n{api.NOTE_MARKER}"

    def issue_state(self, project, iid):
        state_ = self.states.get(int(iid))
        if state_ == "__transient__":
            raise api.ApiTransientError("gitea api unreachable after retry")
        return state_ or "open"

    def list_issue_comments(self, project, iid):
        return self.comments.get(int(iid), [])

    def post_note(self, project, iid, body):
        if self.fail_note_once:
            self.fail_note_once = False
            raise api.ApiTransientError("transient post failure")
        self.posts.append((int(iid), body))


class FakeListener:
    def __init__(self):
        self.queue = queue_mod.Queue()

    def start(self):
        return 4242

    def stop(self):
        pass


class Harness:
    """One sweep-ready orchestrator over a tmp state path, git seams faked."""

    def __init__(self, tmp_path, monkeypatch, states=None):
        self.journal_msgs = []
        self.api = FakeApi(states)
        self.state_path = tmp_path / "state.json"
        (tmp_path / "repo").mkdir(exist_ok=True)
        self.orch = orchestrator.Orchestrator(
            project="group/repo",
            api=self.api,
            journal=self.journal_msgs.append,
            listener=FakeListener(),
            state_path=self.state_path,
            repo_path=str(tmp_path / "repo"),
            remote_url="https://oauth2:SECRET-PAT@fake.invalid/group/repo.git",
            protocol_path=str(tmp_path / "PROTOCOL.md"),
            reconcile_interval=600,
            sweep_interval=1800,
        )
        # git seams
        self.pushes = []        # iids passed to push_fix_branch_resilient
        self.push_error = None
        self.remote_tips = {}   # {iid: sha, "" = branch absent on origin}
        self.branches = []      # [(iid, sha)] for the for-each-ref seam
        self.counts = {}        # {iid: rev-list count}
        self.rev_list_calls = []

        def fake_push_resilient(repo, iid, remote_url, ssl_verify=True):
            self.pushes.append(int(iid))
            if self.push_error is not None:
                raise self.push_error
            return "pushed"

        def fake_remote_tip(repo, remote_url, branch, ssl_verify=True):
            iid = int(branch.removeprefix("issue/").removesuffix("-fix"))
            return self.remote_tips.get(iid, "")

        def fake_run(cmd, cwd, env=None):
            class P:
                returncode = 0
                stdout = ""
                stderr = ""
            args = cmd[1:] if cmd[0] == "git" else cmd
            if "for-each-ref" in args:
                P.stdout = "".join(
                    f"{sha} refs/heads/issue/{iid}-fix\n"
                    for iid, sha in self.branches)
            elif "rev-list" in args:
                iid = int(args[-1].removeprefix("refs/remotes/agent-remote/master..issue/")
                          .removesuffix("-fix"))
                self.rev_list_calls.append(iid)
                P.stdout = f"{self.counts.get(iid, 0)}\n"
            return P()

        monkeypatch.setattr(gitop, "push_fix_branch_resilient",
                            fake_push_resilient)
        monkeypatch.setattr(gitop, "remote_branch_tip", fake_remote_tip)
        monkeypatch.setattr(gitop, "_run", fake_run)

        # 汇报补发探针 seams（ag-a9af5e）＋ 派工/提交面哨兵：探针绝不可触发
        # opencode 回合或 has_new_commits_vs_ref（越界即 AssertionError 留痕）
        self.fix_pushed = False
        self.fix_pushed_error = None
        self.fix_pushed_calls = []
        self.exports = []          # [(session_id, cwd)] to export_last_report
        self.export_result = None
        self.turn_calls = []
        self.has_calls = []

        def fake_fix_pushed(repo, iid, remote_url, ssl_verify=True):
            self.fix_pushed_calls.append((repo, iid, remote_url))
            if self.fix_pushed_error is not None:
                raise self.fix_pushed_error
            return self.fix_pushed

        def fake_export(session_id, cwd, timeout_s=60, run=None):
            self.exports.append((session_id, cwd))
            return self.export_result

        def forbidden_turn(*args, **kwargs):
            self.turn_calls.append(args)
            raise AssertionError("sweep probe must never dispatch a turn")

        def forbidden_has(*args, **kwargs):
            self.has_calls.append(args)
            raise AssertionError("sweep probe must never probe commits")

        monkeypatch.setattr(gitop, "fix_branch_pushed", fake_fix_pushed)
        monkeypatch.setattr(runner, "export_last_report", fake_export)
        monkeypatch.setattr(runner, "new_session", forbidden_turn)
        monkeypatch.setattr(runner, "continue_session", forbidden_turn)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", forbidden_has)

    def sweep(self):
        self.orch._sweep_once()

    def store(self):
        return self.orch._sweep_store_load()

    def journal(self, *needles):
        return [m for m in self.journal_msgs if any(n in m for n in needles)]


# --------------------------------------------------------------------------
# skip rules
# --------------------------------------------------------------------------

class TestSkipRules:
    def test_skip_in_flight(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.orch._heartbeat_register(7)
        h.sweep()
        assert h.pushes == [] and h.api.posts == []
        assert h.journal("skip(in-flight)"), "journal: sweep: issue#7 skip(in-flight)"

    def test_skip_in_flight_ledger_claim(self, tmp_path, monkeypatch):
        """heartbeat_interval=0 deployments: the registry stays empty, the
        ledger's pending claim must gate the sweep alone."""
        h = Harness(tmp_path, monkeypatch)
        state.claim(h.state_path, 7, datetime.now(timezone.utc))
        h.branches = [(7, SHA_A)]
        h.sweep()
        assert h.pushes == [] and h.api.posts == []
        assert h.journal("skip(in-flight)")

    def test_skip_closed(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch, states={7: "closed"})
        h.branches = [(7, SHA_A)]
        h.counts = {7: 3}
        h.sweep()
        assert h.pushes == [] and h.api.posts == []
        assert h.journal("skip(closed)")

    def test_state_probe_failure_treated_open(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch, states={7: "__transient__"})
        h.branches = [(7, SHA_A)]
        h.counts = {7: 0}
        h.sweep()
        assert h.journal("state probe failed")
        assert h.orch._sweep_store_load()["swept_shas"] == {"7": SHA_A}, \
            "no debt -> head recorded"

    def test_skip_no_debt_sha_unchanged_zero_calls(self, tmp_path, monkeypatch):
        """sha unchanged and nothing owed: fully silent — no push, no post,
        not even the rev-list probe (the debounce fires first)."""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.orch._sweep_store_save({"pending_notes": {}, "swept_shas": {7: SHA_A}})
        h.counts = {7: 2}
        h.sweep()
        assert h.pushes == [] and h.api.posts == []
        assert h.rev_list_calls == []
        assert h.journal("skip(no-debt)")


# --------------------------------------------------------------------------
# payoff: push + flush + comment discipline
# --------------------------------------------------------------------------

class TestPayoff:
    def test_count_positive_triggers_push_and_comment(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 2}
        h.remote_tips = {7: SHA_B}  # origin differs -> push rewrites it
        h.sweep()
        assert h.pushes == [7]
        posted = h.api.posts[0][1]
        assert "补推 issue#7 2 提交" in posted
        store = h.store()
        assert store["swept_shas"] == {"7": SHA_A}
        assert store["pending_notes"] == {}

    def test_push_idempotent_no_comment_when_origin_current(self, tmp_path, monkeypatch):
        """First sighting of an already-pushed head: idempotent push, origin
        untouched -> no comment (纯空操作静默), head still recorded."""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 2}
        h.remote_tips = {7: SHA_A}
        h.sweep()
        assert h.pushes == [7]
        assert h.api.posts == []
        assert h.store()["swept_shas"] == {"7": SHA_A}

    def test_push_failure_keeps_debt(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 2}
        h.push_error = gitop.PushFailed("push issue/7-fix failed: boom")
        h.sweep()
        assert h.pushes == [7]
        assert h.api.posts == []
        assert h.store()["swept_shas"] == {}, "failed sweep must not record the sha"

    def test_sweep_flushed_pending_cleared_despite_push_failure(self, tmp_path, monkeypatch):
        """#119/#264 刷屏根因：留言已成功投递后，pending 清账不得被
        push 失败拦下——否则欠账原样保留，下轮 sweep 原样重发同一条。"""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 2}
        h.push_error = gitop.PushFailed("push issue/7-fix failed: boom")
        h.orch._sweep_owe_note(7, "被丢弃的老回复")
        h.sweep()
        assert h.pushes == [7]
        assert len(h.api.posts) == 1, "欠账留言投递成功"
        assert h.store()["pending_notes"] == {}, "投递成功即清账，与 push 成败无关"
        assert h.store()["swept_shas"] == {}, "push 失败不得记录 sha"
        h.sweep()
        assert len(h.api.posts) == 1, "再次 sweep 不得重发已投递留言"

    def test_pending_flush_and_clear(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 0}
        h.orch._sweep_owe_note(7, "被丢弃的老回复")
        h.sweep()
        assert h.pushes == []
        assert len(h.api.posts) == 1
        body = h.api.posts[0][1]
        assert "补发 issue#7 欠账留言" in body and "被丢弃的老回复" in body
        store = h.store()
        assert store["pending_notes"] == {}, "成功才清账"
        assert store["swept_shas"] == {"7": SHA_A}

    def test_flush_failure_keeps_ledger(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.api.fail_note_once = True
        h.branches = [(7, SHA_A)]
        h.counts = {7: 0}
        h.orch._sweep_owe_note(7, "被丢弃的老回复")
        h.sweep()
        assert h.api.posts == []
        assert h.store()["pending_notes"] == {"7": ["被丢弃的老回复"]}
        assert h.journal("note flush failed")

    def test_pending_flush_with_push_merged_comment(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 3}
        h.remote_tips = {7: SHA_B}
        h.orch._sweep_owe_note(7, "原 reply 正文")
        h.sweep()
        assert h.pushes == [7]
        body = h.api.posts[0][1]
        assert "补推 issue#7 3 提交＋原 reply 正文" in body
        assert "原 reply 正文" in body
        assert h.store()["pending_notes"] == {}

    def test_flush_keeps_concurrent_owed_reply(self, tmp_path, monkeypatch):
        """B-F1 回归：flush 的网络窗口内 worker 并发存账的新 reply 不得被
        整组 pop 丢掉——只清本次已投递的快照条目。"""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 0}
        h.orch._sweep_owe_note(7, "旧欠账")
        original_post = h.api.post_note

        def post_and_owe(project, iid, body):
            original_post(project, iid, body)
            h.orch._sweep_owe_note(7, "flush 期间新欠账")

        monkeypatch.setattr(h.api, "post_note", post_and_owe)
        h.sweep()
        assert h.store()["pending_notes"] == {"7": ["flush 期间新欠账"]}

    def test_flush_auth_dead_keeps_ledger(self, tmp_path, monkeypatch):
        """A-F2 回归：PAT 失效（auth 死道）绝不清账——欠账等认证恢复后补投。"""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 0}
        h.orch._sweep_owe_note(7, "欠账")

        def auth_dead(project, iid, body):
            raise api.ApiAuthError("gitea api rejected credentials: HTTP 401")

        monkeypatch.setattr(h.api, "post_note", auth_dead)
        h.sweep()
        assert h.store()["pending_notes"] == {"7": ["欠账"]}
        assert h.journal("comment channel unavailable")

    def test_iid_lock_held_by_running_turn_skips(self, tmp_path, monkeypatch):
        """B-F3 回归：heartbeat 关闭部署下，在飞回合持有 per-iid 锁，
        sweep 必须跳过而不是推走会话中间态。"""
        h = Harness(tmp_path, monkeypatch)
        h.branches = [(7, SHA_A)]
        h.counts = {7: 2}
        lock = h.orch._iid_lock(7)
        lock.acquire()
        try:
            h.sweep()
            assert h.pushes == [] and h.api.posts == []
            assert h.journal("skip(in-flight)")
        finally:
            lock.release()


# --------------------------------------------------------------------------
# defect A: the blocked path owes the reply
# --------------------------------------------------------------------------

class TestBlockedOwesReply:
    def test_push_failed_turn_stores_reply(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.state_path.write_text('{"issues": {}}')
        (tmp_path / "PROTOCOL.md").write_text("# protocol stub\n")
        monkeypatch.setattr(runner, "new_session", lambda *a, **k: runner.TurnResult(
            session_id="ses_1", reply_text="已修复但推送失败前的说明", duration_s=0.01,
            exit_code=0))
        monkeypatch.setattr(gitop, "fetch_base", lambda *a, **k: None)
        monkeypatch.setattr(gitop, "tracked_dirty", lambda repo: False)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", lambda *a, **k: True)
        monkeypatch.setattr(gitop, "push_fix_branch_resilient",
                            lambda *a, **k: (_ for _ in ()).throw(
                                gitop.PushFailed("push issue/9-fix failed: boom")))
        h.orch._handle_event({
            "action": "opened",
            "issue": {"number": 9, "title": "t", "body": "b", "state": "open"},
        })
        assert h.store()["pending_notes"] == {"9": ["已修复但推送失败前的说明"]}
        assert h.journal("reply owed")
        ledger = state.load_state(h.state_path)
        assert ledger["issues"]["9"]["status"] == "blocked"

    def test_push_failed_without_reply_owes_nothing(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.state_path.write_text('{"issues": {}}')
        (tmp_path / "PROTOCOL.md").write_text("# protocol stub\n")
        monkeypatch.setattr(runner, "new_session", lambda *a, **k: runner.TurnResult(
            session_id="ses_1", reply_text="", duration_s=0.01, exit_code=0))
        monkeypatch.setattr(gitop, "fetch_base", lambda *a, **k: None)
        monkeypatch.setattr(gitop, "tracked_dirty", lambda repo: False)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", lambda *a, **k: True)
        monkeypatch.setattr(gitop, "push_fix_branch_resilient",
                            lambda *a, **k: (_ for _ in ()).throw(
                                gitop.PushFailed("push issue/9-fix failed: boom")))
        h.orch._handle_event({
            "action": "opened",
            "issue": {"number": 9, "title": "t", "body": "b", "state": "open"},
        })
        assert h.store()["pending_notes"] == {}
        assert state.load_state(h.state_path)["issues"]["9"]["status"] == "blocked"

    def test_store_failure_still_blocks(self, tmp_path, monkeypatch):
        """B-F2 回归：存账写盘失败不得拦截 blocked 收尾（账本卡 pending）。"""
        h = Harness(tmp_path, monkeypatch)
        h.state_path.write_text('{"issues": {}}')
        (tmp_path / "PROTOCOL.md").write_text("# protocol stub\n")
        monkeypatch.setattr(runner, "new_session", lambda *a, **k: runner.TurnResult(
            session_id="ses_1", reply_text="说明", duration_s=0.01, exit_code=0))
        monkeypatch.setattr(gitop, "fetch_base", lambda *a, **k: None)
        monkeypatch.setattr(gitop, "tracked_dirty", lambda repo: False)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", lambda *a, **k: True)
        monkeypatch.setattr(gitop, "push_fix_branch_resilient",
                            lambda *a, **k: (_ for _ in ()).throw(
                                gitop.PushFailed("push issue/9-fix failed: boom")))

        def full_disk(path, store):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(h.orch, "_sweep_store_save", full_disk)
        h.orch._handle_event({
            "action": "opened",
            "issue": {"number": 9, "title": "t", "body": "b", "state": "open"},
        })
        assert h.journal("reply store failed")
        assert state.load_state(h.state_path)["issues"]["9"]["status"] == "blocked"


# --------------------------------------------------------------------------
# 汇报补发探针（ag-a9af5e）：active＋置位＋预算内的 issue，锁内消费预算→
# 锁外探测（前置/export/去重/补发）→终局锁内清旗。
# --------------------------------------------------------------------------

REPORT = "\n".join([
    "根因：回合结束于汇报产出前，三段式汇报未及产出",
    "",
    "改动文件清单：",
    "- .agent/issue-agent/orchestrator.py",
    "",
    "验证方式：python3 -m pytest .agent/issue-agent/tests/ -q 全绿",
    "",
    "沉淀：知识＝无新增；经验＝待人工审核",
])


def _seed_probe(h, iid=7, session_id="ses_1", count=0):
    """Active issue carrying an armed report probe (+ consumed budget)."""
    st = {"issues": {}}
    state.adopt_existing(st, {iid: [1]})
    entry = st["issues"][str(iid)]
    entry["status"] = "active"
    entry["session_id"] = session_id
    entry["report_probe_pending"] = True
    entry["report_probe_count"] = count
    state.save_state(h.state_path, st)


def _probe_ready(h):
    """Repost preconditions met: fix branch pushed + review loop ran."""
    h.fix_pushed = True
    (Path(h.orch.repo_path) / ".review-loop").mkdir(parents=True, exist_ok=True)


class TestReportProbe:
    def test_hit_posts_once_clears_flag_books_budget(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        _seed_probe(h)
        h.sweep()
        assert h.exports == [("ses_1", h.orch.repo_path)]
        assert len(h.api.posts) == 1 and REPORT in h.api.posts[0][1]
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "delivered -> flag cleared"
        assert entry["report_probe_count"] == 1, "budget booked"
        assert h.journal("auto-repost")
        assert h.turn_calls == [] and h.pushes == [] and h.has_calls == [], (
            "探针零派工/零推送/零提交面")

    def test_report_already_on_wall_posts_nothing_and_clears(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        h.api.comments = {7: [{"id": 9, "body": f"\U0001f916 {REPORT}\n{api.NOTE_MARKER}"}]}
        _seed_probe(h)
        h.sweep()
        assert h.api.posts == [], "已在墙零发"
        assert h.exports == [("ses_1", h.orch.repo_path)], "去重前仍需 export 取原文"
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "已在墙也清旗"
        assert h.journal("dup-skip")

    def test_miss_keeps_flag_second_probe_exhausts_budget(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = None  # session 里没有汇报
        _seed_probe(h)
        h.sweep()
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert entry["report_probe_pending"], "预算内未命中不清旗"
        assert entry["report_probe_count"] == 1
        assert h.journal("no report")
        h.sweep()
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "预算耗尽清旗"
        assert entry["report_probe_count"] == 2
        assert h.journal("probe exhausted")
        h.sweep()
        assert len(h.exports) == 2, "清旗后不再探"

    def test_unpushed_no_review_loop_session_still_delivers(self, tmp_path, monkeypatch):
        """B7（ag-21ea9f）：报告债与推分支/评审环解耦——未推分支＋无评审环
        ＋有 session → 照常走 export/墙读/补投路径（包裹外回合 server 侧
        续跑产报告不再被前置卡死）。"""
        h = Harness(tmp_path, monkeypatch)
        h.export_result = REPORT
        _seed_probe(h)  # 不调 _probe_ready：分支未推、.review-loop 不存在
        h.sweep()
        assert len(h.api.posts) == 1 and REPORT in h.api.posts[0][1]
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "delivered -> flag cleared"
        assert h.journal("auto-repost")
        assert h.fix_pushed_calls == [], "推送探测面已随前置解耦移除"

    def test_missing_session_id_no_export(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        _seed_probe(h, session_id=None)
        h.sweep()
        assert h.exports == [] and h.api.posts == []
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert entry["report_probe_pending"]
        assert entry["report_probe_count"] == 0, "session 缺前置不烧预算"

    def test_paused_not_probed(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        _seed_probe(h)
        st = state.load_state(h.state_path)
        st["issues"]["7"]["status"] = "paused"
        state.save_state(h.state_path, st)
        h.sweep()
        assert h.exports == [] and h.api.posts == []
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert entry["report_probe_pending"] and entry["report_probe_count"] == 0

    def test_auth_dead_post_keeps_flag(self, tmp_path, monkeypatch):
        """ocr R1-1 回归：_post_note 对 auth 死道返回 True——探针直连
        api.post_note，PAT 失效绝不把没发出去的汇报当送达清旗（同 flush
        纪律：sweep 的账只有真的落到 issue 上才清）。"""
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT

        def auth_dead(project, iid, body):
            raise api.ApiAuthError("gitea api rejected credentials: HTTP 401")

        monkeypatch.setattr(h.api, "post_note", auth_dead)
        _seed_probe(h)
        h.sweep()
        assert h.api.posts == []
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert entry["report_probe_pending"], "auth 死道不清旗"
        assert entry["report_probe_count"] == 1
        assert h.journal("comment channel unavailable")

    def test_deliver_exception_on_final_probe_clears_flag(self, tmp_path, monkeypatch):
        """ocr R1-2 回归：锁外执行面异常绕过 deliver 终局面时，except 兜底
        仍执行预算耗尽清旗——旗不永久卡死且 journal 有终局。"""
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)

        def exploding_export(session_id, cwd, timeout_s=60, run=None):
            raise RuntimeError("export exploded")

        monkeypatch.setattr(runner, "export_last_report", exploding_export)
        _seed_probe(h, count=1)
        h.sweep()
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "最后一轮异常 -> 清旗"
        assert entry["report_probe_count"] == 2
        assert h.journal("probe exhausted")

    def test_in_flight_turn_holds_iid_lock_probe_skips(self, tmp_path, monkeypatch):
        """终闸 B1/B2 回归：交付面与在飞回合同锁互斥——回合持有 iid 锁时
        探针跳过（不 export 不补发），预算已消费待下轮。"""
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        _seed_probe(h)
        lock = h.orch._iid_lock(7)
        lock.acquire()
        try:
            h.sweep()
            assert h.exports == [] and h.api.posts == []
            assert h.journal("skip(in-flight)")
            entry = state.load_state(h.state_path)["issues"]["7"]
            assert entry["report_probe_pending"], "skip 不清旗"
            assert entry["report_probe_count"] == 1, "预算已消费"
        finally:
            lock.release()

    def test_skip_at_exhausted_budget_clears_flag(self, tmp_path, monkeypatch):
        """终闸 R2-F2 回归：skip 面不走 deliver 终局面——连续 skip 消费到
        顶格时（count=2）skip 分支顺路终局清旗，不留 pending 卡死死态。"""
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT
        _seed_probe(h, count=1)  # 本次消费后即顶格
        lock = h.orch._iid_lock(7)
        lock.acquire()
        try:
            h.sweep()
        finally:
            lock.release()
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"], "顶格 skip 顺路清旗（死态闭环）"
        assert entry["report_probe_count"] == 2
        assert h.journal("probe exhausted")

    def test_wall_read_failure_keeps_flag_and_budget(self, tmp_path, monkeypatch):
        """评论墙读取失败（transient API）＝未送达且非真探测：不清旗也不烧
        预算，API 恢复后下轮应原价再探（issue#65 空烧实证回归）。"""
        h = Harness(tmp_path, monkeypatch)
        _probe_ready(h)
        h.export_result = REPORT

        def broken_wall(project, iid):
            raise api.ApiTransientError("gitea api unreachable")

        real_wall = h.api.list_issue_comments
        monkeypatch.setattr(h.api, "list_issue_comments", broken_wall)
        _seed_probe(h)
        h.sweep()
        assert h.api.posts == []
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert entry["report_probe_pending"]
        assert entry["report_probe_count"] == 0, "墙读失败不烧预算"
        assert h.journal("comment wall read failed")
        monkeypatch.setattr(h.api, "list_issue_comments", real_wall)
        h.sweep()  # API 恢复：下轮照常探测并送达
        assert len(h.api.posts) == 1 and REPORT in h.api.posts[0][1]
        entry = state.load_state(h.state_path)["issues"]["7"]
        assert not entry["report_probe_pending"]
        assert entry["report_probe_count"] == 1


# --------------------------------------------------------------------------
# wiring: config + thread lifecycle
# --------------------------------------------------------------------------

class TestWiring:
    def test_sweep_off_no_thread(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.orch.sweep_interval = 0
        h.orch.start()
        try:
            assert h.orch._sweep_thread is None, "0 = off: no sweep thread"
        finally:
            h.orch.stop()

    def test_start_spawns_and_stop_joins_sweep_thread(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)
        h.orch.start()
        try:
            assert h.orch._sweep_thread is not None
            assert h.orch._sweep_thread.name == "issue-agent-sweep"
        finally:
            h.orch.stop()
        assert h.orch._sweep_thread is None

    def test_daemon_threads_sweep_interval(self, tmp_path, monkeypatch):
        """daemon.main passes config.sweep_interval into the Orchestrator."""
        import signal
        import threading

        import orchestrator as orch_mod
        from config import Config

        captured = {}
        main_ident = threading.get_ident()

        class FakeOrch:
            def __init__(self, **kwargs):
                captured.update(kwargs)

            def start(self):
                threading.Timer(
                    0.05, signal.pthread_kill,
                    args=(main_ident, signal.SIGTERM)).start()
                return 8644

            def stop(self):
                pass

        cfg = Config(api_base="http://gitea", pat="t",
                     repo_path=str(tmp_path), callback_url="http://cb",
                     secret_token="s", project="grp/repo",
                     sweep_interval=90)
        monkeypatch.setattr(daemon, "load_config", lambda *a, **k: cfg)
        monkeypatch.setattr(daemon, "GitLabClient",
                            lambda *a, **k: types.SimpleNamespace())
        monkeypatch.setattr(orch_mod, "Orchestrator", FakeOrch)
        monkeypatch.setattr(daemon, "_STATE_DIR", tmp_path / "state")
        monkeypatch.setattr(daemon, "_render_protocol",
                            lambda *a, **k: tmp_path / "P")

        assert daemon.main() == 0
        assert captured["sweep_interval"] == 90
