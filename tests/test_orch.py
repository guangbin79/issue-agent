"""TDD tests for orchestrator.py — the serial turn pipeline + reconcile
(plan task 7, the race core).

Seams: FakeListener (real queue.Queue + order-recording start/stop), FakeApi
(canned issues/notes + post recorder), FakeRunner / FakeGitop (monkeypatched
module refs inside runner / gitop). The ledger (state.py) and the fsm are
REAL — no mocking of state or fsm, per the task contract.

Import note: same stdlib-http shadow dance as tests/test_listener.py; see the
comment there. This module imports orchestrator -> api -> urllib, so the
stdlib http package must be cached in sys.modules BEFORE PKG_DIR goes back
onto sys.path.
"""

import json
import os
import queue as queue_mod
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

PKG_DIR = Path(__file__).resolve().parent.parent

# Strip the issue-agent dir from sys.path BEFORE any http-dependent import,
# or our http.py shadows the stdlib http package (pytest puts PKG_DIR on
# sys.path as the tests package rootdir; `python -m pytest` adds the cwd).
sys.path[:] = [p for p in sys.path if p not in ("", str(PKG_DIR))]

import http.client  # noqa: E402,F401  (stdlib http must win before our http.py can shadow it)
import urllib.error  # noqa: E402,F401
import urllib.request  # noqa: E402,F401

sys.path.insert(0, str(PKG_DIR))

import api  # noqa: E402
import gitop  # noqa: E402
import orchestrator  # noqa: E402
import router as router_mod  # noqa: E402
import runner  # noqa: E402
import state  # noqa: E402
from agora_client import (AgoraProtocolError, AgoraRejected,  # noqa: E402
                          AgoraUnavailable)


# --------------------------------------------------------------------------
# event builders (webhook shapes)
# --------------------------------------------------------------------------

def issue_open_event(iid=11, title="构建失败", body="master 上跑不过测试"):
    return {
        "action": "opened",
        "issue": {"number": iid, "title": title, "body": body, "state": "open"},
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "baiguangbin"},
    }


def comment_event(iid=11, body="请尽快修复", comment_id=101, issue_state="open"):
    return {
        "action": "created",
        "issue": {"number": iid, "title": "构建失败", "body": "master 上跑不过测试",
                  "state": issue_state},
        "comment": {"id": comment_id, "body": body, "user": {"login": "someone"}},
        "sender": {"login": "someone"},
    }


def api_comment(comment_id, body):
    return {"id": comment_id, "body": body, "user": {"login": "someone"}}


# --------------------------------------------------------------------------
# fakes
# --------------------------------------------------------------------------

class FakeListener:
    def __init__(self, order):
        self.queue = queue_mod.Queue()
        self.order = order

    def start(self):
        self.order.append("listener-start")
        return 4242

    def stop(self):
        self.order.append("listener-stop")


class FakeApi:
    def __init__(self, issues=None, order=None, note_marker=None):
        self.issues = dict(issues or {})  # {iid: [api note dict, ...]}
        self.issue_records = {}           # optional {iid: {title/body override}}
        self.posts = []                    # (iid, body)
        self.order = order
        self.hooks = []                    # canned list_hooks payload
        self.hook_deletes = []             # hook ids passed to delete_hook
        self.hook_delete_error = None      # raise instead of deleting
        self.note_marker = note_marker or api.NOTE_MARKER
        self.since_seen = []               # since_iso kwargs seen per comments call
        self.closes = []                   # iids passed to close_issue
        self.close_error = None            # raise instead of closing

    def build_note_body(self, text):
        # same shape as GitLabClient.build_note_body (instance marker)
        return f"\U0001f916 {text}\n{self.note_marker}"

    def list_open_issues(self, project):
        out = []
        for iid in sorted(self.issues):
            record = {"number": iid, "title": f"issue {iid}", "body": "", "state": "open"}
            record.update(self.issue_records.get(iid, {}))
            out.append(record)
        return out

    def list_issue_comments(self, project, iid, since_iso=None):
        self.since_seen.append((int(iid), since_iso))
        return self.issues.get(int(iid), [])

    def close_issue(self, project, iid):
        if self.close_error is not None:
            raise self.close_error
        self.closes.append(int(iid))

    def post_note(self, project, iid, body):
        self.posts.append((int(iid), body))
        if self.order is not None:
            self.order.append("note")

    def list_hooks(self, project):
        return list(self.hooks)

    def delete_hook(self, project, hook_id):
        if self.hook_delete_error is not None:
            raise self.hook_delete_error
        self.hook_deletes.append(int(hook_id))

class FakeRunner:
    def __init__(self, order=None):
        self.order = order
        self.new_calls = []
        self.cont_calls = []
        self.new_result = self.cont_result = turn_result()
        self.new_error = None
        self.cont_error = None
        # task 8: the review phase spawns a FRESH new_session whose title
        # starts with "review#" — dispatch on that prefix so tests can fail
        # or answer the review session independently of the fix session.
        self.review_result = turn_result(sid="ses_rev", text="评审总结：无阻塞发现，门禁通过。")
        self.review_error = None
        self.gate = None  # threading.Event: block inside the runner (long turn)
        # 「重发汇报」export seam: records (session_id, cwd), returns *export_result*
        self.export_calls = []
        self.export_result = None

    def _maybe_block(self):
        if self.order is not None:
            self.order.append("runner")
        if self.gate is not None:
            self.gate.wait(timeout=5)

    def fake_new(self, cwd, title, message, timeout_s=None, run=None, on_event=None,
                 cred_prefix=None):
        self.new_calls.append({"cwd": cwd, "title": title, "message": message,
                               "timeout_s": timeout_s, "on_event": on_event,
                               "cred_prefix": cred_prefix})
        self._maybe_block()
        if title.startswith("review#"):
            if self.review_error is not None:
                raise self.review_error
            return self.review_result
        if self.new_error is not None:
            raise self.new_error
        return self.new_result

    def fake_cont(self, cwd, session_id, message, timeout_s=None, run=None, on_event=None,
                  cred_prefix=None):
        self.cont_calls.append({"cwd": cwd, "session_id": session_id, "message": message,
                                "timeout_s": timeout_s, "on_event": on_event,
                                "cred_prefix": cred_prefix})
        self._maybe_block()
        if self.cont_error is not None:
            raise self.cont_error
        return self.cont_result

    def fake_export(self, session_id, cwd, timeout_s=None, run=None):
        self.export_calls.append({"session_id": session_id, "cwd": cwd})
        return self.export_result
    def fake_new_sid(self, cwd, title, message, timeout_s=None, sid_path=None, popen=None,
                     on_event=None, cred_prefix=None):
        """Seam for runner.new_session_publishing_sid (the review phase):
        records the call — including sid_path and whether its parent dir
        already existed AT CALL TIME (proves makedirs runs before the runner)
        — and emulates the streaming sid publication of the real one."""
        call = {"cwd": cwd, "title": title, "message": message,
                "timeout_s": timeout_s, "sid_path": sid_path,
                "on_event": on_event, "cred_prefix": cred_prefix,
                "sid_dir_existed": bool(sid_path)
                and os.path.isdir(os.path.dirname(sid_path))}
        self.new_calls.append(call)
        self._maybe_block()
        if title.startswith("review#"):
            if sid_path and self.review_error is None \
                    and self.review_result.session_id:
                with open(sid_path, "w", encoding="utf-8") as handle:
                    handle.write(self.review_result.session_id + "\n")
            if self.review_error is not None:
                raise self.review_error
            return self.review_result
        if self.new_error is not None:
            raise self.new_error
        return self.new_result



class FakeGitop:
    """Seam for the daemon's thin git guarantee layer: fetch_base /
    tracked_dirty / has_new_commits_vs_ref / push_fix_branch /
    knowledge_changes_present (monkeypatched module refs inside gitop).
    Harness.gitop_argv records every argv that still reaches the REAL
    gitop._run, so tests can assert a turn performs ZERO other git
    operations (no checkout/branch/worktree/...)."""

    def __init__(self, order=None, fetch_error=None, commits=False, dirty=False):
        self.order = order
        self.fetch_error = fetch_error
        self.commits = commits
        self.dirty = dirty
        self.push_error = None
        self.fetch_calls = []   # (repo, remote_url, default_branch)
        self.dirty_calls = []   # repo
        self.has_calls = []     # (repo, iid, base_ref)
        self.push_calls = []    # iid
        self.knowledge_changes = False
        self.knowledge_error = None
        self.knowledge_calls = []  # (repo, iid, base_ref)
        # 「重发汇报」precondition seam
        self.fix_pushed = False
        self.fix_pushed_error = None
        self.fix_pushed_calls = []  # (repo, iid, remote_url)
        # agora 开发分支准备 seam（Task 3）
        self.ensure_calls = []      # (repo, iid, default_branch)
        self.ensure_error = None

    def fake_fetch(self, repo, remote_url, default_branch="master", ssl_verify=True):
        self.fetch_calls.append((repo, remote_url, default_branch))
        if self.order is not None:
            self.order.append("fetch")
        if self.fetch_error is not None:
            raise self.fetch_error
        return None

    def fake_tracked_dirty(self, repo):
        self.dirty_calls.append(repo)
        return self.dirty

    def fake_has_vs_ref(self, repo, iid, base_ref):
        self.has_calls.append((repo, iid, base_ref))
        return self.commits

    def fake_push(self, repo, iid, remote_url, ssl_verify=True):
        self.push_calls.append(iid)
        if self.order is not None:
            self.order.append("push")
        if self.push_error is not None:
            raise self.push_error
        return None

    def fake_knowledge_changes(self, repo, iid, base_ref):
        self.knowledge_calls.append((repo, iid, base_ref))
        if self.knowledge_error is not None:
            raise self.knowledge_error
        return self.knowledge_changes

    def fake_fix_branch_pushed(self, repo, iid, remote_url, ssl_verify=True):
        self.fix_pushed_calls.append((repo, iid, remote_url))
        if self.fix_pushed_error is not None:
            raise self.fix_pushed_error
        return self.fix_pushed

    def fake_ensure_fix_branch(self, repo, iid, default_branch="master",
                               remote_url=None, ssl_verify=True):
        self.ensure_calls.append((repo, iid, default_branch))
        if self.ensure_error is not None:
            raise self.ensure_error
        return "0" * 40


def agora_decision(action="dispatch", **overrides):
    """agora 测试缺省决策：dispatch 到可用模型。"""
    payload = dict(action=action, title="修复 #7",
                   goal="goal-v1\nIssue: #7", model="zai/glm-5.3",
                   reason="跨模块 bug 需强模型", raw={})
    payload.update(overrides)
    return router_mod.RouterDecision(**payload)


class FakeAgoraClient:
    """AgoraClient 替身：建单幂等语义照设计 §8.1——client_key 命中且执行
    payload（goal/project_dir/target_branch/verify_cmd/model/deliverable）
    相同则返回原任务，payload 不同则 AgoraRejected；create_failures 队列
    模拟「服务端已提交但客户端超时」。零网络。"""

    def __init__(self):
        self.tasks = {}          # client_key -> {"task", "payload"}
        self.create_calls = []   # 每次 create 请求的完整 kwargs
        self.created_ids = []    # 服务端真正新建的任务 id（去重后）
        self.create_failures = []   # 提交后抛出的异常队列（超时模拟）
        self.dispatch_calls = []
        self.dispatch_failures = []
        self.doctor_calls = 0
        self.doctor_result = {"models": ["zai/glm-5.3"]}
        # Task 5 持续对话面：post(kind=question) 直通与失败注入
        self.posts = []          # {"task", "kind", "body"}
        self.post_failures = []  # 提交时抛出的异常队列
        self.cancel_calls = []   # task_cancel 调用记账（关单语义必须为零）

    def post(self, tid, kind, body):
        if self.post_failures:
            raise self.post_failures.pop(0)  # 未送达：不记账（先记后抛会让断言虚绿）
        self.posts.append({"task": tid, "kind": kind, "body": body})

    def task_cancel(self, tid):
        self.cancel_calls.append(tid)

    def doctor(self):
        self.doctor_calls += 1
        if isinstance(self.doctor_result, Exception):
            raise self.doctor_result
        return self.doctor_result

    @staticmethod
    def _execution_payload(kwargs):
        return {key: kwargs[key] for key in
                ("goal", "project_dir", "target_branch", "verify_cmd",
                 "model", "deliverable")}

    def create_task(self, *, goal, project_dir, target_branch, verify_cmd,
                    model, title, client_key, deliverable="external_branch",
                    after=None):
        call = dict(goal=goal, project_dir=project_dir,
                    target_branch=target_branch, verify_cmd=verify_cmd,
                    model=model, title=title, client_key=client_key,
                    deliverable=deliverable)
        if after is not None:  # 同真实客户端：None 不含键（wire 形状）
            call["after"] = after
        self.create_calls.append(call)
        key = client_key
        execution = self._execution_payload(call)
        hit = self.tasks.get(key)
        if hit is not None:
            if hit["payload"] != execution:
                raise AgoraRejected(
                    f"client_key {key} reused with a different payload")
            return hit["task"]
        tid = f"ag-{123 + len(self.tasks)}"
        self.tasks[key] = {"task": tid, "payload": execution}
        self.created_ids.append(tid)
        if self.create_failures:  # 服务端已提交，回复没能送达客户端
            raise self.create_failures.pop(0)
        return tid

    def dispatch(self, tid):
        self.dispatch_calls.append(tid)
        if self.dispatch_failures:
            raise self.dispatch_failures.pop(0)


class FakeRouter:
    """SemanticRouter 替身：decide 记录入参并返回脚本化决策（或抛错）。"""

    def __init__(self, decision=None):
        self.decision = decision or agora_decision()
        self.calls = []
        self.error = None

    def decide(self, *, iid, event, issue_context, available_models):
        self.calls.append({"iid": iid, "event": event,
                           "issue_context": issue_context,
                           "available_models": available_models})
        if self.error is not None:
            raise self.error
        return self.decision


class FakeMonitor:
    """AgoraMonitor 替身：reserve/register/release/attach_delivery 调用记账
    （reserve 语义镜像真件：占位 tid=None、同 iid 重入复用己占名额）。
    Task 5 面：active_task 镜像登记簿反查（活动任务门控）、report_for
    返回脚本化持久报告、attach_conversation 捕获接线。"""

    def __init__(self, max_active=1):
        self.max_active = max_active
        self._active = {}
        self.registered = []
        self.released = []
        self.reserved = []
        self.attached = None
        self.conversation = None
        self.report = None  # report_for 脚本化返回（None＝无持久报告）
        self.started = 0
        self.stopped = 0

    def attach_delivery(self, **kwargs):
        self.attached = kwargs

    def attach_conversation(self, **kwargs):
        self.conversation = kwargs

    def active_task(self, iid):
        return self._active.get(int(iid))

    def report_for(self, iid):
        return self.report

    def reserve(self, iid):
        self.reserved.append(int(iid))
        if int(iid) in self._active:
            return True
        if len(self._active) >= self.max_active:
            return False
        self._active[int(iid)] = None
        return True

    def has_capacity(self):
        return len(self._active) < self.max_active

    def register(self, iid, tid):
        self._active[int(iid)] = tid
        self.registered.append((int(iid), tid))

    def release(self, iid, tid=None):
        """compare-and-release 镜像真件：单参只清占位；携 tid 仅当登记
        仍属该任务才释放。"""
        key = int(iid)
        current = self._active.get(key)
        if current is None and key not in self._active:
            return
        if tid is None:
            if current is not None:
                return
        elif current != tid:
            return
        del self._active[key]
        self.released.append(key)

    def start(self):
        self.started += 1

    def stop(self):
        self.stopped += 1

    def poll_once(self):
        return None



def turn_result(sid="ses_1", text="已定位并修复，详见分支。"):
    return runner.TurnResult(session_id=sid, reply_text=text, duration_s=0.01, exit_code=0)


# --------------------------------------------------------------------------
# harness
# --------------------------------------------------------------------------

class Harness:
    """One wired orchestrator over a tmp ledger with all I/O faked."""

    def __init__(self, tmp_path, monkeypatch, api_issues=None, start=True, **kwargs):
        self.order = []
        self.journal_msgs = []
        self.listener = FakeListener(self.order)
        self.api = FakeApi(api_issues, self.order,
                           note_marker=kwargs.pop("note_marker", None))
        self.runner = FakeRunner(self.order)
        self.gitop = FakeGitop(self.order)
        monkeypatch.setattr(runner, "new_session", self.runner.fake_new)
        monkeypatch.setattr(runner, "continue_session", self.runner.fake_cont)
        monkeypatch.setattr(runner, "new_session_publishing_sid", self.runner.fake_new_sid)
        monkeypatch.setattr(gitop, "fetch_base", self.gitop.fake_fetch)
        monkeypatch.setattr(gitop, "tracked_dirty", self.gitop.fake_tracked_dirty)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", self.gitop.fake_has_vs_ref)
        monkeypatch.setattr(gitop, "push_fix_branch", self.gitop.fake_push)
        monkeypatch.setattr(gitop, "knowledge_changes_present", self.gitop.fake_knowledge_changes)
        monkeypatch.setattr(gitop, "fix_branch_pushed", self.gitop.fake_fix_branch_pushed)
        monkeypatch.setattr(gitop, "ensure_fix_branch", self.gitop.fake_ensure_fix_branch)
        monkeypatch.setattr(runner, "export_last_report", self.runner.fake_export)
        # spy on the REAL git runner: with all five turn seams faked above,
        # anything still reaching _run is a git op the turn must not do
        self.gitop_argv = []
        real_run = gitop._run

        def spying_run(cmd, cwd, env=None):
            self.gitop_argv.append(cmd)
            return real_run(cmd, cwd, env=env)

        monkeypatch.setattr(gitop, "_run", spying_run)

        # capture verification reads this module constant at call time; the
        # tmp dir keeps every commits=True turn off the real HOME store
        self.exp_dir = tmp_path / "exp"
        monkeypatch.setattr(orchestrator, "EXPERIENCES_DIR", self.exp_dir)
        self.state_path = tmp_path / "state.json"
        self.protocol_path = tmp_path / "PROTOCOL.md"
        self.protocol_path.write_text("# protocol stub\n")
        (tmp_path / "repo").mkdir(exist_ok=True)  # D6 isdir guard must see a present repo
        self.repo_path = str(tmp_path / "repo")
        base = dict(
            project="group/repo",
            api=self.api,
            journal=self.journal_msgs.append,
            listener=self.listener,
            state_path=self.state_path,
            repo_path=self.repo_path,
            remote_url="https://oauth2:SECRET-PAT@fake.invalid/group/repo.git",
            protocol_path=str(self.protocol_path),
            reconcile_interval=600,
        )
        base.update(kwargs)
        self.orch = orchestrator.Orchestrator(**base)
        if start:
            self.orch.start()

    def journal(self, *needles):
        return [m for m in self.journal_msgs if any(n in m for n in needles)]

    def ledger(self):
        return state.load_state(self.state_path)

    def entry(self, iid):
        class _MissingOk(dict):
            def __missing__(self, key):
                return None
        return _MissingOk(self.ledger()["issues"].get(str(int(iid)), {}))


@pytest.fixture()
def harness(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    yield h
    h.orch.stop()


def wait_until(condition, timeout=3.0, interval=0.01):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return condition()


def seed_active(harness, iid=11, session_id="ses_1"):
    """Pre-track an issue as active (fsm ignores notes on untracked issues)."""
    st = state.load_state(harness.state_path)
    state.adopt_existing(st, {iid: [1]})
    entry = st["issues"][str(int(iid))]
    entry["status"] = "active"
    entry["session_id"] = session_id
    state.save_state(harness.state_path, st)


def turn_done(harness, iid, note_id):
    """True when the turn consuming *note_id* has fully finalized."""
    entry = harness.entry(iid)
    return (entry["status"] == "active" and entry["claimed_at"] is None
            and note_id in entry["processed_note_ids"])


def run_turn_happy(harness, iid=11):
    """Seed, then drive one webhook note through a full successful turn."""
    seed_active(harness, iid)
    harness.listener.queue.put(comment_event(iid=iid, comment_id=101))
    assert wait_until(lambda: turn_done(harness, iid, 101)), harness.journal_msgs


# --------------------------------------------------------------------------
# 1. happy path: new issue -> full turn (ack, gates, runner, push, reply, CAS)
# --------------------------------------------------------------------------

def test_new_issue_full_turn_flow(harness):
    harness.listener.queue.put(issue_open_event(iid=11))
    assert wait_until(lambda: harness.entry(11)["status"] == "active"), harness.journal_msgs

    assert len(harness.runner.new_calls) == 1, "exactly one opencode session"
    assert not harness.runner.cont_calls
    title = harness.runner.new_calls[0]["title"]
    assert title.startswith("issue#11")
    assert "构建失败" in title
    assert str(harness.protocol_path) in harness.runner.new_calls[0]["message"]

    bodies = [body for iid, body in harness.api.posts]
    assert any("已接手 issue#11" in b for b in bodies), "ack note posted"
    assert any("已定位并修复" in b for b in bodies), "reply note posted"
    assert all(harness.api.note_marker in b for b in bodies), "marker on every note"

    assert len(harness.gitop.fetch_calls) == 1, "baseline fetch ran once"
    assert harness.gitop.fetch_calls[0][0] == harness.repo_path
    assert harness.gitop.push_calls == [], "no push when no new commits"
    assert harness.gitop_argv == [], "no other git op may run"
    assert harness.runner.new_calls[0]["cwd"] == harness.repo_path, (
        "opencode sessions root in the MAIN repo; the agent makes its own worktree")

    entry = harness.entry(11)
    assert entry["session_id"] == "ses_1"
    assert entry["branch"] == "issue/11-fix"
    assert entry["claimed_at"] is None
    assert entry["defer_count"] == 0
    assert entry["status"] == "active"


def test_turn_pushes_and_finalizes_when_new_commits(harness):
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=12))
    assert wait_until(lambda: harness.entry(12)["status"] == "active")
    # task 8: fix push + the post-review re-push (the reviewer may commit
    # Trivial fixes on the same branch; a no-op push when it did not)
    assert harness.gitop.push_calls == [12, 12]
    assert harness.gitop_argv == [], "no other git op may run"


def test_turn_deferred_when_baseline_fetch_fails(harness):
    harness.gitop.fetch_error = gitop.GitopError("fetch failed (fake)")
    harness.listener.queue.put(issue_open_event(iid=13))
    assert wait_until(lambda: harness.entry(13)["status"] == "defer")
    assert harness.runner.new_calls == [], "no opencode run past the gate"
    assert any("已接手 issue#13" in b for _, b in harness.api.posts), "ack still posted"




# --------------------------------------------------------------------------
# 2. race: concurrent reconcile during a long turn starts no second session
# --------------------------------------------------------------------------

def test_concurrent_reconcile_during_long_turn_no_second_session(harness):
    run_turn_happy(harness, iid=11)  # issue 11 active, session ses_1, note 101 processed

    assert len(harness.runner.cont_calls) == 1, "first turn was a continue"
    harness.runner.gate = threading.Event()
    harness.api.issues = {11: [api_comment(101, "请尽快修复"), api_comment(102, "补充：日志在附件")]}
    harness.listener.queue.put(comment_event(iid=11, comment_id=102))
    assert wait_until(lambda: len(harness.runner.cont_calls) == 2), "second turn started"

    # reconcile fires WHILE the turn is blocked inside the runner
    harness.orch.reconcile()
    assert len(harness.runner.cont_calls) == 2, "claim protected: no second session"

    harness.runner.gate.set()
    assert wait_until(lambda: turn_done(harness, 11, 102)), harness.journal_msgs
    assert len(harness.runner.cont_calls) == 2
    assert 102 in harness.entry(11)["processed_note_ids"], "claim marked the note"


# --------------------------------------------------------------------------
# 3. race: same note delivered via webhook AND reconcile -> exactly one turn
# --------------------------------------------------------------------------

def test_same_note_dual_delivery_single_turn(harness):
    seed_active(harness)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs

    # the same note is still visible to a full reconcile replay
    harness.api.issues = {11: [api_comment(101, "请尽快修复")]}
    harness.orch.reconcile()

    total = len(harness.runner.new_calls) + len(harness.runner.cont_calls)
    assert total == 1, f"dual delivery must not run a second turn (got {total})"
    assert harness.entry(11)["status"] == "active"


# --------------------------------------------------------------------------
# 4. kill -9: orphan pending on restart is recycled to defer and requeued
# --------------------------------------------------------------------------

def test_orphan_pending_recycled_and_requeued_on_restart(tmp_path, monkeypatch):
    state_path = tmp_path / "state.json"
    st = {"issues": {"21": {
        "status": "pending", "session_id": "ses_old", "branch": "issue/21-fix",
        "processed_note_ids": [1], "last_note_id": 1,
        "claimed_at": "2020-01-01T00:00:00+00:00", "defer_count": 0,
    }}}
    state.save_state(state_path, st)

    harness = Harness(tmp_path, monkeypatch, api_issues={21: []}, start=False)
    harness.orch.start()
    try:
        assert wait_until(lambda: harness.entry(21)["status"] == "active",
                          timeout=3), harness.journal_msgs
        assert harness.journal("orphan pending")
        assert len(harness.runner.cont_calls) == 1, "requeued retry continues the old session"
        assert harness.runner.cont_calls[0]["session_id"] == "ses_old"
        assert harness.entry(21)["claimed_at"] is None
    finally:
        harness.orch.stop()


# --------------------------------------------------------------------------
# 5. close landing mid-turn: finalize CAS must not roll back done
# --------------------------------------------------------------------------

def test_close_during_turn_finalize_keeps_done(harness):
    run_turn_happy(harness, iid=11)

    assert len(harness.runner.cont_calls) == 1
    harness.runner.gate = threading.Event()
    harness.listener.queue.put(comment_event(iid=11, comment_id=102))
    assert wait_until(lambda: len(harness.runner.cont_calls) == 2), "turn 2 in flight"

    # issue 11 is closed on GitLab while the turn is blocked
    harness.api.issues = {}
    harness.orch.reconcile()
    assert harness.entry(11)["status"] == "done", "set-difference closed it mid-turn"

    harness.runner.gate.set()
    assert wait_until(lambda: len(harness.runner.cont_calls) == 2
                      and harness.runner.gate.is_set()
                      and harness.entry(11)["claimed_at"] is None), harness.journal_msgs
    assert harness.entry(11)["status"] == "done", "CAS finalize preserved done"


# --------------------------------------------------------------------------
# 6. seven consecutive defers -> help comment + blocked
# --------------------------------------------------------------------------

def test_seventh_consecutive_defer_blocks_with_help_note(harness):
    seed_active(harness)
    harness.gitop.fetch_error = gitop.GitopError("fetch failed (fake)")
    # Flake hardening: generous bounded deadline (30s vs the 3s default)
    # and monotonic >= predicates, so a loaded machine cannot make the
    # poll miss a step (defer_count only ever grows here).
    for n in range(1, 8):
        harness.listener.queue.put(comment_event(iid=11, comment_id=100 + n))
        if n < 7:
            assert wait_until(lambda n=n: (harness.entry(11)["defer_count"] or 0) >= n,
                              timeout=30), (f"defer #{n} landed: {harness.journal_msgs}")
    assert wait_until(lambda: harness.entry(11)["status"] == "blocked", timeout=30), (
        harness.journal_msgs)

    assert harness.entry(11)["defer_count"] == 7
    # _blocked saves the ledger BEFORE posting the help note: sampling the
    # posts list once can land in that window (same flake-hardening as the
    # status poll above — wait for the note, don't sample)
    assert wait_until(
        lambda: any("人工" in b for _, b in harness.api.posts), timeout=30), (
        harness.journal_msgs)
    helps = [b for _, b in harness.api.posts if "人工" in b]
    assert len(helps) == 1, "exactly one help comment"
    assert not harness.runner.new_calls and not harness.runner.cont_calls


# --------------------------------------------------------------------------
# 7. first-install baseline: adopted + note-id snapshot, second reconcile silent
# --------------------------------------------------------------------------

def test_first_install_baseline_and_second_reconcile_zero_turns(tmp_path, monkeypatch):
    harness = Harness(tmp_path, monkeypatch,
                      api_issues={11: [api_comment(101, "历史评论1"), api_comment(102, "历史评论2")],
                                 12: [api_comment(201, "另一条历史")]},
                      start=False)
    harness.orch.start()
    try:
        entry = harness.entry(11)
        assert entry["status"] == "adopted"
        assert entry["processed_note_ids"] == [101, 102]
        assert harness.entry(12)["processed_note_ids"] == [201]
        assert not harness.runner.new_calls and not harness.runner.cont_calls
        assert not harness.api.posts

        harness.orch.reconcile()  # full replay of the same data
        assert not harness.runner.new_calls and not harness.runner.cont_calls
        assert harness.entry(11)["status"] == "adopted"
        assert harness.entry(12)["status"] == "adopted"
    finally:
        harness.orch.stop()


# --------------------------------------------------------------------------
# 8. set-difference close covers paused/defer, leaves open issues alone
# --------------------------------------------------------------------------

def test_set_difference_close_marks_missing_open_issues_done(harness):
    st = {"issues": {}}
    state.adopt_existing(st, {31: [1], 32: [2], 33: [3], 34: [4]})
    st["issues"]["31"]["status"] = "active"
    st["issues"]["31"]["session_id"] = "ses_31"
    st["issues"]["32"]["status"] = "paused"
    st["issues"]["33"]["status"] = "defer"
    st["issues"]["34"]["status"] = "adopted"
    state.save_state(harness.state_path, st)

    harness.api.issues = {31: [api_comment(1, "open issue note")]}  # only 31 is open
    harness.orch.reconcile()

    assert harness.entry(31)["status"] == "active", "open issue untouched"
    assert harness.entry(32)["status"] == "done"
    assert harness.entry(33)["status"] == "done"
    assert harness.entry(34)["status"] == "adopted", "adopted left alone"


# --------------------------------------------------------------------------
# 9. advisory-2: startup order = baseline reconcile BEFORE webhook consumption
# --------------------------------------------------------------------------

def test_startup_baseline_runs_before_webhook_consumption(tmp_path, monkeypatch):
    harness = Harness(tmp_path, monkeypatch,
                      api_issues={11: [api_comment(101, "存量评论")]}, start=False)
    harness.orch.start()
    try:
        assert "listener-start" in harness.order
        assert harness.entry(11)["status"] == "adopted", "baseline ran before listener"

        # a NEW note wakes the adopted issue; the snapshot must keep 101 silent
        harness.listener.queue.put(comment_event(iid=11, comment_id=102, body="现在开始处理"))
        assert wait_until(lambda: len(harness.runner.new_calls) == 1), harness.journal_msgs
        assert wait_until(lambda: harness.entry(11)["status"] == "active")

        assert harness.order.index("listener-start") < harness.order.index("runner")
        assert harness.order.index("note") < harness.order.index("runner")
    finally:
        harness.orch.stop()


# --------------------------------------------------------------------------
# 10. ack note fires BEFORE the baseline fetch (ordering assertion)
# --------------------------------------------------------------------------

def test_ack_note_precedes_gates(harness):
    seed_active(harness)
    harness.gitop.fetch_error = gitop.GitopError("fetch failed (fake)")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: len(harness.gitop.fetch_calls) >= 1)
    assert wait_until(lambda: harness.entry(11)["status"] == "defer")

    assert harness.order.index("note") < harness.order.index("fetch"), (
        f"ack must precede the gate: {harness.order}")
    assert any("已接手 issue#11" in b for _, b in harness.api.posts)


# --------------------------------------------------------------------------
# 11. SessionInvalid -> explanatory comment + fresh session; TurnTimeout -> defer
# --------------------------------------------------------------------------

def test_session_invalid_new_session_and_explanatory_comment(harness):
    run_turn_happy(harness, iid=11)  # session ses_1 recorded

    harness.runner.cont_error = runner.SessionInvalid("session ses_1 not found")
    harness.listener.queue.put(comment_event(iid=11, comment_id=102))
    assert wait_until(lambda: turn_done(harness, 11, 102)), harness.journal_msgs

    assert len(harness.runner.cont_calls) == 2, "turn1 + the failed session attempt"
    assert len(harness.runner.new_calls) == 1, "fallback to a fresh session"
    bodies = [b for _, b in harness.api.posts]
    assert any("已接手 issue#11" in b for b in bodies)
    assert any("失效" in b and "新会话" in b for b in bodies), "explanatory comment posted"
    assert any("已定位并修复" in b for b in bodies)
    assert harness.entry(11)["session_id"] == "ses_1"


def test_turn_timeout_defers_and_recovers(harness):
    seed_active(harness)
    harness.runner.cont_error = runner.TurnTimeout("opencode exceeded 1800s")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: harness.entry(11)["status"] == "defer"), harness.journal_msgs
    assert harness.entry(11)["defer_count"] == 1
    assert harness.entry(11)["claimed_at"] is None
    assert not any("已定位并修复" in b for _, b in harness.api.posts), "no reply on timeout"

    # recovery: the issue is still open on GitLab, so reconcile requeues it
    harness.runner.cont_error = None
    harness.api.issues = {11: []}
    harness.orch.reconcile()
    assert wait_until(lambda: harness.entry(11)["status"] == "active"), harness.journal_msgs
    assert harness.entry(11)["defer_count"] == 0
    assert len(harness.runner.cont_calls) == 2, "retry continued the recorded session"


# --------------------------------------------------------------------------
# 12. pause / unpause ledger semantics (no turn, 恢复确认 note, held note)
# --------------------------------------------------------------------------

def test_pause_unpause_command_notes(harness):
    run_turn_happy(harness, iid=11)

    harness.listener.queue.put(comment_event(iid=11, body="暂停", comment_id=102))
    assert wait_until(lambda: harness.entry(11)["status"] == "paused")
    assert 102 in harness.entry(11)["processed_note_ids"], "command note consumed"

    harness.listener.queue.put(comment_event(iid=11, body="暂停期间的留言", comment_id=103))
    time.sleep(0.2)
    assert harness.entry(11)["status"] == "paused"
    total = len(harness.runner.new_calls) + len(harness.runner.cont_calls)
    assert total == 1, "no turn while paused"

    harness.listener.queue.put(comment_event(iid=11, body="恢复", comment_id=104))
    assert wait_until(lambda: harness.entry(11)["status"] == "active")
    # unpause 落账在锁内、UNPAUSE_NOTE 在锁外（orchestrator unpause 分支）：
    # status==active 与评论落地之间有窗口——等评论，不采样
    assert wait_until(
        lambda: any("恢复" in b for _, b in harness.api.posts)), (
        "🤖 恢复确认 note posted")
    total = len(harness.runner.new_calls) + len(harness.runner.cont_calls)
    assert total == 1, "unpause itself runs no turn"
    assert 104 in harness.entry(11)["processed_note_ids"]


def test_untracked_close_and_reopen_tolerated(harness):
    harness.listener.queue.put({"action": "closed", "issue": {"number": 999}})
    harness.listener.queue.put({"action": "reopened", "issue": {"number": 999}})
    time.sleep(0.2)
    assert harness.ledger()["issues"] == {}, "untracked close/reopen leaves no entry"
    assert harness.journal("untracked")


# --------------------------------------------------------------------------
# 「重发汇报」轻量补发回合（采集幂等 + 重发通道）
# --------------------------------------------------------------------------

REPOST_REPORT = "\n".join([
    "根因：轮末采集取字面末条 text part，被进度 ping 顶掉",
    "",
    "改动文件清单：",
    "- .agent/issue-agent/runner.py",
    "",
    "验证方式：python3 -m pytest .agent/issue-agent/tests/ -q 全绿",
    "",
    "沉淀：知识＝issue-agent 模块页；经验＝待人工审核",
])


def _repost_setup(harness, iid=11, session_id="ses_1", review_loop=True):
    """Seed an active issue with a session id and the repost preconditions."""
    seed_active(harness, iid=iid, session_id=session_id)
    if review_loop:
        (Path(harness.repo_path) / ".review-loop").mkdir(parents=True, exist_ok=True)
    harness.gitop.fix_pushed = True


def _repost(harness, iid=11, comment_id=300):
    harness.listener.queue.put(comment_event(iid=iid, body="重发汇报",
                                             comment_id=comment_id))


def test_repost_exports_and_reposts_report(harness):
    _repost_setup(harness)
    harness.runner.export_result = REPOST_REPORT
    _repost(harness, comment_id=301)
    assert wait_until(lambda: turn_done(harness, 11, 301)), harness.journal_msgs

    assert any(REPOST_REPORT in body for _, body in harness.api.posts), "汇报原文上墙"
    assert harness.runner.new_calls == [], "零派工"
    assert harness.runner.cont_calls == [], "零提交面（无 opencode 回合）"
    assert harness.gitop.push_calls == [], "零推送"
    assert harness.gitop.has_calls == [], "零提交检测面"
    assert harness.runner.export_calls == [
        {"session_id": "ses_1", "cwd": harness.repo_path}]
    assert harness.gitop.fix_pushed_calls, "前置探测被调用"
    entry = harness.entry(11)
    assert entry["status"] == "active" and entry["claimed_at"] is None, "认领闭环"


def test_repost_degrades_to_continue_when_branch_not_pushed(harness):
    _repost_setup(harness, review_loop=True)
    harness.gitop.fix_pushed = False
    _repost(harness, comment_id=302)
    assert wait_until(lambda: turn_done(harness, 11, 302)), harness.journal_msgs
    assert len(harness.runner.cont_calls) == 1, "降级为普通 continue 回合"
    assert harness.runner.export_calls == [], "前置不满足不 export"


def test_repost_degrades_without_review_loop_dir(harness):
    _repost_setup(harness, review_loop=False)
    _repost(harness, comment_id=303)
    assert wait_until(lambda: turn_done(harness, 11, 303)), harness.journal_msgs
    assert len(harness.runner.cont_calls) == 1, "降级为普通 continue 回合"
    assert harness.runner.export_calls == []


def test_repost_export_miss_posts_not_found_note(harness):
    _repost_setup(harness)
    harness.runner.export_result = None
    _repost(harness, comment_id=304)
    assert wait_until(lambda: turn_done(harness, 11, 304)), harness.journal_msgs

    assert any(orchestrator.REPOST_NOT_FOUND_NOTE in body
               for _, body in harness.api.posts), "未找到提示上墙"
    assert harness.runner.new_calls == [] and harness.runner.cont_calls == [], "零派工"
    entry = harness.entry(11)
    assert entry["status"] == "active" and entry["claimed_at"] is None


def test_repost_degrades_when_push_probe_errors(harness):
    _repost_setup(harness)
    harness.gitop.fix_pushed_error = gitop.GitopError("ls-remote failed")
    _repost(harness, comment_id=305)
    assert wait_until(lambda: turn_done(harness, 11, 305)), harness.journal_msgs
    assert len(harness.runner.cont_calls) == 1, "GitopError 降级普通回合"
    assert harness.runner.export_calls == []


def _seed_paused(harness, iid=11, session_id="ses_1"):
    seed_active(harness, iid=iid, session_id=session_id)
    st = state.load_state(harness.state_path)
    st["issues"][str(int(iid))]["status"] = "paused"
    state.save_state(harness.state_path, st)


def _paused_repost_done(harness, iid=11, note_id=400):
    entry = harness.entry(iid)
    return (entry["status"] == "paused" and entry["claimed_at"] is None
            and note_id in entry["processed_note_ids"])


def test_repost_on_paused_issue_reposts_but_keeps_pause(harness):
    """暂停纪律：paused issue 的重发汇报补发提示文本，但绝不解除暂停。"""
    _seed_paused(harness)
    (Path(harness.repo_path) / ".review-loop").mkdir(parents=True, exist_ok=True)
    harness.gitop.fix_pushed = True
    harness.runner.export_result = REPOST_REPORT
    _repost(harness, comment_id=401)
    assert wait_until(lambda: _paused_repost_done(harness, note_id=401)), (
        harness.journal_msgs)
    assert any(REPOST_REPORT in body for _, body in harness.api.posts), "汇报原文上墙"
    assert harness.runner.new_calls == [] and harness.runner.cont_calls == [], "零派工"
    assert harness.entry(11)["status"] == "paused", "补发不解除暂停"


def test_repost_on_paused_issue_never_degrades_to_turn(harness):
    """暂停纪律：前置不满足的降级绝不对 paused issue 派工——保持暂停闭环。"""
    _seed_paused(harness)
    harness.gitop.fix_pushed = False
    _repost(harness, comment_id=402)
    assert wait_until(lambda: _paused_repost_done(harness, note_id=402)
                      and harness.journal("paused, no fallback turn")), (
        harness.journal_msgs)
    assert harness.runner.new_calls == [] and harness.runner.cont_calls == [], (
        "paused 降级不派工")
    assert harness.runner.export_calls == [], "前置不满足不 export"
    assert harness.journal("paused, no fallback turn")


# --------------------------------------------------------------------------
# 26. 汇报补发探针：置位面（唯一）＋ 清欠面（所有权转移即清欠，ag-a9af5e）
# --------------------------------------------------------------------------

def _seed_probe_fields(harness, iid=11, session_id="ses_1", count=1):
    """Seed an active issue carrying an armed report probe + used budget."""
    seed_active(harness, iid=iid, session_id=session_id)
    st = state.load_state(harness.state_path)
    entry = st["issues"][str(int(iid))]
    entry["report_probe_pending"] = True
    entry["report_probe_count"] = count
    state.save_state(harness.state_path, st)


def test_fragment_reply_arms_report_probe(harness):
    """回合收队时回复非三段式汇报（非「范围外」开头、session 非空）→ 置位。"""
    run_turn_happy(harness, iid=11)
    entry = harness.entry(11)
    assert entry["report_probe_pending"] is True
    assert entry["report_probe_count"] == 0
    assert harness.journal("probe: issue#11 armed"), harness.journal_msgs


def test_arm_report_probe_resets_stale_budget(harness):
    """ocr R1-3 回归：置位面自恢复预算——stale count 归零，不依赖
    _claim 先 pop 的隐式排序。"""
    seed_active(harness, session_id="ses_1")
    st = state.load_state(harness.state_path)
    st["issues"]["11"]["report_probe_count"] = 2
    state.save_state(harness.state_path, st)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    entry = harness.entry(11)
    assert entry["report_probe_pending"] is True
    assert entry["report_probe_count"] == 0


def test_report_reply_does_not_arm_report_probe(harness):
    seed_active(harness)
    harness.runner.cont_result = turn_result(sid="ses_1", text=REPOST_REPORT)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert not harness.entry(11)["report_probe_pending"]


def test_out_of_scope_reply_does_not_arm_report_probe(harness):
    """协议第 0 步合法无汇报回复（「范围外」开头）→ 不置位。"""
    seed_active(harness)
    harness.runner.cont_result = turn_result(
        sid="ses_1", text="范围外：此需求属运维事项，建议转对应仓库。")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert not harness.entry(11)["report_probe_pending"]


# --------------------------------------------------------------------------
# 26b. ag-21ea9f B6: 「范围外」回复上墙后自动关单（fail-open）
# --------------------------------------------------------------------------

_OUT_OF_SCOPE_REPLY = "范围外：此需求属运维事项，建议转对应仓库。"


def test_out_of_scope_reply_closes_issue(harness):
    """B6 ①：「范围外」开头（含前导空白）回复投递成功 → close_issue 被调。"""
    seed_active(harness)
    harness.runner.cont_result = turn_result(
        sid="ses_1", text="  " + _OUT_OF_SCOPE_REPLY)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert harness.api.closes == [11]
    assert not harness.entry(11)["report_probe_pending"], (
        "关单与不置位同口径：前导空白范围外回复不欠汇报")


def test_normal_reply_does_not_close_issue(harness):
    """B6 ②：普通回复不关单。"""
    run_turn_happy(harness, iid=11)
    assert harness.api.closes == []


def test_close_issue_failure_does_not_break_turn(harness):
    """B6 ③：close 抛错只 journal 不上抛——回合照常收队，评论已投递。"""
    seed_active(harness)
    harness.api.close_error = api.ApiTransientError("gitea api unreachable")
    harness.runner.cont_result = turn_result(
        sid="ses_1", text=_OUT_OF_SCOPE_REPLY)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert harness.api.closes == [], "失败不算关单"
    assert any(_OUT_OF_SCOPE_REPLY in b for _, b in harness.api.posts), (
        "评论已投递优先")
    assert harness.journal("out-of-scope close failed"), harness.journal_msgs


def test_push_failed_path_does_not_arm_report_probe(harness):
    """PushFailed → blocked（回复另有 sweep 欠账面）→ 不置位。"""
    seed_active(harness)
    harness.gitop.commits = True
    harness.gitop.push_error = gitop.PushFailed("push issue/11-fix failed (fake)")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: harness.entry(11)["status"] == "blocked", timeout=30), (
        harness.journal_msgs)
    assert not harness.entry(11)["report_probe_pending"]


def test_new_claim_clears_report_probe_fields(harness):
    """_claim 清欠：新回合接管即清旗＋清预算（回合自身回复吸收欠账）。"""
    _seed_probe_fields(harness, count=1)
    # is_report 命中的回复不再置位 → 终态干净，清欠可见
    harness.runner.cont_result = turn_result(sid="ses_1", text=REPOST_REPORT)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert not harness.entry(11)["report_probe_pending"], "claim cleared the flag"
    assert harness.entry(11)["report_probe_count"] == 0, "claim reset the budget"


def test_blocked_clears_report_probe_fields(harness):
    """_blocked 清欠：人工接管，旗与预算不残留。"""
    _seed_probe_fields(harness, count=1)
    harness.runner.cont_result = turn_result(sid="ses_1", text="")  # no reply -> blocked
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: harness.entry(11)["status"] == "blocked", timeout=30), (
        harness.journal_msgs)
    assert not harness.entry(11)["report_probe_pending"]
    assert harness.entry(11)["report_probe_count"] == 0


def test_webhook_close_clears_report_probe_fields(harness):
    _seed_probe_fields(harness, count=1)
    harness.listener.queue.put({"action": "closed",
                                "issue": {"number": 11, "state": "closed"}})
    assert wait_until(lambda: harness.entry(11)["status"] == "done"), \
        harness.journal_msgs
    assert not harness.entry(11)["report_probe_pending"]
    assert harness.entry(11)["report_probe_count"] == 0


def test_reconcile_close_clears_report_probe_fields(harness):
    """reconcile set-difference close（另一处 close 面）同点清欠。"""
    _seed_probe_fields(harness, count=1)
    harness.api.issues = {}  # issue left the open set
    harness.orch.reconcile()
    assert harness.entry(11)["status"] == "done"
    assert not harness.entry(11)["report_probe_pending"]
    assert harness.entry(11)["report_probe_count"] == 0


# --------------------------------------------------------------------------
# 13. lifecycle: worker drives periodic reconcile; stop() joins cleanly
# --------------------------------------------------------------------------

def test_periodic_reconcile_timer_fires(tmp_path, monkeypatch):
    harness = Harness(tmp_path, monkeypatch, api_issues={}, reconcile_interval=0.15,
                     start=False)
    harness.orch.start()
    try:
        assert wait_until(lambda: len(harness.journal("reconcile:")) >= 3, timeout=3), (
            harness.journal_msgs)
    finally:
        harness.orch.stop()
    assert "listener-stop" in harness.order


# --------------------------------------------------------------------------
# 14. claim atomicity: note ids + pending + claimed_at land in ONE write
# --------------------------------------------------------------------------

def test_claim_marks_notes_and_pending_in_single_write(tmp_path, monkeypatch):
    """The claim write must carry status=pending + claimed_at + this turn's
    note ids in ONE save_state payload — a pending write without its note
    ids would let a concurrent reconcile replay the same note as a second
    turn (the exact race claim-before-run exists to close)."""
    written = []  # every payload handed to save_state, in order
    real_save = state.save_state

    def spying_save(path, st):
        written.append(json.loads(json.dumps(st, ensure_ascii=False)))
        return real_save(path, st)

    monkeypatch.setattr(state, "save_state", spying_save)
    harness = Harness(tmp_path, monkeypatch)
    seed_active(harness)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs

    pendings = [p for p in written
                if p.get("issues", {}).get("11", {}).get("status") == "pending"]
    assert pendings, f"no pending write at all: {written}"
    for payload in pendings:
        entry = payload["issues"]["11"]
        assert entry["claimed_at"] is not None
        assert 101 in entry["processed_note_ids"], (
            "a pending write without its note ids would allow a duplicate turn")


# --------------------------------------------------------------------------
# 15. F17: the listener port opens BEFORE the baseline reconcile runs
# --------------------------------------------------------------------------

def test_listener_port_opens_before_baseline_reconcile(tmp_path, monkeypatch):
    harness = Harness(tmp_path, monkeypatch,
                      api_issues={11: [api_comment(101, "存量")]}, start=False)
    seen = {}
    real_reconcile = harness.orch.reconcile

    def spying_reconcile():
        seen["listener_started"] = "listener-start" in harness.order
        return real_reconcile()

    monkeypatch.setattr(harness.orch, "reconcile", spying_reconcile)
    try:
        harness.orch.start()
        assert seen["listener_started"] is True, (
            "port must be open before the (possibly hours-long) baseline reconcile")
        assert harness.entry(11)["status"] == "adopted", "baseline still snapshots"
    finally:
        harness.orch.stop()


# --------------------------------------------------------------------------
# 16. F18: fresh session ALSO SessionInvalid -> blocked, no recursion
# --------------------------------------------------------------------------

def test_double_session_invalid_blocks_without_recursion(harness):
    seed_active(harness)
    harness.runner.cont_error = runner.SessionInvalid("session ses_1 not found")
    harness.runner.new_error = runner.SessionInvalid(
        "no session found in store")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: harness.entry(11)["status"] == "blocked", timeout=30), (
        harness.journal_msgs)
    # the daemon commits nothing anywhere anymore — no WIP path to assert; the
    # ledger flips to blocked BEFORE the help note posts — wait for the note
    assert wait_until(lambda: any("需要人工介入" in b for _, b in harness.api.posts),
                      timeout=30), harness.journal_msgs


# --------------------------------------------------------------------------
# 17. TurnTimeout still defers (WIP-committing is the AGENT's business now;
#     the daemon writes nothing into any working copy)
# --------------------------------------------------------------------------

def test_turn_timeout_defers_without_daemon_git_writes(harness):
    seed_active(harness)
    harness.runner.cont_error = runner.TurnTimeout("opencode exceeded 1800s")
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: harness.entry(11)["status"] == "defer", timeout=30), (
        harness.journal_msgs)
    assert harness.gitop.push_calls == []
    assert harness.gitop_argv == [], "timeout cleanup must not write via git"

# --------------------------------------------------------------------------
# 18. F20: a failed reconcile still advances the interval timestamp
# --------------------------------------------------------------------------

def test_failed_reconcile_keeps_interval_pacing(harness):
    import api as api_mod

    def broken_list(project):
        raise api_mod.ApiTransientError("gitlab down")

    harness.api.list_open_issues = broken_list
    before = harness.orch._last_reconcile
    harness.orch.reconcile()
    assert harness.orch._last_reconcile > before, (
        "early return without pacing turns an API outage into a 1 Hz reconcile storm")


# --------------------------------------------------------------------------
# 18b. issue#51: one issue's comment wall exploding (pagination cap) must not
#      abort the whole reconcile — journal that issue, empty notes, keep going
# --------------------------------------------------------------------------

def test_fetch_notes_single_issue_transient_does_not_abort(harness, monkeypatch):
    """稳态（已 baselined）单 issue 评论墙炸只损失该 issue 的评论：好 issue
    照常返回、炸的置空列表继续；首跑（未 baselined）保持整体 abort 不动。"""
    real_comments = harness.api.list_issue_comments

    def flaky_comments(project, iid, since_iso=None):
        if int(iid) == 51:
            raise api.ApiTransientError(
                "gitea api pagination exceeded 50 pages: GET .../issues/51/comments")
        return real_comments(project, iid, since_iso)

    monkeypatch.setattr(harness.api, "list_issue_comments", flaky_comments)
    harness.api.issues = {7: [api_comment(101, "hello")],
                          51: [api_comment(201, "late reply")]}
    st = harness.ledger()
    st["baselined"] = True
    state.save_state(harness.state_path, st)
    issues, notes = harness.orch._fetch_open_issues_and_notes()
    assert sorted(i["number"] for i in issues) == [7, 51]
    assert [c["id"] for c in notes[7]] == [101], "好 issue 照常返回"
    assert notes[51] == [], "炸的 issue 置空列表继续"
    assert harness.journal("reconcile: issue#51 comments unavailable")

    st["baselined"] = False
    state.save_state(harness.state_path, st)
    assert harness.orch._fetch_open_issues_and_notes() is None, (
        "首跑（未 baselined）快照必须全量：维持整体 abort 不动")
    assert harness.journal("reconcile: aborted, API transient failure")


# --------------------------------------------------------------------------
# 18c. ag-21ea9f B4: reconcile incremental comment pulls via the since store
# --------------------------------------------------------------------------

def _spy_since(harness, monkeypatch):
    """Swap in a comments stub that records the since_iso kwarg per call."""
    real_comments = harness.api.list_issue_comments
    seen = []

    def spy_comments(project, iid, since_iso=None):
        seen.append((int(iid), since_iso))
        return real_comments(project, iid, since_iso)

    monkeypatch.setattr(harness.api, "list_issue_comments", spy_comments)
    return seen


def test_fetch_steady_state_pulls_since_with_overlap_and_rewrites(harness, monkeypatch):
    """B4 ①③：稳态（baselined）拉取传「since 存储时刻−OVERLAP」；成功返回前
    since 文件重写为当前时刻；下次拉取读取新时刻继续带 OVERLAP 回退。"""
    harness.api.issues = {7: [api_comment(101, "hello")]}
    st = harness.ledger()
    st["baselined"] = True
    state.save_state(harness.state_path, st)
    stored = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
    harness.orch._since_store_save(stored)

    seen = _spy_since(harness, monkeypatch)
    harness.orch._fetch_open_issues_and_notes()
    assert seen == [(7, (stored - timedelta(
        seconds=orchestrator.RECONCILE_SINCE_OVERLAP_S)).isoformat())], (
        "稳态拉取必须携带 OVERLAP 回退后的 since")

    with open(harness.orch._since_path, encoding="utf-8") as fh:
        rewritten = datetime.fromisoformat(json.load(fh))
    assert rewritten > stored, "成功返回前 since 文件已重写为拉取时刻"

    seen.clear()
    harness.orch._fetch_open_issues_and_notes()
    assert len(seen) == 1 and seen[0][1] is not None, "下次拉取仍走增量"
    age = (datetime.now(timezone.utc)
           - datetime.fromisoformat(seen[0][1])).total_seconds()
    assert orchestrator.RECONCILE_SINCE_OVERLAP_S <= age <= \
        orchestrator.RECONCILE_SINCE_OVERLAP_S + 10, (
        "下次读取＝新存储时刻−OVERLAP")


def test_fetch_first_run_pulls_full_without_since(harness, monkeypatch):
    """B4 ②：未 baselined（首跑）传 None 全量，语义不动；成功后仍落 since
    文件（下轮起转增量）。"""
    st = harness.ledger()
    st["baselined"] = False
    state.save_state(harness.state_path, st)
    if os.path.exists(harness.orch._since_path):
        os.remove(harness.orch._since_path)
    harness.api.issues = {7: [api_comment(101, "存量评论")]}

    seen = _spy_since(harness, monkeypatch)
    harness.orch._fetch_open_issues_and_notes()
    assert seen == [(7, None)], "首跑保持全量（since=None）"
    assert os.path.exists(harness.orch._since_path), (
        "成功返回前 since 文件已落盘（下轮起转增量）")


def test_fetch_corrupt_since_store_falls_back_to_full(harness, monkeypatch):
    """B4 ④：since 文件损坏 → journal ＋ 本次全量（None）；本次成功后文件
    已被重写，下轮自动恢复增量。"""
    harness.api.issues = {7: [api_comment(101, "hello")]}
    st = harness.ledger()
    st["baselined"] = True
    state.save_state(harness.state_path, st)
    with open(harness.orch._since_path, "w", encoding="utf-8") as fh:
        fh.write("{not json")

    seen = _spy_since(harness, monkeypatch)
    harness.orch._fetch_open_issues_and_notes()
    assert seen == [(7, None)], "损坏 → 全量兜底"
    assert harness.journal("since store unusable"), harness.journal_msgs
    with open(harness.orch._since_path, encoding="utf-8") as fh:
        json.load(fh)  # 不抛即已重写（下轮恢复增量）


def test_fetch_partial_transient_holds_since_store(harness, monkeypatch):
    """评审修复回归：稳态下某 issue 评论墙瞬态失败（置空继续）→ since 游标
    不前移，下轮从旧窗口重拉（「下轮自然重拉」的全量语义不破）；下轮全好
    才落新游标。"""
    harness.api.issues = {7: [api_comment(101, "hello")],
                          51: [api_comment(201, "late reply")]}
    st = harness.ledger()
    st["baselined"] = True
    state.save_state(harness.state_path, st)
    stored = datetime(2026, 11, 1, 12, 0, 0, tzinfo=timezone.utc)
    harness.orch._since_store_save(stored)
    real_comments = harness.api.list_issue_comments

    def flaky_comments(project, iid, since_iso=None):
        if int(iid) == 51:
            raise api.ApiTransientError("gitea api unreachable")
        return real_comments(project, iid, since_iso)

    monkeypatch.setattr(harness.api, "list_issue_comments", flaky_comments)
    issues, notes = harness.orch._fetch_open_issues_and_notes()
    assert notes[51] == [] and notes[7], "失败置空继续、好 issue 照常"
    with open(harness.orch._since_path, encoding="utf-8") as fh:
        assert json.load(fh) == stored.isoformat(), "有拉缺 → 游标不前移"

    monkeypatch.setattr(harness.api, "list_issue_comments", real_comments)
    harness.orch._fetch_open_issues_and_notes()
    with open(harness.orch._since_path, encoding="utf-8") as fh:
        assert json.load(fh) != stored.isoformat(), "全量成功 → 落新游标"


# --------------------------------------------------------------------------
# 19. polling fallback: issues opened AFTER the one-time baseline run turns
# --------------------------------------------------------------------------

def test_reconcile_after_baseline_new_issue_runs_turn(tmp_path, monkeypatch):
    """First reconcile adopts (marker set); a LATER reconcile seeing a new
    untracked issue must feed a synthetic 'opened' event through _handle_event
    -> fsm new -> claim -> turn (fresh session), not adopt it silently."""
    harness = Harness(tmp_path, monkeypatch,
                      api_issues={11: [api_comment(101, "存量评论")]}, start=False)
    harness.orch.start()
    try:
        assert harness.entry(11)["status"] == "adopted"
        assert harness.journal("baseline adopted 1 issue(s) (first run)")

        harness.api.issues[12] = [api_comment(201, "新 issue 的评论")]
        harness.orch.reconcile()
        assert harness.journal("new untracked issue(s)"), harness.journal_msgs
        assert wait_until(lambda: harness.entry(12)["status"] == "active"), (
            harness.journal_msgs)
        assert len(harness.runner.new_calls) == 1, "new issue started a fresh session"
        assert harness.runner.new_calls[0]["title"].startswith("issue#12:")
        assert any("已接手 issue#12" in b for _, b in harness.api.posts)
    finally:
        harness.orch.stop()


def test_restart_does_not_adopt_new_untracked_issue(tmp_path, monkeypatch):
    """A daemon restart (fresh Orchestrator, same state file) must NOT re-adopt:
    the persisted 'baselined' marker routes a new untracked issue into a real
    turn instead of the first-install adoption path."""
    state_path = tmp_path / "state.json"
    first = Harness(tmp_path, monkeypatch, api_issues={11: []}, start=False)
    first.orch.start()
    first.orch.stop()
    assert first.journal("baseline adopted")
    assert state.load_state(state_path).get("baselined") is True

    second = Harness(tmp_path, monkeypatch, api_issues={11: [], 12: []}, start=False)
    second.orch.start()
    try:
        assert second.journal("baseline adopted") == [], "restart re-adopted!"
        assert wait_until(lambda: second.entry(12)["status"] == "active"), (
            second.journal_msgs)
        assert len(second.runner.new_calls) == 1
        assert second.entry(11)["status"] == "adopted", "pre-baseline issue untouched"
    finally:
        second.orch.stop()


def test_baselined_ledger_with_tracked_issues_no_readoption(harness):
    """baselined=True + fully tracked issues: reconcile is a no-op — no
    re-adoption, no turns, ledger unchanged."""
    st = {"issues": {}, "baselined": True}
    state.adopt_existing(st, {11: [101]})
    st["issues"]["11"]["status"] = "active"
    st["issues"]["11"]["session_id"] = "ses_1"
    state.save_state(harness.state_path, st)

    before = len(harness.journal_msgs)
    harness.api.issues = {11: [api_comment(101, "已处理的存量评论")]}
    harness.orch.reconcile()

    entry = harness.entry(11)
    assert entry["status"] == "active" and entry["session_id"] == "ses_1"
    assert entry["processed_note_ids"] == [101]
    total = len(harness.runner.new_calls) + len(harness.runner.cont_calls)
    assert total == 0, "nothing to do for an unchanged tracked issue"
    during = "\n".join(harness.journal_msgs[before:])
    assert "baseline adopted" not in during, "re-adoption ran"
    assert "new untracked" not in during


def test_synthetic_event_carries_title_and_body(tmp_path, monkeypatch):
    """The synthetic opened event must carry the API issue title/body so
    prompt building works exactly like a webhook 'opened' delivery."""
    harness = Harness(tmp_path, monkeypatch, api_issues={}, start=False)
    harness.orch.start()
    try:
        assert harness.journal("baseline adopted")
        harness.api.issues[13] = []
        harness.api.issue_records[13] = {"title": "修复登录超时", "body": "登录 30s 后超时"}
        harness.orch.reconcile()
        assert harness.runner.new_calls, harness.journal_msgs
        call = harness.runner.new_calls[0]
        assert call["title"].startswith("issue#13:")
        assert "修复登录超时" in call["title"]
        assert "登录 30s 后超时" in call["message"], "issue body feeds the prompt"
    finally:
        harness.orch.stop()




# --------------------------------------------------------------------------
# 20. no-commit turn: nothing pushed, daemon git surface = one fetch
# --------------------------------------------------------------------------


def test_no_commit_turn_pushes_nothing_and_runs_no_other_git(harness):
    """A turn whose agent makes NO commits (e.g. out-of-scope rejection) must
    not push and must leave the daemon's git surface at exactly one fetch —
    branch/worktree management is entirely the agent's business now."""
    seed_active(harness)
    harness.listener.queue.put(comment_event(iid=11, comment_id=101))
    assert wait_until(lambda: turn_done(harness, 11, 101)), harness.journal_msgs
    assert len(harness.gitop.fetch_calls) == 1, "baseline fetch ran"
    assert harness.gitop.push_calls == [], "no push on a commitless turn"
    assert harness.gitop_argv == [], "no checkout/branch/worktree anywhere"
    assert harness.journal("no new commits")


# --------------------------------------------------------------------------
# 21. D6 self-cancel: main repo deleted -> warn once, tear down on miss #2
# --------------------------------------------------------------------------


def _self_cancel_harness(tmp_path, monkeypatch, **kwargs):
    """Unstarted orchestrator + FakeApi webhook plumbing + a flag-controlled
    os.path.isdir so a test can flip the repo between present and missing."""
    harness = Harness(tmp_path, monkeypatch, api_issues={}, start=False, **kwargs)
    real_isdir = os.path.isdir
    present = {"ok": True}
    monkeypatch.setattr(os.path, "isdir",
                        lambda p: present["ok"] and real_isdir(p))
    return harness, present


def test_repo_missing_first_reconcile_warns_without_cancel(tmp_path, monkeypatch):
    teardown_calls = []
    harness, present = _self_cancel_harness(
        tmp_path, monkeypatch, callback_url="http://agent:9000",
        teardown=lambda: teardown_calls.append(1))
    harness.api.hooks = [{"id": 7, "config": {"url": "http://agent:9000"}}]
    present["ok"] = False

    harness.orch.reconcile()

    assert harness.journal("warning: repo path missing (1/2)"), harness.journal_msgs
    assert teardown_calls == [], "one miss must not self-cancel"
    assert harness.api.hook_deletes == [], "no webhook touched on the first miss"
    assert not harness.orch._stop.is_set()


def test_repo_missing_twice_self_cancels(tmp_path, monkeypatch):
    teardown_calls = []
    harness, present = _self_cancel_harness(
        tmp_path, monkeypatch, callback_url="http://agent:9000",
        teardown=lambda: teardown_calls.append(1))
    harness.api.hooks = [
        {"id": 7, "config": {"url": "http://agent:9000"}},   # ours -> deleted
        {"id": 8, "config": {"url": "http://other:9999"}},   # foreign -> kept
    ]
    present["ok"] = False

    harness.orch.reconcile()
    harness.orch.reconcile()

    assert teardown_calls == [1], "teardown ran exactly once"
    assert harness.api.hook_deletes == [7], "only the callback-matching hook deleted"
    assert harness.orch._stop.is_set()
    assert harness.journal("CRITICAL: 主仓目录连续缺失"), harness.journal_msgs


def test_repo_present_resets_miss_counter(tmp_path, monkeypatch):
    teardown_calls = []
    harness, present = _self_cancel_harness(
        tmp_path, monkeypatch, callback_url="http://agent:9000",
        teardown=lambda: teardown_calls.append(1))
    present["ok"] = False
    harness.orch.reconcile()                      # miss (1/2)

    present["ok"] = True
    harness.orch.reconcile()                      # back -> counter reset
    harness.orch.reconcile()
    present["ok"] = False
    harness.orch.reconcile()                      # miss again, still (1/2)

    assert teardown_calls == [], "a single fresh miss after recovery must not cancel"
    assert harness.api.hook_deletes == []
    assert not harness.orch._stop.is_set()
    warnings = harness.journal("warning: repo path missing")
    assert len(warnings) == 2 and all("(1/2)" in m for m in warnings), warnings


def test_hook_delete_failure_still_self_cancels(tmp_path, monkeypatch):
    teardown_calls = []
    harness, present = _self_cancel_harness(
        tmp_path, monkeypatch, callback_url="http://agent:9000",
        teardown=lambda: teardown_calls.append(1))
    harness.api.hooks = [{"id": 7, "config": {"url": "http://agent:9000"}}]
    harness.api.hook_delete_error = api.ApiTransientError("gitea down")
    present["ok"] = False

    harness.orch.reconcile()
    harness.orch.reconcile()

    assert teardown_calls == [1], "webhook failure must not block the teardown"
    assert harness.orch._stop.is_set()
    assert harness.journal("webhook deletion failed"), harness.journal_msgs


def test_self_cancel_defaults_are_safe(tmp_path, monkeypatch):
    """No callback_url / no teardown configured: cancel path must not crash."""
    harness, present = _self_cancel_harness(tmp_path, monkeypatch)
    present["ok"] = False

    harness.orch.reconcile()
    harness.orch.reconcile()

    assert harness.orch._stop.is_set()
    assert harness.api.hook_deletes == []


# --------------------------------------------------------------------------
# default_branch passthrough (per-project portability)
# --------------------------------------------------------------------------

def test_default_branch_threaded_into_gitop_calls(tmp_path, monkeypatch):
    """Orchestrator threads its default_branch into every gitop call."""
    harness = Harness(tmp_path, monkeypatch, default_branch="main")
    try:
        harness.listener.queue.put(issue_open_event(iid=21))
        assert wait_until(lambda: harness.entry(21)["status"] == "active"), \
            harness.journal_msgs
        assert [c[2] for c in harness.gitop.fetch_calls] == ["main"]
        assert [c[2] for c in harness.gitop.has_calls] == [
            gitop.agent_tracking_ref("main")]
    finally:
        harness.orch.stop()


# --------------------------------------------------------------------------
# R2: argv size guard — oversized bodies are truncated before the runner
# --------------------------------------------------------------------------

def test_message_and_title_truncates_oversized_issue_body():
    event = issue_open_event(iid=31, title="t" * 300, body="x" * (80 * 1024))
    message, title = orchestrator.Orchestrator._message_and_title(event, 31)
    assert len(message.encode("utf-8")) <= orchestrator.MAX_MESSAGE_BYTES + 100
    assert message.endswith(orchestrator._TRUNCATED_MARK.strip())
    assert "x" * 1000 in message          # body prefix survives verbatim
    assert len(title) == 40               # title keeps its 40-char cap


def test_message_and_title_truncates_oversized_comment_body():
    event = comment_event(iid=32, body="修" * (40 * 1024))  # CJK: 3 bytes/char
    message, title = orchestrator.Orchestrator._message_and_title(event, 32)
    # byte bound holds even for multi-byte text, and the cut is clean UTF-8
    assert len(message.encode("utf-8")) <= orchestrator.MAX_MESSAGE_BYTES + 100
    message.encode("utf-8").decode("utf-8")
    assert message.endswith(orchestrator._TRUNCATED_MARK.strip())
    assert title == "issue#32"


def test_message_and_title_small_bodies_pass_through_untouched():
    event = issue_open_event(iid=33, title="标题", body="正文")
    message, title = orchestrator.Orchestrator._message_and_title(event, 33)
    assert message == "标题\n\n正文"
    assert title == "issue#33: 标题"


# --------------------------------------------------------------------------
# 22. task 8: post-push review phase — the REAL /review-loop in a fresh
#     review# session (non-blocking; daemon re-pushes reviewer commits)
# --------------------------------------------------------------------------

def review_sessions(harness):
    return [c for c in harness.runner.new_calls
            if c["title"].startswith("review#")]


def test_fix_turn_with_commits_spawns_review_loop_session(harness):
    """(i) a turn that pushed bot fix commits spawns a SECOND fresh session
    titled review#<iid>, instructed to run the real /review-loop command
    with the EXPLICIT agent-remote diff scope and the persistent evidence
    dir (not a worktree-local .review-loop/)."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=41))
    assert wait_until(lambda: harness.entry(41)["status"] == "active"), \
        harness.journal_msgs

    reviews = review_sessions(harness)
    assert len(reviews) == 1, f"exactly one review session: {harness.runner.new_calls}"
    call = reviews[0]
    assert call["title"].startswith("review#41"), call["title"]
    assert "/review-loop" in call["message"], "prompt invokes the real command"
    assert ("refs/remotes/agent-remote/master..issue/41-fix"
            in call["message"]), "explicit diff scope, formatted with the turn's values"
    assert (f"{harness.repo_path}/.omo/evidence/issue-agent/review-" in call["message"]), (
        "evidence redirected to the persistent canonical dir")
    assert "review-41-" in call["message"], "evidence dir is per-issue per-timestamp"
    assert "删除本次评审 worktree" in call["message"], "explicit worktree cleanup step"
    assert "git worktree remove" in call["message"], "cleanup command present"
    assert call["cwd"] == harness.repo_path, "review session roots in the main repo"
    assert call["timeout_s"] == harness.orch.review_timeout, "review_timeout threaded"
    assert harness.journal(
        "review: issue#41 start (scope agent-remote/master..issue/41-fix)"), (
        harness.journal_msgs)


def test_reviewer_commits_repushed_and_one_review_comment(harness):
    """(ii) after the review phase the daemon re-checks/re-pushes the branch
    (fix push + post-review push), posts EXACTLY one review comment, and
    never lets the review session touch the issue's ledger session_id."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=42))
    assert wait_until(lambda: harness.entry(42)["status"] == "active"), \
        harness.journal_msgs

    assert harness.gitop.push_calls == [42, 42], "fix push + post-review re-push"
    summaries = [b for _, b in harness.api.posts if "评审总结" in b]
    assert len(summaries) == 1, f"exactly one review comment: {harness.api.posts}"
    assert any("已定位并修复" in b for _, b in harness.api.posts), "fix reply unaffected"
    assert harness.entry(42)["session_id"] == "ses_1", (
        "the ledger keeps the FIX turn's session, never the review session's")


def test_review_disabled_skips_review_session_and_comment(tmp_path, monkeypatch):
    """(iii) review_enabled=False: no second session, no review comment, no
    post-review push."""
    h = Harness(tmp_path, monkeypatch, review_enabled=False)
    try:
        h.gitop.commits = True
        h.listener.queue.put(issue_open_event(iid=43))
        assert wait_until(lambda: h.entry(43)["status"] == "active"), h.journal_msgs
        assert not review_sessions(h), "review disabled: no review# session"
        assert len(h.runner.new_calls) == 1, "only the fix session ran"
        assert not any("评审总结" in b for _, b in h.api.posts)
        assert h.gitop.push_calls == [43], "fix push only; no post-review re-push"
    finally:
        h.orch.stop()


def test_review_timeout_is_non_blocking_issue_still_finalizes(harness):
    """(iv) TurnTimeout in the review session maps NON-blocking: one comment
    mentioning 评审未完成, no defer/blocked — finalize still lands active."""
    harness.gitop.commits = True
    harness.runner.review_error = runner.TurnTimeout("opencode exceeded 1800s")
    harness.listener.queue.put(issue_open_event(iid=44))
    assert wait_until(lambda: harness.entry(44)["status"] == "active"), \
        harness.journal_msgs

    failed = [b for _, b in harness.api.posts if "评审未完成" in b]
    assert len(failed) == 1, f"exactly one review-failure comment: {harness.api.posts}"
    assert "已推送" in failed[0] and "人工" in failed[0]
    assert not any("评审总结" in b for _, b in harness.api.posts), "no summary on failure"
    entry = harness.entry(44)
    assert entry["status"] == "active", "review failure must not defer or block"
    assert entry["defer_count"] == 0
    assert entry["claimed_at"] is None
    assert harness.journal("review: issue#44"), harness.journal_msgs


def test_no_review_of_review_exactly_two_sessions(harness):
    """(v) recursion guard: a fix+review turn runs EXACTLY two opencode
    sessions total (1 fix + 1 review) — the review phase never schedules
    another review."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=45))
    assert wait_until(lambda: harness.entry(45)["status"] == "active"), \
        harness.journal_msgs

    total = len(harness.runner.new_calls) + len(harness.runner.cont_calls)
    assert total == 2, f"fix + review sessions only (got {total})"
    assert len(review_sessions(harness)) == 1
    time.sleep(0.3)  # any late self-scheduling would fire by now
    assert len(harness.runner.new_calls) + len(harness.runner.cont_calls) == 2


# --------------------------------------------------------------------------
# 23. streaming sid publication — _run_review_phase pre-creates the
#     evidence dir, hands the runner a session-id.txt path inside it, and
#     REVIEW_PROMPT's step 0 tells the reviewer to read that file first
# --------------------------------------------------------------------------

def test_review_phase_publishes_sid_into_precreated_evidence_dir(harness):
    """The evidence dir exists BEFORE the runner is called (the streaming
    sid write would fail on a missing dir), and sid_path is a
    session-id.txt INSIDE this run's evidence dir."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=46))
    assert wait_until(lambda: harness.entry(46)["status"] == "active"), \
        harness.journal_msgs

    reviews = [c for c in harness.runner.new_calls
               if c["title"].startswith("review#46")]
    assert len(reviews) == 1, harness.runner.new_calls
    call = reviews[0]
    assert call["sid_dir_existed"], ("evidence_dir was created BEFORE the"
                                     " new_session_publishing_sid call")
    sid_path = call["sid_path"]
    assert os.path.basename(sid_path) == "session-id.txt"
    evidence_dir = os.path.dirname(sid_path)
    assert (f"{harness.repo_path}/.omo/evidence/issue-agent/review-46-"
            in evidence_dir), evidence_dir
    assert os.path.isdir(evidence_dir), "dir still present after the phase"
    with open(sid_path, encoding="utf-8") as fh:
        assert fh.read() == "ses_rev\n", "fake seam emulated the sid write"


def test_review_prompt_step0_names_sid_file_and_wait_cap(harness):
    """REVIEW_PROMPT (constant AND as rendered for this run) carries the
    step-0 paragraph: read {evidence_dir}/session-id.txt for the session
    ID before /review-loop step 1, retry ≤15s, fall back to the command's
    own lookup only if still missing."""
    assert "session-id.txt" in orchestrator.REVIEW_PROMPT
    assert "≤15" in orchestrator.REVIEW_PROMPT
    assert "第 0 步" in orchestrator.REVIEW_PROMPT

    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=47))
    assert wait_until(lambda: harness.entry(47)["status"] == "active"), \
        harness.journal_msgs

    reviews = [c for c in harness.runner.new_calls
               if c["title"].startswith("review#47")]
    assert len(reviews) == 1, harness.runner.new_calls
    prompt = reviews[0]["message"]
    evidence_dir = os.path.dirname(reviews[0]["sid_path"])
    assert "session-id.txt" in prompt and "≤15" in prompt
    assert evidence_dir in prompt, "step 0 names THIS run's evidence dir"
    assert prompt.index("第 0 步") < prompt.index("/review-loop"), (
        "step 0 precedes the command invocation")


def test_review_prompt_one_shot_discipline():
    """REVIEW_PROMPT carries the one-shot process discipline paragraph: the
    review session runs inside a single headless `opencode run` process,
    so ending the turn exits the process and kills every background lane —
    lanes still launch in parallel but MUST be collected in-turn via
    background_output(block=true); never end the turn to wait for system
    notifications."""
    assert "一次性进程纪律" in orchestrator.REVIEW_PROMPT
    assert "block=true" in orchestrator.REVIEW_PROMPT
    assert "结束回合" in orchestrator.REVIEW_PROMPT


# --------------------------------------------------------------------------
# 24. post-push capture verification — read-only observability: a missing
#     knowledge ingest / experience draft is a journal warning, never a block
# --------------------------------------------------------------------------

def test_verify_capture_missing_warns(harness):
    """Both captures missing (no knowledge diff, no draft dir): one
    capture journal line + warning; the turn still finalizes active."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=61))
    assert wait_until(lambda: harness.entry(61)["status"] == "active"), \
        harness.journal_msgs

    assert harness.journal(
        "capture: issue#61 knowledge=missing experience=missing"), \
        harness.journal_msgs
    assert [m for m in harness.journal_msgs
            if "capture" in m and "warning" in m], \
        "missing captures must leave a warning line"
    assert harness.gitop.knowledge_calls == [
        (harness.repo_path, 61, gitop.agent_tracking_ref("master"))]
    entry = harness.entry(61)
    assert entry["status"] == "active" and entry["claimed_at"] is None


def test_verify_capture_present_silent(harness):
    """Knowledge diff + a same-file draft hit (issue#N word-boundary AND
    project: <owner/repo>): ok/ok line, no warning. A decoy draft with
    issue#620 (same project) must NOT satisfy the issue#62 probe."""
    harness.gitop.commits = True
    harness.gitop.knowledge_changes = True
    harness.exp_dir.mkdir()
    (harness.exp_dir / "20261009-120000-abcd.md").write_text(
        "---\nid: 20261009-120000-abcd\nstatus: draft\n"
        "project: group/repo\ntags: issue-agent,issue#62\n---\n"
        "# 修复经验\n根因定位与验证方式……\n", encoding="utf-8")
    (harness.exp_dir / "20261009-130000-decoy.md").write_text(
        "---\nid: 20261009-130000-decoy\nstatus: draft\n"
        "project: group/repo\ntags: issue-agent,issue#620\n---\n"
        "# 另一 issue 的草稿\n", encoding="utf-8")

    harness.listener.queue.put(issue_open_event(iid=62))
    assert wait_until(lambda: harness.entry(62)["status"] == "active"), \
        harness.journal_msgs

    assert harness.journal(
        "capture: issue#62 knowledge=ok experience=ok"), harness.journal_msgs
    assert not [m for m in harness.journal_msgs
                if "capture" in m and "warning" in m], harness.journal_msgs


def test_verify_capture_left_boundary_decoy(harness):
    """Left word-boundary guard: body text `myissue#62` (letter-run
    prefix) plus the matching project line must NOT satisfy the
    issue#62 probe — experience=missing + warning (decoy rejected)."""
    harness.gitop.commits = True
    harness.gitop.knowledge_changes = True
    harness.exp_dir.mkdir()
    (harness.exp_dir / "20261009-140000-lftdecoy.md").write_text(
        "---\nid: 20261009-140000-lftdecoy\nstatus: draft\n"
        "project: group/repo\ntags: issue-agent\n---\n"
        "# 修复经验\n前缀 myissue#62 伪装\n", encoding="utf-8")

    harness.listener.queue.put(issue_open_event(iid=62))
    assert wait_until(lambda: harness.entry(62)["status"] == "active"), \
        harness.journal_msgs

    assert harness.journal(
        "capture: issue#62 knowledge=ok experience=missing"), \
        harness.journal_msgs
    assert [m for m in harness.journal_msgs
            if "capture" in m and "warning" in m], \
        harness.journal_msgs


def test_verify_capture_gitop_error_nonfatal(harness):
    """GitopError from the knowledge probe degrades to knowledge=missing
    + warning; the turn never raises and still finalizes active."""
    harness.gitop.commits = True
    harness.gitop.knowledge_error = gitop.GitopError("diff failed (fake)")
    harness.listener.queue.put(issue_open_event(iid=63))
    assert wait_until(lambda: harness.entry(63)["status"] == "active"), \
        harness.journal_msgs

    assert harness.journal(
        "capture: issue#63 knowledge=missing experience=missing"), \
        harness.journal_msgs
    assert harness.journal("warning: issue#63 knowledge probe failed"), \
        harness.journal_msgs
    entry = harness.entry(63)
    assert entry["status"] == "active" and entry["defer_count"] == 0


def test_claim_timeout_covers_full_turn_budget():
    assert orchestrator.DEFAULT_CLAIM_TIMEOUT_S >= (
        orchestrator.DEFAULT_REVIEW_TIMEOUT_S + orchestrator.DEFAULT_OPENCODE_TIMEOUT_S)


# --- close-push: a close outside a turn must still land the branch ------------


def _seed_active_issue(h, iid=11):
    st = {"issues": {}, "baselined": True}
    state.adopt_existing(st, {iid: [101]})
    st["issues"][str(iid)]["status"] = "active"
    st["issues"][str(iid)]["session_id"] = "ses_1"
    state.save_state(h.state_path, st)


def test_close_pushes_unpushed_branch(harness):
    """Commits can land on issue/<iid>-fix outside any turn (TUI takeover,
    manual branch work); when the issue then closes, reconcile must push the
    branch before marking done — issue#139's branch never reached the remote."""
    _seed_active_issue(harness)
    harness.api.issues = {}        # issue closed out-of-band
    harness.gitop.commits = True   # branch holds commits beyond the baseline
    harness.orch.reconcile()

    assert harness.entry(11)["status"] == "done"
    assert 11 in harness.gitop.push_calls


def test_webhook_close_pushes_unpushed_branch(harness):
    """The webhook 'closed' event is the PRIMARY close channel — it must
    push a since-advanced branch too (review-loop round 1 finding: the
    reconcile fallback alone left the main path unfixed)."""
    _seed_active_issue(harness)
    harness.gitop.commits = True
    close_event = issue_open_event(iid=11)
    close_event["action"] = "closed"
    close_event["issue"]["state"] = "closed"
    harness.listener.queue.put(close_event)

    assert wait_until(lambda: harness.entry(11)["status"] == "done"), \
        harness.journal_msgs
    assert 11 in harness.gitop.push_calls


def test_close_without_new_commits_skips_push(harness):
    _seed_active_issue(harness)
    harness.api.issues = {}
    harness.orch.reconcile()

    assert harness.entry(11)["status"] == "done"
    assert harness.gitop.push_calls == []


def test_close_push_failure_is_non_blocking(harness):
    """The issue is already closed by a human — a failed best-effort push
    journals + comments but must never break the close (or raise)."""
    _seed_active_issue(harness)
    harness.api.issues = {}
    harness.gitop.commits = True
    harness.gitop.push_error = gitop.PushFailed("non-fast-forward (fake)")
    harness.orch.reconcile()

    assert harness.entry(11)["status"] == "done"
    assert any("close-push failed" in m for m in harness.journal_msgs)


# --- bounded parallel turns (max_parallel) -------------------------------------


def _seed_two(h):
    _seed_active_issue(h, 11)
    _seed_active_issue(h, 12)


def test_parallel_turns_overlap(tmp_path, monkeypatch):
    """max_parallel=2: two DIFFERENT issues' turns run CONCURRENTLY — both
    are inside the runner while the first is still blocked there."""
    h = Harness(tmp_path, monkeypatch,
                api_issues={11: [api_comment(101, "存量")],
                            12: [api_comment(201, "存量")]},
                max_parallel=2, start=False)
    h.orch.start()
    try:
        h.runner.gate = threading.Event()
        h.listener.queue.put(comment_event(iid=11, comment_id=102, body="第一"))
        h.listener.queue.put(comment_event(iid=12, comment_id=202, body="第二"))
        assert wait_until(lambda: len(h.runner.new_calls) == 2), \
            [c["title"] for c in h.runner.new_calls] + h.journal_msgs
        h.runner.gate.set()
        assert wait_until(lambda: h.entry(11)["status"] == "active"
                          and h.entry(12)["status"] == "active"), h.journal_msgs
    finally:
        h.orch.stop()


def test_same_issue_turns_serialize(tmp_path, monkeypatch):
    """Same-issue events NEVER overlap even at max_parallel=2: the second
    comment's turn waits for the first to leave the runner."""
    h = Harness(tmp_path, monkeypatch,
                api_issues={11: [api_comment(101, "存量")]},
                max_parallel=2, start=False)
    h.orch.start()
    try:
        h.runner.gate = threading.Event()
        h.listener.queue.put(comment_event(iid=11, comment_id=102, body="第一"))
        assert wait_until(lambda: len(h.runner.new_calls) == 1), h.journal_msgs
        h.listener.queue.put(comment_event(iid=11, comment_id=103, body="第二"))
        time.sleep(0.3)
        assert len(h.runner.new_calls) == 1, \
            "same-issue second turn entered the runner while the first held it"
        h.runner.gate.set()
        # the second turn proceeds AFTER the first left the runner (its claim
        # predates the first finalize, so it opens a fresh session too)
        assert wait_until(lambda: len(h.runner.new_calls) == 2), h.journal_msgs
    finally:
        h.orch.stop()


def test_claim_budget_below_turn_budget_warns(tmp_path, monkeypatch):
    """The recycle threshold must exceed one full turn budget: with
    max_parallel>1 a same-issue event waits on the iid lock for up to
    opencode+review while its claim ages — a smaller claim_timeout_s makes
    the recycle fire mid-wait and requeue a duplicate retry."""
    h = Harness(tmp_path, monkeypatch, claim_timeout_s=5000, start=False)
    h.orch.start()
    try:
        assert any("claim_timeout_s" in m and "below the full turn budget" in m
                   for m in h.journal_msgs), h.journal_msgs
    finally:
        h.orch.stop()


def test_claim_budget_default_no_warning(harness):
    """Default 5500s covers opencode 1800 + review 3600 + margin — no warning."""
    assert not any("below the full turn budget" in m for m in harness.journal_msgs)


def test_parallel_bound_is_honest(tmp_path, monkeypatch):
    """max_parallel=2 with THREE queued issues: at most 2 runners in flight
    while all are blocked — the third waits for a slot."""
    h = Harness(tmp_path, monkeypatch,
                api_issues={11: [api_comment(101, "x")],
                            12: [api_comment(201, "x")],
                            13: [api_comment(301, "x")]},
                max_parallel=2, start=False)
    h.orch.start()
    try:
        h.runner.gate = threading.Event()
        for iid, nid in ((11, 102), (12, 202), (13, 302)):
            h.listener.queue.put(comment_event(iid=iid, comment_id=nid, body="x"))
        assert wait_until(lambda: len(h.runner.new_calls) == 2), h.journal_msgs
        time.sleep(0.3)
        assert len(h.runner.new_calls) == 2, "parallel bound exceeded"
        h.runner.gate.set()
        assert wait_until(lambda: len(h.runner.new_calls) == 3), h.journal_msgs
    finally:
        h.orch.stop()


# --------------------------------------------------------------------------
# 25. delayed review push — an abnormally-terminated review phase (2026-10-20
#     LLM outage: TurnCrash while the review session stayed alive server-side
#     and committed review fixes AFTER the daemon's one-shot re-push check)
#     marks review_push_pending_at; reconcile re-pushes the late commits ONCE
#     after a grace period; a new turn's claim clears the mark.
# --------------------------------------------------------------------------

def _seed_push_pending(harness, iid, age_s=None):
    """Active, still-open issue carrying the review-crash flag;
    age_s=None stamps it just now, else backdates it age_s seconds."""
    seed_active(harness, iid)
    harness.api.issues = {iid: [api_comment(1, "still open")]}  # keep it open
    st = state.load_state(harness.state_path)
    ts = datetime.now(timezone.utc)
    if age_s is not None:
        ts -= timedelta(seconds=age_s)
    st["issues"][str(int(iid))]["review_push_pending_at"] = ts.isoformat()
    state.save_state(harness.state_path, st)


def test_review_crash_sets_push_pending_flag(harness):
    """TurnCrash in the review phase records review_push_pending_at (ISO8601)
    in the ledger entry; journal/comment behavior unchanged (still one
    评审未完成 comment, still finalizes active)."""
    harness.gitop.commits = True
    harness.runner.review_error = runner.TurnCrash("No active API keys")
    harness.listener.queue.put(issue_open_event(iid=61))
    assert wait_until(lambda: harness.entry(61)["status"] == "active"), \
        harness.journal_msgs
    flag = harness.entry(61)["review_push_pending_at"]
    assert flag, "the crash path must arm the delayed re-push"
    datetime.fromisoformat(flag)  # ISO8601
    failed = [b for _, b in harness.api.posts if "评审未完成" in b]
    assert len(failed) == 1, "journal/comment behavior unchanged"


def test_successful_review_sets_no_push_pending_flag(harness):
    """Hard constraint: ONLY the abnormal-terminal path arms the flag — a
    healthy review (and any non-review turn) never does."""
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=62))
    assert wait_until(lambda: harness.entry(62)["status"] == "active"), \
        harness.journal_msgs
    assert not harness.entry(62)["review_push_pending_at"]


def test_delayed_push_waits_for_grace(harness):
    """Flag set but the grace period has not elapsed: reconcile neither
    pushes nor clears the mark."""
    _seed_push_pending(harness, 63, age_s=1)
    harness.orch.reconcile()
    assert harness.gitop.push_calls == []
    assert harness.entry(63)["review_push_pending_at"], "kept for a later pass"


def test_delayed_push_after_grace_pushes_once_and_clears(harness):
    """Grace elapsed + late reviewer commits: exactly one push, the
    delayed-re-push journal + issue comment, and the mark cleared."""
    grace = orchestrator.DEFAULT_REVIEW_PUSH_GRACE_S
    _seed_push_pending(harness, 64, age_s=grace + 1)
    harness.gitop.commits = True
    harness.orch.reconcile()
    assert harness.gitop.push_calls == [64], "exactly one delayed push"
    assert not harness.entry(64)["review_push_pending_at"]
    assert harness.journal("延迟补推"), harness.journal_msgs
    assert any("延迟补推" in b for _, b in harness.api.posts), harness.api.posts


def test_delayed_push_after_grace_without_commits_clears_only(harness):
    """Grace elapsed but the branch gained nothing: the mark clears with no
    push and no comment (the review session died without committing)."""
    grace = orchestrator.DEFAULT_REVIEW_PUSH_GRACE_S
    _seed_push_pending(harness, 65, age_s=grace + 1)
    harness.orch.reconcile()
    assert harness.gitop.push_calls == []
    assert not harness.entry(65)["review_push_pending_at"]
    assert not any("延迟补推" in b for _, b in harness.api.posts)


def test_delayed_push_failure_blocks_once_and_clears(harness):
    """A failed delayed push maps to the existing PushFailed->blocked
    semantics and still clears the mark — never an automatic retry."""
    grace = orchestrator.DEFAULT_REVIEW_PUSH_GRACE_S
    _seed_push_pending(harness, 66, age_s=grace + 1)
    harness.gitop.commits = True
    harness.gitop.push_error = gitop.PushFailed("pre-receive hook declined")
    harness.orch.reconcile()
    assert harness.entry(66)["status"] == "blocked"
    assert not harness.entry(66)["review_push_pending_at"]
    assert len(harness.gitop.push_calls) == 1, "one attempt, no retry"


def test_new_claim_clears_push_pending_flag(harness):
    """A new turn's claim clears the mark: the turn's own post-turn and
    post-review pushes own the branch from that point on."""
    _seed_push_pending(harness, 67, age_s=1)
    harness.listener.queue.put(comment_event(iid=67, comment_id=201))
    assert wait_until(lambda: turn_done(harness, 67, 201)), harness.journal_msgs
    assert not harness.entry(67)["review_push_pending_at"]


def test_delayed_push_skips_non_active_entries(harness):
    """done/blocked entries never delay-push: close-push and humans own them
    (guards manual work from being swept into the automatic re-push)."""
    grace = orchestrator.DEFAULT_REVIEW_PUSH_GRACE_S
    _seed_push_pending(harness, 68, age_s=grace + 1)
    st = state.load_state(harness.state_path)
    st["issues"]["68"]["status"] = "done"
    state.save_state(harness.state_path, st)
    harness.orch.reconcile()
    assert harness.gitop.push_calls == []
    assert harness.entry(68)["review_push_pending_at"], "left as-is for done"


def test_webhook_close_clears_push_pending_flag(harness):
    """A close takes branch ownership (close-push): the mark must not
    survive it — after a reopen a stale mark would auto-push manual/TUI
    commits landed meanwhile (hard constraint: only the review-crash path
    may arm the automatic re-push)."""
    _seed_push_pending(harness, 69, age_s=1)
    harness.listener.queue.put({"action": "closed",
                                "issue": {"number": 69, "state": "closed"}})
    assert wait_until(lambda: harness.entry(69)["status"] == "done"), \
        harness.journal_msgs
    assert not harness.entry(69)["review_push_pending_at"]


def test_review_crash_then_push_failure_blocked_leaves_no_flag(harness, monkeypatch):
    """Crash arms the mark, the immediate post-review re-push fails ->
    blocked: the blocked entry carries no mark (blocked is human-ownership;
    the delayed path must never fire for it later)."""
    harness.gitop.commits = True
    calls = harness.gitop.push_calls

    def push_fails_second(repo, iid, remote_url, ssl_verify=True):
        calls.append(iid)
        if len(calls) >= 2:  # 1st = fix push (ok), 2nd = post-review re-push
            raise gitop.PushFailed("pre-receive declined")
        return None

    monkeypatch.setattr(gitop, "push_fix_branch", push_fails_second)
    harness.runner.review_error = runner.TurnCrash("No active API keys")
    harness.listener.queue.put(issue_open_event(iid=70))
    assert wait_until(lambda: harness.entry(70)["status"] == "blocked"), \
        harness.journal_msgs
    assert not harness.entry(70)["review_push_pending_at"]


# --------------------------------------------------------------------------
# heartbeat (A+C): long-turn liveness notes quoting the latest activity
# --------------------------------------------------------------------------

class TestHeartbeat:
    """A 侧（runner on_event）只写内存，C 侧心跳线程是唯一评论出口；
    _heartbeat_scan 是纯记账函数——到点产 (iid, 文案)，扫描即记账。"""

    def _aged(self, orch, iid=5, age=600, activity=None):
        orch._heartbeat_register(iid)
        entry = orch._in_flight[iid]
        entry["start"] -= age
        entry["last_post"] -= age
        if activity is not None:
            entry["activity"] = activity
        return entry

    def test_scan_posts_due_turn_with_activity(self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch, activity="bash: pytest -q")
        assert orch._heartbeat_scan() == [
            (5, "仍在进行：已运行 10 分钟，当前正在：bash: pytest -q")]

    def test_scan_omits_activity_suffix_when_none_seen(self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch)
        assert orch._heartbeat_scan() == [(5, "仍在进行：已运行 10 分钟")]

    def test_scan_silent_until_interval_then_refrains_until_next(
            self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        orch._heartbeat_register(5)
        orch._in_flight[5]["start"] -= 700
        assert orch._heartbeat_scan() == [], (
            "the turn may be old, but a fresh last_post is not due")
        orch._in_flight[5]["last_post"] -= 700
        assert len(orch._heartbeat_scan()) == 1
        assert orch._heartbeat_scan() == [], "scan books itself: no re-fire"

    def test_unregister_stops_heartbeat(self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch)
        orch._heartbeat_unregister(5)
        assert orch._heartbeat_scan() == []
        assert orch._in_flight == {}

    def test_activity_note_updates_latest_only(self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch)
        orch._heartbeat_note(5, "read: /a")
        orch._heartbeat_note(5, "bash: pytest -q")
        assert orch._in_flight[5]["activity"] == "bash: pytest -q"

    def test_interval_zero_disables_register_and_thread(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch, heartbeat_interval=0)
        h.orch._heartbeat_register(5)
        assert h.orch._in_flight == {}, "0 = off: registration is a no-op"
        assert h.orch._hb_thread is None, "0 = off: no heartbeat thread"
        h.orch.stop()

    def test_turn_registration_is_symmetric(self, harness):
        run_turn_happy(harness)
        # the CAS save in _finalize precedes the finally-pop by microseconds:
        # wait for the drain instead of sampling that window
        assert wait_until(lambda: harness.orch._in_flight == {}), (
            "no exit path (done/defer/blocked) may leak a registration")

    def test_on_event_wired_only_when_enabled(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch, heartbeat_interval=1)
        h.listener.queue.put(issue_open_event(iid=11))  # new-session path
        assert wait_until(lambda: h.entry(11)["status"] == "active")
        assert h.runner.new_calls[0]["on_event"] is not None
        h.orch.stop()

    def test_on_event_absent_when_disabled(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch, heartbeat_interval=0)
        h.listener.queue.put(issue_open_event(iid=12))
        assert wait_until(lambda: h.entry(12)["status"] == "active")
        assert h.runner.new_calls[0]["on_event"] is None

    def test_review_turn_receives_on_event_too(self, harness):
        harness.gitop.commits = True  # push -> review phase runs
        harness.listener.queue.put(issue_open_event(iid=21))
        assert wait_until(lambda: harness.entry(21)["status"] == "active")
        review_calls = [c for c in harness.runner.new_calls
                        if c["title"].startswith("review#")]
        assert review_calls and review_calls[0]["on_event"] is not None

    def test_stop_joins_heartbeat_thread(self, tmp_path, monkeypatch):
        h = Harness(tmp_path, monkeypatch)  # default 600 -> thread starts
        hb = h.orch._hb_thread
        assert hb is not None
        h.orch.stop()
        assert not hb.is_alive()

    def test_heartbeat_thread_survives_scan_error(self, tmp_path, monkeypatch):
        """ocr round-1: _post_note's exception surface is not closed (cert
        errors re-raise raw, resp.read() can raise bare OSErrors) — the
        heartbeat thread must mirror the worker's never-die discipline."""
        h = Harness(tmp_path, monkeypatch, heartbeat_interval=1)
        real_scan = h.orch._heartbeat_scan
        calls = {"n": 0}

        def flaky_scan():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            return real_scan()

        monkeypatch.setattr(h.orch, "_heartbeat_scan", flaky_scan)
        assert wait_until(lambda: any(
            "heartbeat: unhandled error" in m for m in h.journal_msgs)), \
            h.journal_msgs
        assert h.orch._hb_thread.is_alive(), "the thread must never die"
        h.orch.stop()

    def test_deliver_skips_finalized_turn(self, tmp_path, monkeypatch):
        """ocr round-1 scan-then-post race: a turn that finalized between the
        scan and the post must not receive a stale 「仍在进行」 note."""
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch)
        candidates = orch._heartbeat_scan()
        assert candidates
        orch._heartbeat_unregister(5)
        posts_before = len(orch.api.posts)
        orch._heartbeat_deliver(*candidates[0])
        assert len(orch.api.posts) == posts_before, "no stale post"

    def test_deliver_posts_while_registered(self, tmp_path, monkeypatch):
        orch = Harness(tmp_path, monkeypatch, start=False).orch
        self._aged(orch)
        candidates = orch._heartbeat_scan()
        orch._heartbeat_deliver(*candidates[0])
        assert any("仍在进行" in body for _, body in orch.api.posts)

# --------------------------------------------------------------------------
# 23. Task 2: every runner call site carries the agora credential prefix
#     (fix continue / fix new via _run_opencode, review new via
#     new_session_publishing_sid — the review lane spawns fresh sessions
#     only, so there is no review-continue call site)
# --------------------------------------------------------------------------

def cred_shape_ok(value, iid, repo_path):
    lines = (value or "").split("\n")
    return (len(lines) == 4
            and lines[0] == f"[agora] task-issue-{iid}: issue-agent round"
            and lines[1] == f"env: {os.path.abspath(repo_path)}"
            and lines[2] == f"[agora] credential task-issue-{iid}"
            and lines[3].startswith("token: "))


def test_fix_continue_and_fix_new_carry_cred_prefix(harness):
    run_turn_happy(harness, iid=11)  # tracked issue -> continue path
    assert harness.runner.cont_calls, "fix continue path exercised"
    call = harness.runner.cont_calls[0]
    assert cred_shape_ok(call["cred_prefix"], 11, harness.repo_path), (
        "kwarg threaded; actual prepending is locked in test_runner")

    harness.listener.queue.put(issue_open_event(iid=12))
    assert wait_until(lambda: harness.entry(12)["status"] == "active"), \
        harness.journal_msgs
    new_calls = [c for c in harness.runner.new_calls
                 if not c["title"].startswith("review#")]
    assert len(new_calls) == 1, harness.runner.new_calls
    assert cred_shape_ok(new_calls[0]["cred_prefix"], 12, harness.repo_path)


def test_review_session_carries_cred_prefix(harness):
    harness.gitop.commits = True
    harness.listener.queue.put(issue_open_event(iid=41))
    assert wait_until(lambda: harness.entry(41)["status"] == "active"), \
        harness.journal_msgs
    reviews = [c for c in harness.runner.new_calls
               if c["title"].startswith("review#")]
    assert len(reviews) == 1, harness.runner.new_calls
    assert cred_shape_ok(reviews[0]["cred_prefix"], 41, harness.repo_path)


def test_session_invalid_fallback_new_session_keeps_cred_prefix(harness):
    run_turn_happy(harness, iid=11)
    harness.runner.cont_error = runner.SessionInvalid("session ses_1 not found")
    harness.listener.queue.put(comment_event(iid=11, comment_id=102))
    assert wait_until(lambda: turn_done(harness, 11, 102)), harness.journal_msgs
    assert len(harness.runner.new_calls) == 1, "fallback to a fresh session"
    assert cred_shape_ok(harness.runner.new_calls[0]["cred_prefix"], 11,
                         harness.repo_path), "prefix survives the degrade path"


# --- ledger lock discipline (plan Task 2 / OCR review round 3) ----------------


def test_reconcile_and_orphan_cleanup_write_ledger_under_lock(
        tmp_path, monkeypatch):
    """claim_timeout_recycle 自带 load→mutate→save，两处调用（startup 孤儿
    回收＋reconcile 陈旧 claim 回收）都必须在账本锁内——「本类全部账本写
    在 _ledger_lock 内」的契约不能有无锁旁路，否则 Router 的 sid persist
    与之交错会丢更新。"""
    import threading

    class LockProbe:
        def __init__(self):
            self._lock = threading.RLock()
            self.depth = 0

        def __enter__(self):
            self._lock.acquire()
            self.depth += 1
            return self

        def __exit__(self, *exc):
            self.depth -= 1
            self._lock.release()
            return False

    probe = LockProbe()
    observed = []

    def fake_recycle(path, now, timeout_s):
        observed.append(probe.depth > 0)
        return []

    monkeypatch.setattr(orchestrator.state, "claim_timeout_recycle",
                        fake_recycle)
    h = Harness(tmp_path, monkeypatch, start=False, ledger_lock=probe)
    h.orch._startup_orphan_cleanup()
    h.orch._fetch_open_issues_and_notes = lambda: None  # F20 早退：回收段即本路径全部账本写
    h.orch._safe_reconcile()
    assert observed == [True, True], \
        "claim_timeout_recycle 两处调用都必须持锁"


# --- agora mode turns (plan Task 3: 幂等派发) ----------------------------------


def agora_harness(tmp_path, monkeypatch, client=None, router_=None,
                  monitor=None, **kwargs):
    """agora 模式 harness：AgoraClient/Router/Monitor 三替身注入（零外网）。"""
    return Harness(
        tmp_path, monkeypatch,
        agora_client=client if client is not None else FakeAgoraClient(),
        router=router_ if router_ is not None else FakeRouter(),
        agora_monitor=monitor if monitor is not None else FakeMonitor(),
        **kwargs)


def turn_finalized(harness, iid):
    entry = harness.entry(iid)
    return entry["status"] == "active" and entry["claimed_at"] is None


def requeue_retry(harness, iid):
    """以 requeue 路径重试（claim-timeout 回收后的真实通道）：置 defer 再
    _handle_requeue——它保持 last_note_id，故 client_key 与原回合一致。"""
    st = state.load_state(harness.state_path)
    st["issues"][str(int(iid))]["status"] = "defer"
    state.save_state(harness.state_path, st)
    harness.orch._handle_requeue(int(iid))


def test_run_turn_returns_after_dispatch_and_releases_issue_lock(
        tmp_path, monkeypatch):
    """计划锚例：agora 回合 ack→fetch+ensure→Router→create→落账→dispatch→
    register 后立即返回并释放 issue 锁——绝不同步等待 worker，零 legacy
    opencode run。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        assert monitor.registered == [(7, "ag-123")]
        lock = h.orch._iid_lock(7)
        assert wait_until(lambda: lock.acquire(blocking=False)), \
            "回合返回后 issue 锁必须随即释放（finalize 在锁内，释放紧随其后）"
        lock.release()

        assert h.runner.new_calls == [] and h.runner.cont_calls == [], \
            "agora 模式不得走 legacy opencode run"
        assert client.dispatch_calls == ["ag-123"]
        assert h.gitop.fetch_calls and h.gitop.ensure_calls, \
            "基线 fetch 与开发分支准备都必须发生"

        entry = h.entry(7)
        assert entry["agora_task_id"] == "ag-123"
        assert entry["agora_pending"] is False
        assert entry["router_goal"] == router_.decision.goal
        assert entry["router_model"] == "zai/glm-5.3"
        assert entry["branch"] == "issue/7-fix"
        assert entry["session_id"] is None, "legacy session_id 不被 agora 改写"

        call = client.create_calls[0]
        assert call["client_key"] == "issue-agent:issue-7:note-0"
        assert call["target_branch"] == "issue/7-fix"
        assert call["deliverable"] == "external_branch"
        assert call["verify_cmd"] == h.orch.verify_command
        assert call["model"] == "zai/glm-5.3"
        assert call["goal"] == router_.decision.goal
        assert call["project_dir"] == h.repo_path
        assert "after" not in call, "首轮无上轮任务可链"
        assert router_.calls[0]["available_models"] == ["zai/glm-5.3"], \
            "available_models 必须在调用点从 doctor 取"
        assert any("已接手 issue#7" in b for _, b in h.api.posts)
        # Task 4 接线断言：orchestrator 把交付上下文交给 monitor——daemon
        # 构造面不变（T3 wiring 锁定），ledger_lock 与回合侧共用同一把
        assert monitor.attached["repo_path"] == h.repo_path
        assert monitor.attached["remote_url"] == h.orch.remote_url
        assert monitor.attached["state_path"] == h.state_path
        assert monitor.attached["ledger_lock"] is h.orch._ledger_lock
        assert h.gitop.push_calls == [], "agora 回合零 legacy origin push"
    finally:
        h.orch.stop()


def test_create_timeout_recovers_same_task(tmp_path, monkeypatch):
    """计划锚例：create 服务端已提交但客户端超时 → 同 client_key＋同
    payload 重试取回原任务，不重复建单。"""
    client = FakeAgoraClient()
    client.create_failures.append(AgoraUnavailable("create timeout (fake)"))
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        assert client.created_ids == ["ag-123"], "不得重复建单"
        assert len(client.create_calls) == 2, "超时后必须原地重试一次"
        first, second = client.create_calls
        assert FakeAgoraClient._execution_payload(second) \
            == FakeAgoraClient._execution_payload(first)
        assert second["client_key"] == first["client_key"]
        assert client.dispatch_calls == ["ag-123"]
        assert monitor.registered == [(7, "ag-123")]
    finally:
        h.orch.stop()


def test_crash_between_create_and_dispatch_recovers_original_task(
        tmp_path, monkeypatch):
    """计划 L224：create 落账后 dispatch 前中断 → 账本留 agora_task_id＋
    pending；重试走持久化 goal/model＋同 client_key，只恢复原任务。"""
    client = FakeAgoraClient()
    client.dispatch_failures.append(AgoraUnavailable("dispatch down (fake)"))
    router_ = FakeRouter()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        # 回合完全退出（finally release＝keep_pending 落账之后的信号）才能
        # 安全重排：agora_task_id/agora_pending 在回合中途即可观察，提前
        # requeue 会与 keep_pending 的账本写并发互噬（verify 红根因之一）
        assert wait_until(
            lambda: h.entry(7)["agora_task_id"] == "ag-123"
            and 7 in monitor.released), h.journal_msgs
        assert h.entry(7)["status"] == "pending"
        assert h.entry(7)["agora_pending"] is True
        assert monitor.registered == [], "dispatch 未成不得登记"

        requeue_retry(h, 7)
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        assert client.created_ids == ["ag-123"], "恢复不得建新任务"
        assert len(router_.calls) == 1, "恢复复用持久化决策，不重新路由"
        first, second = client.create_calls
        assert FakeAgoraClient._execution_payload(second) \
            == FakeAgoraClient._execution_payload(first)
        assert second["client_key"] == first["client_key"]
        assert client.dispatch_calls == ["ag-123", "ag-123"]
        assert monitor.registered == [(7, "ag-123")]
        assert h.entry(7)["agora_pending"] is False
    finally:
        h.orch.stop()


def test_turn_keeps_issue_pending_when_monitor_at_capacity(
        tmp_path, monkeypatch):
    """计划 L224：活动名额（与 max_parallel 双重限制）满员时保留待派发，
    不超额建单；名额释放后重试照常派发。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=1)
    monitor.register(99, "ag-older")  # 占满名额
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: h.entry(7)["agora_pending"] is True), \
            h.journal_msgs
        assert h.entry(7)["status"] == "pending", "保留待派发＝claim 留存"
        assert client.create_calls == []
        assert router_.calls == []
        assert h.gitop.fetch_calls == [] and h.gitop.ensure_calls == [], \
            "容量闸在 git 工作之前"

        monitor.release(99, "ag-older")  # 名额到终态释放（compare-and-release）
        requeue_retry(h, 7)
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        assert client.created_ids == ["ag-123"]
        assert monitor.registered[-1] == (7, "ag-123")
    finally:
        h.orch.stop()


def test_agora_turn_failure_paths_release_reserved_slot(
        tmp_path, monkeypatch):
    """F3（T3 移交）：reserve 后任何未 register 的出口（fetch 失败 defer、
    Router ignore 出单）都必须 release；register 成功后名额占用到终态
    （释放权归 monitor 的交付状态机）。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=3)
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.gitop.fetch_error = gitop.GitopError("baseline fetch down (fake)")
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: h.entry(7)["status"] == "defer"
                          and monitor.released == [7]), \
            h.journal_msgs  # _defer 先落账、release 在 finally——两态齐备才算落定
        assert 7 not in monitor._active

        h.gitop.fetch_error = None
        h.listener.queue.put(issue_open_event(iid=8))  # 正常派发
        assert wait_until(lambda: turn_finalized(h, 8)), h.journal_msgs
        assert monitor.registered[-1] == (8, "ag-123")
        assert 8 not in monitor.released, "登记后名额占用到终态（monitor 释放）"

        router_.decision = agora_decision(action="ignore", reason="属于前端仓")
        h.listener.queue.put(issue_open_event(iid=9))
        assert wait_until(lambda: turn_finalized(h, 9)
                          and 9 in monitor.released), h.journal_msgs
        assert 9 in monitor.released, "out_of_band 出口必须释放名额"
    finally:
        h.orch.stop()


def test_doctor_unavailable_keeps_pending_without_legacy_fallback(
        tmp_path, monkeypatch):
    """设计 §9.2：agora 不可用→保留待派发并重试，绝不降级 legacy
    opencode run（防超时后实际已建单造成双 worker）。"""
    client = FakeAgoraClient()
    client.doctor_result = AgoraUnavailable("agora down (fake)")
    h = agora_harness(tmp_path, monkeypatch, client=client)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: h.entry(7)["agora_pending"] is True), \
            h.journal_msgs
        assert h.runner.new_calls == [] and h.runner.cont_calls == []
        assert h.entry(7)["status"] == "pending"

        client.doctor_result = {"models": ["zai/glm-5.3"]}  # 服务恢复
        requeue_retry(h, 7)
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        assert client.created_ids == ["ag-123"]
    finally:
        h.orch.stop()


def test_router_ignore_does_not_create_and_closes(tmp_path, monkeypatch):
    """计划 L224：Router ignore 不建单——走现有范围外评论＋关单策略（关单＝
    终态暂停语义，收口交互归 Task 5）。"""
    client = FakeAgoraClient()
    router_ = FakeRouter(agora_decision(action="ignore", reason="属于前端仓"))
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        assert client.create_calls == []
        assert client.dispatch_calls == []
        assert monitor.registered == []
        assert h.entry(7)["agora_task_id"] is None
        assert any("范围外：属于前端仓" in b for _, b in h.api.posts)
        assert h.api.closes == [7]
        assert h.gitop.ensure_calls, "分支准备先于路由，照常发生"
    finally:
        h.orch.stop()


def test_router_question_does_not_create_only_asks(tmp_path, monkeypatch):
    """计划 L224：Router question 不建单——只发澄清评论，不关单不派发。"""
    client = FakeAgoraClient()
    router_ = FakeRouter(agora_decision(action="question", reason="缺复现步骤"))
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        assert client.create_calls == []
        assert client.dispatch_calls == []
        assert monitor.registered == []
        assert h.entry(7)["agora_task_id"] is None
        assert any("需要人工澄清：缺复现步骤" in b for _, b in h.api.posts)
        assert h.api.closes == [], "question 不关单"
    finally:
        h.orch.stop()


def test_new_note_round_redecides_and_chains_after_previous_tid(
        tmp_path, monkeypatch):
    """新 note＝新回合：重新路由、client_key 换 note 段、after 链上轮 tid；
    legacy session_id 全程不被改写。Task 5 起：上轮任务非终态时评论直通
    worker（见 test_active_comment_reaches_worker_...），新回合只在终态
    后发生——释放登记模拟终态再评论。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=2)  # 上轮任务未终态：名额须并行
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        seed_active(h, iid=7, session_id="ses_legacy")
        h.listener.queue.put(comment_event(iid=7, comment_id=101))
        assert wait_until(
            lambda: h.entry(7)["agora_task_id"] == "ag-123"
            and turn_finalized(h, 7)), h.journal_msgs
        assert h.entry(7)["session_id"] == "ses_legacy"

        monitor.release(7, "ag-123")  # 上轮任务终态：新评论才会重新路由
        h.listener.queue.put(comment_event(iid=7, comment_id=102))
        assert wait_until(
            lambda: h.entry(7)["agora_task_id"] == "ag-124"
            and turn_finalized(h, 7)), h.journal_msgs

        assert len(router_.calls) == 2
        first, second = client.create_calls
        assert first["client_key"] == "issue-agent:issue-7:note-101"
        assert second["client_key"] == "issue-agent:issue-7:note-102"
        assert second["after"] == "ag-123", "同 issue 上轮 tid 若在账须为 after"
        assert client.created_ids == ["ag-123", "ag-124"]
        assert h.entry(7)["session_id"] == "ses_legacy", "全程不被 agora 覆盖"
    finally:
        h.orch.stop()


def test_agora_ledger_fields_synced_across_construction_paths(tmp_path):
    """设计 §9.6 四处同步：state._new_issue、_normalize 与 orchestrator
    ._empty_entry 的新字段默认值一致（docstring 由 test_state 锁定）。"""
    import state as state_mod
    fresh = state_mod._new_issue("active")
    empty = orchestrator._empty_entry()
    for key, default in (("agora_task_id", None), ("agora_status", None),
                         ("delivery_commit", None), ("agora_pending", False),
                         ("router_goal", None), ("router_model", None)):
        assert fresh[key] == default, key
        assert empty[key] == default, key
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"issues": {"5": {"status": "active"}}}))
    normed = state_mod.load_state(path)["issues"]["5"]
    assert normed["agora_pending"] is False
    assert normed["agora_task_id"] is None


# --- agora review-round fixes (评审环 20261113-1435 F1/F2) ---------------------


def test_default_ledger_lock_is_reentrant_across_agora_defer(tmp_path,
                                                             monkeypatch):
    """评审环 F1：未注入 ledger_lock 的构造（缺省回退锁）下，_agora_route
    持锁内调 _defer 同线程再取锁——普通 Lock 确定性自锁（回合挂死、issue
    永远 pending）；缺省必须是 RLock。"""
    router_ = FakeRouter()
    router_.error = router_mod.RouterDecisionError("bad json (fake)")
    h = agora_harness(tmp_path, monkeypatch, router_=router_,
                      ledger_lock=None)  # 缺省回退路径
    try:
        assert isinstance(h.orch._ledger_lock, type(threading.RLock()))
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: h.entry(7)["status"] == "defer"), \
            h.journal_msgs  # 修复前：worker 线程自锁，状态永远停在 pending
    finally:
        h.orch.stop()


def test_doctor_rejected_blocks_with_help_note(tmp_path, monkeypatch):
    """评审环 F2：doctor 的确定性 4xx（如牌轮换）→ blocked＋求助评论，
    不静默无限重试。"""
    client = FakeAgoraClient()
    client.doctor_result = AgoraRejected("credential rotated (fake)")
    h = agora_harness(tmp_path, monkeypatch, client=client)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(
            lambda: h.entry(7)["status"] == "blocked"
            and any("doctor 被拒" in b for _, b in h.api.posts)), \
            h.journal_msgs  # _blocked 先写账本后发评论：两态齐备才算落定
        assert h.entry(7)["claimed_at"] is None
        assert client.create_calls == []
    finally:
        h.orch.stop()


def test_create_protocol_error_recovers_same_task(tmp_path, monkeypatch):
    """评审环 F2：create 返回 200 非 JSON（协议错乱，服务端可能已提交）→
    与超时同处置：同 client_key 重试取回原任务，不重复建单。"""
    client = FakeAgoraClient()
    client.create_failures.append(AgoraProtocolError("not json (fake)"))
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        assert client.created_ids == ["ag-123"]
        assert len(client.create_calls) == 2
        first, second = client.create_calls
        assert second["client_key"] == first["client_key"]
        assert monitor.registered == [(7, "ag-123")]
    finally:
        h.orch.stop()


# --- agora continuous conversation & terminal semantics (plan Task 5) -----------


def seed_active_agora_task(h, *, iid=7, tid="ag-123"):
    """账本登记 agora 任务＋monitor 在案：模拟派发完成后的稳态（issue 活跃、
    任务非终态）。"""
    st = state.load_state(h.state_path)
    entry = st["issues"].setdefault(str(iid), state._new_issue("active"))
    entry["agora_task_id"] = tid
    state.save_state(h.state_path, st)
    h.orch.agora_monitor.register(iid, tid)
    return tid


def test_active_comment_reaches_worker_without_waiting_for_turn(
        tmp_path, monkeypatch):
    """计划锚例（T4-B1 根修）：awaiting_external 交付窗口（任务非终态）内
    同 issue 新评论 → daemon 直接 post(kind=question) 给在案 worker（body
    带 issue/note id），零 task_create、零 after 链、登记 tid 不变。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        assert monitor.registered == [(7, "ag-123")]

        h.listener.queue.put(
            comment_event(iid=7, body="请改用方案 B", comment_id=102))
        assert wait_until(lambda: any(p["kind"] == "question"
                                      for p in client.posts)), h.journal_msgs

        question = next(p for p in client.posts if p["kind"] == "question")
        assert question["task"] == "ag-123"
        assert "请改用方案 B" in question["body"]
        assert "102" in question["body"], "body 必须带 note id"
        assert "7" in question["body"], "body 必须带 issue 标识"
        assert client.created_ids == ["ag-123"], "活动窗口零新任务"
        assert len(router_.calls) == 1, "零重路由"
        assert h.entry(7)["agora_task_id"] == "ag-123", "登记 tid 不被覆盖"
        assert turn_finalized(h, 7), "直通回合照常收尾（claim 闭环）"
    finally:
        h.orch.stop()


def test_terminal_comment_uses_router_for_new_round(tmp_path, monkeypatch):
    """计划锚例：任务已终态（名额释放、账本 tid 让位候选）→ 新评论重新走
    Router 建新回合，且 after 链上轮 tid。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=2)
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        monitor.release(7, "ag-123")  # 任务终态：monitor 收口释放

        h.listener.queue.put(
            comment_event(iid=7, body="继续修复边界情况", comment_id=102))
        assert wait_until(lambda: len(client.created_ids) == 2), h.journal_msgs

        assert len(router_.calls) == 2, "终态后必须重新路由"
        assert client.create_calls[1]["after"] == "ag-123"
        assert client.posts == [], "终态后不直通旧 worker"
    finally:
        h.orch.stop()


def test_active_comment_relay_unavailable_keeps_pending(tmp_path, monkeypatch):
    """直通投递遇 agora 不可用 → 保留待派发重试（与建单/派发同口径），
    绝不丢失评论也绝不降级 legacy。"""
    client = FakeAgoraClient()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        client.post_failures.append(AgoraUnavailable("post down (fake)"))

        h.listener.queue.put(
            comment_event(iid=7, body="请补充测试", comment_id=102))
        assert wait_until(lambda: h.entry(7)["agora_pending"] is True), \
            h.journal_msgs
        assert h.entry(7)["status"] == "pending", "claim 留存待重试"
        assert client.created_ids == ["ag-123"]
    finally:
        h.orch.stop()


def test_stale_registry_rejected_relay_falls_back_to_router(
        tmp_path, monkeypatch):
    """注册表陈旧竞态（board 已终态、poll 未及释放）→ 直通被服务端拒
    （AgoraRejected）→ 回落 Router 新回合，不得卡死在直通路径。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=2)
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        monitor.release(7, "ag-123")  # board 侧已终态
        monitor.register(7, "ag-123")  # 注册表陈旧（poll 未及收敛）
        client.post_failures.append(AgoraRejected("任务已收口 (fake)"))

        h.listener.queue.put(
            comment_event(iid=7, body="还有问题", comment_id=102))
        assert wait_until(lambda: len(client.created_ids) == 2), h.journal_msgs
        assert len(router_.calls) == 2
    finally:
        h.orch.stop()


def test_repost_uses_persisted_report_zero_dispatch(tmp_path, monkeypatch):
    """重发汇报（设计 §9.5）：agora 模式复用持久化 worker 报告——零派工、
    零提交、零推送；无报告时只发事实型未找到评论（同样零派工）。"""
    client = FakeAgoraClient()
    monitor = FakeMonitor()
    report = "根因：竞态窗口\n改动：orchestrator.py\n坑位：tests/fake.py 是替身"
    h = agora_harness(tmp_path, monkeypatch, client=client, monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        created_before = list(client.create_calls)

        monitor.report = report
        h.listener.queue.put(comment_event(iid=7, body="重发汇报",
                                           comment_id=102))
        assert wait_until(lambda: any(report in b for _, b in h.api.posts)), \
            h.journal_msgs
        assert client.create_calls == created_before, "零派工"
        assert h.gitop.push_calls == [], "零推送"
        assert h.entry(7)["status"] == "active"
        assert h.entry(7)["claimed_at"] is None
    finally:
        h.orch.stop()


def test_repost_without_report_posts_factual_note_zero_dispatch(
        tmp_path, monkeypatch):
    """无持久化报告的重发汇报：只发事实型「未找到」评论，不打扰 worker、
    不降级成新回合。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        created_before = list(client.create_calls)
        router_calls_before = len(router_.calls)

        h.listener.queue.put(comment_event(iid=7, body="重发汇报",
                                           comment_id=102))
        assert wait_until(
            lambda: any("重发" in b or "汇报" in b for _, b in h.api.posts)
            and h.entry(7)["claimed_at"] is None), h.journal_msgs
        assert client.create_calls == created_before, "零派工"
        assert len(router_.calls) == router_calls_before, "零路由"
    finally:
        h.orch.stop()


def test_pause_blocks_new_dispatch_but_monitor_continues(
        tmp_path, monkeypatch):
    """暂停语义（设计 §9.5）：禁新派工不取消在飞——monitor 登记不动、
    暂停期间新评论不建任务不路由；恢复后按任务状态路由（活动任务直通）。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        created_before = list(client.create_calls)

        h.listener.queue.put(comment_event(iid=7, body="暂停", comment_id=102))
        assert wait_until(lambda: h.entry(7)["status"] == "paused"), \
            h.journal_msgs
        assert h.orch.agora_monitor._active.get(7) == "ag-123", \
            "暂停不取消在飞 monitor 登记"

        h.listener.queue.put(comment_event(iid=7, body="随便聊聊",
                                           comment_id=103))
        time.sleep(0.3)
        assert client.create_calls == created_before, "暂停期零派工"
        assert client.posts == [], "暂停期零直通零路由"

        h.listener.queue.put(comment_event(iid=7, body="恢复", comment_id=104))
        assert wait_until(lambda: h.entry(7)["status"] == "active"), \
            h.journal_msgs
        h.listener.queue.put(
            comment_event(iid=7, body="请改用方案 B", comment_id=105))
        assert wait_until(lambda: any(p["kind"] == "question"
                                      for p in client.posts)), h.journal_msgs
        assert client.create_calls == created_before, \
            "恢复后活动任务仍在：直通而非新回合"
    finally:
        h.orch.stop()


def test_close_keeps_inflight_and_reopen_restores_routing(
        tmp_path, monkeypatch):
    """关单＝终态暂停（2026-11-13 裁定）：不调 task_cancel、在飞任务由
    monitor 走完、close-push 照旧；close 后新评论不建任务；重开恢复正常
    路由（新回合 after 链上轮 tid）。"""
    client = FakeAgoraClient()
    router_ = FakeRouter()
    monitor = FakeMonitor(max_active=2)
    h = agora_harness(tmp_path, monkeypatch, client=client, router_=router_,
                      monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs
        created_before = list(client.create_calls)

        close = issue_open_event(iid=7)
        close["action"] = "closed"
        h.listener.queue.put(close)
        assert wait_until(lambda: h.entry(7)["status"] == "done"), \
            h.journal_msgs
        assert client.cancel_calls == [], "关单绝不调 task_cancel"
        assert h.orch.agora_monitor._active.get(7) == "ag-123", \
            "在飞任务由 monitor 继续走完（登记不动）"

        h.listener.queue.put(
            comment_event(iid=7, body="关单后的留言", comment_id=102,
                          issue_state="closed"))
        time.sleep(0.3)
        assert client.create_calls == created_before, "关单后不建新任务"
        assert client.posts == []

        reopen = issue_open_event(iid=7)
        reopen["action"] = "reopened"
        h.listener.queue.put(reopen)
        assert wait_until(lambda: h.entry(7)["status"] == "active"), \
            h.journal_msgs
        monitor.release(7, "ag-123")  # 在飞任务此后由 monitor 交付收口
        h.listener.queue.put(
            comment_event(iid=7, body="重开后继续修", comment_id=103))
        assert wait_until(lambda: len(client.created_ids) == 2), h.journal_msgs
        assert client.create_calls[-1]["after"] == "ag-123", "重开恢复路由"
    finally:
        h.orch.stop()


# --- agora review round 1 fixes (ocr 035ba80a) ----------------------------------


def test_monitor_recovers_before_replays_on_startup(tmp_path, monkeypatch):
    """ocr R1 #1：monitor（内含账本恢复）必须先于 listener 与 baseline
    reconcile 启动——重启窗口内注册表为空，停机期到达的评论会被重放并
    绕过直通门控，为在飞任务重复建单。"""
    class OrderMonitor(FakeMonitor):
        shared = []  # 构造后指向 harness.order，与 listener/reconcile 共序

        def start(self):
            self.shared.append("monitor-start")
            super().start()

    monitor = OrderMonitor()
    h = agora_harness(tmp_path, monkeypatch, monitor=monitor, start=False)
    monitor.shared = h.order
    try:
        h.orch.start()
        assert "monitor-start" in h.order
        assert h.order.index("monitor-start") < \
            h.order.index("listener-start"), \
            f"monitor 必须先于 listener 启动（实际 {h.order}）"
        assert h.order.count("monitor-start") == 1, "幂等：只启动一次"
    finally:
        h.orch.stop()


def test_relay_unavailable_retry_preserves_comment_body(tmp_path,
                                                        monkeypatch):
    """ocr R1 #2：relay 不可达保留待派发后，重试（requeue 通道）必须转达
    原评论正文——不得把工人可读的用户留言换成 RETRY_MESSAGE。"""
    client = FakeAgoraClient()
    monitor = FakeMonitor()
    h = agora_harness(tmp_path, monkeypatch, client=client, monitor=monitor)
    try:
        h.listener.queue.put(issue_open_event(iid=7))
        assert wait_until(lambda: turn_finalized(h, 7)), h.journal_msgs

        h.api.issues[7] = [{"id": 102, "body": "请改用方案 B",
                            "user": {"login": "someone"}}]
        client.post_failures.append(AgoraUnavailable("post down (fake)"))
        h.listener.queue.put(
            comment_event(iid=7, body="请改用方案 B", comment_id=102))
        assert wait_until(lambda: h.entry(7)["agora_pending"] is True), \
            h.journal_msgs

        client.post_failures.clear()
        requeue_retry(h, 7)
        assert wait_until(lambda: any(p["kind"] == "question"
                                      for p in client.posts)), h.journal_msgs
        question = client.posts[-1]  # 重试通道发出的那条
        assert "请改用方案 B" in question["body"], \
            "重试必须转达原评论正文，而非 RETRY_MESSAGE"
        assert "系统重试" not in question["body"]
    finally:
        h.orch.stop()

