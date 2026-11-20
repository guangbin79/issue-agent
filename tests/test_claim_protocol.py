"""双机认领协议 v2（纯轮询＋粘住式认领＋无抢占）模拟测试。

两台机（两个 Orchestrator、两本账本）共用一面评论墙（WallApi＝Gitea
事件源）。覆盖任务断言清单：并发双认领裁决（id 小者胜）、认输不重试、
三态分类、空槽两级优先、满槽不认领、认领单入口（reconcile 不 claim）、
让位 id 比较判据、认领前复核、candidate 崩溃恢复、旧 state.json 兼容、
machine_id 配置面。

Run: python3 -m pytest tests/test_claim_protocol.py -q
"""

import json
import re
from pathlib import Path

import pytest

from tests.test_orch import (FakeListener, FakeRunner, FakeGitop,  # noqa: F401
                             turn_result)
import api  # noqa: F401
import fsm
import gitop
import orchestrator
import runner
import state
from config import load_config

PKG_DIR = Path(__file__).resolve().parent.parent

MACHINE_A = "aaaa1111"
MACHINE_B = "bbbb2222"


# --------------------------------------------------------------------------
# 共享评论墙（两机同源的事件面）
# --------------------------------------------------------------------------

class WallApi:
    """Gitea 面：人类评论＋机器认领评论同墙；发帖即分配递增 id。"""

    def __init__(self, issues=None):
        self.issues = {int(i): list(n) for i, n in (issues or {}).items()}
        self.next_id = 5000
        self.posts = []          # (iid, body)
        self.on_post = None      # hook(iid, body, cid)：测试注入他机并发认领
        self.closes = []
        self.note_marker = "<!-- issue-agent-a -->"

    # --- 与 GitLabClient 同形的 note 面 -------------------------------

    def build_note_body(self, text):
        return f"\U0001f916 {text}\n{self.note_marker}"

    def build_claim_body(self, machine_id):
        return (f"\U0001f916 认领本 issue（machine={machine_id}）\n"
                f"<!--claim:machine={machine_id}-->\n{self.note_marker}")

    def add_comment(self, iid, body, cid=None):
        if cid is None:
            self.next_id += 1
            cid = self.next_id
        self.issues.setdefault(int(iid), []).append(
            {"id": cid, "body": body, "user": {"login": "someone"}})
        return cid

    def claims(self, iid):
        return sorted((cid, m) for cid, m in
                      fsm.claim_comments(self.issues.get(int(iid), [])))

    # --- api client 面 -------------------------------------------------

    def list_open_issues(self, project):
        return [{"number": iid, "title": f"issue {iid}", "body": f"正文 {iid}",
                 "state": "open"} for iid in sorted(self.issues)]

    def list_issue_comments(self, project, iid, since_iso=None):
        return [dict(c) for c in self.issues.get(int(iid), [])]

    def get_issue(self, project, number):
        return {"number": int(number), "title": f"issue {number}",
                "body": f"正文 {number}", "state": "open"}

    def get_issue_comment(self, project, comment_id):
        for comments in self.issues.values():
            for c in comments:
                if c["id"] == int(comment_id):
                    return dict(c)
        raise api.ApiTransientError("not found")

    def post_note(self, project, iid, body):
        cid = self.add_comment(iid, body)
        self.posts.append((int(iid), body))
        if self.on_post is not None:
            self.on_post(int(iid), body, cid)
        return {"id": cid}

    def post_claim(self, project, iid, machine_id):
        return self.post_note(project, iid, self.build_claim_body(machine_id))

    def close_issue(self, project, iid):
        self.closes.append(int(iid))


class FakeAgoraClient:
    """让位取消面：board＋task_cancel 记录器。"""

    def __init__(self):
        self.cancels = []
        self.board_state = "running"

    def board(self, tid):
        return {"task": {"state": self.board_state}}

    def task_cancel(self, tid, force=False):
        self.cancels.append((tid, force))
        return {"cancelled": 1}


class MachineHarness:
    """一台机：独立账本＋独立 listener，api 指向共享墙。"""

    def __init__(self, tmp_path, monkeypatch, wall, machine_id,
                 agora_client=None, max_parallel=1, name="m"):
        # 与 test_orch.Harness 相同的模块面 fake 接线（runner/gitop 全 fake）
        self.order = []
        self.journal_msgs = []
        self.listener = FakeListener(self.order)
        self.runner = FakeRunner(self.order)
        self.gitop = FakeGitop(self.order)
        monkeypatch.setattr(runner, "new_session", self.runner.fake_new)
        monkeypatch.setattr(runner, "continue_session", self.runner.fake_cont)
        monkeypatch.setattr(runner, "export_last_report", self.runner.fake_export)
        monkeypatch.setattr(gitop, "fetch_base", self.gitop.fake_fetch)
        monkeypatch.setattr(gitop, "tracked_dirty", self.gitop.fake_tracked_dirty)
        monkeypatch.setattr(gitop, "has_new_commits_vs_ref", self.gitop.fake_has_vs_ref)
        monkeypatch.setattr(gitop, "push_fix_branch", self.gitop.fake_push)
        monkeypatch.setattr(gitop, "ensure_fix_branch", self.gitop.fake_ensure_fix_branch)
        self.dir = tmp_path / name
        self.dir.mkdir()
        self.state_path = self.dir / "state.json"
        (self.dir / "repo").mkdir(exist_ok=True)
        protocol = self.dir / "PROTOCOL.md"
        protocol.write_text("# stub\n")
        self.orch = orchestrator.Orchestrator(
            project="group/repo", api=wall,
            journal=self.journal_msgs.append, listener=self.listener,
            state_path=self.state_path, repo_path=str(self.dir / "repo"),
            remote_url="https://x@fake.invalid/group/repo.git",
            protocol_path=str(protocol), machine_id=machine_id,
            max_parallel=max_parallel, agora_client=agora_client)

    def journal(self, *needles):
        return [m for m in self.journal_msgs if any(n in m for n in needles)]

    def ledger(self):
        return state.load_state(self.state_path)

    def entry(self, iid):
        return self.ledger()["issues"].get(str(int(iid)))

    def seed(self, iid, **fields):
        st = state.load_state(self.state_path)
        entry = st["issues"].setdefault(str(int(iid)), state._new_issue("active"))
        entry.update(fields)
        state.save_state(self.state_path, st)
        return entry


@pytest.fixture(autouse=True)
def _fast_arbitration(monkeypatch):
    monkeypatch.setattr(orchestrator, "ARBITRATION_WINDOW_S", 0)


@pytest.fixture()
def wall():
    return WallApi()


def one_machine(tmp_path, monkeypatch, wall, machine_id=MACHINE_A, **kw):
    # 他机行为全部经评论墙注入（on_post/预置评论）模拟——只建本机 harness，
    # 避免第二个 harness 覆盖模块级 runner/gitop 补丁面。
    return MachineHarness(tmp_path, monkeypatch, wall, machine_id, name="a", **kw)


def two_machines(tmp_path, monkeypatch, wall, **kw):
    a = one_machine(tmp_path, monkeypatch, wall, MACHINE_A, **kw)
    b = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_B, name="b", **kw)
    return a, b


# --------------------------------------------------------------------------
# 1. 并发双认领裁决：id 小者胜
# --------------------------------------------------------------------------

def test_arbitration_smaller_claim_id_wins(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "构建失败，请修复")  # 人类 issue 正文评论
    # A 发认领评论的瞬间，B 抢先一步（评论 id 更小）——裁决窗内同场竞技
    wall.on_post = lambda iid, body, cid: wall.add_comment(
        iid, wall.build_claim_body(MACHINE_B), cid - 1)
    a.orch._scan_once()
    entry = a.entry(7)
    assert entry is not None, "A 已入账（candidate→认输）"
    assert entry["claim_state"] == "yielded", "A id 大 → 认输"
    assert entry["owner"]["machine"] == MACHINE_B
    other_id = entry["owner"]["claim_comment_id"]
    my_id = max(cid for cid, m in wall.claims(7) if m == MACHINE_A)
    assert other_id < my_id, "胜者＝认领评论 id 最小者"
    assert not a.runner.new_calls, "认输方不得开工"


def test_arbitration_larger_peer_id_loses_and_takeover(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "构建失败，请修复")
    # B 迟到（评论 id 更大）——A 赢
    wall.on_post = lambda iid, body, cid: wall.add_comment(
        iid, wall.build_claim_body(MACHINE_B), cid + 10)
    a.orch._scan_once()
    entry = a.entry(7)
    assert entry["claim_state"] == "owned"
    assert entry["owner"]["machine"] == MACHINE_A
    assert len(a.runner.new_calls) == 1, "赢方接手开工（一轮）"
    assert "正文 7" in a.runner.new_calls[0]["message"], "首回合携带 issue 正文"
    assert a.entry(7)["status"] == "active", "回合收尾回落 active"
    # 粘住：再次扫描不重复认领、不重复开工
    a.orch._scan_once()
    assert len(a.runner.new_calls) == 1


# --------------------------------------------------------------------------
# 2. 认输不重试
# --------------------------------------------------------------------------

def test_loser_never_retries(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "正文")
    wall.on_post = lambda iid, body, cid: wall.add_comment(
        iid, wall.build_claim_body(MACHINE_B), cid - 1)
    a.orch._scan_once()
    assert a.entry(7)["claim_state"] == "yielded"
    claims_after_loss = wall.claims(7)
    wall.on_post = None
    a.orch._scan_once()
    a.orch._scan_once()
    assert wall.claims(7) == claims_after_loss, "认输后不得再发认领评论"
    assert not a.runner.new_calls


# --------------------------------------------------------------------------
# 3. 认领前复核
# --------------------------------------------------------------------------

def test_preclaim_recheck_skips_claimed_issue(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "正文")
    wall.add_comment(7, wall.build_claim_body(MACHINE_B), 300)
    a.orch._scan_once()
    assert wall.claims(7) == [(300, MACHINE_B)], "不发认领评论"
    assert a.entry(7)["claim_state"] == "yielded"
    assert a.entry(7)["owner"]["claim_comment_id"] == 300
    assert not a.runner.new_calls


# --------------------------------------------------------------------------
# 4. 三态分类（fsm 纯函数）
# --------------------------------------------------------------------------

@pytest.mark.parametrize("entry,fresh,expected", [
    # in-flight：pending 且无未消评论、非停靠
    ({"status": "pending", "agora_pending": False}, [], "in_flight"),
    # triggered：pending 未消（崩溃恢复）／停靠待派／水位新动
    ({"status": "pending", "agora_pending": False}, [88], "triggered"),
    ({"status": "pending", "agora_pending": True}, [], "triggered"),
    ({"status": "defer"}, [], "triggered"),
    ({"status": "active"}, [88], "triggered"),
    ({"status": "adopted"}, [88], "triggered"),
    # dormant：终报后水位未动（永不重开）；paused 永不自动唤醒；blocked 人工出口
    ({"status": "active"}, [], "dormant"),
    ({"status": "adopted"}, [], "dormant"),
    ({"status": "paused"}, [88], "dormant"),
    ({"status": "blocked"}, [88], "dormant"),
    ({"status": "done"}, [], "dormant"),
])
def test_triage_three_states(entry, fresh, expected):
    assert fsm.triage(entry, fresh) == expected


def test_triage_rejects_garbage():
    assert fsm.triage(None, []) == "dormant"
    assert fsm.triage({}, []) == "dormant"


# --------------------------------------------------------------------------
# 5. 空槽两级优先（triggered 先于新认领）
# --------------------------------------------------------------------------

def test_triggered_pool_beats_new_claim(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "新无主 issue")
    # 我名下 issue#5 挂起待重试（triggered 池有需求）——唯一空槽归它
    a.seed(5, status="defer")
    a.orch._scan_once()
    assert wall.claims(7) == [], "triggered 需求在场时不得认领新 issue"
    assert a.entry(7) is None, "不预占新 issue 的账"


def test_full_slots_no_claim(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(7, "新无主 issue")
    a.seed(5, status="pending")  # 在飞占满（max_parallel=1）
    a.orch._scan_once()
    assert wall.claims(7) == [], "满槽不认领"
    assert a.entry(7) is None


# --------------------------------------------------------------------------
# 6. 认领单入口：reconcile 不 claim
# --------------------------------------------------------------------------

def test_reconcile_never_initiates_claim(tmp_path, monkeypatch, wall):
    a = one_machine(tmp_path, monkeypatch, wall)
    a.orch.reconcile()  # 空墙基线（无存量可认领）
    # 基线之后新出现的无主 issue＋人类评论
    wall.add_comment(7, "人类评论")
    a.orch.reconcile()
    assert wall.claims(7) == [], "reconcile 不得发认领评论"
    assert not a.runner.new_calls, "reconcile 不得对无主新 issue 开工"
    entry = a.entry(7)
    assert entry is None or entry["status"] != "pending", "不得预占"
    # 扫描循环才是认领入口
    a.orch._scan_once()
    assert wall.claims(7), "扫描循环认领"


def test_adopted_legacy_issue_wakes_via_claim_not_replay(tmp_path, monkeypatch, wall):
    """存量 adopted issue 的人类新评论不走重放——转认领面接管（双机不双吃）。"""
    a = one_machine(tmp_path, monkeypatch, wall)
    wall.add_comment(5, "存量评论")  # 基线前评论
    a.orch.reconcile()  # 基线认领快照：5＝adopted，评论 5001 已快照
    assert a.entry(5)["status"] == "adopted"
    wall.add_comment(5, "现在开始处理")  # 人类唤醒评论
    a.orch.reconcile()
    assert not a.runner.new_calls, "存量唤醒不直通重放"
    a.orch._scan_once()
    assert wait_turn(a), "认领接管开工"
    assert any("现在开始处理" in c["message"] for c in a.runner.new_calls), \
        "唤醒评论正文直通接管回合"


def wait_turn(h):
    import time as _t
    deadline = _t.monotonic() + 3
    while _t.monotonic() < deadline:
        if a_runner_ran(h):
            return True
        _t.sleep(0.01)
    return a_runner_ran(h)


def a_runner_ran(h):
    return bool(h.runner.new_calls) and h.journal("finalized")


# --------------------------------------------------------------------------
# 7. 让位：认领评论 id 比较判据
# --------------------------------------------------------------------------

def _seed_owned(a, claim_id=100):
    a.seed(5, status="active", processed_note_ids=[90],
           claim_state="owned",
           owner={"machine": MACHINE_A, "claim_comment_id": claim_id},
           agora_task_id="ag-1")


def test_yield_only_when_peer_claim_id_smaller(tmp_path, monkeypatch, wall):
    agora = FakeAgoraClient()
    a = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_A,
                       agora_client=agora)
    _seed_owned(a, claim_id=100)
    wall.issues[5] = [
        {"id": 90, "body": "人类：请修复", "user": {"login": "h"}},
        {"id": 120, "body": wall.build_claim_body(MACHINE_B),
         "user": {"login": "bot"}},  # 他机迟到认领（id 更大＝他输）
    ]
    a.orch.reconcile()
    assert a.entry(5)["claim_state"] == "owned", "id 大的迟到认领不得停我在飞回合"
    assert agora.cancels == []
    # 他机更早认领评论出现（id 更小）→ 让位
    wall.add_comment(5, wall.build_claim_body(MACHINE_B), 95)
    a.orch.reconcile()
    entry = a.entry(5)
    assert entry["claim_state"] == "yielded"
    assert entry["owner"] == {"machine": MACHINE_B, "claim_comment_id": 95}
    assert agora.cancels == [("ag-1", False)], "让位取消在飞任务"
    # 让位后停派：新人类评论不再触发回合
    wall.add_comment(5, "人类追问")
    a.orch.reconcile()
    assert not a.runner.new_calls


def test_yielded_issue_skips_close_push(tmp_path, monkeypatch, wall):
    a = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_A)
    a.seed(5, status="done", claim_state="yielded",
           owner={"machine": MACHINE_B, "claim_comment_id": 95})
    a.orch._push_on_close(5)
    assert a.gitop.push_calls == [], "让位后停推送"


# --------------------------------------------------------------------------
# 8. candidate 崩溃恢复（死机重启自续）
# --------------------------------------------------------------------------

def test_candidate_recovers_via_reconcile(tmp_path, monkeypatch, wall):
    a = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_A)
    wall.add_comment(7, wall.build_claim_body(MACHINE_A), 105)
    a.seed(7, status="adopted", claim_state="candidate",
           owner={"machine": MACHINE_A, "claim_comment_id": 105})
    a.orch.reconcile()
    entry = a.entry(7)
    assert entry["claim_state"] == "owned", "窗口期崩溃→对账恢复为 owned"
    assert len(a.runner.new_calls) == 1, "恢复后自续开工"


def test_arbitrate_skips_when_takeover_already_dispatched(tmp_path, monkeypatch, wall):
    """B 册终闸：裁决窗内 reconcile 已接管派发（pending）→ 窗后不得复派。"""
    a = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_A)
    wall.add_comment(7, wall.build_claim_body(MACHINE_A), 105)
    a.seed(7, status="pending", claim_state="owned",
           owner={"machine": MACHINE_A, "claim_comment_id": 105})
    assert a.orch._arbitrate(7, wall.get_issue(None, 7)) is True
    assert not a.runner.new_calls, "已在飞（pending）不得重复开工"
    assert a.journal("接管派发")


def test_yielded_defer_issue_not_requeued(tmp_path, monkeypatch, wall):
    """B 册终闸：让位后的 defer 条目不得经 requeue 车道复活（双机双跑）。"""
    a = MachineHarness(tmp_path, monkeypatch, wall, MACHINE_A)
    a.seed(7, status="defer", claim_state="yielded",
           owner={"machine": MACHINE_B, "claim_comment_id": 90})
    a.orch._handle_requeue(7)
    assert not a.runner.new_calls
    assert a.entry(7)["status"] == "defer", "未被置 pending 复活"


# --------------------------------------------------------------------------
# 9. 旧 state.json 兼容
# --------------------------------------------------------------------------

def test_legacy_state_json_loads(tmp_path):
    legacy = {"issues": {"11": {"status": "active", "session_id": "s",
                                "branch": "issue/11-fix",
                                "processed_note_ids": [1, 2],
                                "claimed_at": None, "defer_count": 0}}}
    path = tmp_path / "state.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")
    st = state.load_state(path)
    entry = st["issues"]["11"]
    assert entry["claim_state"] == "none"
    assert entry["owner"] == {"machine": None, "claim_comment_id": None}
    state.save_state(path, st)  # 往返不丢
    again = state.load_state(path)
    assert again["issues"]["11"]["claim_state"] == "none"


def test_state_rejects_garbage_claim_state(tmp_path):
    # 与坏 status 同口径：load_state 吞 ValueError 回落空账本（warn 面留痕）
    bad = {"issues": {"11": {"status": "active", "claim_state": "bogus"}}}
    path = tmp_path / "state.json"
    path.write_text(json.dumps(bad), encoding="utf-8")
    warnings = []
    st = state.load_state(path, warn=warnings.append)
    assert st == {"issues": {}}
    assert any("unreadable" in w for w in warnings)


# --------------------------------------------------------------------------
# 10. machine_id 配置面
# --------------------------------------------------------------------------

def test_config_machine_id_loaded(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "api_base": "http://g", "pat": "t", "repo_path": str(tmp_path),
        "canonical_path": str(tmp_path), "callback_url": "http://cb",
        "machine_id": "ab12cd34"}), encoding="utf-8")
    cfg = load_config(cfg_file)
    assert cfg.machine_id == "ab12cd34"


def test_config_machine_id_optional_and_callback_tolerated(tmp_path):
    cfg_file = tmp_path / "config.json"
    cfg_file.write_text(json.dumps({
        "api_base": "http://g", "pat": "t", "repo_path": str(tmp_path),
        "canonical_path": str(tmp_path)}), encoding="utf-8")
    cfg = load_config(cfg_file)  # 无 callback_url/machine_id 也得载入
    assert cfg.machine_id == ""


# --------------------------------------------------------------------------
# 11. 认领评论识别（fsm）＋文档锚点
# --------------------------------------------------------------------------

def test_claim_machine_parse():
    assert fsm.claim_machine("🤖 认领\n<!--claim:machine=ab12cd34-->尾") == "ab12cd34"
    assert fsm.claim_machine("普通人类评论") is None
    assert fsm.claim_machine("") is None


def test_claim_comments_sorted():
    comments = [
        {"id": 12, "body": "<!--claim:machine=ffff0000-->"},
        {"id": 10, "body": "人类评论"},
        {"id": 11, "body": "<!--claim:machine=aaaa1111-->"},
    ]
    assert fsm.claim_comments(comments) == [(11, "aaaa1111"), (12, "ffff0000")]


def test_protocol_template_documents_claim_protocol():
    text = (PKG_DIR / "PROTOCOL.template.md").read_text(encoding="utf-8")
    assert "双机认领" in text
    assert "孤儿恢复" in text
    readme = (PKG_DIR / "README.md").read_text(encoding="utf-8")
    assert "双机" in readme


def test_claim_body_carries_markers(tmp_path):
    wall = WallApi()
    body = wall.build_claim_body(MACHINE_A)
    assert re.search(r"<!--claim:machine=%s-->" % MACHINE_A, body)
    assert wall.note_marker in body, "嵌既有 note marker，fsm 过滤不打环"
    assert "\U0001f916" in body, "通用 🤖 前缀兜底过滤"


def test_production_claim_body_carries_markers():
    """A 册终闸：生产 GitLabClient.build_claim_body（非测试替身）必须同时
    嵌 machine 标记＋note marker＋🤖 前缀——丢了就是双机互打环静默回归。"""
    client = api.GitLabClient("http://gitea", "t")
    body = client.build_claim_body(MACHINE_A)
    assert fsm.claim_machine(body) == MACHINE_A
    assert client.note_marker in body
    assert "\U0001f916" in body


def test_claim_machine_length_cap():
    """C 册终闸：超长 hex machine 标记＝敌意 body 持久化 DoS 面，拒识别。"""
    assert fsm.claim_machine("<!--claim:machine=%s-->" % ("a" * 8))
    assert fsm.claim_machine("<!--claim:machine=%s-->" % ("a" * 16))
    assert fsm.claim_machine("<!--claim:machine=%s-->" % ("a" * 17)) is None
