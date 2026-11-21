"""ag-72646a：评论授权执行器断言清单（HTTP/文件全 mock——api/agora 替身＋
真实 monitor 裁定 store 于 tmp_path，零网络零真实路径）。

背景（真实事故 ag-0f7d7f）：verify 建单即锁后 dispatcher/worker 的
task_update 均 403，最终人工以 root HTTP 落账。用户裁定改 issue 面板
批准制：批准链纯代码零 LLM；批准＝引用已公示裁定 id（禁自由文本）；
单用途（consumed 落盘跨重启）。断言清单：
1. 合法批准 → 恰 1 次 task_update 且参数与公示逐字节一致＋结果评论＋
   consumed 落盘；
2. 非白名单作者 → 拒，零调用，拒绝评论含原因；
3. consumed 后重放同 id → 拒；
4. 正文回读不符（篡改）→ 拒；
5. 零 LLM：注入 mock Router，批准全链路 router 从未被调用；
6. store 重载（模拟重启）后 consumed 状态仍拒重放。

Run: python3 -m pytest tests/test_comment_approval.py -q
"""

import json
import threading
from pathlib import Path

import pytest

import approval
import config
import fsm
import orchestrator
from agora_client import AgoraRejected
from agora_monitor import AgoraMonitor
from tests.test_config import write_config

IID = 7
TID = "ag-77"
RID = "abcd1234"
APPROVER = "alice"
OLD_CMD = "make check-old"
NEW_CMD = "python3 -m pytest tests/test_x.py -q"


def seed_ruling(state_path, rid=RID, **merge):
    """按 T3 (ag-791c36) store 形态落一条可执行裁定。"""
    ruling = {"task": TID, "iid": IID, "action": "task_update",
              "params": {"verify_cmd_old": OLD_CMD, "verify_cmd": NEW_CMD},
              "time": 1.0}
    ruling.update(merge)
    path = Path(str(state_path) + ".agora-rulings.json")
    path.write_text(json.dumps({"tasks": {}, "rulings": {rid: ruling}}),
                    encoding="utf-8")
    return ruling


def rulings_of(state_path):
    path = Path(str(state_path) + ".agora-rulings.json")
    return json.loads(path.read_text(encoding="utf-8"))["rulings"]


class FakeAgora:
    """board 复核面＋root 执行面记账（镜像 wire 形状）。"""

    def __init__(self, current_cmd=OLD_CMD):
        self.current_cmd = current_cmd
        self.board_reads = []
        self.root_updates = []
        self.error = None

    def board(self, tid):
        self.board_reads.append(tid)
        return {"task": {"state": "running",
                         "verify": {"cmd": self.current_cmd}},
                "events": []}

    def root_task_update(self, tid, verify_cmd):
        self.root_updates.append({"task": tid, "verify_cmd": verify_cmd})
        if self.error is not None:
            raise self.error
        self.current_cmd = verify_cmd


class FakeApi:
    """评论回读面（不信任扫描载荷——按 id 回读核实）。"""

    def __init__(self, comment):
        self.comment = comment
        self.reads = []

    def get_issue_comment(self, project, comment_id):
        self.reads.append((project, comment_id))
        return self.comment

    def build_note_body(self, text):
        return f"🤖 {text}\n<!-- issue-agent -->"

    def post_note(self, project, number, body):
        self.notes = getattr(self, "notes", [])
        self.notes.append((project, number, body))
        return {"id": 1}


def make_executor(tmp_path, *, agora=None, comment=None,
                  approvers=(APPROVER,)):
    """接线完毕的执行器：真实 monitor store 于 tmp_path，api/agora 替身。"""
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {str(IID): {
        "status": "active", "agora_task_id": TID}}}), encoding="utf-8")
    monitor = AgoraMonitor(client=object(), journal=lambda m: None,
                           max_active=1)
    monitor.attach_delivery(repo_path=None,
                            remote_url="https://x@invalid/g/r.git",
                            ssl_verify=True, state_path=state_path,
                            ledger_lock=threading.RLock())
    agora = agora or FakeAgora()
    replies, journal = [], []
    ex = approval.CommentApproval(
        api=FakeApi(comment or {"id": 555, "body": f"批准 {RID}",
                                "user": {"login": APPROVER}}),
        project="group/repo", agora_client=agora, monitor=monitor,
        approvers=approvers, journal=journal.append,
        post_note=lambda iid, text: (replies.append((iid, text)), True)[1])
    return ex, agora, replies, journal, state_path


# --- 指令分类：fsm 严格全行匹配 -------------------------------------------


def test_fsm_routes_strict_approval_command():
    ledger = {"7": {"status": "active", "processed_note_ids": []}}

    def decide(body):
        return fsm.decide(
            {"action": "created", "issue": {"number": 7, "state": "open"},
             "comment": {"id": 1, "body": body, "user": {"login": "x"}}},
            ledger)

    assert decide("批准 abcd1234")["action"] == "approve"
    assert decide(" 批准 abcd1234 \n")["action"] == "approve"  # 全行 strip
    assert decide("批准 ABCD1234")["action"] == "continue"  # 大写不认
    assert decide("批准 abcd1234 请尽快")["action"] == "continue"
    assert decide("请批准 abcd1234")["action"] == "continue"
    assert decide("批准 abcd123")["action"] == "continue"  # 7 位不认


# --- 1) 合法批准：恰 1 次 root task_update＋结果评论＋consumed 落盘 --------


def test_valid_approval_executes_once_with_exact_params(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)

    ex.handle(IID, 555)

    assert agora.root_updates == [{"task": TID, "verify_cmd": NEW_CMD}], \
        "恰 1 次且参数与公示裁定逐字节一致"
    assert len(replies) == 1, replies
    assert replies[0][0] == IID and "批准执行" in replies[0][1] \
        and NEW_CMD in replies[0][1], replies
    assert rulings_of(state_path)[RID]["consumed"] is True
    assert TID in str(agora.board_reads)


# --- 2) 非白名单作者 → 拒，零调用，拒绝评论含原因 --------------------------


def test_non_whitelist_author_rejected_zero_calls(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    ex._api.comment["user"]["login"] = "mallory"

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert len(replies) == 1 and "白名单" in replies[0][1], replies
    assert any("rejected" in m for m in journal), journal
    assert rulings_of(state_path)[RID].get("consumed") is not True


# --- 3) consumed 后重放同 id → 拒 ------------------------------------------


def test_consumed_ruling_replay_rejected(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)

    ex.handle(IID, 555)
    ex.handle(IID, 556)  # 新评论重放同裁定 id

    assert len(agora.root_updates) == 1, "重放不得二次执行"
    assert "已消费" in replies[-1][1], replies


# --- 4) 正文回读不符（篡改）→ 拒 -------------------------------------------


def test_tampered_body_rejected(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    ex._api.comment["body"] = f"批准 {RID} 另加自由文本"

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert len(replies) == 1 and "不符" in replies[0][1], replies
    assert rulings_of(state_path)[RID].get("consumed") is not True


# --- 5) 零 LLM：批准全链路 router 从未被调用 -------------------------------


class SpyRouter:
    """任何属性访问即记录并炸断——零 LLM 断言面。"""

    def __init__(self):
        self.touches = []

    def __getattr__(self, name):
        self.touches.append(name)
        raise AssertionError(f"approval chain touched the router: {name}")


def test_zero_llm_full_chain_via_orchestrator(tmp_path):
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {str(IID): {
        "status": "active", "agora_task_id": TID}}}), encoding="utf-8")
    seed_ruling(state_path)
    api_face = FakeApi({"id": 555, "body": f"批准 {RID}",
                        "user": {"login": APPROVER}})
    agora = FakeAgora()
    monitor = AgoraMonitor(client=object(), journal=lambda m: None,
                           max_active=1)
    protocol = tmp_path / "PROTOCOL.md"
    protocol.write_text("# stub\n")
    (tmp_path / "repo").mkdir()
    spy_router = SpyRouter()
    orch = orchestrator.Orchestrator(
        project="group/repo", api=api_face, journal=lambda m: None,
        state_path=state_path, repo_path=str(tmp_path / "repo"),
        remote_url="https://x@invalid/g/r.git", protocol_path=str(protocol),
        agora_client=agora, router=spy_router, agora_monitor=monitor,
        approvers=(APPROVER,))

    orch._handle_event({
        "action": "created", "issue": {"number": IID, "state": "open"},
        "comment": {"id": 555, "body": f"批准 {RID}",
                    "user": {"login": APPROVER}}})

    assert spy_router.touches == [], "批准链必须纯代码零 LLM"
    assert agora.root_updates == [{"task": TID, "verify_cmd": NEW_CMD}]
    assert api_face.notes and "批准执行" in api_face.notes[0][2]
    assert rulings_of(state_path)[RID]["consumed"] is True
    ledger = json.loads(state_path.read_text(encoding="utf-8"))
    assert 555 in ledger["issues"][str(IID)]["processed_note_ids"], \
        "批准评论消费即记 note id"


# --- 6) store 重载（模拟重启）后 consumed 仍拒重放 --------------------------


def test_store_reload_still_rejects_replay(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    ex.handle(IID, 555)

    # 模拟重启：全新 monitor/执行器，同一 store 文件
    ex2, agora2, replies2, journal2, _ = make_executor(tmp_path)
    ex2.handle(IID, 700)

    assert agora2.root_updates == [], "重启后重放已消费裁定不得执行"
    assert "已消费" in replies2[-1][1], replies2
    assert len(agora.root_updates) == 1


# --- 拒绝面补充：未知 id／不可执行动作／board 基线偏离 ----------------------


def test_unknown_ruling_id_rejected(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path, rid="ffff0000")  # store 里没有 RID

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert "未知裁定" in replies[-1][1], replies


def test_unsupported_ruling_action_rejected(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path, action="stuck_escalation",
                params={"kind": "identical_notes"})

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert "task_update" in replies[-1][1], replies
    assert rulings_of(state_path)[RID].get("consumed") is not True


def test_board_baseline_drift_rejected(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    agora.current_cmd = "someone-else-changed-it"  # 既非旧值也非新值

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert "偏离" in replies[-1][1], replies
    assert rulings_of(state_path)[RID].get("consumed") is not True


def test_board_contract_external_shape_rejected(tmp_path):
    """评审环 R2（ocr medium）：board verify 真值非 dict 不得让异常逃逸。"""
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)

    def bad_board(tid):
        return {"task": {"state": "running", "verify": "garbage"}}
    ex._agora.board = bad_board
    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert "偏离" in replies[-1][1], replies
    assert rulings_of(state_path)[RID].get("consumed") is not True


def test_cross_issue_ruling_rejected(tmp_path):
    """评审环 R2（ocr low）：裁定只可在其公示 issue 面板上批准。"""
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path, iid=99)  # 公示于别的 issue

    ex.handle(IID, 555)

    assert agora.root_updates == []
    assert "issue#99" in replies[-1][1] and "未执行" in replies[-1][1], replies


def test_malformed_rulings_container_rejected_not_crash(tmp_path):
    """评审环 B 册 B-2：rulings 段非 dict（坏档）按空表处理，绝不炸。"""
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    Path(str(state_path) + ".agora-rulings.json").write_text(
        json.dumps({"tasks": {}, "rulings": []}), encoding="utf-8")

    ex.handle(IID, 555)  # 原 AttributeError 逃逸形态

    assert agora.root_updates == []
    assert "未知裁定" in replies[-1][1], replies


# --- 幂等保护：已同值 → 零 root 调用收口＋消费 ------------------------------


def test_idempotent_same_value_skips_root_call(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    agora.current_cmd = NEW_CMD  # 上次执行已落账（回包丢失形态）

    ex.handle(IID, 555)

    assert agora.root_updates == [], "已同值不再打 root 面"
    assert "幂等" in replies[-1][1], replies
    assert rulings_of(state_path)[RID]["consumed"] is True


# --- 执行异常：一次即转人工，不自动重试；人工重发即重试通道 ------------------


def test_execution_failure_escalates_no_auto_retry(tmp_path):
    ex, agora, replies, journal, state_path = make_executor(tmp_path)
    seed_ruling(state_path)
    agora.error = AgoraRejected("boom")

    ex.handle(IID, 555)

    assert len(agora.root_updates) == 1, "恰尝试一次"
    assert "转人工" in replies[-1][1], replies
    assert rulings_of(state_path)[RID].get("consumed") is not True

    agora.error = None  # 人工重发批准评论（新评论 id）＝重试通道
    ex.handle(IID, 556)
    assert len(agora.root_updates) == 2
    assert rulings_of(state_path)[RID]["consumed"] is True


# --- config：approvers 缺省 [gitea_user]／显式列表 --------------------------


def test_config_approvers_default_and_explicit(tmp_path, monkeypatch):
    for var in ("ISSUE_AGENT_CONFIG", "AGORA_HOME", "ISSUE_AGENT_SSL_VERIFY"):
        monkeypatch.delenv(var, raising=False)
    cfg = config.load_config(write_config(tmp_path, gitea_user="bot1"))
    assert cfg.approvers == ("bot1",), "缺省白名单＝[gitea_user]"
    cfg = config.load_config(write_config(
        tmp_path, gitea_user="bot1", approvers=["alice", "bob"]))
    assert cfg.approvers == ("alice", "bob")
    cfg = config.load_config(write_config(tmp_path, gitea_user="bot1",
                                          approvers=[]))
    assert cfg.approvers == (), "显式空表＝关闭批准（不回落 gitea_user）"
    cfg = config.load_config(write_config(
        tmp_path, approvers=["alice", "", "bob"]))
    assert cfg.approvers == ("alice", "bob"), "空串条目不入白名单"
    with pytest.raises(config.ConfigError, match="approvers"):
        config.load_config(write_config(tmp_path, approvers="alice"))
    with pytest.raises(config.ConfigError, match="approvers"):
        config.load_config(write_config(tmp_path, approvers=True))
