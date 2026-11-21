"""ag-791c36：死等检测＋issue 处置卡断言清单（mock api client，纯内存/
tmp_path，零网络）。

背景事故（ag-0f7d7f 死等 18h 放大器）：worker 每 30min 逐字同文 note＋
daemon 逐轮 nudge，无组件识别「同文申报循环＝死锁」，需人裁决事项从未
在 issue 面板形成待办。断言清单：
1. 连续 3 条相同 body note → 恰 1 张处置卡（含裁定 id）且后续轮不再
   nudge 该任务；
2. 第 2 条起 body 变化 → 计数重置不触发；
3. awaiting_external 超 stuck_await_s 发卡、未超不发；
4. 同卡点第二轮对账不重复发卡；
5. grep 断言零邮件路径（smtp|send_mail|import email 于仓 *.py 无命中）。

Run: python3 -m pytest tests/test_stuck_escalation.py -q
"""

import json
import re
import subprocess
import threading
from pathlib import Path

import agora_monitor
from agora_monitor import AgoraMonitor
from tests.test_agora_monitor import (PAT_URL, TID, ConversationFace,
                                      FakeAgoraClient)

IID = 7
REPO_ROOT = Path(__file__).resolve().parent.parent
_RID_RE = re.compile(r"裁定id ([0-9a-f]{8})")


class FakeClock:
    """可控时钟（替换 agora_monitor.time——检测阈值全在 time.time 域）。"""

    def __init__(self, start=1_000_000.0):
        self.now = float(start)

    def time(self):
        return self.now

    def monotonic(self):
        return self.now


def note(mid, body):
    """wire 形状 worker note。"""
    return {"id": mid, "ts": 0, "task": TID, "kind": "note",
            "from": "worker", "to": "dispatcher", "body": body}


def stuck_monitor(tmp_path, client, *, stall_timeout=1800,
                  stuck_note_rounds=3, stuck_await_s=7200):
    """接线完毕的 monitor（iid=7 已登记，账本在案，repo 未接＝零 git）。"""
    face = ConversationFace()
    journal = []
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": {str(IID): {
        "status": "active", "agora_task_id": TID}}}), encoding="utf-8")
    monitor = AgoraMonitor(client=client, journal=journal.append,
                           max_active=1, stall_timeout=stall_timeout,
                           stuck_note_rounds=stuck_note_rounds,
                           stuck_await_s=stuck_await_s)
    monitor.attach_delivery(repo_path=None, remote_url=PAT_URL,
                            ssl_verify=True, state_path=state_path,
                            ledger_lock=threading.RLock())
    monitor.attach_conversation(post_note=face.post_note,
                                answer_question=face.answer_question,
                                heartbeat_interval=0)
    monitor.register(IID, TID)
    return monitor, face, journal, state_path


def rulings_of(state_path):
    path = Path(str(state_path) + ".agora-rulings.json")
    return json.loads(path.read_text(encoding="utf-8"))["rulings"]


def running_task():
    client = FakeAgoraClient()
    client.tasks[TID] = {"state": "running",
                         "deliverable": "external_branch", "delivery": {},
                         "created": 0.0, "last_seen": 0.0}
    return client


# --- 1) 连续 3 条同文 note → 恰 1 卡 + 停 nudge ---------------------------


def test_three_identical_notes_one_card_and_nudge_stops(tmp_path, monkeypatch):
    clock = FakeClock(10_000.0)
    monkeypatch.setattr(agora_monitor, "time", clock)
    client = running_task()
    monitor, face, journal, state_path = stuck_monitor(
        tmp_path, client, stall_timeout=100)

    monitor.poll_once()  # 零板面活动超阈值：第一档 nudge（证明阶梯已武装）
    assert len(client.nudges) == 1, client.nudges

    body = "等待 root 改账"
    client.mail = [note(1, body), note(2, body), note(3, body)]
    clock.now += 200
    monitor.poll_once()
    assert len(face.posts) == 1, face.posts
    card = face.posts[0][1]
    assert TID in card and "需人工处理" in card
    rid = _RID_RE.search(card)
    assert rid is not None, card
    assert len(client.nudges) == 1, "升级后本轮即不再 nudge"
    assert client.dispatch_calls == [], "死等卡点不自动重派"

    client.mail = [note(4, body)]  # 同卡点再来一轮
    clock.now += 200
    monitor.poll_once()
    assert len(face.posts) == 1, "同卡点不重复发卡"
    assert len(client.nudges) == 1, "后续轮不再 nudge 该任务"

    record = rulings_of(state_path)[rid.group(1)]
    assert record["task"] == TID
    assert record["action"] == "stuck_escalation"
    assert record["params"]["kind"] == "identical_notes"
    assert record["params"]["body"] == body


# --- 2) 第 2 条起 body 变化 → 计数重置不触发 ------------------------------


def test_body_variation_resets_counter(tmp_path):
    client = running_task()
    monitor, face, journal, state_path = stuck_monitor(
        tmp_path, client, stall_timeout=0)

    client.mail = [note(1, "A"), note(2, "B"), note(3, "B")]
    monitor.poll_once()
    assert face.posts == [], face.posts

    client.mail = [note(4, "A"), note(5, "A"), note(6, "B"), note(7, "B")]
    monitor.poll_once()
    assert face.posts == [], "body 变化重置计数：最长连响 2 < 3 不触发"
    assert rulings_of(state_path) == {}


# --- 3) awaiting_external 超/未超 stuck_await_s ---------------------------


def test_awaiting_external_threshold(tmp_path, monkeypatch):
    clock = FakeClock(5_000.0)
    monkeypatch.setattr(agora_monitor, "time", clock)
    client = FakeAgoraClient()  # 缺省即 done + awaiting_external + hex40
    monitor, face, journal, state_path = stuck_monitor(
        tmp_path, client, stall_timeout=0, stuck_await_s=7200)

    monitor.poll_once()  # 首见：起算，不发卡
    assert face.posts == [], face.posts

    clock.now += 7199
    monitor.poll_once()
    assert face.posts == [], "未超阈值不发卡"

    clock.now += 2  # 累计 7201s > 7200s
    monitor.poll_once()
    assert len(face.posts) == 1, face.posts
    card = face.posts[0][1]
    assert TID in card and "需人工处理" in card
    rid = _RID_RE.search(card)
    assert rid is not None, card
    record = rulings_of(state_path)[rid.group(1)]
    assert record["params"]["kind"] == "awaiting_external"
    assert record["params"]["await_s"] >= 7200


# --- 4) 同卡点第二轮对账不重复发卡 ----------------------------------------


def test_awaiting_no_duplicate_card_on_second_round(tmp_path, monkeypatch):
    clock = FakeClock(5_000.0)
    monkeypatch.setattr(agora_monitor, "time", clock)
    client = FakeAgoraClient()
    monitor, face, journal, state_path = stuck_monitor(
        tmp_path, client, stall_timeout=0, stuck_await_s=7200)

    monitor.poll_once()  # 首见起算（远端悬置时长只能从首见观测起算）
    clock.now += 7201
    monitor.poll_once()
    assert len(face.posts) == 1, face.posts

    clock.now += 600
    monitor.poll_once()  # 同卡点第二轮对账
    assert len(face.posts) == 1, "不重复发卡"
    assert len(rulings_of(state_path)) == 1


# --- 5) 零邮件：grep -riE "smtp|send_mail|import email" *.py 无命中 -------


def test_repo_python_files_have_no_email_paths():
    files = sorted(str(p) for p in REPO_ROOT.glob("*.py"))
    assert files, "repo root must have *.py files"
    proc = subprocess.run(
        ["grep", "-riE", "smtp|send_mail|import email", *files],
        capture_output=True, text=True)
    assert proc.returncode == 1, (
        f"email path leaked (rc={proc.returncode}):\n{proc.stdout}")
