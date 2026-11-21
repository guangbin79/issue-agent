"""ag-03f9f3：AgoraMonitor 运行时账本对账补登记（纯内存＋tmp_path）。

事故背景（2026-11-20 HXNativeApp #294 / ag-dfd3e2）：任务 blocked→
daemon _finish_blocked 释放名额→外部经 issue 评论重派复活至 done+
awaiting_external→daemon 仅有「派发回合 register」与「启动恢复」两个
登记时点→交付停摆 18 小时。修复＝poll 每轮从账本重导缺失登记（零网络，
不调 board；真相对账交给既有 board 周期）。

Run: python3 -m pytest tests/test_monitor_recover.py -q
"""

import json
import threading
import time

import agora_monitor
from agora_monitor import AgoraMonitor

TID = "ag-dfd3e2"
IID = 7


class PendingBoardClient:
    """零网络替身：check_mail 恒空、board 恒 pending（poll 本轮无动作态
    ——补登记后不联动释放，补登记行为与 board 真态解耦断言）。"""

    def __init__(self):
        self.board_calls = []

    def poll_interval(self):
        return 15.0

    def check_mail(self):
        return []

    def board(self, tid):
        self.board_calls.append(tid)
        return {"task": {"state": "pending"}}


def recover_monitor(tmp_path, issues):
    """账本接线完毕的 monitor（tmp_path 账本；repo/remote 为哑接线——
    补登记路径零网络零 git）。"""
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({"issues": issues}), encoding="utf-8")
    journal = []
    monitor = AgoraMonitor(client=PendingBoardClient(),
                           journal=journal.append, max_active=1)
    monitor.attach_delivery(repo_path=str(tmp_path / "repo"),
                            remote_url="http://unused", ssl_verify=True,
                            state_path=state_path,
                            ledger_lock=threading.RLock())
    return monitor, journal


def test_release_then_one_round_reregisters_from_ledger(tmp_path):
    """断言1（事故形态）：register→release（名额已还、账本仍活动）→
    一轮 poll →登记恢复，且同轮即交既有 board 对账接管。"""
    monitor, journal = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID,
        "agora_status": "blocked"}})  # 释放后账本停在前次同步的非终态
    monitor.register(IID, TID)
    monitor.release(IID, TID)  # 模拟 _finish_blocked 释放名额
    assert monitor.tasks() == []

    monitor.poll_once()

    assert (IID, TID) in monitor.tasks()
    assert monitor._client.board_calls == [TID], "同轮即交 board 对账"
    assert any("re-registered" in m for m in journal)


def test_done_issue_or_terminal_agora_status_not_registered(tmp_path):
    """断言2：issue 账本 status=='done' 或 agora_status 头段∈
    merged/closed/cancelled →不登记（终态口径与 _recover_from_ledger
    一致）；无 agora_task_id 的条目亦不登记。"""
    monitor, journal = recover_monitor(tmp_path, {
        "7": {"status": "done", "agora_task_id": "ag-a",
              "agora_status": "blocked"},
        "8": {"status": "active", "agora_task_id": "ag-b",
              "agora_status": "merged/delivered"},
        "9": {"status": "active", "agora_task_id": "ag-c",
              "agora_status": "closed"},
        "10": {"status": "active", "agora_task_id": "ag-d",
               "agora_status": "cancelled"},
        "11": {"status": "active", "agora_task_id": None},
    })

    monitor.poll_once()

    assert monitor.tasks() == []
    assert monitor._client.board_calls == []
    assert not any("re-registered" in m for m in journal)


def test_two_rounds_at_most_one_entry_per_issue(tmp_path):
    """断言3：连跑两轮补登记 →_active 每 iid 至多一条（幂等不重复）。"""
    monitor, _ = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID,
        "agora_status": "dispatched"}})

    monitor.poll_once()
    monitor.poll_once()

    assert monitor.tasks() == [(IID, TID)]


def test_round_preserves_existing_registration_and_heartbeat(tmp_path):
    """已登记（含 reserve 占位）的 iid 不重登记：register 会复位心跳
    基线，每轮无差别重登记会把「已运行 X 分钟」永远清零。"""
    monitor, _ = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID}})
    monitor.register(IID, TID)
    start = monitor._hb[IID]["start"]

    monitor.poll_once()

    assert monitor.tasks() == [(IID, TID)]
    assert monitor._hb[IID]["start"] == start, "不得复位心跳基线"


def test_exhausted_stall_row_not_reregistered(tmp_path):
    """评审环 R1（ocr-1）：僵态终档（exhausted）释放后不重登记——
    ladder 对 exhausted 行恒 no-op，重登记会把已释放名额永久占位
    （max_active=1 时饿死全部派发）。"""
    monitor, _ = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID,
        "agora_status": "running"}})
    agora_monitor._stall_save(str(monitor._state_path), {TID: {
        "nudges": 2, "redispatched": True, "exhausted": True,
        "epoch": 0.0, "last_seen": 0.0}})
    monitor.register(IID, TID)
    monitor.release(IID, TID)  # 模拟僵态终档释放名额

    monitor.poll_once()

    assert monitor.tasks() == []
    assert monitor._client.board_calls == []


def test_retry_cooldown_throttles_dormant_reregistration(tmp_path):
    """评审环 R1（B-1）：休眠任务（释放后无人工处置）不逐轮
    register→board→release 抖动——节流窗内不重试，窗过后恢复补登记
    （复活接管时延上界＝一个节流窗）。"""
    monitor, journal = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID,
        "agora_status": "blocked"}})
    monitor.register(IID, TID)
    monitor.release(IID, TID)

    monitor.poll_once()  # 首次补登记（无节流档案）
    assert (IID, TID) in monitor.tasks()
    assert monitor._client.board_calls == [TID]

    monitor.release(IID, TID)  # 模拟 blocked 再收口（休眠稳态）
    monitor.poll_once()  # 节流窗内：不重登记、零 board 调用
    assert monitor.tasks() == []
    assert monitor._client.board_calls == [TID]
    assert len([m for m in journal if "re-registered" in m]) == 1

    with monitor._guard:  # 人为拨老节流档案：模拟一个窗口流逝
        monitor._reconcile_retry[(IID, TID)] -= (
            agora_monitor._RECONCILE_RETRY_S + 1)
    monitor.poll_once()  # 窗过后：恢复补登记
    assert (IID, TID) in monitor.tasks()
    assert monitor._client.board_calls == [TID, TID]
    assert len([m for m in journal if "re-registered" in m]) == 2


def test_delivered_settled_skipped_but_awaiting_external_registered(
        tmp_path):
    """ocr R2：交付已确认（done/delivered 恢复出口）不补登记——头段 done
    非终态但交付无待办；事故终态 done/awaiting_external 交付待办在案，
    必须仍可补登记（18h 停摆的复活接管形态）。"""
    monitor, _ = recover_monitor(tmp_path, {
        "7": {"status": "active", "agora_task_id": "ag-a",
              "agora_status": "done/delivered"},
        "8": {"status": "active", "agora_task_id": "ag-b",
              "agora_status": "done/awaiting_external"}})

    monitor.poll_once()

    assert monitor.tasks() == [(8, "ag-b")], \
        "delivered 不登记；awaiting_external（事故终态）必须登记"


def test_register_if_absent_is_compare_and_register(tmp_path):
    """评审环 R1（B-2/ocr-2）：补登记与派发回合并发——iid 在案（含
    占位）时 compare-and-register 不写入、不覆写新 tid、不复位心跳；
    缺席时登记并返回 True。"""
    monitor, _ = recover_monitor(tmp_path, {"7": {
        "status": "active", "agora_task_id": TID}})
    monitor.reserve(IID)  # 派发回合在途占位
    assert monitor._register_if_absent(IID, "ag-old") is False
    assert monitor.tasks() == [(IID, None)], "不得转正在途占位"

    monitor.release(IID)  # 回合失败出口清占位
    monitor.register(IID, "ag-new")
    start = monitor._hb[IID]["start"]
    assert monitor._register_if_absent(IID, "ag-old") is False
    assert monitor.tasks() == [(IID, "ag-new")], "陈旧 tid 不得覆写新登记"
    assert monitor._hb[IID]["start"] == start

    assert monitor._register_if_absent(8, "ag-fresh") is True
    assert (8, "ag-fresh") in monitor.tasks()
