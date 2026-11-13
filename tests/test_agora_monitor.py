"""AgoraMonitor 骨架测试（计划 Task 3）：登记簿幂等/容量/释放、start/stop
轮询生命周期。外部推送与 non_ff 回路归 Task 4、终态汇报归 Task 5——本单
poll_once 是零网络骨架，测试锁住「骨架不得偷跑网络/线程不得死」两件事。

Run: python3 -m pytest tests/test_agora_monitor.py -q
"""

import threading
import time

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


def test_capacity_fills_at_max_active_and_release_frees_the_slot():
    """活动名额到任务终态才释放：register 占名额、release 归还；超额登记
    本身不拒绝（容量检查是派发方 create 前的职责，登记簿只反映事实）。"""
    monitor = make_monitor(max_active=2)
    assert monitor.has_capacity()
    monitor.register(7, "ag-1")
    assert monitor.has_capacity()
    monitor.register(8, "ag-2")
    assert not monitor.has_capacity()
    monitor.release(7)  # 任务到终态
    assert monitor.has_capacity()
    monitor.release(7)  # 幂等
    assert monitor.has_capacity()
    assert monitor.tasks() == [(8, "ag-2")]


# --- poll_once 骨架 ------------------------------------------------------------


def test_poll_once_skeleton_is_registry_only_zero_client_calls():
    """Task 3 骨架：poll_once 可空转，不触任何客户端面（Task 4 才接 board/
    deliver）。"""
    monitor = make_monitor()
    monitor.register(7, "ag-123")
    assert monitor.poll_once() is None
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
