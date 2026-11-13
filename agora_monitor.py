"""agora 任务监视器（计划 Task 3 骨架）——单后台线程轮询本实例登记的
活动任务（设计 §9.2）。

本单只落骨架与登记簿：外部推送与 non_ff 回路归 Task 4，终态汇报归
Task 5，poll_once 在此之前是零网络占位。登记簿 iid→tid 为进程内状态，
daemon 重启后的账本恢复归 Task 5。

活动名额（max_active，接线 config.max_parallel）到任务终态才经
release 归还——_run_turn 派发前查 has_capacity，与 orchestrator 的
turn 槽形成双重限制；_run_turn 返回绝不释放名额。
"""

import threading


class AgoraMonitor:
    """本实例活动 agora 任务的登记簿＋轮询线程。"""

    def __init__(self, *, client, journal=None, max_active=1):
        self._client = client
        self._journal = journal if journal is not None else (
            lambda message: None)
        self._max_active = max(1, int(max_active))
        self._guard = threading.Lock()
        self._active = {}  # iid(int) -> tid(str)：一 issue 一活动任务
        self._stop = threading.Event()
        self._thread = None

    # ---- 登记簿 ----

    def register(self, iid, tid) -> None:
        """幂等登记：同 iid 重登记覆盖为最新 tid（回合链推进时旧任务
        已终态让位）；容量检查是派发方 create 前的职责，登记只反映事实。"""
        with self._guard:
            self._active[int(iid)] = str(tid)

    def release(self, iid) -> None:
        """任务终态：归还活动名额（幂等）。"""
        with self._guard:
            self._active.pop(int(iid), None)

    def has_capacity(self) -> bool:
        with self._guard:
            return len(self._active) < self._max_active

    def tasks(self) -> list:
        """[(iid, tid)] 快照（升序），测试与 Task 5 恢复面共用。"""
        with self._guard:
            return sorted(self._active.items())

    # ---- 轮询 ----

    def poll_once(self):
        """一轮状态 reconciliation。Task 3 骨架：登记簿已就绪，本方法
        刻意零网络——外部推送/non_ff（Task 4）与终态汇报（Task 5）在
        此扩展，届时由 board 驱动 release 归还名额。"""
        return None

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
