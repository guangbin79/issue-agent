"""Turn orchestrator + reconcile for the GitLab issue agent (plan task 7).

Wires the six sibling modules (http listener / fsm / state ledger / api /
runner / gitop) into ONE serial turn pipeline with claim-before-run race
protection.

Design invariants (binding, from the plan's dual review):
- the daemon is a thin guarantee layer: per turn it does baseline fetch
  (agent-remote ref, in the main repo) -> run the agent (cwd = main repo;
  the agent builds its OWN worktree per its global rules) -> detect
  issue/<iid>-fix commits -> push -> reply. It NEVER checks out, creates
  worktrees, or commits anywhere. After a pushed fix turn (task 8) it
  spawns ONE fresh review# session running the REAL /review-loop with an
  explicit agent-remote..issue/<iid>-fix scope (non-blocking; the daemon
  re-pushes any Trivial fixes the reviewer commits). ONE worker thread
  consumes webhook events; a process-wide turn lock guarantees two
  opencode turns NEVER overlap.
- claim-before-run: status=pending + claimed_at + this turn's note ids are
  marked processed in ONE in-memory mutation and ONE atomic save_state,
  inside the ledger lock, so a concurrent reconcile can neither double-run
  the same note nor start a second session for the issue.
- finalize is CAS: only writes "active" when the current on-disk status is
  not "done" — a close that landed mid-turn is never rolled back.
- reply first, finalize after: a kill -9 between the two replays at most one
  duplicate reply (bounded, and filtered by the agent marker downstream).
- the listener opens its port at startup BEFORE the baseline reconcile runs
  (so GitLab hook deliveries succeed even while the reconcile works
  through a backlog of turns); the worker thread still starts consuming
  events only AFTER the baseline note-id snapshot, preserving the
  #RQ|  advisory-2 no-double-processing guarantee; periodic reconciles run from
#NN|  the worker loop, replay open issues' notes through the SAME fsm +
#VS|  per-note-id path as webhooks, close ledger entries whose issue left
#QH|  the open set (advisory-1), retry deferred issues, and — after the one-
#XW|  time "baselined" marker — turn untracked open issues that APPEARED since
#YB|  the baseline into synthetic "opened" events (polling fallback: new issues
#ZM|  get full turns even when the server blocks webhook delivery).

The listener is injected (anything with .queue/.start()/.stop()) because
http.py must be loaded under its importlib alias by the daemon.
"""

import json
import os
import queue as _queue
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional

import api
from api import NOTE_MARKER as _DEFAULT_NOTE_MARKER
import fsm
import gitop
import runner
import state

DEFAULT_RECONCILE_INTERVAL_S = 600
DEFAULT_OPENCODE_TIMEOUT_S = 1800
DEFAULT_REVIEW_TIMEOUT_S = 3600
DEFAULT_CLAIM_TIMEOUT_S = 5500  # ≥ review+opencode 全回合预算+余量(3600+1800+100)；防止 >1900s 的回合被误判 recycled stale claim 刷 defer_count(defer_limit=6)
DEFAULT_DEFER_LIMIT = 6
# gt-9f24: an abnormally-terminated review phase arms ONE delayed re-push;
# reconcile only acts once the review session had this long to land its
# late commits (2026-10-20 outage: reviewer committed 55min after the crash).
DEFAULT_REVIEW_PUSH_GRACE_S = 1800
DEFAULT_HEARTBEAT_INTERVAL_S = 600  # 0 = off: long-turn liveness note cadence
DEFAULT_SWEEP_INTERVAL_S = 1800  # 0 = off: 欠账巡检 (debt sweep) cadence
# ag-a9af5e: sweep report-repost probe budget — an armed issue is probed at
# most this many sweeps before the flag clears (retry window for a missing
# report / failed push probe: two sweeps, then journal + give up).
MAX_REPORT_PROBES = 2

# Global experience-draft store (the agent-experience capture flow the
# protocol's 沉淀 step writes to): the post-push capture verification
# scans it for this issue's draft. Module constant read at CALL time, so
# tests can monkeypatch it per-harness.
EXPERIENCES_DIR = Path.home() / ".dotfiles/.agent-experiences/experiences"
# E2BIG guard: one argv entry is capped at 128 KiB by the kernel, and an
# oversized issue/comment body used to surface as a bare OSError outside
# the runner's mapped failures. Truncate the message (UTF-8 aware, so the
# byte bound holds for CJK too) at ~64 KiB and mark the cut; the title
# keeps its existing 40-char cap.
MAX_MESSAGE_BYTES = 64 * 1024
_TRUNCATED_MARK = "\n…[内容过长，已截断]"

# Synthetic retry job on the worker queue (a Gitea payload never has keys
# starting with "__", so this cannot collide with a real webhook event).
# starting with "__", so this cannot collide with a real webhook event).
_REQUEUE = "__requeue__"

RETRY_MESSAGE = "(系统重试) 请继续处理本 issue 的未尽事项。"
UNPAUSE_NOTE = "收到「恢复」，本 issue 的自动化处理已恢复。"
SESSION_INVALID_NOTE = "原会话已失效，已自动开启新会话继续处理。"
REVIEW_FAILED_NOTE = "评审未完成（超时/崩溃），修复已推送，建议人工复查。"
REPOST_NOT_FOUND_NOTE = ("「重发汇报」：本会话历史中未找到三段式汇报"
                         "（根因/改动文件清单/验证方式），请人工核查。")

# task 8: prompt for the post-push review phase. The spawned headless child
# runs the REAL /review-loop command (resolved via its own user-level
# opencode config — the command and module matrix are NOT restated here).
# Adaptations over a plain interactive review:
# - the scope is EXPLICIT (never derived from .last-reviewed);
# - all review-loop evidence goes to the persistent per-instance dir
#   (<repo>/.omo/evidence/<service_name>/...) because the reviewer's
#   worktree is ephemeral;
# - the reviewer may fix only Trivial findings, never push (the daemon
#   pushes after the phase returns), never touch the main working copy.
# Placeholders {evidence_dir}/{verify_command} are instance data threaded
# from config by the orchestrator (service_name/verify_command fields).
# - step 0: the orchestrator streams the session ID into
#   <evidence_dir>/session-id.txt while the review session is still
#   starting; the reviewer reads it (before /review-loop step 1) instead
#   of guessing its own ID — the command's own sqlite lookup is fallback.
# - final step: the reviewer deletes its own review worktree (keep the fix branch) before the summary.
# - one-shot process discipline——lanes parallel but collected in-turn, ending the turn exits the process and kills the lanes;
REVIEW_PROMPT = """第 0 步（本会话 ID 获取）：本会话的 ID 已由编排方流式写入 {evidence_dir}/session-id.txt（会话启动后数秒内出现，原子落盘）。执行 /review-loop 第 1 步之前，先读取该文件取得本会话 ID（若文件尚未出现则等待重读，累计 ≤15 秒）；该 ID 用于防重入标记与证据文件命名；若等待后仍取不到，才退回命令自带的会话 ID 获取法。

本轮为 bot 修复推送后的自动评审（issue#{iid}）。请执行 /review-loop 命令（真实命令与模块矩阵，按你的用户级配置自行解析，勿在此复述模块内容），评审范围显式指定为：

refs/remotes/agent-remote/{default_branch}..issue/{iid}-fix

范围以上述显式 diff 区间为准，禁止使用 .last-reviewed 推导。

评审执行方式（遵循你自己的全局 worktree 规则）：在主仓同级父目录建临时 worktree 并检出 issue/{iid}-fix 分支，在该 worktree 内运行项目门禁：{verify_command}。但所有 review-loop 证据文件一律改写到持久目录：{evidence_dir}/（不要写 worktree 本地的 .review-loop/，worktree 是临时的，结束即清）。

一次性进程纪律：本会话由一次性 headless opencode run 进程承载，结束回合即进程退出，所有后台任务随之湮灭。评审巷道照常并行发射，但收集必须在本回合内以 background_output(block=true) 逐条阻塞收齐（单次超时便再次 block 续收）；严禁以结束回合等待系统通知来收束收集。

委派边界：仅当发现处于 Trivial 界内（≤1 个文件且 ≤20 行改动）时，才可在同一分支 issue/{iid}-fix 上以最小提交修复，提交信息前缀 review(issue-agent): 并带 trailer Issue: #{iid}；超出 Trivial 界内的发现只上报不修复（升级人工处理）；严禁推送（推送统一由 daemon 执行）；严禁触碰主工作副本、git 默认分支与基线分支 {default_branch}。

收尾清理（无论评审结论如何、是否有 Trivial 修复）：删除本次评审 worktree——在主仓执行 git worktree remove <本次评审 worktree 路径>（保留 issue/{iid}-fix 分支与提交，严禁删分支）；若因未跟踪构建产物（如 build.log/ctest.log/build.exit）阻删，改用 git worktree remove --force。

最后以简明中文评审总结收尾（发现、修复、门禁结果、豁免/限度），该总结将作为 issue 评论发出。"""

# Generic gate wording when the instance config carries no verify_command.
DEFAULT_VERIFY_COMMAND = ("运行项目自带的验证手段（测试/lint/构建；若均无则做语法级自检），"
                          "例如 python3 -m pytest tests/ -q")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _truncate_utf8(text, max_bytes):
    """Clip *text* to at most *max_bytes* of UTF-8, appending the truncation
    mark when a cut happened (mid-character splits are dropped)."""
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore") + _TRUNCATED_MARK


def _empty_entry() -> dict:
    """A fresh ledger entry (schema documented as stable in state.py)."""
    return {
        "status": "active",
        "session_id": None,
        "branch": None,
        "processed_note_ids": [],
        "last_note_id": None,
        "claimed_at": None,
        "defer_count": 0,
        "review_push_pending_at": None,
        "report_probe_pending": False,
        "report_probe_count": 0,
    }


class Orchestrator:
    """Turn pipeline over the injected listener queue and API client.

    Concurrency model: max_parallel worker threads consume the queue; a
    BOUNDED slot semaphore caps in-flight turns, and a per-issue lock keeps
    every same-issue event sequence strictly serial. max_parallel=1 (the
    default) reproduces the historical one-worker turn lock exactly."""

    def __init__(self, *, project, api, journal, listener, state_path, repo_path,
                 remote_url, protocol_path, ssl_verify=True,
                 default_branch="master",
                 note_marker="", service_name="issue-agent", verify_command="",
                 reconcile_interval=DEFAULT_RECONCILE_INTERVAL_S,
                 opencode_timeout=DEFAULT_OPENCODE_TIMEOUT_S,
                 review_enabled=True, review_timeout=DEFAULT_REVIEW_TIMEOUT_S,
                 claim_timeout_s=DEFAULT_CLAIM_TIMEOUT_S,
                 defer_limit=DEFAULT_DEFER_LIMIT,
                 review_push_grace_s=DEFAULT_REVIEW_PUSH_GRACE_S,
                 heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL_S,
                 sweep_interval: int = DEFAULT_SWEEP_INTERVAL_S,
                 max_parallel: int = 1,
                 callback_url: str = "",
                 worktree_path: str = "",
                 teardown: Optional[Callable[[], None]] = None):
        self.project = project
        self.api = api
        self.listener = listener
        self.queue = listener.queue
        self.state_path = state_path
        self.repo_path = repo_path
        self._journal = journal if journal is not None else (lambda message: None)
        self.worktree_path = worktree_path  # DEPRECATED: kept for config
        # compat; the agent manages its own worktrees now. Only logged.
        if worktree_path:
            self._journal(
                "config: worktree_path is deprecated and ignored "
                "(the agent creates/manages its own worktrees)")
        self.remote_url = remote_url
        self.protocol_path = protocol_path
        self.ssl_verify = ssl_verify
        self.default_branch = default_branch
        # Instance identity: the note marker filters the fsm (injected into
        # every decide() call) and mirrors what the api client stamps on
        # every posted note; service_name namespaces evidence dirs.
        self.note_marker = note_marker or _DEFAULT_NOTE_MARKER
        self._fsm_markers = (self.note_marker,)
        self.service_name = service_name
        self.verify_command = verify_command or DEFAULT_VERIFY_COMMAND
        self.reconcile_interval = reconcile_interval
        self.opencode_timeout = opencode_timeout
        self.review_enabled = review_enabled
        self.review_timeout = review_timeout
        self.claim_timeout_s = claim_timeout_s
        self.defer_limit = defer_limit
        self.review_push_grace_s = max(1, int(review_push_grace_s))
        # heartbeat (A+C): A side (runner on_event) only writes memory; the
        # C side heartbeat thread is the SOLE comment outlet, so the note
        # rate is hard-capped at one per interval per in-flight turn no
        # matter how fast the activity description churns.
        self.heartbeat_interval = max(0, int(heartbeat_interval))
        self._in_flight = {}  # iid -> {start, activity, last_post} (guard below)
        self._hb_guard = threading.Lock()
        self._hb_thread = None
        # 欠账巡检 (debt sweep): one thread re-pushing fix branches that lead
        # origin and re-posting replies owed by the blocked path. The debt
        # store lives BESIDE the ledger as an orchestrator-owned file: the
        # sweep records reply text + swept head shas, which state.py's frozen
        # schema has no room for.
        self.sweep_interval = max(0, int(sweep_interval))
        self._sweep_path = os.fspath(state_path) + ".sweep.json"
        self._sweep_guard = threading.Lock()
        self._sweep_thread = None
        self.callback_url = callback_url
        self._teardown = teardown
        self.max_parallel = max(1, int(max_parallel))
        # ledger lock: every load-mutate-save sequence (and decide+claim) runs
        # under it, so worker and a concurrent reconcile never interleave writes.
        self._ledger_lock = threading.Lock()
        # bounded turn slots + per-issue locks replace the historical process-
        # wide turn lock: at most max_parallel turns in flight, and a given
        # issue's turns never overlap (same-issue events queue behind the lock).
        self._turn_slots = threading.BoundedSemaphore(self.max_parallel)
        self._iid_locks = {}
        self._iid_locks_guard = threading.Lock()
        self._stop = threading.Event()
        self._workers = []
        # queue-wait budget invariant: a same-issue event claimed and queued
        # behind another turn of that issue ages its claim for up to one full
        # turn budget (opencode + review). If the recycle threshold is below
        # that, the recycle fires mid-wait and requeues a duplicate retry —
        # deterministic config check, journaled once at startup.
        if self.opencode_timeout + self.review_timeout + 100 > self.claim_timeout_s:
            self._journal(
                f"config: claim_timeout_s ({self.claim_timeout_s}) is below the "
                f"full turn budget (opencode {self.opencode_timeout} + review "
                f"{self.review_timeout} + margin) — a long turn can outlive a "
                "queued same-issue claim and trigger a spurious recycle retry; "
                "raise claim_timeout_s")
        self._last_reconcile = time.monotonic()
        self._poll_s = min(1.0, self.reconcile_interval)
        self._repo_misses = 0

    # ------------------------------------------------------------------ setup

    def start(self):
        """Orphan cleanup -> listener (port opens EARLY, F17: hook deliveries
        must succeed even while the reconcile below grinds through backlog
        turns that each run up to opencode_timeout) -> baseline reconcile ->
        worker LAST. The listener only enqueues, so events queue up during
        the reconcile and are consumed after the baseline snapshot — the
        advisory-2 ordering (snapshot before any webhook consumption) holds.
        """
        self._startup_orphan_cleanup()
        port = self.listener.start()
        # D1: the startup reconcile is the one call not yet behind a guard —
        # a failure (API down at boot) must journal, never crash the daemon;
        # the worker retries it on the interval via the same path.
        self._safe_reconcile()
        self._workers = [
            threading.Thread(
                target=self._worker_loop, name=f"issue-agent-worker-{n}", daemon=True)
            for n in range(self.max_parallel)]
        for worker in self._workers:
            worker.start()
        if self.heartbeat_interval:
            self._hb_thread = threading.Thread(
                target=self._heartbeat_loop, name="issue-agent-heartbeat",
                daemon=True)
            self._hb_thread.start()
        if self.sweep_interval:
            self._sweep_thread = threading.Thread(
                target=self._sweep_loop, name="issue-agent-sweep", daemon=True)
            self._sweep_thread.start()
        self._journal(f"startup: listener up (port={port}), baseline reconciled, "
                      f"workers running (max_parallel={self.max_parallel})")
        return port

    def stop(self):
        self._stop.set()
        try:
            self.listener.stop()
        except Exception as exc:  # listener teardown must not mask shutdown
            self._journal(f"shutdown: listener stop failed: {exc!r}")
        for worker in self._workers:
            worker.join(timeout=10)
        self._workers = []
        if self._hb_thread is not None:
            self._hb_thread.join(timeout=5)
            self._hb_thread = None
        if self._sweep_thread is not None:
            self._sweep_thread.join(timeout=5)
            self._sweep_thread = None

    def _startup_orphan_cleanup(self):
        """No worker thread exists yet, so every pending claim is an orphan
        (kill -9 between claim and finalize): recycle to defer and requeue."""
        recycled = state.claim_timeout_recycle(
            self.state_path, datetime.now(timezone.utc), 0)
        for iid in recycled:
            self._journal(f"startup: orphan pending issue#{iid} recycled to defer, requeued")
            self.queue.put({_REQUEUE: int(iid), "reason": "orphan-pending"})

    # ------------------------------------------------------------------ worker

    def _worker_loop(self):
        while not self._stop.is_set():
            try:
                job = self.queue.get(timeout=self._poll_s)
            except _queue.Empty:
                # claim the pacing window BEFORE running: with max_parallel>1
                # several workers hit this branch simultaneously and the
                # check-then-act would double-fire reconcile without it
                if time.monotonic() - self._last_reconcile >= self.reconcile_interval:
                    self._last_reconcile = time.monotonic()
                    self._safe_reconcile()
                continue
            try:
                self._dispatch(job)
            except Exception as exc:  # the worker must never die
                self._journal(f"worker: unhandled error processing job: {exc!r}")

    def _dispatch(self, job):
        if isinstance(job, dict) and _REQUEUE in job:
            self._handle_requeue(int(job[_REQUEUE]))
        else:
            self._handle_event(job)

    def _safe_reconcile(self):
        try:
            self.reconcile()
        except Exception as exc:
            self._journal(f"reconcile: failed: {exc!r}")

    # ------------------------------------------------------------------ events

    def _handle_event(self, event):
        """One webhook-shaped event -> fsm decision -> side effects."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            decision = fsm.decide(event, ledger["issues"], markers=self._fsm_markers)
            action, iid = decision["action"], decision["iid"]
            if action == "ignore":
                self._journal(f"event ignored: {decision['reason']} (iid={iid})")
                return
            if action in ("new", "continue", "repost"):
                note_ids = self._note_ids(event)
                pre_entry = ledger["issues"].get(str(int(iid)))
                pre_status = pre_entry.get("status") if isinstance(pre_entry, dict) else None
                self._claim(iid, note_ids, ledger)
                session_id = ledger["issues"][str(int(iid))].get("session_id")
                message, title = self._message_and_title(event, iid)
        # ack/gates/opencode run OUTSIDE the ledger lock
        if action == "repost":
            # same-issue serialization as a real turn (the export sweep and
            # note post must never interleave with an in-flight turn)
            with self._iid_lock(iid), self._turn_slots:
                reposted = self._repost_report(iid, session_id, pre_status)
            if reposted:
                return
            if pre_status == "paused":
                # 暂停纪律：降级绝不对 paused issue 派工——保持暂停、闭环认领
                with self._ledger_lock:
                    ledger = state.load_state(self.state_path, warn=self._journal)
                    entry = ledger["issues"].get(str(int(iid)))
                    if entry is not None:
                        if entry["status"] == "pending":  # close 落在探测期间则不回卷
                            entry["status"] = "paused"
                        entry["claimed_at"] = None
                        state.save_state(self.state_path, ledger)
                self._journal(f"repost: issue#{iid} paused, no fallback turn")
                return
            action = "continue"
        if action in ("new", "continue"):
            self._run_turn(iid, message=message, title=title,
                           session_id=session_id, action=action)
            return
        # pause / unpause / close / reopen: ledger mutation inside the lock
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            key = str(int(iid))
            entry = ledger["issues"].get(key)
            if entry is None:
                self._journal(f"{action} for untracked issue#{iid}: tolerated, ignored")
                return
            note_ids = self._note_ids(event)
            if note_ids:
                state.mark_processed(ledger, iid, note_ids)
                entry = ledger["issues"][key]
            entry["status"] = {"pause": "paused", "unpause": "active",
                               "close": "done", "reopen": "active"}[action]
            if action == "close":
                entry["claimed_at"] = None
                # close-push owns the branch from here; a surviving mark
                # would auto-push post-close manual work after a reopen
                entry.pop("review_push_pending_at", None)
                # ag-a9af5e: close ownership — no report probe past a close
                entry.pop("report_probe_pending", None)
                entry.pop("report_probe_count", None)
            state.save_state(self.state_path, ledger)
        self._journal(f"ledger: issue#{iid} -> {entry['status']} ({decision['reason']})")
        if action == "close":
            # the PRIMARY close channel (webhook delivered): a fix branch that
            # only gained commits after the last turn (TUI takeover, manual
            # work) would otherwise never reach the remote (issue#139)
            self._push_on_close(int(iid))
        if action == "unpause":
            self._post_note(iid, UNPAUSE_NOTE)

    def _repost_report(self, iid, session_id, pre_status=None):
        """「重发汇报」轻量补发回合：零派工/零提交/零推送。

        Preconditions: the fix branch is verifiably on the remote (nothing
        to push) AND the issue's review loop ran (<repo>/.review-loop/
        present). Anything else — including a failed probe — journals and
        returns False so the caller degrades to a normal continue turn
        (never for a paused issue — the caller gates that).
        On success the last session report (export sweep) goes up as-is,
        or REPOST_NOT_FOUND_NOTE when no report is found, and the claim is
        closed (claimed_at None; status active — EXCEPT a paused issue
        keeps its pause: 重发汇报 must never silently resume automation).
        """
        try:
            pushed = gitop.fix_branch_pushed(self.repo_path, iid, self.remote_url,
                                             self.ssl_verify)
        except gitop.GitopError as exc:
            self._journal(f"repost: issue#{iid} skipped, push probe failed: {exc}")
            return False
        review_ran = os.path.isdir(os.path.join(self.repo_path, ".review-loop"))
        if not pushed or not review_ran:
            self._journal(f"repost: issue#{iid} skipped "
                          f"(pushed={pushed}, review_loop_dir={review_ran})")
            return False
        report = None
        if session_id:
            report = runner.export_last_report(session_id, self.repo_path)
        if not self._post_note(iid, report or REPOST_NOT_FOUND_NOTE):
            return False  # transient channel failure: degrade to a full turn
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            issues = ledger.setdefault("issues", {})
            key = str(int(iid))
            if key not in issues:
                issues[key] = _empty_entry()
            entry = issues[key]
            # CAS: only the claim's own in-flight "pending" may be finalized;
            # a close (done) / pause / unpause landing mid-repost survives
            # (same gate as the caller's degradation rollback)
            if entry["status"] == "pending":
                entry["status"] = "paused" if pre_status == "paused" else "active"
            entry["claimed_at"] = None
            state.save_state(self.state_path, ledger)
        note_kind = "report re-posted" if report else "not-found note posted"
        suffix = "" if entry["status"] == "active" else f" (status={entry['status']})"
        self._journal(f"repost: issue#{iid} {note_kind}{suffix}")
        return True

    # ------------------------------------------------------------------ turns

    def _mark_review_push_pending(self, iid):
        """gt-9f24: the review phase terminated abnormally, but its session
        may still be alive on the opencode service side and land commits on
        the branch LATER — mark the entry so reconcile owes the branch ONE
        delayed re-push after the grace period. ONLY this path may set the
        flag (manual/TUI work must never be swept into an automatic push)."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None:
                return
            entry["review_push_pending_at"] = _now_iso()
            state.save_state(self.state_path, ledger)
        self._journal(f"review: issue#{iid} marked for delayed re-push "
                      f"(grace {self.review_push_grace_s}s)")

    def _arm_report_probe(self, iid):
        """ag-a9af5e: the fix reply posted WITHOUT a §5 report — the turn may
        have ended before the report was produced, so the sweep owes this
        issue a budget-bounded report re-post probe. ONLY the turn's reply
        path may arm it (PushFailed/blocked/defer have their own delivery
        surfaces; the review phase never arms)."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None:
                return
            entry["report_probe_pending"] = True
            entry["report_probe_count"] = 0  # 不依赖 _claim 先 pop 的隐式排序
            state.save_state(self.state_path, ledger)
        self._journal(f"probe: issue#{iid} armed (fix reply was not a report)")

    def _claim(self, iid, note_ids, ledger):
        """Claim-before-run: pending + claimed_at + this turn's note ids in
        ONE in-memory mutation and ONE save_state (same-transaction feel)."""
        issues = ledger.setdefault("issues", {})
        key = str(int(iid))
        if note_ids:
            state.mark_processed(ledger, iid, note_ids)  # creates the entry if absent
        if key not in issues:
            issues[key] = _empty_entry()
        entry = issues[key]
        entry["status"] = "pending"
        entry["claimed_at"] = _now_iso()
        # the turn's own post-turn/post-review pushes own the branch now
        entry.pop("review_push_pending_at", None)
        # ag-a9af5e: the new turn's own reply absorbs any report debt —
        # clear the probe flag and its consumed budget (ownership transfer)
        entry.pop("report_probe_pending", None)
        entry.pop("report_probe_count", None)
        state.save_state(self.state_path, ledger)
        self._journal(f"claim: issue#{iid} -> pending ({len(note_ids)} note id(s) consumed)")

    def _push_on_close(self, iid):
        """A close that lands outside a turn (webhook closed event, reconcile
        set-difference, TUI takeover, manual branch commits) must still land
        the branch: push issue/<iid>-fix when it holds commits beyond the
        agent baseline. Best-effort — the issue is already closed by a human,
        so ANY failure journals + one comment and never breaks the caller
        (a raised error here would abort the reconcile close loop and skip
        the remaining issues' pushes)."""
        try:
            gitop.fetch_base(self.repo_path, self.remote_url,
                             self.default_branch, self.ssl_verify)
            if gitop.has_new_commits_vs_ref(
                    self.repo_path, iid,
                    gitop.agent_tracking_ref(self.default_branch)):
                gitop.push_fix_branch_resilient(self.repo_path, iid, self.remote_url,
                                                self.ssl_verify)
                self._journal(f"reconcile: issue#{iid} close-push "
                              f"{gitop.fix_branch(iid)}")
        except gitop.GitopError as exc:
            self._journal(f"warning: issue#{iid} close-push failed: {exc}")
            self._close_push_note(iid, exc)
        except Exception as exc:  # api errors must not abort a close batch
            self._journal(f"warning: issue#{iid} close-push failed: {exc!r}")

    def _close_push_note(self, iid, exc):
        try:
            self._post_note(iid, f"issue#{iid} 关单补推 {gitop.fix_branch(iid)} "
                                 f"失败，需要人工处理：{exc}")
        except Exception as exc2:
            self._journal(f"warning: issue#{iid} close-push comment failed: {exc2!r}")

    def _iid_lock(self, iid):
        """The per-issue serialization lock (one Lock per issue ever touched —
        trivial memory; a same-issue event waits here while another turn of
        the SAME issue holds it)."""
        with self._iid_locks_guard:
            return self._iid_locks.setdefault(str(int(iid)), threading.Lock())

    def _run_turn(self, iid, *, message, title, session_id, action):
        # bounded concurrency: at most max_parallel turns process-wide, and
        # this issue's own turns strictly serialize (iid lock held across the
        # whole turn so a queued same-issue comment can never interleave).
        # A same-issue event may therefore wait on the lock for up to one
        # full turn budget while its claim ages — claim_timeout_s must stay
        # above that budget (checked at __init__, warning) or the recycle
        # can requeue a duplicate retry mid-wait.
        with self._iid_lock(iid), self._turn_slots:
            self._journal(f"turn: issue#{iid} start ({action}, session={session_id or 'new'})")
            # ack BEFORE the gates: humans get instant feedback even when the
            # turn is going to be deferred by a failed baseline fetch
            if not self._post_note(iid, f"已接手 issue#{iid}"):
                self._defer(iid, session_id)
                return
            self._heartbeat_register(iid)
            try:
                # thin guarantee layer step 1: baseline fetch IN the main repo
                # (agent-remote ref only — no checkout, no worktree, no gate on
                # the user's working copy; the agent branches off this ref in
                # its OWN worktree)
                try:
                    gitop.fetch_base(self.repo_path, self.remote_url,
                                     self.default_branch, self.ssl_verify)
                except gitop.GitopError as exc:
                    self._journal(f"turn: issue#{iid} deferred (baseline fetch failed): {exc}")
                    self._defer(iid, session_id)
                    return

                prompt = runner.build_prompt(self.protocol_path, message)
                result = self._run_opencode(iid, prompt, title, session_id)
                if result is None:  # failure mapping already applied (defer/blocked)
                    return

                # post-turn observability: the agent must never touch the main
                # working copy (its protocol forbids it). Journal CRITICAL only —
                # no automatic action; a dirty tree is the agent's own violation.
                try:
                    if gitop.tracked_dirty(self.repo_path):
                        self._journal(
                            f"CRITICAL: issue#{iid} agent modified the main working "
                            "copy (protocol violation) — observability note, no action taken")
                except gitop.GitopError as exc:
                    self._journal(f"warning: issue#{iid} main-copy probe failed: {exc}")

                # steps 2+3: detect commits the agent landed on issue/<iid>-fix
                # (vs the agent-remote baseline ref) and push them from the main
                # repo — refs are global, so this works even while the branch is
                # still checked out in the agent's own worktree.
                pushed = False
                try:
                    pushed = gitop.has_new_commits_vs_ref(
                        self.repo_path, iid,
                        gitop.agent_tracking_ref(self.default_branch))
                    if pushed:
                        gitop.push_fix_branch_resilient(self.repo_path, iid, self.remote_url,
                                                        self.ssl_verify)
                    else:
                        self._journal(
                            f"turn: issue#{iid} no new commits on "
                            f"{gitop.fix_branch(iid)}; push skipped")
                except gitop.PushFailed as exc:
                    # 缺陷A: the blocked exit would drop the turn's reply —
                    # store it for the sweep's deferred delivery first (a
                    # store write failure must not skip the blocked finalize)
                    if result.reply_text:
                        try:
                            self._sweep_owe_note(iid, result.reply_text)
                        except Exception as store_exc:
                            self._journal(
                                f"warning: issue#{iid} reply store failed "
                                f"(debt lost): {store_exc!r}")
                    self._blocked(iid, result.session_id or session_id,
                                  f"推送 issue/{iid}-fix 失败，需要人工处理：{exc}")
                    return
                except gitop.GitopError as exc:
                    self._blocked(iid, result.session_id or session_id,
                                  f"turn 收尾失败，需要人工处理 issue#{iid}：{exc}")
                    return

                # post-push capture verification (read-only observability): a
                # missing knowledge ingest / experience draft is a journal
                # warning only, never a block (Trivial turns legitimately skip)
                if pushed:
                    self._verify_capture(iid)

                reply = result.reply_text
                if not reply:
                    self._blocked(iid, result.session_id or session_id,
                                  f"issue#{iid} 本轮未产生回复文本，需要人工查看")
                    return
                if not self._post_note(iid, reply):
                    # transient: the turn result stays in the ledger, reply deferred
                    self._defer(iid, result.session_id or session_id)
                    return
                # ag-a9af5e 置位面（唯一）：回复上墙了但不是三段式汇报——
                # 回合可能收队于汇报产出前，交给 sweep 预算内探测补发。
                # 「范围外」开头是协议第 0 步的合法无汇报回复，不欠汇报。
                if (result.session_id and not runner.is_report(reply)
                        and not reply.startswith("范围外")):
                    self._arm_report_probe(iid)
                # task 8: post-push review phase — ONLY for turns that pushed
                # bot fix commits; this is the single call site (no review-of-
                # review), and its failures are mapped NON-blocking inside.
                if pushed and self.review_enabled:
                    if not self._run_review_phase(iid, title,
                                                   result.session_id or session_id):
                        return
                # reply FIRST, finalize after: kill -9 in between replays at most
                # one duplicate reply (bounded, marker-filtered downstream)
                self._finalize(iid, result.session_id or session_id,
                               gitop.fix_branch(iid))
            finally:
                self._heartbeat_unregister(iid)

    def _run_opencode(self, iid, prompt, title, session_id):
        """Run one opencode turn with the runner's failure mapping.

        Returns TurnResult, or None when the failure path already applied
        (defer/blocked) inside here. A SessionInvalid fallback to a fresh
        session happens at most ONCE (iterative loop, not recursion — a
        session-less run that reports SessionInvalid again is mapped to
        blocked instead of recursing to RecursionError, F18). Partial edits
        from an interrupted turn live in the AGENT's own worktree — the
        daemon writes nothing into any working copy (F19's daemon-side WIP
        commit is retired with the delegated-worktree architecture).
        """
        effective_session = session_id
        on_event = self._heartbeat_events(iid)
        # Task 2 回合发牌：composed ONCE before the loop so both paths AND
        # the SessionInvalid degrade to a fresh session all carry the prefix.
        cred_prefix = runner.credential_prefix(iid, self.repo_path)
        while True:
            try:
                if effective_session:
                    return runner.continue_session(
                        self.repo_path, effective_session, prompt,
                        self.opencode_timeout, on_event=on_event,
                        cred_prefix=cred_prefix)
                return runner.new_session(
                    self.repo_path, title, prompt, self.opencode_timeout,
                    on_event=on_event, cred_prefix=cred_prefix)
            except runner.SessionInvalid as exc:
                if effective_session is None:
                    self._journal(f"turn: fresh session also rejected, blocking: {exc}")
                    self._blocked(iid, session_id,
                                  f"opencode 新建会话也被拒绝，需要人工介入：{exc}")
                    return None
                self._journal(f"turn: session invalid, starting fresh: {exc}")
                self._post_note(iid, SESSION_INVALID_NOTE)
                effective_session = None
            except runner.TurnTimeout as exc:
                self._journal(f"turn: deferred (opencode timeout): {exc}")
                self._defer(iid, session_id)
                return None
            except runner.TurnCrash as exc:
                self._journal(f"turn: opencode crash: {exc}")
                self._blocked(iid, session_id, f"opencode 连续崩溃，需要人工介入：{exc}")
                return None

    def _run_review_phase(self, iid, title, fix_session_id):
        """Task 8: deterministic post-push review of bot fix commits.

        Spawns a FRESH headless opencode session (review# title prefix —
        the issue's ledger session_id is never touched by it) instructed to
        run the REAL /review-loop command with an EXPLICIT diff scope,
        persistent evidence, and Trivial-only fix delegation. Review
        failures are NON-blocking: the fix phase already succeeded, so a
        timeout/crash only journals + ONE human-review comment. After the
        phase returns (success or not) the branch is re-checked and pushed
        again in case the reviewer committed Trivial fixes — the daemon, not
        the reviewer, owns pushing. Returns False only when the caller must
        skip finalize (post-review push failed -> blocked).
        """
        self._journal(
            f"review: issue#{iid} start "
            f"(scope agent-remote/{self.default_branch}..issue/{iid}-fix)")
        evidence_dir = (f"{self.repo_path}/.omo/evidence/{self.service_name}"
                        f"/review-{iid}-"
                        f"{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}")
        os.makedirs(evidence_dir, exist_ok=True)
        sid_path = os.path.join(evidence_dir, "session-id.txt")
        prompt = REVIEW_PROMPT.format(
            iid=iid, default_branch=self.default_branch,
            verify_command=self.verify_command,
            evidence_dir=evidence_dir)
        cred_prefix = runner.credential_prefix(iid, self.repo_path)
        try:
            review = runner.new_session_publishing_sid(
                self.repo_path, f"review#{iid}: {title}"[:40], prompt,
                self.review_timeout, sid_path,
                on_event=self._heartbeat_events(iid), cred_prefix=cred_prefix)
        except (runner.TurnTimeout, runner.TurnCrash, runner.SessionInvalid) as exc:
            self._journal(f"review: issue#{iid} 未完成 ({type(exc).__name__}): {exc}")
            self._post_note(iid, REVIEW_FAILED_NOTE)
            self._mark_review_push_pending(iid)
            review = None
        if review is not None:
            if review.reply_text:
                self._post_note(iid, review.reply_text)
            else:
                self._journal(f"review: issue#{iid} produced no summary text")
        # the reviewer may have committed Trivial fixes on the same branch:
        # re-check + push again (same pattern as the fix push; an idempotent
        # no-op push when the reviewer committed nothing new)
        try:
            if gitop.has_new_commits_vs_ref(
                    self.repo_path, iid,
                    gitop.agent_tracking_ref(self.default_branch)):
                gitop.push_fix_branch_resilient(self.repo_path, iid, self.remote_url,
                                                self.ssl_verify)
            else:
                self._journal(
                    f"review: issue#{iid} no new commits after review; push skipped")
        except gitop.PushFailed as exc:
            self._blocked(iid, fix_session_id,
                          f"评审后推送 issue/{iid}-fix 失败，需要人工处理：{exc}")
            return False
        except gitop.GitopError as exc:
            self._blocked(iid, fix_session_id,
                          f"评审后收尾失败，需要人工处理 issue#{iid}：{exc}")
            return False
        return True

    def _verify_capture(self, iid):
        """Post-push read-only capture verification: did the turn leave the
        protocol's 沉淀 artifacts (knowledge ingest on the fix branch + an
        experience draft in the global store)?

        Observability ONLY: one journal line
        `capture: issue#<iid> knowledge=ok|missing experience=ok|missing`
        plus a warning when either is missing. Trivial/exempt turns
        legitimately skip capture, so a miss never blocks, defers, or
        posts. Any failure (GitopError, missing store dir, scan error)
        degrades to the corresponding missing — this never raises.
        """
        knowledge = experience = "missing"
        try:
            if gitop.knowledge_changes_present(
                    self.repo_path, iid,
                    gitop.agent_tracking_ref(self.default_branch)):
                knowledge = "ok"
        except Exception as exc:  # read-only probe: degrade, never raise
            self._journal(f"warning: issue#{iid} knowledge probe failed: {exc}")
        try:
            # same-file double gate: `issue#<iid>` with 双侧词边界
            # (trailing-digit guard: issue#8 must not match issue#80;
            # left `(?<!\w)` guard: `myissue#62`/`sub-issue#62` letter-run
            # prefixes cannot match) AND the exact frontmatter line
            # `project: <owner/repo>` (line-anchored so `project:
            # owner/repo2` cannot substring-match owner/repo; the store
            # is shared across projects)
            pattern = re.compile(rf"(?<!\w)issue#{iid}(?!\d)")
            project_gate = re.compile(
                rf"^project:\s*{re.escape(str(self.project))}\s*$",
                re.MULTILINE)
            for draft in EXPERIENCES_DIR.glob("*.md"):
                text = draft.read_text(encoding="utf-8", errors="replace")
                if pattern.search(text) and project_gate.search(text):
                    experience = "ok"
                    break
        except Exception as exc:  # missing dir / scan failure: degrade
            self._journal(f"warning: issue#{iid} experience draft scan failed: {exc}")
        self._journal(
            f"capture: issue#{iid} knowledge={knowledge} experience={experience}")
        if "missing" in (knowledge, experience):
            self._journal(
                f"warning: issue#{iid} capture incomplete "
                f"(knowledge={knowledge}, experience={experience})")

    # ------------------------------------------------------------- outcomes

    def _defer(self, iid, session_id):
        deferred = False
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            key = str(int(iid))
            issues = ledger.setdefault("issues", {})
            if key not in issues:
                issues[key] = _empty_entry()
            entry = issues[key]
            if session_id:
                entry["session_id"] = session_id
            entry["claimed_at"] = None
            entry["defer_count"] += 1
            if entry["defer_count"] > self.defer_limit:
                entry["status"] = "blocked"
                deferred = "blocked"
            else:
                entry["status"] = "defer"
            count = entry["defer_count"]
            state.save_state(self.state_path, ledger)
        if deferred == "blocked":
            self._journal(f"defer: issue#{iid} hit the retry limit ({count}), blocked")
            self._post_note(iid, f"issue#{iid} 已连续 {count} 次重试失败，暂停自动处理，请人工介入。")
        else:
            self._journal(f"defer: issue#{iid} -> defer ({count}/{self.defer_limit})")

    def _blocked(self, iid, session_id, help_text):
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            issues = ledger.setdefault("issues", {})
            key = str(int(iid))
            if key not in issues:
                issues[key] = _empty_entry()
            entry = issues[key]
            entry["status"] = "blocked"
            entry["claimed_at"] = None
            # blocked = human intervention: no automatic re-push may fire later
            entry.pop("review_push_pending_at", None)
            # ag-a9af5e: human takeover — no automatic report probe later either
            entry.pop("report_probe_pending", None)
            entry.pop("report_probe_count", None)
            if session_id:
                entry["session_id"] = session_id
            state.save_state(self.state_path, ledger)
        self._journal(f"blocked: issue#{iid}: {help_text}")
        self._post_note(iid, help_text)

    def _finalize(self, iid, session_id, branch):
        """CAS write-back: only set active when the current status is not
        'done' (a close that landed mid-turn must not be rolled back)."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            issues = ledger.setdefault("issues", {})
            key = str(int(iid))
            if key not in issues:
                issues[key] = _empty_entry()
            entry = issues[key]
            if session_id:
                entry["session_id"] = session_id
            if branch:
                entry["branch"] = branch
            entry["claimed_at"] = None
            if entry["status"] != "done":
                entry["status"] = "active"
                entry["defer_count"] = 0
            final_status = entry["status"]
            state.save_state(self.state_path, ledger)
        self._journal(f"turn: issue#{iid} finalized (status={final_status})")

    def _post_note(self, iid, text):
        """Post a marked note. Returns False ONLY on a transient failure (the
        caller defers); an auth-dead channel is journaled CRITICAL and returns
        True so a finished turn is not derailed by a missing comment."""
        try:
            self.api.post_note(self.project, iid, self.api.build_note_body(text))
            return True
        except api.ApiAuthError as exc:
            self._journal(f"CRITICAL: comment channel unavailable: {exc}")
            return True
        except api.ApiTransientError as exc:
            self._journal(f"warning: comment post failed transiently: {exc}")
            return False

    # --------------------------------------------------------------- heartbeat

    def _heartbeat_register(self, iid):
        """Track one in-flight turn for the heartbeat thread (no-op when the
        heartbeat is off)."""
        if not self.heartbeat_interval:
            return
        now = time.monotonic()
        with self._hb_guard:
            self._in_flight[iid] = {"start": now, "activity": "",
                                    "last_post": now}

    def _heartbeat_unregister(self, iid):
        with self._hb_guard:
            self._in_flight.pop(iid, None)

    def _heartbeat_note(self, iid, activity):
        """A side: quote the latest describable activity into the registry
        (memory only — never posts)."""
        with self._hb_guard:
            entry = self._in_flight.get(iid)
            if entry is not None:
                entry["activity"] = activity

    def _heartbeat_scan(self, now=None):
        """Pure bookkeeping: for every in-flight turn whose interval has
        elapsed, produce (iid, note text) and re-arm last_post — a scan
        books itself, so repeats before the next interval find nothing."""
        now = time.monotonic() if now is None else now
        due = []
        with self._hb_guard:
            for iid, entry in self._in_flight.items():
                if now - entry["last_post"] < self.heartbeat_interval:
                    continue
                entry["last_post"] = now
                text = f"仍在进行：已运行 {int((now - entry['start']) // 60)} 分钟"
                if entry["activity"]:
                    text += f"，当前正在：{entry['activity']}"
                due.append((iid, text))
        return due

    def _heartbeat_deliver(self, iid, text):
        """Post one heartbeat note — unless the turn finalized since the
        scan produced it (scan-then-post race). The guard NARROWS, not
        closes, the stale window: the turn can still finalize between the
        membership re-check and the HTTP post completing; closing it fully
        would mean posting inside _hb_guard — not worth it (bounded ≤1
        note per interval, ledger unaffected)."""
        with self._hb_guard:
            if iid not in self._in_flight:
                return
        if not self._post_note(iid, text):
            self._journal(f"heartbeat: issue#{iid} note failed transiently")

    def _heartbeat_loop(self):
        """C side — the ONLY comment outlet for heartbeats: cadence is
        bounded by the scan's interval bookkeeping (one note per in-flight
        turn per interval at most), whatever the activity stream does.
        The thread must never die: _post_note's exception surface is not
        closed (api re-raises cert errors raw; resp.read() can raise bare
        OSErrors) — same discipline as _worker_loop."""
        while not self._stop.wait(1.0):
            try:
                for iid, text in self._heartbeat_scan():
                    self._heartbeat_deliver(iid, text)
            except Exception as exc:  # the heartbeat thread must never die
                self._journal(f"heartbeat: unhandled error: {exc!r}")

    def _heartbeat_events(self, iid):
        """on_event seam for runner turns: feed describable NDJSON lines into
        the registry. Returns None when the heartbeat is off, so the runner
        keeps its historical blocking path unchanged."""
        if not self.heartbeat_interval:
            return None

        def on_event(line_text):
            # 契约：回调不可炸 select 循环（会杀整回合）——activity_from_event
            # 的 except 面不含 RecursionError（超深嵌套行的理论路径），在此兜底
            try:
                activity = runner.activity_from_event(line_text)
            except Exception:
                return
            if activity:
                self._heartbeat_note(iid, activity)

        return on_event

    # ------------------------------------------------------------------ sweep

    def _sweep_loop(self):
        """欠账巡检线程：每 sweep_interval 醒一次，补推本地领先 origin 的 fix
        分支、补投 blocked 路径存下的 reply。全部动作只打 journal；评论纪律见
        _sweep_issue（纯空操作零评论）。The thread must never die — same
        discipline as _worker_loop/_heartbeat_loop."""
        while not self._stop.wait(self.sweep_interval):
            try:
                self._sweep_once()
            except Exception as exc:  # the sweep thread must never die
                self._journal(f"sweep: unhandled error: {exc!r}")

    def _sweep_once(self):
        """One sweep pass over every local issue/*-fix branch."""
        branches = self._sweep_local_branches()
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
        for iid, sha in branches:
            try:
                self._sweep_issue(iid, sha, ledger)
            except Exception as exc:  # one bad issue must not stop the sweep
                self._journal(f"sweep: issue#{iid} failed: {exc!r}")
        self._sweep_report_probes()

    def _sweep_issue(self, iid, sha, ledger):
        """Debt check + payoff for one issue branch (skip rules per the spec:
        in-flight -> closed -> sha debounce -> push/count>0 + pending flush;
        a comment only when origin was rewritten or a flush landed)."""
        key = str(int(iid))
        # in-flight: live heartbeat registry first, then the ledger snapshot's
        # pending claim (covers heartbeat_interval=0 deployments whose
        # registry stays empty)
        if int(iid) in self._in_flight or \
                (ledger["issues"].get(key) or {}).get("status") == "pending":
            self._journal(f"sweep: issue#{iid} skip(in-flight)")
            return
        # running turns hold this lock for their whole duration: taking it
        # non-blocking makes the in-flight gate race-free even with the
        # heartbeat off (a turn that claims mid-sweep queues behind us and
        # runs after the sweep released it)
        lock = self._iid_lock(iid)
        if not lock.acquire(blocking=False):
            self._journal(f"sweep: issue#{iid} skip(in-flight)")
            return
        try:
            self._sweep_payoff(iid, sha, ledger)
        finally:
            lock.release()

    def _sweep_payoff(self, iid, sha, ledger):
        """The gated body of _sweep_issue: closed-probe -> debounce ->
        push (count>0) -> pending flush -> sha/ledger bookkeeping."""
        key = str(int(iid))
        try:
            if self.api.issue_state(self.project, iid) != "open":
                self._journal(f"sweep: issue#{iid} skip(closed)")
                return
        except Exception as exc:  # 网络失败按 open 处理，下轮再试
            self._journal(f"sweep: issue#{iid} state probe failed "
                          f"(treated open): {exc}")
        with self._sweep_guard:
            store = self._sweep_store_load()
        pending = list(store["pending_notes"].get(key) or [])
        if sha == store["swept_shas"].get(key) and not pending:
            # 防抖：this exact head was already paid off and nothing is owed
            self._journal(f"sweep: issue#{iid} skip(no-debt)")
            return
        count = self._sweep_commit_count(iid)
        changed_origin = False
        push_ok = True  # no push owed counts as paid
        if count > 0:
            try:
                remote_tip = gitop.remote_branch_tip(
                    self.repo_path, self.remote_url, gitop.fix_branch(iid),
                    self.ssl_verify)
                gitop.push_fix_branch_resilient(self.repo_path, iid,
                                                self.remote_url, self.ssl_verify)
                changed_origin = remote_tip != sha
                self._journal(f"sweep: issue#{iid} push {gitop.fix_branch(iid)} "
                              f"({count} ahead, origin "
                              f"{'updated' if changed_origin else 'unchanged'})")
            except (gitop.PushFailed, gitop.GitopError) as exc:
                self._journal(f"sweep: issue#{iid} push failed, "
                              f"retry next round: {exc}")
                push_ok = False
        flushed = False
        if pending:
            if changed_origin:
                head = f"欠账巡检：补推 issue#{iid} {count} 提交＋原 reply 正文："
            else:
                head = f"欠账巡检：补发 issue#{iid} 欠账留言："
            # 直连 api.post_note 而非 _post_note：auth 死道时 _post_note 会
            # 返回 True（为回合主路径不脱轨），这里照单全收会把永远发不出去
            # 的欠账静默清掉——sweep 的账只有真的落到 issue 上才清
            body = head + "\n\n" + "\n\n".join(pending)
            try:
                self.api.post_note(self.project, iid,
                                   self.api.build_note_body(body))
                flushed = True
                self._journal(f"sweep: issue#{iid} flushed {len(pending)} note(s)")
            except api.ApiTransientError as exc:
                self._journal(f"sweep: issue#{iid} note flush failed, "
                              f"kept for next round: {exc}")
            except api.ApiAuthError as exc:
                self._journal(f"CRITICAL: sweep: issue#{iid} comment channel "
                              f"unavailable, debt kept: {exc}")
        elif changed_origin:
            self._post_note(iid, f"欠账巡检：补推 issue#{iid} {count} 提交。")
        if push_ok:
            with self._sweep_guard:
                store = self._sweep_store_load()
                store["swept_shas"][key] = sha
                if flushed:
                    remaining = store["pending_notes"].get(key)
                    if remaining is not None:
                        # 只清本次已投递的快照条目：flush 期间并发
                        # _sweep_owe_note 追加的 reply 只会在尾部，必须保留
                        del remaining[:min(len(pending), len(remaining))]
                        if not remaining:
                            store["pending_notes"].pop(key, None)
                self._sweep_store_save(store)

    def _sweep_owe_note(self, iid, text):
        """缺陷A: a turn blocked by PushFailed would drop its reply — store it
        for the sweep's deferred delivery (same ledger family as _defer's
        note ids, but reply TEXT lives in the orchestrator-owned store beside
        the ledger; state.py's schema is out of the change scope)."""
        key = str(int(iid))
        with self._sweep_guard:
            store = self._sweep_store_load()
            store["pending_notes"].setdefault(key, []).append(text)
            owed = len(store["pending_notes"][key])
            self._sweep_store_save(store)
        self._journal(f"sweep: issue#{iid} reply owed ({owed} pending)")

    def _sweep_report_probes(self):
        """汇报补发探针（ag-a9af5e）：回合收队于汇报产出前且无人「重发汇报」
        时，session 里后来才产出的三段式汇报永不自动送达。sweep 周期对
        active＋置位＋预算内的 issue 探测补发。凭证模式同 gt-9f24 延迟补推：
        锁内原子消费一次预算，锁外做 git/export/评论墙工作，终局锁内清旗。"""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            due = []
            for key, entry in ledger["issues"].items():
                if entry["status"] != "active" or not entry.get("report_probe_pending"):
                    continue
                count = int(entry.get("report_probe_count") or 0)
                if count >= MAX_REPORT_PROBES:
                    continue
                entry["report_probe_count"] = count + 1
                due.append((key, count + 1, entry.get("session_id")))
            if due:
                state.save_state(self.state_path, ledger)
        for key, count, session_id in due:
            iid = int(key)
            # 交付面与在飞回合/「重发汇报」同锁互斥（同 _sweep_issue 锁纪律，
            # 非阻塞抢占）：关闭「双发/并发 export」与「陈旧探针误清新回合
            # 刚置位的旗」两道竞态（终闸 B1/B2）
            lock = self._iid_lock(iid)
            if not lock.acquire(blocking=False):
                self._journal(f"probe: issue#{iid} skip(in-flight)")
                # 预算已顶格时顺路终局清旗（终闸 R2-F2）：skip 不走 deliver
                # 的终局面，两次 skip 会留下 pending 卡死＋count 顶格死态
                self._probe_exhausted(iid, count)
                continue
            self._journal(f"probe: issue#{iid} start "
                          f"(budget {count}/{MAX_REPORT_PROBES})")
            try:
                self._sweep_report_probe_deliver(iid, count, session_id)
            except Exception as exc:  # one bad issue must not stop the sweep
                self._journal(f"probe: issue#{iid} failed: {exc!r}")
                # 异常绕过 deliver 的终局面时也执行耗尽清旗（旗不永久卡死）
                self._probe_exhausted(iid, count)
            finally:
                lock.release()

    def _sweep_report_probe_deliver(self, iid, count, session_id):
        """锁外执行面：前置探测 → export → 评论墙去重 → 补发。前置不满足/
        墙读失败＝非真探测：只 journal 并退还预算；未命中/GitopError/瞬态
        失败预算内不清旗，预算耗尽则锁内清旗；送达或在墙立即锁内清旗。"""
        try:
            pushed = gitop.fix_branch_pushed(self.repo_path, iid, self.remote_url,
                                             self.ssl_verify)
        except gitop.GitopError as exc:
            self._journal(f"probe: issue#{iid} push probe failed "
                          f"(budget {count}/{MAX_REPORT_PROBES}): {exc}")
            return self._probe_exhausted(iid, count)
        review_ran = os.path.isdir(os.path.join(self.repo_path, ".review-loop"))
        if not pushed or not review_ran or not session_id:
            # 前置不满足＝本次不是真探测：只 journal，不烧预算不清旗
            # （worker 迟后会推分支/跑评审，届时零成本再探——issue#65
            # 实证「precondition unmet...budget 1/2」空烧）
            self._journal(f"probe: issue#{iid} precondition unmet "
                          f"(pushed={pushed}, review_loop={review_ran}, "
                          f"session={bool(session_id)}) — budget held")
            return self._probe_unburn(iid, count)
        report = runner.export_last_report(session_id, self.repo_path)
        if not report:
            self._journal(f"probe: issue#{iid} no report in session "
                          f"(budget {count}/{MAX_REPORT_PROBES})")
            return self._probe_exhausted(iid, count)
        # 去重判 containment：墙上评论经 build_note_body 带 marker 前后缀，
        # 汇报原文仍内含其中
        try:
            wall = self.api.list_issue_comments(self.project, iid)
        except Exception as exc:
            # 墙读失败＝未送达且未判命中：只 journal 不烧预算，API 恢复后
            # 下轮再探（issue#65 空烧教训）
            self._journal(f"probe: issue#{iid} comment wall read failed: "
                          f"{exc!r} — budget held")
            return self._probe_unburn(iid, count)
        if any(report in (c.get("body") or "") for c in wall):
            return self._probe_clear_flag(iid, "dup-skip")
        # 直连 api.post_note 而非 _post_note（同 _sweep_payoff flush 纪律）：
        # auth 死道时 _post_note 返回 True，探针会把没发出去的汇报当送达清旗
        try:
            self.api.post_note(self.project, iid,
                               self.api.build_note_body(report))
        except api.ApiTransientError as exc:
            self._journal(f"probe: issue#{iid} repost failed transiently "
                          f"(budget {count}/{MAX_REPORT_PROBES}): {exc}")
            return self._probe_exhausted(iid, count)
        except api.ApiAuthError as exc:
            self._journal(f"probe: issue#{iid} comment channel unavailable "
                          f"(budget {count}/{MAX_REPORT_PROBES}): {exc}")
            return self._probe_exhausted(iid, count)
        self._probe_clear_flag(iid, "auto-repost")

    def _probe_exhausted(self, iid, count):
        """预算耗尽仍未送达：锁内清旗（同置位周期不再探）；预算内则保留
        待下轮 sweep 再探。"""
        if count < MAX_REPORT_PROBES:
            return
        self._probe_clear_flag(iid, "probe exhausted")

    def _probe_unburn(self, iid, count):
        """本次消费作废（前置不满足/墙读失败＝非真探测）：锁内退还刚消费
        的预算（CAS 同 _probe_clear_flag），旗保留待下轮 sweep 零成本再探。"""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None or not entry.get("report_probe_pending"):
                return
            entry["report_probe_count"] = max(0, count - 1)
            state.save_state(self.state_path, ledger)

    def _probe_clear_flag(self, iid, reason):
        """锁内清旗（CAS：claim/blocked/close 已中途接管则不动账、不记因）。"""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None or not entry.get("report_probe_pending"):
                return
            entry.pop("report_probe_pending", None)
            state.save_state(self.state_path, ledger)
        self._journal(f"probe: issue#{iid} {reason}")

    def _sweep_store_load(self) -> dict:
        """The debt store, or an empty one when missing/corrupt (worst case of
        a lost store: one idempotent re-push, never a wrong push)."""
        try:
            with open(self._sweep_path, encoding="utf-8") as fh:
                raw = json.load(fh)
            notes, shas = raw.get("pending_notes"), raw.get("swept_shas")
            if isinstance(notes, dict) and isinstance(shas, dict):
                return {"pending_notes": {str(k): [str(t) for t in v]
                                          for k, v in notes.items()
                                          if isinstance(v, list)},
                        "swept_shas": {str(k): str(v)
                                       for k, v in shas.items()}}
        except (OSError, ValueError, AttributeError):
            pass
        return {"pending_notes": {}, "swept_shas": {}}

    def _sweep_store_save(self, store: dict) -> None:
        """Atomic tmp+fsync+rename, state.py's write pattern."""
        tmp = self._sweep_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(store, fh, ensure_ascii=False)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self._sweep_path)

    def _sweep_local_branches(self):
        """[(iid, head sha)] over local refs/heads/issue/*-fix (read-only,
        credential-free). Routed through gitop._run for the test seam; gitop
        itself stays untouched per the task's change scope."""
        proc = gitop._run(["git", "-C", self.repo_path, "for-each-ref",
                           "--format=%(objectname) %(refname)",
                           "refs/heads/issue/"], cwd=self.repo_path)
        if proc.returncode != 0:
            self._journal(f"sweep: branch enumeration failed: "
                          f"{(proc.stderr or proc.stdout).strip()}")
            return []
        out = []
        for line in proc.stdout.splitlines():
            m = re.fullmatch(r"([0-9a-f]{40}) refs/heads/issue/(\d+)-fix",
                             line.strip())
            if m:
                out.append((int(m.group(2)), m.group(1)))
        return out

    def _sweep_commit_count(self, iid) -> int:
        """The int behind has_new_commits_vs_ref: commits on the local fix
        branch beyond the agent-remote baseline (0 on any probe failure)."""
        proc = gitop._run(
            ["git", "-C", self.repo_path, "rev-list", "--count",
             f"{gitop.agent_tracking_ref(self.default_branch)}..{gitop.fix_branch(iid)}"],
            cwd=self.repo_path)
        try:
            return int(proc.stdout.strip()) if proc.returncode == 0 else 0
        except ValueError:
            return 0

    # --------------------------------------------------------------- helpers

    @staticmethod
    def _note_ids(event):
        if not isinstance(event, dict):
            return []
        comment = event.get("comment")
        if not isinstance(comment, dict):
            return []
        note_id = comment.get("id")
        return [int(note_id)] if note_id is not None else []

    @staticmethod
    def _message_and_title(event, iid):
        """Reply context for the runner: issue body for opens, comment body else.

        Single choke point for argv size: the message is byte-truncated to
        MAX_MESSAGE_BYTES and the title to its 40-char cap BEFORE the prompt
        and argv are built, so the runner never sees an E2BIG-sized argument.
        """
        issue = (event.get("issue") or {}) if isinstance(event, dict) else {}
        if not isinstance(event, dict) or not isinstance(event.get("comment"), dict):
            title = issue.get("title") or f"issue#{iid}"
            body = issue.get("body") or ""
            message = f"{title}\n\n{body}".strip()
            out_title = f"issue#{iid}: {title}"[:40]
        else:
            message = event["comment"].get("body") or ""
            out_title = f"issue#{iid}"[:40]
        return _truncate_utf8(message, MAX_MESSAGE_BYTES), out_title

    # ------------------------------------------------------------ self-cancel

    def _self_cancel(self):
        """D6: the main repo is gone (2 consecutive reconciles) — deregister.

        Best-effort webhook removal by matching callback_url, then stop +
        teardown (systemd unit + snapshot removal in daemon wiring). Config
        and the state ledger are deliberately KEPT so a reinstall does not
        replay history. Never raises."""
        self._journal("CRITICAL: 主仓目录连续缺失——自注销（停服+删unit+删webhook+删快照；配置与账本保留）")
        try:
            for hook in self.api.list_hooks(self.project):
                if (hook.get("config") or {}).get("url") == self.callback_url:
                    self.api.delete_hook(self.project, hook["id"])
                    self._journal(f"self-cancel: webhook#{hook['id']} deleted")
        except Exception as exc:
            self._journal(f"self-cancel: webhook deletion failed (continuing): {exc!r}")
        self._stop.set()
        if self._teardown is not None:
            try:
                self._teardown()
            except Exception as exc:
                self._journal(f"self-cancel: teardown failed: {exc!r}")

    # -------------------------------------------------------------- reconcile

    def reconcile(self):
        """Full anti-entropy pass; safe to call concurrently with the worker."""
        if not os.path.isdir(self.repo_path):
            self._repo_misses += 1
            self._journal(f"warning: repo path missing ({self._repo_misses}/2): {self.repo_path}")
            if self._repo_misses >= 2:
                self._self_cancel()
                return
        else:
            self._repo_misses = 0
        self._journal("reconcile: start")
        now = datetime.now(timezone.utc)
        # 1. stale claims -> defer (+defer_count) -> requeue
        for iid in state.claim_timeout_recycle(self.state_path, now, self.claim_timeout_s):
            self._journal(f"reconcile: recycled stale claim issue#{iid}, requeued")
            self.queue.put({_REQUEUE: int(iid), "reason": "claim-timeout"})

        # 2. fetch open issues + all their notes
        fetched = self._fetch_open_issues_and_notes()
        if fetched is None:
            # F20: keep the reconcile-interval pacing even on early return,
            # or the worker's ~1s Empty poll turns an API outage into a
            # ~1 Hz reconcile storm.
            self._last_reconcile = time.monotonic()
            return  # API unavailable; next pass retries
        open_issues, open_notes = fetched

        # 3. first-run baseline vs new-issue detection (polling fallback):
        #    - ledger without the "baselined" marker -> first install: adopt
        #      ALL untracked open issues + note-id snapshot and set the marker
        #      (one time only; it persists in state.json across restarts)
        #    - marker present -> an open issue MISSING from the ledger was
        #      created after the baseline (e.g. webhook delivery blocked by
        #      server policy): synthesize a Gitea "opened" event and feed it
        #      through the SAME fsm -> claim -> turn path as webhooks
        new_events = []
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            if ledger.get("baselined") is not True:
                known = set(ledger["issues"])
                state.adopt_existing(ledger, {
                    iid: [n["id"] for n in notes if n.get("id") is not None]
                    for iid, notes in open_notes.items()})
                adopted = sum(1 for iid in open_notes
                              if str(int(iid)) not in known)
                ledger["baselined"] = True
                state.save_state(self.state_path, ledger)
                self._journal(
                    f"reconcile: baseline adopted {adopted} issue(s) (first run)")
            else:
                new_events = [
                    {"action": "opened",
                     "issue": {"number": int(issue["number"]),
                               "title": issue.get("title") or "",
                               "body": issue.get("body") or "",
                               "state": "open"}}
                    for issue in open_issues
                    if issue.get("number") is not None
                    and str(int(issue["number"])) not in ledger["issues"]]
                if new_events:
                    self._journal(
                        f"reconcile: {len(new_events)} new untracked issue(s) "
                        f"→ processing as new")
        # _handle_event takes the ledger lock itself: feed OUTSIDE it (same
        # pattern as the note replay below)
        for event in new_events:
            self._handle_event(event)

        # 4. feed every note through the SAME fsm + per-note-id path; issues
        #    with a live claim are skipped entirely (their owner will finish)
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            claimed = {iid for iid, e in ledger["issues"].items()
                       if e["status"] == "pending"}
            feedable = {}
            for iid, notes in open_notes.items():
                if str(iid) in claimed:
                    continue
                fresh = [n for n in notes
                         if n.get("id") is not None
                         and not state.is_note_processed(ledger, iid, n["id"])]
                if fresh:
                    feedable[iid] = fresh
        for iid, notes in feedable.items():
            for note in notes:
                self._handle_event(self._note_event(iid, note))

        # 5. advisory-1 set-difference close: active/paused/defer/pending but
        #    no longer in the open set -> done
        open_keys = {str(iid) for iid in open_notes}
        closed = []
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            for iid, entry in ledger["issues"].items():
                if entry["status"] in ("active", "paused", "defer", "pending") \
                        and iid not in open_keys:
                    entry["status"] = "done"
                    entry["claimed_at"] = None
                    # close-push owns the branch; drop any pending mark
                    entry.pop("review_push_pending_at", None)
                    # ag-a9af5e: close ownership — drop the probe fields too
                    entry.pop("report_probe_pending", None)
                    entry.pop("report_probe_count", None)
                    closed.append(iid)
            if closed:
                state.save_state(self.state_path, ledger)
        for iid in closed:
            self._journal(f"reconcile: issue#{iid} left the open set -> done")
            self._push_on_close(int(iid))

        # 5.5 gt-9f24 delayed review re-push: one grace-bounded attempt per
        #     abnormally-terminated review phase; active entries only (a
        #     live claim or a closed issue owns the branch otherwise)
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            due = [int(iid) for iid, entry in ledger["issues"].items()
                   if entry["status"] == "active"
                   and self._review_push_due(entry, now)]
        for iid in due:
            self._delayed_review_push(iid)

        # 6. deferred issues retry on the next event or this reconcile
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            deferred = [int(iid) for iid, e in ledger["issues"].items()
                        if e["status"] == "defer"]
        for iid in deferred:
            self._journal(f"reconcile: requeued deferred issue#{iid} for retry")
            self.queue.put({_REQUEUE: iid, "reason": "defer-retry"})

        self._last_reconcile = time.monotonic()
        self._journal("reconcile: done")

    def _review_push_due(self, entry, now) -> bool:
        """True when the entry's delayed-re-push mark exists and the grace
        period has elapsed (a malformed mark is due once, so it gets cleared
        instead of haunting the ledger forever)."""
        pending_at = entry.get("review_push_pending_at")
        if not pending_at:
            return False
        try:
            age = (now - datetime.fromisoformat(pending_at)).total_seconds()
        except (TypeError, ValueError):
            return True
        return age >= self.review_push_grace_s

    def _delayed_review_push(self, iid):
        """The ONE delayed push attempt owed to an abnormally-terminated
        review phase (gt-9f24): push the late reviewer commits, journal +
        comment. The mark is consumed ATOMICALLY under the ledger lock
        BEFORE any git work — a _claim (or a concurrent reconcile pass)
        that lands first owns the branch instead, and the attempt stays
        exactly-once. A push failure maps to the existing
        PushFailed->blocked semantics and is never retried automatically."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(int(iid)))
            if entry is None or entry["status"] != "active" \
                    or not entry.get("review_push_pending_at"):
                return
            entry.pop("review_push_pending_at", None)
            state.save_state(self.state_path, ledger)
        pushed = False
        try:
            gitop.fetch_base(self.repo_path, self.remote_url,
                             self.default_branch, self.ssl_verify)
            if gitop.has_new_commits_vs_ref(
                    self.repo_path, iid,
                    gitop.agent_tracking_ref(self.default_branch)):
                gitop.push_fix_branch_resilient(self.repo_path, iid, self.remote_url,
                                                self.ssl_verify)
                pushed = True
            else:
                self._journal(f"reconcile: issue#{iid} delayed re-push found "
                              "no new commits; mark cleared")
        except gitop.PushFailed as exc:
            self._blocked(iid, None, f"评审异常后延迟补推 issue/{iid}-fix 失败，"
                                     f"需要人工处理：{exc}")
        except gitop.GitopError as exc:
            self._blocked(iid, None, f"评审异常后延迟补推收尾失败，"
                                     f"需要人工处理 issue#{iid}：{exc}")
        # outside the gitop-only try (mirrors _run_review_phase): an api
        # failure must never abort the remaining reconcile work
        if pushed:
            self._journal(f"reconcile: issue#{iid} 评审异常后延迟补推 "
                          f"{gitop.fix_branch(iid)}")
            self._post_note(iid, f"issue#{iid} 评审异常后延迟补推 "
                                 f"{gitop.fix_branch(iid)}"
                                 "（评审会话晚到提交已推送）。")

    def _fetch_open_issues_and_notes(self):
        """(issue records, {iid: [api comment, ...]}) for every open issue,
        or None when the API is unavailable right now."""
        open_notes = {}
        try:
            issues = self.api.list_open_issues(self.project)
        except api.ApiAuthError as exc:
            self._journal(f"CRITICAL: reconcile cannot read the API: {exc}")
            return None
        except api.ApiTransientError as exc:
            self._journal(f"reconcile: aborted, API transient failure: {exc}")
            return None
        for issue in issues:
            iid = issue.get("number")
            if iid is None:
                continue
            try:
                open_notes[int(iid)] = self.api.list_issue_comments(self.project, iid)
            except api.ApiAuthError as exc:
                self._journal(f"CRITICAL: reconcile cannot read the API: {exc}")
                return None
            except api.ApiTransientError as exc:
                # 稳态单 issue 评论墙炸不拖垮整个 reconcile（issue#51：
                # >2500 条评论触发分页超限，495 次 reconcile 全体流产），
                # 置空列表继续，下轮 reconcile 自然重拉。首跑例外：未
                # baselined 时快照必须全量，残缺会毒化基线（旧评论日后
                # 被当新事件重放）——维持整体 abort 不动。
                with self._ledger_lock:
                    baselined = state.load_state(
                        self.state_path, warn=self._journal).get("baselined") is True
                if not baselined:
                    self._journal(f"reconcile: aborted, API transient failure: {exc}")
                    return None
                self._journal(f"reconcile: issue#{iid} comments unavailable: {exc}")
                open_notes[int(iid)] = []
        return issues, open_notes

    @staticmethod
    def _note_event(iid, comment):
        """API comment -> the exact Gitea issue_comment shape the fsm expects."""
        user = comment.get("user") or {}
        return {
            "action": "created",
            "issue": {"number": iid, "state": "open"},
            "comment": {
                "id": comment.get("id"),
                "body": comment.get("body") or "",
                "user": {"login": user.get("login")},
            },
        }

    # ---------------------------------------------------------------- requeue

    def _handle_requeue(self, iid):
        """Retry turn for a recycled/deferred issue; skips if the ledger moved on."""
        with self._ledger_lock:
            ledger = state.load_state(self.state_path, warn=self._journal)
            entry = ledger["issues"].get(str(iid))
            if entry is None or entry["status"] != "defer":
                status = entry["status"] if entry else "untracked"
                self._journal(f"requeue: issue#{iid} skipped (status={status})")
                return
            entry["status"] = "pending"
            entry["claimed_at"] = _now_iso()
            session_id = entry.get("session_id")
            state.save_state(self.state_path, ledger)
        self._run_turn(iid, message=RETRY_MESSAGE, title=f"issue#{iid}: retry"[:40],
                       session_id=session_id, action="continue")
