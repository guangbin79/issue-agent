"""Delegated-worktree architecture tests (user mandate).

The daemon NEVER creates or manages any worktree: the opencode agent inside
each turn does that itself (per its global AGENTS.md worktree rules). What
the daemon guarantees per turn, and ONLY that:

    claim -> ack -> fetch_base (agent-remote ref, in the main repo)
    -> runner turn with cwd = the MAIN repo
    -> post-turn main-copy tracked-dirty check (CRITICAL journal only)
    -> has_new_commits_vs_ref -> push_fix_branch -> reply -> CAS finalize

Groups:
1. runner cwd and every gitop target == the MAIN repo path
2. a turn performs ZERO daemon git operations besides fetch/detect/push
   (the gitop._run spy fails if any checkout/worktree/branch/... appears)
3. post-turn dirt check journals CRITICAL but never derails the turn
4. worktree_path config is ignored (deprecated), logged exactly once

Same fake seams as tests/test_orch.py (Harness/FakeGitop/spy included).

Run: python3 -m pytest tests/test_delegate.py -q   (from scripts/issue-agent/)
"""

import sys
from pathlib import Path

PKG_DIR = Path(__file__).resolve().parent.parent

# Same stdlib-http shadow dance as tests/test_orch.py: strip the issue-agent
# dir from sys.path BEFORE any http-dependent import so our http.py cannot
# shadow the stdlib http package, then put it back for the module imports.
sys.path[:] = [p for p in sys.path if p not in ("", str(PKG_DIR))]

import http.client  # noqa: E402,F401  (stdlib http must win)
import urllib.request  # noqa: E402,F401

sys.path.insert(0, str(PKG_DIR))

import gitop  # noqa: E402

from tests.test_orch import (  # noqa: E402
    Harness,
    issue_open_event,
)


# --------------------------------------------------------------------------
# 1. wiring: everything targets the MAIN repo
# --------------------------------------------------------------------------


def test_runner_cwd_and_gitop_targets_are_the_main_repo(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    h.orch._handle_event(issue_open_event(iid=11))  # synchronous turn

    assert h.runner.new_calls[0]["cwd"] == h.repo_path, (
        "opencode sessions root in the MAIN repo; the agent makes its own worktree")
    assert h.gitop.fetch_calls[0][0] == h.repo_path
    assert h.gitop.has_calls[0][0] == h.repo_path
    assert h.gitop.dirty_calls == [h.repo_path]
    assert h.entry(11)["status"] == "active"


def test_commit_turn_gits_are_exactly_fetch_detect_push(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    h.gitop.commits = True
    h.orch._handle_event(issue_open_event(iid=11))

    assert len(h.gitop.fetch_calls) == 1
    assert h.gitop.has_calls and h.gitop.has_calls[0][1] == 11
    # task 8: fix push + the post-review re-push (review on by default)
    assert h.gitop.push_calls == [11, 11]
    assert h.gitop_argv == [], (
        "no other git op may run: checkout/worktree/branch/... are the agent's")
    assert h.gitop_argv == [], (
        "no other git op may run: checkout/worktree/branch/... are the agent's")


# --------------------------------------------------------------------------
# 2. zero daemon git ops besides fetch/detect/push
# --------------------------------------------------------------------------


def test_zero_daemon_git_ops_besides_fetch_push(tmp_path, monkeypatch):
    """gitop._run spy: with the four turn seams faked, NOTHING may reach the
    real git runner — re-introducing any checkout/branch/worktree/merge call
    into the daemon's turn path fails here."""
    h = Harness(tmp_path, monkeypatch)
    h.gitop.commits = True
    h.orch._handle_event(issue_open_event(iid=11))
    assert h.gitop_argv == []


def test_push_failure_blocks_turn(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    h.gitop.commits = True
    h.gitop.push_error = gitop.PushFailed("push rejected (fake)")
    h.orch._handle_event(issue_open_event(iid=11))
    assert h.entry(11)["status"] == "blocked"
    assert any("推送" in b for _, b in h.api.posts), "help note posted"


# --------------------------------------------------------------------------
# 3. post-turn dirt check: CRITICAL journal, observability only
# --------------------------------------------------------------------------


def test_post_turn_dirt_check_journals_critical(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    h.gitop.dirty = True  # the agent illegally edited the main copy
    h.orch._handle_event(issue_open_event(iid=11))
    assert h.entry(11)["status"] == "active", "observability only: turn completes"
    assert h.journal("CRITICAL"), h.journal_msgs
    assert h.journal("protocol violation"), h.journal_msgs


def test_dirty_main_copy_does_not_block_push(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch)
    h.gitop.commits = True
    h.gitop.dirty = True
    h.orch._handle_event(issue_open_event(iid=11))
    # task 8: fix push + post-review re-push — dirt never suppresses either
    assert h.gitop.push_calls == [11, 11], "dirt is journaled, never acted on"
    assert h.entry(11)["status"] == "active"


# --------------------------------------------------------------------------
# 4. worktree_path: deprecated, ignored, logged once
# --------------------------------------------------------------------------


def test_worktree_path_deprecated_logged_once_and_ignored(tmp_path, monkeypatch):
    h = Harness(tmp_path, monkeypatch, worktree_path=str(tmp_path / "old_wt"))
    assert len(h.journal("worktree_path is deprecated and ignored")) == 1, (
        h.journal_msgs)

    h.orch._handle_event(issue_open_event(iid=11))
    assert len(h.journal("worktree_path is deprecated and ignored")) == 1, (
        "logged once, not per turn")
    assert h.runner.new_calls[0]["cwd"] == h.repo_path, (
        "ignored: the runner cwd stays the MAIN repo")
    assert h.gitop.fetch_calls[0][0] == h.repo_path, (
        "ignored: git targets stay the MAIN repo")
