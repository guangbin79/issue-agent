"""gitop tests: baseline fetch (agent-remote ref), main-copy cleanliness
probe, worktree pruning, fix-branch-only push whitelist, PAT scrubbing, and
push-from-main-repo while the fix branch sits checked out in a foreign
worktree (the delegated-worktree architecture).

The daemon is a thin guarantee layer: fetch/detect/push are its ONLY turn
git operations — no checkout, no branch creation, no worktree management
(the opencode agent does all of that in its own worktree).

All fixtures are REAL temporary git repositories (a bare "Gitea" remote at
tmp_path/remote.git driven over file:// URLs); the only monkeypatched piece
is gitop._run in the http.sslVerify argv-construction tests.

Run: python3 -m pytest tests/test_gitop.py -q   (from scripts/issue-agent/)
"""

import itertools
import shutil
import subprocess
from pathlib import Path

import pytest

import gitop
from gitop import (
    GitopError,
    PushFailed,
    RefspecViolation,
    agent_tracking_ref,
    ensure_fix_branch,
    fetch_base,
    fetch_fix_branch_tip,
    fix_branch,
    fix_branch_pushed,
    has_new_commits_vs_ref,
    knowledge_changes_present,
    prune_worktrees,
    push_verified_commit,
    sweep_idle_review_worktrees,
    push,
    push_fix_branch,
    rebase_safe_to_force,
    push_fix_branch_resilient,
    tracked_dirty,
)

_advance_counter = itertools.count()


def git(*args, cwd):
    return subprocess.run(
        ["git", *args], cwd=str(cwd), capture_output=True, text=True, check=True
    ).stdout.strip()


def config_user(repo):
    git("config", "user.email", "tester@example.com", cwd=repo)
    git("config", "user.name", "tester", cwd=repo)


def commit_file(repo, name, content, msg):
    (repo / name).write_text(content)
    git("add", "-A", cwd=repo)
    git("commit", "-m", msg, cwd=repo)
    return git("rev-parse", "HEAD", cwd=repo)


def advance_remote(tmp_path, remote_url, content, branch="master"):
    """Push one extra commit to the bare remote's branch; return its sha."""
    adv = tmp_path / f"adv{next(_advance_counter)}"
    git("clone", remote_url, str(adv), cwd=tmp_path)
    config_user(adv)
    return_sha = commit_file(adv, "remote.txt", content, f"remote: {content}")
    git("push", "origin", f"HEAD:{branch}", cwd=adv)
    return return_sha


def current_branch(repo):
    return git("rev-parse", "--abbrev-ref", "HEAD", cwd=repo)


@pytest.fixture()
def bare_remote(tmp_path):
    seed = tmp_path / "seed"
    seed.mkdir()
    git("init", "-b", "master", cwd=seed)
    config_user(seed)
    (seed / "README.md").write_text("base\n")
    git("add", "-A", cwd=seed)
    git("commit", "-m", "base", cwd=seed)
    remote = tmp_path / "remote.git"
    git("clone", "--bare", str(seed), str(remote), cwd=tmp_path)
    return remote


@pytest.fixture()
def remote_url(bare_remote):
    return f"file://{bare_remote}"


@pytest.fixture()
def repo(tmp_path, bare_remote):
    work = tmp_path / "work"
    git("clone", str(bare_remote), str(work), cwd=tmp_path)
    config_user(work)
    return work


class FakeRun:
    """Stand-in for gitop._run that records argv (and credential env)
    and fakes clean output."""

    def __init__(self):
        self.calls = []
        self.envs = []

    def __call__(self, cmd, cwd, env=None):
        self.calls.append(cmd)
        self.envs.append(env or {})
        args = cmd[3:]  # after ["git", "-C", repo]
        if "status" in args:
            out = ""
        elif "abbrev-ref" in args:
            out = "master\n"
        else:
            out = ""
        return subprocess.CompletedProcess(cmd, 0, stdout=out, stderr="")


# --- fetch_base ---------------------------------------------------------------


def test_fetch_base_writes_agent_remote_ref_only(repo, remote_url, tmp_path):
    """The baseline fetch must ONLY move the agent-remote tracking ref:
    local master, HEAD, branch, and the working copy stay exactly as-is."""
    old_tip = git("rev-parse", "HEAD", cwd=repo)
    tip = advance_remote(tmp_path, remote_url, "r1", branch="master")

    fetch_base(repo, remote_url)

    assert git("rev-parse", agent_tracking_ref("master"), cwd=repo) == tip
    assert git("rev-parse", "master", cwd=repo) == old_tip, "local master untouched"
    assert current_branch(repo) == "master"
    assert git("rev-parse", "HEAD", cwd=repo) == old_tip
    assert not (repo / "remote.txt").exists(), "working copy untouched"


def test_fetch_base_has_new_commits_vs_ref_sees_branch_commits(
        repo, remote_url, tmp_path):
    base = agent_tracking_ref("master")
    old_tip = git("rev-parse", "HEAD", cwd=repo)
    advance_remote(tmp_path, remote_url, "r0", branch="master")
    fetch_base(repo, remote_url)
    assert has_new_commits_vs_ref(repo, 7, base) is False, "no branch yet"

    git("branch", fix_branch(7), base, cwd=repo)
    assert has_new_commits_vs_ref(repo, 7, base) is False, "fresh branch"
    # the agent commits on the branch from ITS worktree; the main repo
    # never checks anything out
    agent_wt = tmp_path / "agent_wt"
    git("worktree", "add", str(agent_wt), fix_branch(7), cwd=repo)
    config_user(agent_wt)
    commit_file(agent_wt, "fix.py", "fix\n", "fix")
    git("worktree", "remove", str(agent_wt), cwd=repo)
    assert has_new_commits_vs_ref(repo, 7, base) is True
    assert git("rev-parse", "HEAD", cwd=repo) == old_tip, (
        "the main repo HEAD never moves")

def test_fetch_base_default_branch_main(tmp_path):
    seed = tmp_path / "seed-main"
    seed.mkdir()
    git("init", "-b", "main", cwd=seed)
    config_user(seed)
    (seed / "README.md").write_text("base\n")
    git("add", "-A", cwd=seed)
    git("commit", "-m", "base", cwd=seed)
    remote = tmp_path / "remote-main.git"
    git("clone", "--bare", str(seed), str(remote), cwd=tmp_path)
    work = tmp_path / "work-main"
    git("clone", str(remote), str(work), cwd=tmp_path)
    config_user(work)

    fetch_base(work, f"file://{remote}", default_branch="main")
    assert git("rev-parse", agent_tracking_ref("main"), cwd=work) == git(
        "rev-parse", "HEAD", cwd=work)


def test_fetch_base_argv_and_pat_env(repo, monkeypatch):
    """F14 parity for the baseline fetch: credential-free URL on the argv,
    PAT riding in the env (GIT_ASKPASS); ssl flag only for http(s)."""
    fake = FakeRun()
    monkeypatch.setattr("gitop._run", fake)
    https_url = "https://oauth2:SECRETPAT@gitlab.example.com/group/repo.git"
    fetch_base(repo, https_url, "master", ssl_verify=False)
    fetch_calls = [c for c in fake.calls if "fetch" in c]
    assert fetch_calls == [
        ["git", "-C", str(repo), "-c", "http.sslVerify=false", "fetch",
         "https://gitlab.example.com/group/repo.git",
         "+refs/heads/master:refs/remotes/agent-remote/master"]
    ]
    assert all("SECRETPAT" not in arg for c in fake.calls for arg in c)
    fetch_env = fake.envs[fake.calls.index(fetch_calls[0])]
    assert fetch_env.get("UIA_GIT_PAT") == "SECRETPAT"
    assert fetch_env.get("GIT_ASKPASS"), "askpass helper wired"

    fake.calls.clear()
    fake.envs.clear()
    fetch_base(repo, "file:///tmp/remote.git", "master")
    assert all("http.sslVerify" not in c for c in fake.calls)
    assert all("GIT_ASKPASS" not in e for e in fake.envs), (
        "file URLs carry no credentials, no askpass needed")


def test_fetch_base_failure_scrubs_pat(repo, monkeypatch):
    def failing_run(cmd, cwd, env=None):
        return subprocess.CompletedProcess(
            cmd, 128, stdout="",
            stderr="fatal: unable to access "
                  "'https://oauth2:SECRETPAT123@127.0.0.1:1/gitlab/repo.git/'",
        )

    monkeypatch.setattr("gitop._run", failing_run)
    url = "https://oauth2:SECRETPAT123@127.0.0.1:1/gitlab/repo.git"
    with pytest.raises(GitopError) as excinfo:
        fetch_base(repo, url, "master", ssl_verify=False)
    assert "SECRETPAT123" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


# --- knowledge_changes_present ------------------------------------------------


def test_knowledge_changes_present_branch_side_only(
        repo, remote_url, tmp_path):
    """ocr#1/F1: only branch-side knowledge changes count. A base-side
    ingest (another issue's knowledge merged while this branch was out)
    must not light the probe up — the two-dot diff wrongly counted it.
    """
    base = agent_tracking_ref("master")
    fetch_base(repo, remote_url)  # pin the baseline
    branch = fix_branch(9)

    git("branch", branch, base, cwd=repo)
    agent_wt = tmp_path / "agent_wt"
    git("worktree", "add", str(agent_wt), branch, cwd=repo)
    config_user(agent_wt)
    (agent_wt / "docs" / "agents" / "knowledge").mkdir(parents=True)
    commit_file(agent_wt, "docs/agents/knowledge/k1.md", "k1\n",
                "knowledge: k1")
    git("worktree", "remove", str(agent_wt), cwd=repo)
    assert knowledge_changes_present(repo, 9, base) is True

    # next turn: the previous branch is history; a fresh branch restarts
    # at the still-pinned baseline with a code fix only (no knowledge)
    pinned = git("rev-parse", base, cwd=repo)
    git("branch", "-D", branch, cwd=repo)
    git("branch", branch, pinned, cwd=repo)
    agent_wt2 = tmp_path / "agent_wt2"
    git("worktree", "add", str(agent_wt2), branch, cwd=repo)
    config_user(agent_wt2)
    commit_file(agent_wt2, "fix.py", "fix\n", "fix")
    git("worktree", "remove", str(agent_wt2), cwd=repo)

    # another issue's knowledge ingest lands on master; baseline advances
    adv = tmp_path / "adv_k2"
    git("clone", remote_url, str(adv), cwd=tmp_path)
    config_user(adv)
    (adv / "docs" / "agents" / "knowledge").mkdir(parents=True)
    commit_file(adv, "docs/agents/knowledge/k2.md", "k2\n",
                "knowledge: k2")
    git("push", "origin", "HEAD:master", cwd=adv)
    fetch_base(repo, remote_url)

    assert knowledge_changes_present(repo, 9, base) is False, (
        "base-side-only knowledge change must not count")


# --- tracked_dirty ------------------------------------------------------------


def test_tracked_dirty_counts_only_tracked_changes(repo):
    assert tracked_dirty(repo) is False
    (repo / "docs_new").mkdir()
    (repo / "docs_new" / "x.md").write_text("untracked\n")
    assert tracked_dirty(repo) is False, "untracked files never count"
    (repo / "README.md").write_text("user is editing\n")
    assert tracked_dirty(repo) is True


# --- prune_worktrees ----------------------------------------------------------


def test_prune_worktrees_clears_stale_registration(repo, tmp_path):
    stale = tmp_path / "stale_wt"
    git("worktree", "add", "--detach", str(stale), "HEAD", cwd=repo)
    shutil.rmtree(stale)  # deleted without unregistering
    assert "stale_wt" in git("worktree", "list", "--porcelain", cwd=repo)

    prune_worktrees(repo)
    assert "stale_wt" not in git("worktree", "list", "--porcelain", cwd=repo)


def test_prune_worktrees_non_repo_raises(tmp_path):
    notrepo = tmp_path / "not_a_repo"
    notrepo.mkdir()
    with pytest.raises(GitopError):
        prune_worktrees(notrepo)


# --- sweep_idle_review_worktrees ----------------------------------------------


def add_review_worktree(repo, tmp_path, name, branch):
    wt = tmp_path / name
    git("worktree", "add", str(wt), "-b", branch, cwd=repo)
    return wt


def names(entries):
    return sorted(Path(p).name for p in entries)


def test_sweep_removes_idle_review_worktree(repo, tmp_path):
    wt = add_review_worktree(repo, tmp_path, "work__wt-review-x", fix_branch(9))

    report = sweep_idle_review_worktrees(repo)

    assert names(report.removed) == ["work__wt-review-x"]
    assert report.kept == [] and report.refused == []
    assert not wt.exists(), "worktree directory gone"
    porcelain = git("worktree", "list", "--porcelain", cwd=repo)
    assert porcelain.count("worktree ") == 1, "registration gone, main only"


def test_sweep_keeps_dirty_review_worktree(repo, tmp_path):
    wt = add_review_worktree(repo, tmp_path, "work__wt-review-y", fix_branch(9))
    (wt / "untracked.txt").write_text("dirty\n")

    report = sweep_idle_review_worktrees(repo)

    assert names(report.kept) == ["work__wt-review-y"], "git refused the dirty one"
    assert report.removed == [] and report.refused == []
    assert wt.exists(), "dirty worktree left in place"


def test_sweep_skips_worktree_on_non_fix_branch(repo, tmp_path):
    wt = add_review_worktree(repo, tmp_path, "work__wt-review-z", "experiment")

    report = sweep_idle_review_worktrees(repo)

    assert report.removed == [] and report.kept == [] and report.refused == []
    assert wt.exists()
    assert "work__wt-review-z" in git("worktree", "list", "--porcelain", cwd=repo)


def test_sweep_skips_decoy_naming(repo, tmp_path):
    decoy = add_review_worktree(repo, tmp_path, "dotfiles__wt-x", fix_branch(9))

    report = sweep_idle_review_worktrees(repo)

    assert report.removed == [] and report.kept == [] and report.refused == []
    assert decoy.exists()
    assert "dotfiles__wt-x" in git("worktree", "list", "--porcelain", cwd=repo)


def test_sweep_refuses_occupied_worktree(repo, tmp_path):
    wt = add_review_worktree(repo, tmp_path, "work__wt-review-o", fix_branch(9))

    def occupied_probe(path):
        return Path(path) == wt

    report = sweep_idle_review_worktrees(repo, cwd_probe=occupied_probe)

    assert names(report.refused) == ["work__wt-review-o"]
    assert report.removed == [] and report.kept == []
    assert wt.exists(), "occupied worktree never touched"


def test_sweep_main_repo_never_candidate(repo, tmp_path):
    add_review_worktree(repo, tmp_path, "work__wt-review-m", fix_branch(9))

    report = sweep_idle_review_worktrees(repo)

    all_reported = report.removed + report.kept + report.refused
    assert Path(repo) not in [Path(p) for p in all_reported]
    assert names(report.removed) == ["work__wt-review-m"], "only the real candidate"


def test_sweep_skips_worktree_outside_parent(repo, tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    wt = elsewhere / "work__wt-review-far"
    git("worktree", "add", str(wt), "-b", fix_branch(9), cwd=repo)

    report = sweep_idle_review_worktrees(repo)

    assert report.removed == [] and report.kept == [] and report.refused == []
    assert wt.exists()


def test_sweep_skips_detached_head_worktree(repo, tmp_path):
    wt = tmp_path / "work__wt-review-d"
    git("worktree", "add", "--detach", str(wt), "HEAD", cwd=repo)

    report = sweep_idle_review_worktrees(repo)

    assert report.removed == [] and report.kept == [] and report.refused == []
    assert wt.exists()


def test_sweep_enumeration_failure_raises(tmp_path):
    notrepo = tmp_path / "not_a_repo"
    notrepo.mkdir()
    with pytest.raises(GitopError):
        sweep_idle_review_worktrees(notrepo)


def test_default_cwd_probe_boundary(tmp_path):
    from gitop import _cwd_occupied
    holder = tmp_path / "probe-holder"
    (holder / "sub").mkdir(parents=True)
    proc = subprocess.Popen(
        ["python3", "-c", "import time; time.sleep(30)"], cwd=str(holder / "sub"))
    try:
        assert _cwd_occupied(holder) is True, "cwd in subdir counts as inside"
        assert _cwd_occupied(tmp_path / "unrelated") is False
    finally:
        proc.kill()
        proc.wait()


# --- push: whitelist + PAT scrubbing ------------------------------------------


def test_push_rejects_non_whitelisted_refspec(repo, remote_url):
    for bad in ("master:master", "issue/7-fix:master", "issue/7-fix:issue/8-fix"):
        with pytest.raises(RefspecViolation):
            push(repo, remote_url, bad)


def test_push_failure_scrubs_pat(repo, monkeypatch):
    git("branch", fix_branch(7), cwd=repo)
    for var in ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
        monkeypatch.delenv(var, raising=False)
    url = "https://oauth2:SECRETPAT123@127.0.0.1:1/gitlab/repo.git"
    with pytest.raises(PushFailed) as excinfo:
        push_fix_branch(repo, 7, url, ssl_verify=False)
    assert "SECRETPAT123" not in str(excinfo.value)


def test_push_scrubs_pat_from_git_stderr(repo, monkeypatch):
    """Deterministic scrub check: git stderr echoes the credentialed URL."""
    url = "https://oauth2:SECRETPAT123@127.0.0.1:1/gitlab/repo.git"

    def failing_run(cmd, cwd, env=None):
        return subprocess.CompletedProcess(
            cmd, 128, stdout="",
            stderr="fatal: unable to access "
                  "'https://oauth2:SECRETPAT123@127.0.0.1:1/gitlab/repo.git/'",
        )

    monkeypatch.setattr("gitop._run", failing_run)
    with pytest.raises(PushFailed) as excinfo:
        push_fix_branch(repo, 7, url, ssl_verify=False)
    assert "SECRETPAT123" not in str(excinfo.value)
    assert "***" in str(excinfo.value)


def test_push_fix_branch_argv_whitelist_and_ssl_flag(repo, monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr("gitop._run", fake)
    url = "https://oauth2:SECRETPAT@host/repo.git"
    push_fix_branch(repo, 7, url, ssl_verify=False)
    assert fake.calls == [
        ["git", "-C", str(repo), "-c", "http.sslVerify=false", "push",
         "https://host/repo.git", "issue/7-fix:issue/7-fix"]
    ]
    # F14: credential-free URL on argv; PAT in the env only
    assert "SECRETPAT" not in " ".join(fake.calls[0])
    assert fake.envs[0].get("UIA_GIT_PAT") == "SECRETPAT"
    assert fake.envs[0].get("GIT_ASKPASS")


def test_push_works_while_branch_checked_out_in_foreign_worktree(
        repo, remote_url, bare_remote, tmp_path):
    """The daemon pushes FROM the main repo while the agent's worktree still
    has issue/<iid>-fix checked out — refs are global, the push must work."""
    agent_wt = tmp_path / "agent_wt"
    git("worktree", "add", str(agent_wt), "-b", fix_branch(7), cwd=repo)
    config_user(agent_wt)
    commit_file(agent_wt, "fix.py", "fix\n", "fix")

    push_fix_branch(repo, 7, remote_url)  # from the MAIN repo

    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == \
        git("rev-parse", fix_branch(7), cwd=repo)
    assert git("branch", "--show-current", cwd=agent_wt) == fix_branch(7), (
        "the foreign worktree keeps its checkout; only the remote moved")


def test_net_env_rejects_username_without_password():
    """A2/ocr#2: a token-as-username URL (username, no password) must raise
    instead of sailing onto the git argv — and the message must not echo
    the credential-bearing URL."""
    from gitop import _net_env
    with pytest.raises(GitopError) as excinfo:
        _net_env("https://SECRETPAT@gitlab.example.com/group/repo.git")
    assert "SECRETPAT" not in str(excinfo.value)
    assert "username" in str(excinfo.value)


# --- push_fix_branch_resilient: rebase blind spot (issue#38) ------------------


def _reject_first_push_run(real_run, branch):
    """Intercept ONLY the first git push argv and fail it with the server's
    non-FF rejection (file:// bare remotes lack receive.denyNonFastForwards,
    so the real daemon failure mode must be simulated); everything else —
    including the lease retry push — runs for real."""
    import gitop
    state = {"n": 0}

    def run(argv, **kw):
        if "push" in argv:
            state["n"] += 1
            if state["n"] == 1:
                return subprocess.CompletedProcess(
                    argv, 1, "",
                    "! [rejected] {b} -> {b} (non-fast-forward)\nhint: Updates "
                    "were rejected because a pushed branch tip is behind its "
                    "remote".format(b=branch))
        return real_run(argv, **kw)
    return run


def test_resilient_push_plain_path(repo, remote_url, bare_remote):
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")

    assert push_fix_branch_resilient(repo, 7, remote_url) == "pushed"
    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == \
        git("rev-parse", fix_branch(7), cwd=repo)


def test_resilient_push_force_after_rebase(repo, remote_url, bare_remote,
                                           monkeypatch):
    """The pushed chain is base+fix; local then rebases the SAME patch onto
    the same base (new sha, identical patch — the merge-time-rebase flow).
    Plain push is rejected non-FF; the lease retry must land the rewritten
    tip because the range-diff orphan check finds no lost commit."""
    import gitop
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)
    old = git("rev-parse", fix_branch(7), cwd=bare_remote)

    git("reset", "--hard", "HEAD~1", cwd=repo)
    rewritten = commit_file(repo, "fix.py", "fix\n", "fix (rebased)")
    assert rewritten != old

    monkeypatch.setattr("gitop._run",
                        _reject_first_push_run(gitop._run, fix_branch(7)))
    assert push_fix_branch_resilient(repo, 7, remote_url) == "force-pushed"
    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == \
        rewritten


def test_resilient_push_refuses_unsafe_rewrite(repo, remote_url, bare_remote,
                                               monkeypatch):
    """A rewrite that DROPS the pushed patch (a different file changed
    instead) must be refused: the range-diff orphan check fails, the
    PushFailed carries the reason, and the remote stays untouched."""
    import gitop
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)
    old = git("rev-parse", fix_branch(7), cwd=bare_remote)

    git("reset", "--hard", "HEAD~1", cwd=repo)
    commit_file(repo, "other.py", "other\n", "other")

    monkeypatch.setattr("gitop._run",
                        _reject_first_push_run(gitop._run, fix_branch(7)))
    with pytest.raises(PushFailed) as excinfo:
        push_fix_branch_resilient(repo, 7, remote_url)
    assert "rewrite unsafe" in str(excinfo.value)
    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == old


def test_resilient_unsafe_rewrite_failure_is_permanent(repo, remote_url,
                                                       bare_remote,
                                                       monkeypatch):
    """issue#264: the rewrite-unsafe refusal is deterministic — same chain,
    same verdict — so it must classify as PermanentPushFailed (still a
    PushFailed), letting the sweep escalate once per tip instead of
    spinning every round."""
    import gitop
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)

    git("reset", "--hard", "HEAD~1", cwd=repo)
    commit_file(repo, "other.py", "other\n", "other")

    monkeypatch.setattr("gitop._run",
                        _reject_first_push_run(gitop._run, fix_branch(7)))
    with pytest.raises(gitop.PermanentPushFailed) as excinfo:
        push_fix_branch_resilient(repo, 7, remote_url)
    assert isinstance(excinfo.value, PushFailed)
    assert "rewrite unsafe" in str(excinfo.value)


def test_push_force_with_lease_argv_shape_and_sha_gate(repo, monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr("gitop._run", fake)
    url = "https://oauth2:SECRETPAT@host/repo.git"
    tip = "a" * 40
    push(repo, url, "issue/7-fix:issue/7-fix", ssl_verify=False,
         force_with_lease=f"issue/7-fix:{tip}")
    assert fake.calls[0][-3] == f"--force-with-lease=issue/7-fix:{tip}"
    assert "SECRETPAT" not in " ".join(fake.calls[0])
    with pytest.raises(RefspecViolation):
        push(repo, url, "issue/7-fix:issue/7-fix", ssl_verify=False,
             force_with_lease="issue/7-fix:not-a-sha")


def test_rebase_safe_to_force_remote_ancestor_is_safe(repo, remote_url,
                                                      bare_remote):
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)
    local_tip = git("rev-parse", fix_branch(7), cwd=repo)
    commit_file(repo, "more.py", "more\n", "more")  # 本地领先远端
    ok, reason = rebase_safe_to_force(
        repo, fix_branch(7), local_tip, git("rev-parse", fix_branch(7), cwd=repo))
    assert ok and "ancestor" in reason


def test_resilient_push_refuses_when_local_behind(repo, remote_url,
                                                  bare_remote, monkeypatch):
    """远端被别处推进（严格领先本地）时，普通 push 的回卷尝试同样带
    non-FF 标记；rescue 必须以「本地落后」拒绝而非空 range 崩溃（B-F1）。"""
    import gitop
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)
    old = git("rev-parse", fix_branch(7), cwd=bare_remote)

    clone2 = bare_remote.parent / "remote.git2"
    git("clone", remote_url, str(clone2), cwd=bare_remote.parent)
    config_user(clone2)
    git("checkout", fix_branch(7), cwd=clone2)
    commit_file(clone2, "extra.txt", "extra\n", "extra (elsewhere)")
    git("push", "origin", fix_branch(7), cwd=clone2)
    git("fetch", "origin", fix_branch(7), cwd=repo)  # 主仓获知远端推进
    new_remote = git("rev-parse", fix_branch(7), cwd=bare_remote)
    assert new_remote != old

    monkeypatch.setattr("gitop._run",
                        _reject_first_push_run(gitop._run, fix_branch(7)))
    with pytest.raises(PushFailed) as excinfo:
        push_fix_branch_resilient(repo, 7, remote_url)
    assert "local is behind remote" in str(excinfo.value)
    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == \
        new_remote


def test_resilient_push_subject_quoting_no_false_refusal(repo, remote_url,
                                                         bare_remote,
                                                         monkeypatch):
    """保留对（patch-id 相同）的主题里恰好引用了 range-diff 输出文本时，
    行首锚定正则不得误判为孤儿行（B-F3 假阳性面）。"""
    import gitop
    git("checkout", "-B", fix_branch(7), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 7, remote_url)

    git("reset", "--hard", "HEAD~1", cwd=repo)
    quoted = commit_file(repo, "fix.py", "fix\n",
                         "fix — 9: 497fc9b < -: -------- (quoted)")
    monkeypatch.setattr("gitop._run",
                        _reject_first_push_run(gitop._run, fix_branch(7)))
    assert push_fix_branch_resilient(repo, 7, remote_url) == "force-pushed"
    assert git("rev-parse", f"refs/heads/{fix_branch(7)}", cwd=bare_remote) == \
        quoted


# --- fix_branch_pushed -------------------------------------------------------


def test_fix_branch_pushed_true_when_tips_equal(repo, remote_url):
    git("checkout", "-B", fix_branch(9), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    push_fix_branch(repo, 9, remote_url)
    assert fix_branch_pushed(repo, 9, remote_url) is True


def test_fix_branch_pushed_false_when_remote_branch_missing(repo, remote_url):
    """远端分支缺失（空 tip）＝从未推送，是普通 False 而非错误。"""
    git("checkout", "-B", fix_branch(9), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    assert fix_branch_pushed(repo, 9, remote_url) is False


def test_fix_branch_pushed_ls_remote_error_propagates(repo, remote_url,
                                                      monkeypatch):
    """ls-remote（网络/认证）失败按既有语义抛 GitopError——查询失败
    绝不能被误读成「未推送」。"""
    import gitop
    git("checkout", "-B", fix_branch(9), cwd=repo)
    commit_file(repo, "fix.py", "fix\n", "fix")
    real_run = gitop._run

    def broken_ls_run(cmd, cwd, env=None):
        if "ls-remote" in cmd:
            return subprocess.CompletedProcess(cmd, 128, stdout="",
                                               stderr="connection refused")
        return real_run(cmd, cwd, env=env)

    monkeypatch.setattr("gitop._run", broken_ls_run)
    with pytest.raises(GitopError):
        fix_branch_pushed(repo, 9, remote_url)


# --- ensure_fix_branch (plan Task 3: 开发分支准备) ---------------------------


def _assert_main_checkout_untouched(repo):
    """ensure_fix_branch 绝不切主检出：分支不变、无新 worktree 注册。"""
    assert current_branch(repo) == "master"
    worktrees = git("worktree", "list", cwd=repo).splitlines()
    assert len(worktrees) == 1


def test_ensure_fix_branch_creates_from_agent_remote_default_when_remote_absent(
        repo, remote_url):
    """新 issue：远端无开发支 → 从最新 agent-remote/<default> 创建本地分支，
    返回其 tip。"""
    fetch_base(repo, remote_url, "master")
    tip = ensure_fix_branch(repo, 7, "master", remote_url)
    baseline = git("rev-parse", agent_tracking_ref("master"), cwd=repo)
    assert tip == baseline
    assert git("rev-parse", "refs/heads/issue/7-fix", cwd=repo) == baseline
    _assert_main_checkout_untouched(repo)


def test_ensure_fix_branch_uses_remote_position_when_local_absent(
        repo, remote_url, tmp_path):
    """远端开发支已存在 → fetch 其有效位置进 agent-remote 命名空间，本地
    分支从该位置创建。"""
    remote_tip = advance_remote(tmp_path, remote_url, "fix1",
                                branch="issue/7-fix")
    fetch_base(repo, remote_url, "master")
    tip = ensure_fix_branch(repo, 7, "master", remote_url)
    assert tip == remote_tip
    assert git("rev-parse", "refs/heads/issue/7-fix", cwd=repo) == remote_tip
    assert git("rev-parse", "refs/remotes/agent-remote/issue/7-fix",
               cwd=repo) == remote_tip
    _assert_main_checkout_untouched(repo)


def test_ensure_fix_branch_never_resets_existing_local_branch(
        repo, remote_url, tmp_path):
    """已有本地分支不被重置：远端位置只进 tracking ref，本地 tip 原样返回。"""
    fetch_base(repo, remote_url, "master")
    baseline = git("rev-parse", agent_tracking_ref("master"), cwd=repo)
    git("branch", fix_branch(7), baseline, cwd=repo)
    remote_tip = advance_remote(tmp_path, remote_url, "fix2",
                                branch="issue/7-fix")
    tip = ensure_fix_branch(repo, 7, "master", remote_url)
    assert tip == baseline, "本地分支 tip 不得被远端位置重置"
    assert git("rev-parse", "refs/remotes/agent-remote/issue/7-fix",
               cwd=repo) == remote_tip, "远端有效位置仍须 fetch 进 tracking ref"
    _assert_main_checkout_untouched(repo)


# --- agora external delivery: push_verified_commit / fetch_fix_branch_tip ----


def _commit_on_fix_branch(repo, name, content, msg, iid=7):
    branch = fix_branch(iid)
    exists = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet",
         f"refs/heads/{branch}"], capture_output=True)
    if exists.returncode != 0:
        git("branch", branch, cwd=repo)
    git("checkout", branch, cwd=repo)
    sha = commit_file(repo, name, content, msg)
    git("checkout", "master", cwd=repo)
    return sha


def test_push_verified_commit_exact_refspec_never_force(
        repo, remote_url, bare_remote, monkeypatch):
    """计划 Task 4：非 force 的 <sha>:refs/heads/issue/<iid>-fix 精确推送——
    argv 恒此形态（无 --force、无任何其他 ref），默认分支 SHA 不动。"""
    delivery = _commit_on_fix_branch(repo, "fix.txt", "fix\n", "fix work")
    master_before = git("rev-parse", "refs/heads/master", cwd=bare_remote)
    argv = []
    real = gitop._run

    def spying_run(cmd, cwd, env=None):
        argv.append(cmd)
        return real(cmd, cwd, env=env)

    monkeypatch.setattr(gitop, "_run", spying_run)
    assert push_verified_commit(repo, 7, delivery, remote_url) == "ok"
    assert argv == [["git", "-C", str(repo), "push", remote_url,
                     f"{delivery}:refs/heads/issue/7-fix"]]
    assert git("rev-parse", "refs/heads/issue/7-fix",
               cwd=bare_remote) == delivery
    assert git("rev-parse", "refs/heads/master", cwd=bare_remote) \
        == master_before, "默认分支 SHA 不动"


def test_push_verified_commit_https_ssl_flag_and_credential_env(
        repo, monkeypatch):
    fake = FakeRun()
    monkeypatch.setattr(gitop, "_run", fake)
    url = "https://oauth2:SECRETPAT@host/repo.git"
    assert push_verified_commit(repo, 7, "a" * 40, url, ssl_verify=False) == "ok"
    assert fake.calls == [
        ["git", "-C", str(repo), "-c", "http.sslVerify=false", "push",
         "https://host/repo.git", f"{'a' * 40}:refs/heads/issue/7-fix"]]
    # F14 同口径：argv 只见无凭据 URL，PAT 只走 env
    assert "SECRETPAT" not in " ".join(fake.calls[0])
    assert fake.envs[0].get("UIA_GIT_PAT") == "SECRETPAT"
    assert fake.envs[0].get("GIT_ASKPASS")


def test_push_verified_commit_requires_full_hex_sha(repo, remote_url):
    for bad in ("", "abc123", "a" * 39, "a" * 41, "Z" * 40, 12345):
        with pytest.raises(RefspecViolation):
            push_verified_commit(repo, 7, bad, remote_url)


def test_push_verified_commit_non_ff_when_remote_moved(
        repo, remote_url, tmp_path):
    """远端 issue/<iid>-fix 已前进：精确推送返回 non_ff，远端位置不动。"""
    base = _commit_on_fix_branch(repo, "base.txt", "base\n", "base")
    assert push_verified_commit(repo, 7, base, remote_url) == "ok"
    adv = tmp_path / "adv-fix"
    git("clone", remote_url, str(adv), cwd=tmp_path)
    git("checkout", fix_branch(7), cwd=adv)  # 从远端有效位置出发
    config_user(adv)
    remote_tip = commit_file(adv, "remote.txt", "moved\n", "remote advance")
    git("push", "origin", f"HEAD:{fix_branch(7)}", cwd=adv)
    local = _commit_on_fix_branch(repo, "local.txt", "local\n", "local work")
    assert push_verified_commit(repo, 7, local, remote_url) == "non_ff"
    assert git("ls-remote", remote_url, f"refs/heads/{fix_branch(7)}",
               cwd=repo).split()[0] == remote_tip, "non_ff 后远端不动（绝不 force）"


def test_push_verified_commit_deterministic_rejection_returns_failed(
        repo, remote_url):
    """确定性失败（本地不存在该对象——重试同 commit 永不可能成功）返回
    failed，不抛异常（monitor 按 task_deliver(failed) 终局分流）。"""
    assert push_verified_commit(repo, 7, "e" * 40, remote_url) == "failed"


def test_push_verified_commit_transport_failure_raises_scrubbed(
        repo, monkeypatch):
    """传输病（拒连）抛 PushFailed（网络类，monitor 保留 awaiting_external
    按轮询节奏重试），异常文本零 PAT 泄漏。"""
    for var in ("http_proxy", "https_proxy", "all_proxy",
                "HTTP_PROXY", "HTTPS_PROXY"):
        monkeypatch.delenv(var, raising=False)
    sha = _commit_on_fix_branch(repo, "t.txt", "t\n", "t")
    url = "https://oauth2:SECRETPAT123@127.0.0.1:1/gitea/repo.git"
    with pytest.raises(PushFailed) as excinfo:
        push_verified_commit(repo, 7, sha, url, ssl_verify=False)
    assert "SECRETPAT123" not in str(excinfo.value)


def test_fetch_fix_branch_tip_reads_remote_into_agent_remote_namespace(
        repo, remote_url):
    """显式 URL fetch issue/<iid>-fix 到 agent-remote 命名空间并返回 tip：
    不改默认支、不碰主检出；本地无该分支也能取（tip 完全来自远端）。"""
    tip = _commit_on_fix_branch(repo, "fix.txt", "fix\n", "fix work")
    git("push", remote_url,
        f"refs/heads/{fix_branch(7)}:refs/heads/{fix_branch(7)}", cwd=repo)
    git("branch", "-D", fix_branch(7), cwd=repo)
    assert fetch_fix_branch_tip(repo, 7, remote_url) == tip
    assert git("rev-parse", f"refs/remotes/agent-remote/{fix_branch(7)}",
               cwd=repo) == tip
    assert current_branch(repo) == "master", "主检出不动"
    probe = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet",
         "refs/remotes/agent-remote/master"],
        capture_output=True)
    assert probe.returncode != 0, "默认支跟踪 ref 不得被创建"
