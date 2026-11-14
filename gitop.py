"""Issue-agent git operations — the daemon's thin guarantee layer.

Per the delegated-worktree architecture the daemon NEVER creates, checks
out, or manages any worktree: the opencode agent inside each turn does all
of that in its own worktree (per its global rules). The daemon's git
surface is exactly:

- fetch_base: refresh refs/remotes/agent-remote/<default> IN the main repo
  (forced refspec, no checkout, no working-copy contact) — the baseline the
  agent branches from and the daemon compares against
- has_new_commits_vs_ref: detect commits the agent landed on issue/<iid>-fix
- push_fix_branch: push that branch (and nothing else) to the remote
- push_verified_commit: agora external delivery — exact <sha> push to
  issue/<iid>-fix, three-state ok/non_ff/failed (never force)
- fetch_fix_branch_tip: agora external delivery — explicit-URL fetch of
  issue/<iid>-fix into the agent-remote namespace, returns the tip sha
- local_branch_tip: read-only local branch position probe ('' when absent,
  never creates the branch) — the agora align-rewrite guard proves a
  force-with-lease push would land exactly the verified handoff commit
- fix_branch_pushed: read-only tip-equality probe (the repost precondition)
- tracked_dirty: post-turn observability probe — did the agent illegally
  modify the main working copy?
- knowledge_changes_present: read-only diff probe — did the fix branch
  touch docs/agents/knowledge/ (post-turn capture verification)?
- prune_worktrees: best-effort stale-registration cleanup at startup
- sweep_idle_review_worktrees: startup-only sweep of idle leftover review
  worktrees (plain `git worktree remove`, never --force — git's own
  dirty/untracked check is the gate; refusals come back as "kept").
  Startup-only use, mandate relaxation user-approved 2026-10-09; scan
  root = parent(config.repo_path), never touches ~/.dotfiles or anything
  outside that parent

Safety contract:
- push accepts exactly one refspec shape, issue/<iid>-fix:issue/<iid>-fix;
  anything else (notably any default-branch refspec) raises RefspecViolation
  before a single git byte is executed
- force pushes go only through push_fix_branch_resilient: plain push first;
  on a non-fast-forward rejection (a later round rebased the branch onto a
  moved baseline — the merge-time-rebase flow, issue#38) retry ONCE with
  --force-with-lease anchored at the observed remote tip, and only after a
  range-diff orphan check proves the rewrite lost no old-chain commit; any
  uncertainty re-raises the original PushFailed (blocked + human note path
  unchanged). The agora monitor composes the same guarded primitives
  (rebase_safe_to_force + push force_with-lease) at its own layer for the
  rebase-round non_ff landing — push_verified_commit itself stays never-force
- the remote URL carries the credentials (https://{user}:{PAT}@host/...); the
  credential never reaches a git argv: fetch/push rewrite the URL without
  userinfo and feed the PAT to git via a 0700 GIT_ASKPASS helper reading
  the process env (only the owner can read /proc/<pid>/environ, unlike
  the world-readable /proc/<pid>/cmdline); every message this module
  raises is additionally scrubbed so the PAT never leaks

The remote is always addressed by its explicit URL (never a named remote), so
fetch/push hit exactly the URL the caller vetted. Every default-branch
reference goes through a *default_branch* parameter (config-derived;
"master" when unset).
"""

import atexit
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import NamedTuple, Union
from urllib.parse import urlparse, urlunparse

PathLike = Union[str, "os.PathLike[str]"]

_REFSPEC_OK = re.compile(r"^issue/(\d+)-fix:issue/\1-fix$")

_REVIEW_BRANCH_OK = re.compile(r"^refs/heads/issue/\d+-fix$")


class GitopError(Exception):
    """Any git failure; the message is always PAT-scrubbed."""


class PushFailed(GitopError):
    """Pushing the fix branch failed: orchestrator blocks and asks for help."""


class PermanentPushFailed(PushFailed):
    """Deterministic push failure (rebase_safe_to_force refusal): retrying
    the same branch tip can never succeed — the sweep escalates once per
    tip instead of spinning every round (issue#264)."""


class ForcePushRejected(PushFailed):
    """Deterministic force-push rejection (protected branch / pre-receive /
    deny-non-fast-forward, ag-cad1d6 High-1): retrying the same rewrite can
    never succeed — the caller rotates or terminates, never retries the
    force landing itself."""


class RefspecViolation(GitopError):
    """Refspec outside the issue/<iid>-fix whitelist (e.g. a default-branch refspec)."""


def _scrub(text: str, remote_url: str) -> str:
    """Replace credential material embedded in remote_url with ***."""
    parsed = urlparse(remote_url)
    if parsed.password:
        secrets = [parsed.password]
        if parsed.username:
            secrets.append(f"{parsed.username}:{parsed.password}@")
        for secret in secrets:
            text = text.replace(secret, "***")
    return text


def _run(cmd, cwd, env=None):
    """Run git with an explicit argv list; module-level so tests can swap it.

    *env* (optional) carries extra variables (GIT_ASKPASS credential feed).
    """
    full_env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    if env:
        full_env.update(env)
    return subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True,
                         env=full_env)


def _git(repo: PathLike, args, remote_url: str = None, env=None) -> str:
    """Run a git subcommand; raise GitopError (scrubbed) on non-zero exit."""
    proc = _run(["git", "-C", str(repo), *args], cwd=repo, env=env)
    if proc.returncode != 0:
        message = "git {} failed: {}".format(
            " ".join(args), (proc.stderr or proc.stdout).strip()
        )
        if remote_url:
            message = _scrub(message, remote_url)
        raise GitopError(message)
    return proc.stdout


def _ssl_args(remote_url: str, ssl_verify: bool):
    """http.sslVerify flag, only meaningful (and only injected) for http(s)."""
    if remote_url.startswith(("http://", "https://")):
        return ["-c", "http.sslVerify={}".format(str(bool(ssl_verify)).lower())]
    return []


_ASKPASS_PASSWORD_ENV = "UIA_GIT_PAT"
_ASKPASS_USER_ENV = "UIA_GIT_USER"
_askpass_path = None


def _unlink_quiet(path):
    try:
        os.unlink(path)
    except OSError:
        pass


def _askpass_helper() -> str:
    """0700 helper script answering git's credential prompts from the
    (owner-only) process env; created once per process (F14)."""
    global _askpass_path
    if _askpass_path is None:
        fd, path = tempfile.mkstemp(prefix="issue-agent-askpass-", suffix=".sh")
        with os.fdopen(fd, "w") as fh:
            fh.write('#!/bin/sh\n'
                      'case "$1" in\n'
                      '  *Username*) echo "${UIA_GIT_USER:-git}" ;;\n'
                      '  *) echo "$UIA_GIT_PAT" ;;\n'
                      'esac\n')
        os.chmod(path, 0o700)
        atexit.register(_unlink_quiet, path)
        _askpass_path = path
    return _askpass_path


def _net_env(remote_url: str):
    """(env, clean_url) for a URL that may embed credentials (F14).

    The credential-free URL goes onto the git argv; the PAT travels in
    the subprocess env for GIT_ASKPASS, which only the owning user can
    read. URLs without credentials pass through unchanged; a username
    WITHOUT a password (token-as-username form) is refused — passing it
    through would put the credential on the git argv.
    """
    parsed = urlparse(remote_url)
    if parsed.username and parsed.password is None:
        raise GitopError(
            "remote URL embeds a username without a password "
            "(token-as-username form); refusing to pass the credential "
            "on the git argv")
    if parsed.password is None:
        return None, remote_url
    env = {
        "GIT_ASKPASS": _askpass_helper(),
        _ASKPASS_PASSWORD_ENV: parsed.password,
    }
    if parsed.username:
        env[_ASKPASS_USER_ENV] = parsed.username
    netloc = parsed.netloc
    at = netloc.rfind("@")
    clean_netloc = netloc[at + 1:] if at != -1 else netloc
    clean_url = urlunparse(parsed._replace(netloc=clean_netloc))
    return env, clean_url


def _branch_exists(repo: PathLike, branch: str) -> bool:
    proc = _run(
        ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet",
         f"refs/heads/{branch}"],
        cwd=repo,
    )
    return proc.returncode == 0


def fix_branch(iid) -> str:
    return f"issue/{iid}-fix"


# --- the daemon's turn-path guarantee layer -----------------------------------

AGENT_REMOTE_NS = "refs/remotes/agent-remote"


def agent_tracking_ref(default_branch: str) -> str:
    """Dedicated tracking ref for the turn baseline (never origin/*)."""
    return f"{AGENT_REMOTE_NS}/{default_branch}"


def fetch_base(main_repo: PathLike, remote_url: str,
               default_branch: str = "master",
               ssl_verify: bool = True) -> None:
    """Refresh the agent-remote baseline ref, IN the main repo.

    Fetch only: `+refs/heads/<default>:refs/remotes/agent-remote/<default>`
    — no checkout, no worktree, no working-copy contact, and the forced
    refspec means the baseline can never diverge. The agent branches
    issue/<iid>-fix off this ref in its OWN worktree; has_new_commits_vs_ref
    compares against it after the turn.
    """
    env, clean_url = _net_env(remote_url)
    _git(main_repo,
         [*_ssl_args(remote_url, ssl_verify), "fetch", clean_url,
          f"+refs/heads/{default_branch}:{agent_tracking_ref(default_branch)}"],
         remote_url=remote_url, env=env)


def prune_worktrees(main_repo: PathLike) -> None:
    """`git worktree prune` in the main repo: clear stale registrations the
    agent may have left by deleting its worktree dir out-of-band. The daemon
    itself creates no worktree; this is cheap startup hygiene only."""
    _git(main_repo, ["worktree", "prune"])


def ensure_fix_branch(main_repo: PathLike, iid, default_branch: str,
                      remote_url: str, ssl_verify: bool = True) -> str:
    """Prepare the issue/<iid>-fix TARGET REF only — never touch the main
    checkout (agora workers branch from this position in their own envs;
    设计 §5.2).

    - remote issue/<iid>-fix exists: fetch its effective position into the
      agent-remote namespace (forced refspec, fetch_base style)
    - remote absent: local branch (when created) starts from the latest
      refs/remotes/agent-remote/<default_branch> — callers run fetch_base
      first, that ref IS the fresh baseline
    - an existing local branch is NEVER reset (a later round may legitimately
      lead the remote position)

    Returns the local branch tip sha.
    """
    branch = fix_branch(iid)
    tracking = f"{AGENT_REMOTE_NS}/{branch}"
    remote_tip = remote_branch_tip(main_repo, remote_url, branch, ssl_verify)
    if remote_tip:
        env, clean_url = _net_env(remote_url)
        _git(main_repo,
             [*_ssl_args(remote_url, ssl_verify), "fetch", clean_url,
              f"+refs/heads/{branch}:{tracking}"],
             remote_url=remote_url, env=env)
    if not _branch_exists(main_repo, branch):
        source = tracking if remote_tip else agent_tracking_ref(default_branch)
        _git(main_repo, ["branch", branch, source])
    return _git(main_repo,
                ["rev-parse", "--verify", f"refs/heads/{branch}"]).strip()


class SweepReport(NamedTuple):
    """Startup sweep outcome: removed = worktrees successfully removed;
    kept = candidates git refused (dirty/untracked — left in place, the
    list is journalable); refused = candidates skipped WITHOUT attempting
    removal because a live process cwd sits inside (occupied)."""

    removed: list
    kept: list
    refused: list


def _cwd_occupied(target: PathLike) -> bool:
    """True when any live process has its cwd at/inside *target*.

    Read-only /proc scan; prefix match with a trailing-slash boundary
    (a cwd of <target>/sub counts as inside). Purely observational —
    the sweep's cwd_probe parameter exists for test injection."""
    target = os.path.normpath(str(target))
    prefix = target + os.sep
    try:
        pids = os.listdir("/proc")
    except OSError:
        return False
    for pid in pids:
        if not pid.isdigit():
            continue
        try:
            cwd = os.readlink(f"/proc/{pid}/cwd")
        except OSError:
            continue
        cwd = os.path.normpath(cwd)
        if cwd == target or cwd.startswith(prefix):
            return True
    return False


def _worktree_entries(main_repo: PathLike):
    """(path, branch-or-None) per registered worktree; None = detached HEAD."""
    out = _git(main_repo, ["worktree", "list", "--porcelain"])
    entries = []
    path = branch = None
    for line in out.splitlines():
        if line.startswith("worktree "):
            path, branch = line[len("worktree "):], None
        elif line.startswith("branch "):
            branch = line[len("branch "):]
        elif not line and path is not None:
            entries.append((path, branch))
            path = branch = None
    if path is not None:
        entries.append((path, branch))
    return entries


def sweep_idle_review_worktrees(main_repo: PathLike,
                                cwd_probe=None) -> SweepReport:
    """Remove idle leftover review worktrees; startup-only, never --force.

    A candidate must be a REGISTERED worktree that is (1) not the main
    repo, (2) a sibling of it (parent(main_repo)), (3) named
    <main_repo>__wt-*, (4) checked out on issue/<iid>-fix (detached HEAD
    is never a candidate), and (5) unoccupied per cwd_probe — probed
    immediately before each removal (TOCTOU guard). Removal is plain
    `git worktree remove`: git's own dirty/untracked check is the dirty
    gate, refusals land in kept. Enumeration failure raises GitopError;
    the daemon caller wraps in try/except like prune_worktrees.
    """
    if cwd_probe is None:
        cwd_probe = _cwd_occupied
    report = SweepReport([], [], [])
    main_path = Path(os.path.abspath(str(main_repo)))
    prefix = f"{main_path.name}__wt-"
    for entry_path, branch in _worktree_entries(main_repo):
        path = Path(entry_path)
        if (path == main_path
                or path.parent != main_path.parent
                or not path.name.startswith(prefix)
                or branch is None
                or not _REVIEW_BRANCH_OK.match(branch)):
            continue
        if cwd_probe(entry_path):
            report.refused.append(entry_path)
            continue
        try:
            _git(main_repo, ["worktree", "remove", entry_path])
        except GitopError:
            report.kept.append(entry_path)
        else:
            report.removed.append(entry_path)
    return report


def tracked_dirty(repo: PathLike) -> bool:
    """True when the repo has tracked local changes (untracked files are
    ignored). Post-turn observability for the main copy: a True result means
    the agent violated the protocol by editing it."""
    return bool(_git(repo, ["status", "--porcelain", "--untracked-files=no"]).strip())


def has_new_commits_vs_ref(repo: PathLike, iid, base_ref: str) -> bool:
    """True when issue/<iid>-fix holds commits beyond *base_ref* (the
    agent-remote ref fetch_base keeps fresh, not a stale local branch)."""
    branch = fix_branch(iid)
    if not _branch_exists(repo, branch):
        return False
    count = _git(repo, ["rev-list", "--count", f"{base_ref}..{branch}"]).strip()
    return int(count) > 0


def knowledge_changes_present(repo: PathLike, iid, base_ref: str) -> bool:
    """True when the fix branch's branch-side changes since the merge-base
    with *base_ref* touch docs/agents/knowledge/ — the post-turn capture
    verification probe (read-only; a GitopError is left to the caller to
    swallow as a journal warning). Three-dot range: knowledge files that
    entered the base side (another issue's ingest merged in the meantime)
    must not count as this branch's capture."""
    branch = fix_branch(iid)
    if not _branch_exists(repo, branch):
        return False
    changed = _git(repo, ["diff", "--name-only", f"{base_ref}...{branch}",
                          "--", "docs/agents/knowledge/"])
    return bool(changed.strip())


def _push_core(repo: PathLike, remote_url: str, refspec: str,
               ssl_verify: bool, force_with_lease: str,
               pin_english: bool = False) -> None:
    """push() 的共享执行核：refspec/lease 白名单校验＋argv 组装＋运行；
    pin_english=True 时钉 LC_ALL=C——失败输出要喂词表分类（ag-cad1d6），
    本地化 git 的译文才不会把分类标记 miss 掉（push_verified_commit
    同款纪律）。legacy push() 不钉，行为与抽取前逐字节一致。"""
    if not isinstance(refspec, str) or not _REFSPEC_OK.match(refspec):
        raise RefspecViolation(
            f"refspec {refspec!r} outside issue/<iid>-fix whitelist"
        )
    env, clean_url = _net_env(remote_url)
    if pin_english:
        env = {"LC_ALL": "C", **(env or {})}
    argv = ["git", "-C", str(repo), *_ssl_args(remote_url, ssl_verify),
            "push"]
    if force_with_lease:
        lease_branch, _, lease_tip = force_with_lease.partition(":")
        if not _REFSPEC_OK.match(lease_branch + ":" + lease_branch) \
                or not re.fullmatch(r"[0-9a-f]{40}", lease_tip or ""):
            raise RefspecViolation(
                _scrub(f"force-with-lease {force_with_lease[:200]!r} "
                       "outside whitelist/sha", remote_url)
            )
        argv.append("--force-with-lease=" + force_with_lease)
    argv += [clean_url, refspec]
    proc = _run(argv, cwd=repo, env=env)
    if proc.returncode != 0:
        raise PushFailed(
            _scrub(
                "push {} failed: {}".format(refspec, (proc.stderr or proc.stdout).strip()),
                remote_url,
            )
        )


def push(repo: PathLike, remote_url: str, refspec: str, ssl_verify: bool = True,
         force_with_lease: str = "") -> None:
    """Push to the explicit URL, restricted to the fix-branch refspec shape.

    Any other refspec (in particular anything targeting the default
    branch) raises RefspecViolation before git is invoked.

    force_with_lease (optional) = "<branch>:<expected-remote-tip-sha>":
    emits --force-with-lease so the update lands only if the remote is
    still at the observed tip (never blind-overwrites a moved branch).
    The branch part must match the whitelisted refspec shape; the tip
    must be a full hex sha.
    """
    _push_core(repo, remote_url, refspec, ssl_verify, force_with_lease)


def push_force_classified(repo: PathLike, remote_url: str, refspec: str,
                          ssl_verify: bool = True,
                          force_with_lease: str = "") -> None:
    """变基轮 force 落地专用（ag-cad1d6 High-1）：与 push() 同形执行＋
    LC_ALL=C 钉英文输出，失败按 _FORCE_REJECT_MARKERS 分类——确定性
    拒绝抛 ForcePushRejected（改写永不可能被接受，调用方轮转或终局，
    绝不无限重试）；传输病/词表未命中形态照旧抛 PushFailed（poll 层
    有界重试）。"""
    try:
        _push_core(repo, remote_url, refspec, ssl_verify, force_with_lease,
                   pin_english=True)
    except PushFailed as exc:
        lowered = str(exc).lower()
        if any(marker in lowered for marker in _FORCE_REJECT_MARKERS):
            raise ForcePushRejected(str(exc)) from exc
        raise


def remote_branch_tip(repo: PathLike, remote_url: str, branch: str,
                      ssl_verify: bool = True) -> str:
    """Current tip of refs/heads/<branch> on the remote ('' when absent).

    ls-remote failure (network/auth) raises GitopError — an empty result
    means the branch is absent, not that the query failed (A-F1/B-F2).
    """
    env, clean_url = _net_env(remote_url)
    ls = _run(["git", "-C", str(repo), *_ssl_args(remote_url, ssl_verify),
               "ls-remote", clean_url, f"refs/heads/{branch}"],
              cwd=repo, env=env)
    if ls.returncode != 0:
        raise GitopError(
            _scrub("ls-remote {} failed: {}".format(
                branch, (ls.stderr or ls.stdout).strip()), remote_url)
        )
    return ls.stdout.split()[0] if ls.stdout.strip() else ""


def local_branch_tip(repo: PathLike, branch: str) -> str:
    """Tip sha of the local refs/heads/<branch> ('' when absent).

    Read-only position probe — never creates the branch. The agora
    align-rewrite guard uses it to prove a force-with-lease push would
    land exactly the verified handoff commit.
    """
    if not _branch_exists(repo, branch):
        return ""
    return _git(repo, ["rev-parse", "--verify",
                       f"refs/heads/{branch}"]).strip()


def fix_branch_pushed(repo: PathLike, iid, remote_url: str,
                      ssl_verify: bool = True) -> bool:
    """True when the local issue/<iid>-fix tip is already on the remote.

    The 「重发汇报」 precondition: the fix work is verifiably landed, so a
    repost never needs to push. A missing local branch raises GitopError
    (rev-parse --verify, existing semantics); a missing remote branch is a
    plain False (never pushed); an ls-remote failure raises — a failed
    query is never misread as "not pushed".
    """
    branch = fix_branch(iid)
    local_tip = _git(repo, ["rev-parse", "--verify",
                            f"refs/heads/{branch}"]).strip()
    remote_tip = remote_branch_tip(repo, remote_url, branch, ssl_verify)
    return bool(remote_tip) and local_tip == remote_tip


def rebase_safe_to_force(repo: PathLike, branch: str, remote_tip: str,
                         local_tip: str) -> tuple:
    """True when rewriting remote <branch> to <local_tip> loses nothing.

    Range-diff orphan check (issue#38): every commit of the pushed chain
    (remote_tip, minus commits already in local via the moved baseline)
    must have a counterpart (= or !) in the rewritten chain. Context drift
    across upstream changes is expected and allowed; a lost commit is not.
    Returns (ok, reason).
    """
    mb = _git(repo, ["merge-base", local_tip, remote_tip]).strip()
    if mb == remote_tip:
        # 远端侧为空 range：远端 tip 是本地 tip 的祖先，强推不丢任何提交
        return True, "remote tip is ancestor of local tip"
    if mb == local_tip:
        # 本地严格落后远端：强推会抹掉远端独有提交（B-F1 空 range 崩溃场景）
        return False, "local is behind remote; forcing would erase remote commits"
    rd = _git(repo, ["range-diff", f"{mb}..{remote_tip}", f"{mb}..{local_tip}"])
    for line in rd.splitlines():
        # 行首锚定（B-F3）：孤儿行结构恒为 `N: <sha> < -: --------`，
        # 配对行内主题引用 range-diff 文本不得误触发
        if re.match(r"\s*\d+:\s*\S+\s*<\s*-+:\s*-", line):
            return False, "old-chain commit lost in rewrite: " + line.strip()[:100]
    return True, "range-diff: no old-chain commit lost"


def push_fix_branch_resilient(repo: PathLike, iid, remote_url: str,
                              ssl_verify: bool = True) -> str:
    """push_fix_branch with a guarded rescue for the rebase blind spot.

    Plain push first. On a non-fast-forward rejection — the signature of a
    later round rebasing issue/<iid>-fix onto a moved baseline (the
    merge-time-rebase flow) — verify the rewrite via rebase_safe_to_force
    against the observed remote tip and retry ONCE with --force-with-lease.
    Anything uncertain re-raises the original PushFailed (blocked + human
    note); a proven-lossy rewrite raises PermanentPushFailed (deterministic,
    same chain same verdict). Returns "pushed" | "force-pushed".
    """
    try:
        push_fix_branch(repo, iid, remote_url, ssl_verify)
        return "pushed"
    except PushFailed as exc:
        if "non-fast-forward" not in str(exc):
            raise
    branch = fix_branch(iid)
    local_tip = _git(repo, ["rev-parse", f"refs/heads/{branch}"]).strip()
    remote_tip = remote_branch_tip(repo, remote_url, branch, ssl_verify)
    if not remote_tip:
        raise PushFailed(
            _scrub("non-fast-forward but remote tip vanished; refusing "
                   "force push", remote_url)
        )
    ok, reason = rebase_safe_to_force(repo, branch, remote_tip, local_tip)
    if not ok:
        raise PermanentPushFailed(
            _scrub(f"rewrite unsafe, refusing force push: {reason}", remote_url)
        )
    push(repo, remote_url, f"{branch}:{branch}", ssl_verify,
         force_with_lease=f"{branch}:{remote_tip}")
    return "force-pushed"


def push_fix_branch(repo: PathLike, iid, remote_url: str,
                    ssl_verify: bool = True) -> None:
    """Push issue/<iid>-fix and nothing else, to the explicit remote URL.

    Safe to call from the main repo even while the branch is checked out
    in the agent's worktree: pushing FROM a repository never touches any
    working copy, and refs are shared across worktrees.
    """
    push(repo, remote_url, f"{fix_branch(iid)}:{fix_branch(iid)}", ssl_verify)


# --- agora external delivery (plan Task 4) --------------------------------------

_HEX40_OK = re.compile(r"[0-9a-f]{40}")

# git push output markers for the three-way outcome classification. The
# transport markers say "the push never got judged" (retry can help); the
# non-ff markers say "the remote branch moved" (rotate the handoff);
# anything else (src refspec matches no object, remote rejected the ref by
# hook/permission) is deterministic — retrying the same commit can never
# succeed, so the caller reports task_deliver(failed). Marker matching is
# why the subprocess below pins LC_ALL=C: a localized git prints localized
# diagnostics and every marker would miss (non_ff misread as "failed" —
# a terminal verdict).
_NON_FF_MARKERS = ("(fetch first)", "non-fast-forward")
_TRANSPORT_MARKERS = ("unable to access", "could not read from remote "
                      "repository", "could not resolve", "connection refused",
                      "connection timed out", "connection reset",
                      "rpc failed", "the remote end hung up",
                      "unexpected disconnect")

# ag-cad1d6（High-1）变基轮 force 落地的确定性拒绝形态：受保护分支禁
# force / pre-receive 钩子拒绝 / 服务器配置性拒绝改写。命中＝同一改写的
# 重试永不可能成功（调用方轮转或终局）；未命中维持 PushFailed 上抛
# （poll 层有界重试）——词表纪律同 _TRANSPORT_MARKERS：LC_ALL=C 下的
# 英文小写子串匹配，miss 只会退回重试（保守面），不会误终局。
_FORCE_REJECT_MARKERS = (
    "protected branch",            # GitHub GH006 / GitLab / Gitea
    "cannot force-push",           # GitHub "Cannot force-push to this
                                   #  protected branch"
    "force push is not allowed",
    "pre-receive hook declined",   # 通用远端钩子拒绝
    "denying non-fast-forward",    # receive.denyNonFastForwards
)


def push_verified_commit(repo: PathLike, iid, commit: str, remote_url: str,
                         ssl_verify: bool = True) -> str:
    """Push the exact verified commit to refs/heads/issue/<iid>-fix on the
    explicit remote (agora external delivery, 设计 §6.2/§7.2).

    The refspec is ALWAYS `<sha>:refs/heads/issue/<iid>-fix` — never force,
    never any other ref, so the default branch cannot be touched by shape.
    *commit* must be a full 40-hex sha — all-zero null OID included in the
    refusal, since in a push refspec it means "delete the remote branch"
    (RefspecViolation otherwise — the same whitelist concept as the legacy
    push gate).

    Returns "ok" | "non_ff" | "failed" (the monitor state machine branches
    on exactly this):
    - ok: the remote branch is now at *commit*
    - non_ff: the remote branch moved — the caller rotates the handoff via
      task_deliver(non_ff, base=<fetched remote tip>)
    - failed: deterministic rejection (missing object, hook/permission
      denial) — the caller reports task_deliver(outcome=failed)
    Transport failures (unreachable remote / connection refused) raise
    PushFailed (scrubbed): transient, the caller keeps awaiting_external
    and retries on its own poll cadence.
    """
    if not isinstance(commit, str) or not _HEX40_OK.fullmatch(commit) \
            or commit == "0" * 40:
        # 全零 OID 是唯一带特殊语义的合法 hex40：push refspec 里它的含义
        # 是「删远端分支」（子进程返回 0）——board 是半可信输入，须拒绝
        raise RefspecViolation(
            _scrub(f"commit {str(commit)[:44]!r} is not a pushable full "
                   f"40-hex sha; refspec {commit}:refs/heads/"
                   f"{fix_branch(iid)} refused", remote_url))
    env, clean_url = _net_env(remote_url)
    refspec = f"{commit}:refs/heads/{fix_branch(iid)}"
    # 分类读 stderr：钉英文输出（LC_ALL 压过继承的 LANG），本地化 git 的
    # 标记翻译才不会把 non_ff/瞬态病误判成确定性 failed 终局
    env = {"LC_ALL": "C", **(env or {})}
    proc = _run(["git", "-C", str(repo), *_ssl_args(remote_url, ssl_verify),
                 "push", clean_url, refspec], cwd=repo, env=env)
    if proc.returncode != 0:
        out = (proc.stderr or proc.stdout).strip()
        lowered = out.lower()
        if any(marker in lowered for marker in _NON_FF_MARKERS):
            return "non_ff"
        message = "push {} failed: {}".format(refspec, out)
        if any(marker in lowered for marker in _TRANSPORT_MARKERS):
            raise PushFailed(_scrub(message, remote_url))
        return "failed"
    return "ok"


def fetch_fix_branch_tip(repo: PathLike, iid, remote_url: str,
                         ssl_verify: bool = True) -> str:
    """Fetch refs/heads/issue/<iid>-fix from the explicit remote URL into
    the agent-remote namespace and return the 40-hex remote tip (the
    non_ff rotation's *base*). Forced refspec, fetch_base style — the
    tracking ref may legitimately rewind across rotations; no checkout, no
    default-branch ref, no working-copy contact. A missing remote ref
    raises GitopError (the fetch fails) — by definition non_ff implies the
    branch exists.
    """
    branch = fix_branch(iid)
    tracking = f"{AGENT_REMOTE_NS}/{branch}"
    env, clean_url = _net_env(remote_url)
    _git(repo,
         [*_ssl_args(remote_url, ssl_verify), "fetch", clean_url,
          f"+refs/heads/{branch}:{tracking}"],
         remote_url=remote_url, env=env)
    return _git(repo, ["rev-parse", "--verify", tracking]).strip()
