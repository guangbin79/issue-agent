"""GitLab-issue-driven opencode agent daemon — the wiring entrypoint.

config -> GitLabClient + WebhookListener + Orchestrator (listener + worker +
reconcile timer). Runs until SIGTERM/KeyboardInterrupt, then shuts the
orchestrator down. stdout is the journal under systemd (Restart=on-failure).

http.py shadows the stdlib http package when this directory is first on
sys.path, so it is loaded under its importlib alias (the supported pattern,
see tests/test_listener.py) before anything imports urllib.
"""

import importlib.util
import json
import os
import shutil
import signal
import subprocess
import sys
import threading
from pathlib import Path
from urllib.parse import urlparse

from api import GitLabClient
from agora_client import AgoraClient
from config import ConfigError, load_config
from router import SemanticRouter
import gitop
import orchestrator

_PKG_DIR = Path(__file__).resolve().parent
# Instance identity: the systemd unit passes these (install.sh writes them);
# defaults keep a generic single-install behavior (per-instance identity
# lives in config.json — service_name/identity/out_of_scope fields).
_SERVICE_NAME = os.environ.get("ISSUE_AGENT_SERVICE_NAME", "issue-agent")
_STATE_DIR = Path.home() / ".local" / "state" / _SERVICE_NAME
_SNAPSHOT_DIR = Path.home() / ".local" / "lib" / _SERVICE_NAME
_DEFAULT_PORT = int(os.environ.get("ISSUE_AGENT_PORT", "8644"))


def _teardown_self_cancel():
    """D6 self-cancel cleanup: stop the service, drop the user unit, reload
    systemd, and remove the snapshot ONLY when the daemon actually runs from
    it (a dev checkout must survive). Config and the state ledger stay."""
    subprocess.run(["systemctl", "--user", "disable", "--now",
                    _SERVICE_NAME], check=False)
    unit = Path("~/.config/systemd/user").expanduser() / f"{_SERVICE_NAME}.service"
    unit.unlink(missing_ok=True)
    subprocess.run(["systemctl", "--user", "daemon-reload"], check=False)
    snap = Path(__file__).resolve().parent
    if snap == _SNAPSHOT_DIR:
        shutil.rmtree(snap, ignore_errors=True)


def _load_http_module():
    spec = importlib.util.spec_from_file_location("issue_agent_http", _PKG_DIR / "http.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _git_default_branch(repo_path: str, fallback: str) -> str:
    """origin/HEAD of *repo_path* ("origin/<branch>" -> "<branch>"), else
    *fallback* (the configured baseline keeps the protocol coherent when
    origin/HEAD is unset)."""
    proc = subprocess.run(
        ["git", "-C", repo_path, "symbolic-ref", "--short",
         "refs/remotes/origin/HEAD"],
        capture_output=True, text=True)
    ref = proc.stdout.strip() if proc.returncode == 0 else ""
    if ref.startswith("origin/"):
        return ref[len("origin/"):]
    return fallback


def _render_protocol(pkg_dir: Path, config) -> Path:
    """Render PROTOCOL.template.md with the instance's config fields into
    PROTOCOL.rendered.md inside the snapshot dir, and return its path.

    The orchestrator/runner keep receiving a plain protocol_path (zero
    interface change); the rendered file is redeployed with every install
    (install.sh copies the template into the snapshot) and removed with
    the snapshot at uninstall. Loading point is deliberately HERE, not in
    orchestrator/runner (R2-F2).
    """
    template = (pkg_dir / "PROTOCOL.template.md").read_text(encoding="utf-8")
    rendered = (template
                .replace("{{IDENTITY}}",
                         config.identity or "本仓库")
                .replace("{{OUT_OF_SCOPE}}",
                         "、".join(config.out_of_scope) or "其它产品线/兄弟仓的问题")
                .replace("{{BASELINE_BRANCH}}", config.default_branch)
                .replace("{{GIT_DEFAULT_BRANCH}}",
                         _git_default_branch(config.repo_path,
                                             config.default_branch)))
    dest = pkg_dir / "PROTOCOL.rendered.md"
    dest.write_text(rendered, encoding="utf-8")
    return dest


def _project_from_origin(repo_path: str) -> str:
    """GitLab project path (group/repo) from the repo's origin remote."""
    proc = subprocess.run(
        ["git", "-C", repo_path, "remote", "get-url", "origin"],
        capture_output=True, text=True)
    if proc.returncode != 0 or not proc.stdout.strip():
        raise ConfigError(f"cannot read origin remote of {repo_path}: "
                          f"{(proc.stderr or '').strip()}")
    url = proc.stdout.strip()
    tail = url.split("://", 1)[-1]
    path = tail.split(":", 1)[-1] if "://" not in url else urlparse(url).path
    path = path.strip("/")
    if path.endswith(".git"):
        path = path[:-4]
    if not path:
        raise ConfigError(f"cannot derive a project path from origin url: {url}")
    return path


def _remote_url(api_base: str, pat: str, project: str,
               gitea_user: str = "") -> str:
    """Gitea basic-auth https URL for fetch/push:
    https://{gitea_user| x-access-token}:{PAT}@host/{owner}/{repo}.git —
    the username is NEVER the bare PAT (a passwordless credential URL
    would ride the git argv; gitop refuses that form); gitop scrubs it
    from errors."""
    parsed = urlparse(api_base)
    base_path = parsed.path.rstrip("/")
    user = f"{gitea_user or 'x-access-token'}:{pat}"
    return f"{parsed.scheme}://{user}@{parsed.netloc}{base_path}/{project}.git"


def main(argv=None) -> int:
    try:
        config = load_config(
            os.environ.get("ISSUE_AGENT_CONFIG")
            or Path.home() / ".config" / _SERVICE_NAME / "config.json")
        if not config.secret_token:
            raise ConfigError("config missing secret_token (written by install.sh)")
        # config.project ("owner/repo") wins when set; empty = derive from origin
        project = config.project or _project_from_origin(config.repo_path)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1

    journal = lambda message: print(message, flush=True)  # noqa: E731 (systemd steers stdout)
    _STATE_DIR.mkdir(parents=True, exist_ok=True)
    protocol_path = _render_protocol(_PKG_DIR, config)

    # startup hygiene: the agent removes its own worktrees, but a dir deleted
    # out-of-band leaves a stale registration — prune cheaply, best-effort
    # (the daemon itself creates no worktree)
    try:
        gitop.prune_worktrees(config.repo_path)
        journal("startup: stale worktree registrations pruned")
    except gitop.GitopError as exc:
        journal(f"warning: worktree prune failed (continuing): {exc}")

    # same best-effort hygiene as prune: sweep leftover review worktrees
    # BEFORE the listener opens (no turn can start mid-sweep); dirs git kept
    # (dirty/busy) are journaled for manual follow-up
    try:
        report = gitop.sweep_idle_review_worktrees(config.repo_path)
        journal(f"startup: swept {len(report.removed)} idle review worktree(s), kept {len(report.kept) + len(report.refused)} (dirty/busy)")
        for path in report.kept + report.refused:
            journal(f"startup: worktree kept (dirty/busy): {path}")
    except gitop.GitopError as exc:
        journal(f"warning: worktree sweep failed (continuing): {exc}")

    client = GitLabClient(config.api_base, config.pat, config.ssl_verify,
                           warn=journal, note_marker=config.note_marker)
    # agora 模式才构造服务客户端（构造惰性：不触网不读盘）；legacy 传 None
    # 且不读 agora home——现有部署零 agora 依赖（计划 Task 1）。
    agora_client = (AgoraClient(service_name=config.service_name,
                                home=config.agora_home,
                                poll_interval=config.agora_poll_interval)
                    if config.execution_mode == "agora" else None)
    # 持久语义 Router（计划 Task 2）：agora 模式才构造（构造只读模板与
    # 账本，零网络；session 建立在首次 decide）；orchestrator 消费归
    # Task 3，此处仅惰性接线。legacy 恒 None、不读模板。
    semantic_router = (SemanticRouter(
        template_path=_PKG_DIR / "ROUTER.template.md",
        cwd=config.repo_path,
        router_model=config.router_model,
        state_path=_STATE_DIR / "state.json",
        timeout_s=config.opencode_timeout)
        if config.execution_mode == "agora" else None)
    http_mod = _load_http_module()
    # Port: the unit's ISSUE_AGENT_PORT env wins; config.port otherwise
    # (pre-unit installs and manual config edits keep working unchanged).
    port = _DEFAULT_PORT if os.environ.get("ISSUE_AGENT_PORT") else config.port
    listener = http_mod.WebhookListener(
        port, config.secret_token, warn=journal, host="0.0.0.0")
    orch = orchestrator.Orchestrator(
        project=project,
        api=client,
        journal=journal,
        listener=listener,
        state_path=_STATE_DIR / "state.json",
        repo_path=config.repo_path,
        remote_url=_remote_url(config.api_base, config.pat, project, config.gitea_user),
        protocol_path=protocol_path,
        ssl_verify=config.ssl_verify,
        default_branch=config.default_branch,
        note_marker=config.note_marker,
        service_name=config.service_name,
        verify_command=config.verify_command,
        reconcile_interval=config.reconcile_interval,
        opencode_timeout=config.opencode_timeout,
        review_enabled=config.review_enabled,
        review_timeout=config.review_timeout,
        review_push_grace_s=config.review_push_grace_s,
        heartbeat_interval=config.heartbeat_interval,
        sweep_interval=config.sweep_interval,
        max_parallel=config.max_parallel,
        callback_url=config.callback_url,
        worktree_path=config.worktree_path,
        agora_client=agora_client,
        router=semantic_router,
        teardown=_teardown_self_cancel,
    )
    port = orch.start()
    journal(f"daemon: ready on :{port} (project={project})")
    # F16: systemd stop sends SIGTERM; route it to the same shutdown path
    # as KeyboardInterrupt so listener + worker unwind gracefully.
    stop_event = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    try:
        stop_event.wait()  # serve until SIGTERM/KeyboardInterrupt
    except KeyboardInterrupt:
        pass
    finally:
        orch.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
