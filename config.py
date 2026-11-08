"""Issue-agent configuration: JSON file pointed at by ISSUE_AGENT_CONFIG.

Default location: ~/.config/<snapshot-dir-name>/config.json (0600, written
by install.sh — never committed to the repo). The default DERIVES from the
directory this module is deployed into (dirname(__file__)): install.sh
deploys ~/.local/lib/<service>/ and writes ~/.config/<service>/config.json,
so a daemon started from any snapshot finds its per-instance config with no
env pointer (R3-F1).

Identity (canonical_path, service_name, identity, out_of_scope,
verify_command) is per-instance data — there are NO project constants here;
install.sh copies them from <project>/.issue-agent.json into config.json.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_PATH = (Path.home() / ".config"
                       / Path(__file__).resolve().parent.name / "config.json")
_REQUIRED_FIELDS = ("api_base", "pat", "repo_path", "callback_url",
                    "canonical_path")
_TRUTHY = ("1", "true", "yes", "on")


class ConfigError(RuntimeError):
    """Raised when the config file is missing, unreadable, or incomplete."""


@dataclass(frozen=True)
class Config:
    api_base: str
    pat: str = field(repr=False)  # secret: keep out of logs/repr
    repo_path: str
    callback_url: str
    canonical_path: str = ""  # pinned checkout this instance is bound to
    secret_token: str = field(default="", repr=False)  # webhook token (install.sh writes it)
    port: int = 8644
    ssl_verify: bool = False
    reconcile_interval: int = 600
    opencode_timeout: int = 1800
    project: str = ""  # "owner/repo" override (empty = derive from origin)
    gitea_user: str = ""  # bot account login on the Gitea server
    service_name: str = "issue-agent"  # instance identity (paths, markers)
    identity: str = ""  # one-line project description (PROTOCOL render)
    out_of_scope: tuple = ()  # things the agent must route elsewhere
    verify_command: str = ""  # project gate command (empty = generic wording)
    default_branch: str = "master"  # pinned baseline; install.sh writes it
    worktree_path: str = ""  # DEPRECATED, unused: the agent manages its own
    # worktrees (kept so old config files keep loading)
    review_enabled: bool = True   # task 8: post-push /review-loop for bot fix commits
    review_timeout: int = 3600    # seconds, one review session (TitanNavi gate exceeded 30min twice in one day, journal-verified)
    review_push_grace_s: int = 1800  # gt-9f24: delay before reconcile re-pushes an abnormal-review's late commits
    heartbeat_interval: int = 600   # long-turn liveness note cadence; 0 = off
    sweep_interval: int = 1800      # 欠账巡检 (debt sweep) cadence; 0 = off
    max_parallel: int = 1         # bounded concurrent turns (1 = historical serial)

    @property
    def note_marker(self) -> str:
        """Self-identification marker appended to every bot note.

        Derived from service_name so an instance and its fsm filter stay in
        lockstep with zero configuration: same service name -> same marker
        -> old bot comments keep being recognized across restarts.
        """
        return f"<!-- {self.service_name} -->"

def _env_ssl_verify():
    """ISSUE_AGENT_SSL_VERIFY opt-in/out override; None when unset (F13).

    The shipped default stays ssl_verify=false (self-signed LAN GitLab, a
    user-approved design); the env var is the chain-wide verification
    opt-out for deployments with a real CA.
    """
    raw = os.environ.get("ISSUE_AGENT_SSL_VERIFY", "").strip().lower()
    if not raw:
        return None
    return raw in _TRUTHY


def _bounded_int(raw, field, default, minimum, maximum=None):
    """int(raw) within [minimum, maximum]; ConfigError (not bare ValueError)
    on garbage, so load_config keeps its error contract (F23)."""
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise ConfigError(
            f"config field {field} must be an integer, got {raw!r}") from exc
    if value < minimum or (maximum is not None and value > maximum):
        top = maximum if maximum is not None else "unbounded"
        raise ConfigError(
            f"config field {field} out of range [{minimum}, {top}]: {value}")
    return value


def load_config(path: "str | Path | None" = None) -> Config:
    """Load Config from *path*, else $ISSUE_AGENT_CONFIG, else the default path.

    Raises ConfigError with a human-readable message on any failure.
    """
    resolved = Path(path or os.environ.get("ISSUE_AGENT_CONFIG") or DEFAULT_CONFIG_PATH)
    if not resolved.is_file():
        raise ConfigError(
            f"config file not found: {resolved} "
            f"(set ISSUE_AGENT_CONFIG or create {DEFAULT_CONFIG_PATH})"
        )
    try:
        raw = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"config file unreadable: {resolved}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError(f"config root must be a JSON object: {resolved}")
    missing = [key for key in _REQUIRED_FIELDS if not raw.get(key)]
    if missing:
        raise ConfigError(f"config missing required fields: {', '.join(missing)}")
    ssl_env = _env_ssl_verify()
    cfg = Config(
        api_base=str(raw["api_base"]).rstrip("/"),
        pat=str(raw["pat"]),
        repo_path=str(raw["repo_path"]),
        callback_url=str(raw["callback_url"]),
        canonical_path=str(raw["canonical_path"]),
        secret_token=str(raw.get("secret_token", "")),
        port=_bounded_int(raw.get("port", 8644), "port", 8644, 1, 65535),
        ssl_verify=(ssl_env if ssl_env is not None
                    else bool(raw.get("ssl_verify", False))),
        reconcile_interval=_bounded_int(
            raw.get("reconcile_interval", 600), "reconcile_interval", 600, 1),
        opencode_timeout=_bounded_int(
            raw.get("opencode_timeout", 1800), "opencode_timeout", 1800, 1),
        max_parallel=_bounded_int(
            raw.get("max_parallel", 1), "max_parallel", 1, 1, 4),
        project=str(raw.get("project", "")),
        gitea_user=str(raw.get("gitea_user", "")),
        service_name=str(raw.get("service_name", "") or "issue-agent"),
        identity=str(raw.get("identity", "")),
        out_of_scope=tuple(str(item) for item in raw.get("out_of_scope", ()) or ()),
        verify_command=str(raw.get("verify_command", "") or ""),
        default_branch=str(raw.get("default_branch", "master") or "master"),
        worktree_path=str(raw.get("worktree_path", "") or ""),
        review_enabled=bool(raw.get("review_enabled", True)),
        review_timeout=_bounded_int(
            raw.get("review_timeout", 3600), "review_timeout", 3600, 1),
        review_push_grace_s=_bounded_int(
            raw.get("review_push_grace_s", 1800), "review_push_grace_s", 1800, 1),
        heartbeat_interval=_bounded_int(
            raw.get("heartbeat_interval", 600), "heartbeat_interval", 600, 0),
        sweep_interval=_bounded_int(
            raw.get("sweep_interval", 1800), "sweep_interval", 1800, 0),
    )
    # Canonical-path pin: after all parsing so every pre-existing error
    # contract (missing fields, bad numerics) fires unchanged. Both sides
    # resolve through symlinks so a canonical path reached via a link still
    # pins the real checkout (B6). The expectation comes from the instance's
    # OWN config (install.sh copies it from <project>/.issue-agent.json).
    real_repo = os.path.realpath(cfg.repo_path)
    canonical = os.path.realpath(cfg.canonical_path)
    if real_repo != canonical:
        raise ConfigError(
            f"config repo_path is not the pinned canonical repository: "
            f"expected {cfg.canonical_path}, got {cfg.repo_path} "
            f"(realpath {real_repo})")
    return cfg
