"""daemon._remote_url tests: Gitea basic-auth URL construction
(https://{gitea_user|x-access-token}:{PAT}@host/... — the username is
never the bare PAT — and api_base subpath preservation).

Run: python3 -m pytest tests/test_daemon.py -q   (from scripts/issue-agent/)
"""

import daemon
from daemon import _remote_url


def test_remote_url_with_user():
    assert _remote_url(
        "http://192.168.1.100:3000", "TOKEN123", "acme/widget",
        "baiguangbin") == "http://baiguangbin:TOKEN123@192.168.1.100:3000/acme/widget.git"


def test_remote_url_without_user_falls_back_to_x_access_token():
    assert _remote_url(
        "http://192.168.1.100:3000", "TOKEN123", "acme/widget",
        "") == "http://x-access-token:TOKEN123@192.168.1.100:3000/acme/widget.git"


def test_remote_url_keeps_base_path():
    assert _remote_url(
        "https://git.example.com/gitea", "TOKEN123", "acme/widget",
        "baiguangbin") == "https://baiguangbin:TOKEN123@git.example.com/gitea/acme/widget.git"


def test_daemon_passes_default_branch_to_orchestrator(tmp_path, monkeypatch):
    """daemon.main threads config.default_branch into the Orchestrator."""
    import signal
    import threading
    import types

    import orchestrator as orch_mod

    from config import Config

    captured = {}
    main_ident = threading.get_ident()

    class FakeOrch:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            # End main()'s stop_event.wait() the systemd way (SIGTERM), aimed
            # at the MAIN thread specifically: a process-directed signal may
            # be absorbed by any thread and never wake the waiting one.
            threading.Timer(
                0.05, signal.pthread_kill,
                args=(main_ident, signal.SIGTERM)).start()
            return 8644

        def stop(self):
            pass

    cfg = Config(api_base="http://gitea", pat="t",
                 repo_path=str(tmp_path), callback_url="http://cb",
                 secret_token="s", project="grp/repo", default_branch="main")
    monkeypatch.setattr(daemon, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(daemon, "GitLabClient",
                        lambda *a, **k: types.SimpleNamespace())
    monkeypatch.setattr(orch_mod, "Orchestrator", FakeOrch)
    monkeypatch.setattr(daemon, "_STATE_DIR", tmp_path / "state")

    assert daemon.main() == 0
    assert captured["default_branch"] == "main"
    assert captured["worktree_path"] == "", "legacy default: no worktree"
    assert captured["review_enabled"] is True, "task 8: review config threaded"
    assert captured["review_timeout"] == 3600


def test_daemon_passes_worktree_path_to_orchestrator(tmp_path, monkeypatch):
    """daemon.main threads config.worktree_path into the Orchestrator."""
    import signal
    import threading
    import types

    import orchestrator as orch_mod
    from config import Config

    captured = {}
    main_ident = threading.get_ident()

    class FakeOrch:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            threading.Timer(
                0.05, signal.pthread_kill,
                args=(main_ident, signal.SIGTERM)).start()
            return 8644

        def stop(self):
            pass

    cfg = Config(api_base="http://gitea", pat="t",
                 repo_path=str(tmp_path), callback_url="http://cb",
                 secret_token="s", project="grp/repo",
                 worktree_path="/repo_agent")
    monkeypatch.setattr(daemon, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(daemon, "GitLabClient",
                        lambda *a, **k: types.SimpleNamespace())
    monkeypatch.setattr(orch_mod, "Orchestrator", FakeOrch)
    monkeypatch.setattr(daemon, "_STATE_DIR", tmp_path / "state")

    assert daemon.main() == 0
    assert captured["worktree_path"] == "/repo_agent"  # passed through; the orchestrator ignores it


def _prune_scaffold(tmp_path, monkeypatch, prune_impl, sweep_impl=None, events=None):
    """daemon.main with a fake orchestrator + injected prune/sweep seams."""
    import signal
    import threading
    import types

    import gitop as gitop_mod
    import orchestrator as orch_mod
    from config import Config

    captured = {}
    if events is None:
        events = []
    main_ident = threading.get_ident()

    class FakeOrch:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            events.append("start")
            threading.Timer(
                0.05, signal.pthread_kill,
                args=(main_ident, signal.SIGTERM)).start()
            return 8644

        def stop(self):
            pass

    cfg = Config(api_base="http://gitea", pat="t",
                 repo_path=str(tmp_path), callback_url="http://cb",
                 secret_token="s", project="grp/repo")
    monkeypatch.setattr(daemon, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(daemon, "GitLabClient",
                        lambda *a, **k: types.SimpleNamespace())
    monkeypatch.setattr(orch_mod, "Orchestrator", FakeOrch)
    monkeypatch.setattr(daemon, "_STATE_DIR", tmp_path / "state")
    monkeypatch.setattr(gitop_mod, "prune_worktrees", prune_impl)
    if sweep_impl is None:
        def noop_sweep(repo):
            return gitop_mod.SweepReport([], [], [])
        sweep_impl = noop_sweep
    monkeypatch.setattr(gitop_mod, "sweep_idle_review_worktrees", sweep_impl)
    return captured, events


def test_daemon_prunes_worktrees_at_startup(tmp_path, monkeypatch):
    """daemon.main runs `git worktree prune` on the main repo once at startup
    (cheap stale-registration cleanup; the daemon itself creates no worktree)."""
    pruned = []
    _prune_scaffold(tmp_path, monkeypatch, lambda repo: pruned.append(repo))
    assert daemon.main() == 0
    assert pruned == [str(tmp_path)]


def test_daemon_startup_survives_prune_failure(tmp_path, monkeypatch, capsys):
    """A prune failure (e.g. repo gone) only journals; startup continues."""
    import gitop as gitop_mod

    def broken_prune(repo):
        raise gitop_mod.GitopError("not a git repository (fake)")

    _prune_scaffold(tmp_path, monkeypatch, broken_prune)
    assert daemon.main() == 0
    assert "worktree prune failed" in capsys.readouterr().out


def test_daemon_sweeps_worktrees_at_startup(tmp_path, monkeypatch, capsys):
    """daemon.main sweeps idle review worktrees once at startup, BEFORE the
    orchestrator's listener opens (no turn can start mid-sweep)."""
    from gitop import SweepReport

    events = []
    sweep_calls = []

    def fake_sweep(repo):
        sweep_calls.append(repo)
        events.append("sweep")
        return SweepReport(removed=["/x/kept_a"], kept=["/x/kept_b"], refused=[])

    _prune_scaffold(tmp_path, monkeypatch, lambda repo: None,
                    sweep_impl=fake_sweep, events=events)
    assert daemon.main() == 0
    assert sweep_calls == [str(tmp_path)]
    assert events == ["sweep", "start"], "sweep must precede listener start"
    out = capsys.readouterr().out
    assert "startup: swept 1 idle review worktree(s), kept 1 (dirty/busy)" in out
    assert "startup: worktree kept (dirty/busy): /x/kept_b" in out


def test_daemon_sweep_failure_continues(tmp_path, monkeypatch, capsys):
    """A sweep failure (e.g. not a git repo) only journals; startup continues."""
    import gitop as gitop_mod

    def broken_sweep(repo):
        raise gitop_mod.GitopError("boom")

    _prune_scaffold(tmp_path, monkeypatch, lambda repo: None, sweep_impl=broken_sweep)
    assert daemon.main() == 0
    assert "warning: worktree sweep failed (continuing): boom" in capsys.readouterr().out


# --- agora wiring (plan Task 1) ---------------------------------------------


def _wiring_scaffold(tmp_path, monkeypatch, cfg):
    """daemon.main with a fake orchestrator capturing its kwargs."""
    import signal
    import threading
    import types

    import orchestrator as orch_mod

    captured = {}
    main_ident = threading.get_ident()

    class FakeOrch:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def start(self):
            threading.Timer(
                0.05, signal.pthread_kill,
                args=(main_ident, signal.SIGTERM)).start()
            return 8644

        def stop(self):
            pass

    monkeypatch.setattr(daemon, "load_config", lambda *a, **k: cfg)
    monkeypatch.setattr(daemon, "GitLabClient",
                        lambda *a, **k: types.SimpleNamespace())
    monkeypatch.setattr(orch_mod, "Orchestrator", FakeOrch)
    monkeypatch.setattr(daemon, "_STATE_DIR", tmp_path / "state")
    return captured


def _agora_cfg(tmp_path, execution_mode="agora"):
    from config import Config
    return Config(api_base="http://gitea", pat="t",
                  repo_path=str(tmp_path), callback_url="http://cb",
                  secret_token="s", project="grp/repo",
                  execution_mode=execution_mode,
                  verify_command="python3 -m pytest tests/ -q",
                  router_model="zai-coding-plan/glm-5.3",
                  agora_home=str(tmp_path / "plaza"))


def test_daemon_agora_mode_builds_client_and_threads_it(tmp_path, monkeypatch):
    """agora 模式：daemon 构造 AgoraClient（惰性）并以 agora_client= 注入。"""
    built = []

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            built.append(self)

    captured = _wiring_scaffold(tmp_path, monkeypatch, _agora_cfg(tmp_path))
    monkeypatch.setattr(daemon, "AgoraClient", FakeClient)
    assert daemon.main() == 0
    assert len(built) == 1
    assert built[0].kwargs == {"service_name": "issue-agent",
                               "home": str(tmp_path / "plaza"),
                               "poll_interval": 15}
    assert captured["agora_client"] is built[0]


def test_daemon_legacy_mode_passes_none_and_never_builds_client(
        tmp_path, monkeypatch):
    """legacy 传 None 且不构造客户端（＝不读 agora home），零 agora 依赖。"""
    built = []

    class FakeClient:
        def __init__(self, **kwargs):
            built.append(self)

    captured = _wiring_scaffold(
        tmp_path, monkeypatch, _agora_cfg(tmp_path, execution_mode="legacy"))
    monkeypatch.setattr(daemon, "AgoraClient", FakeClient)

    def no_router(**kwargs):
        raise AssertionError("legacy must not build a router")

    monkeypatch.setattr(daemon, "SemanticRouter", no_router)
    assert daemon.main() == 0
    assert built == []
    assert captured["agora_client"] is None
    assert captured["router"] is None


# --- agora router wiring (plan Task 2) ----------------------------------------


def test_daemon_agora_mode_builds_router_and_threads_it(tmp_path, monkeypatch):
    """agora 模式：daemon 构造 SemanticRouter（模板/自身模型/账本根路径）
    并以 router= 注入 orchestrator（消费归 Task 3，此处仅惰性接线）。"""
    built = []

    class FakeRouter:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            built.append(self)

    captured = _wiring_scaffold(tmp_path, monkeypatch, _agora_cfg(tmp_path))
    monkeypatch.setattr(daemon, "SemanticRouter", FakeRouter)
    assert daemon.main() == 0
    assert len(built) == 1
    assert built[0].kwargs == {
        "template_path": daemon._PKG_DIR / "ROUTER.template.md",
        "cwd": str(tmp_path),
        "router_model": "zai-coding-plan/glm-5.3",
        "state_path": tmp_path / "state" / "state.json",
        "timeout_s": 1800,
    }
    assert captured["router"] is built[0]


def test_daemon_router_reads_the_same_ledger_as_orchestrator(
        tmp_path, monkeypatch):
    """Router 的 session id 持久化与 orchestrator 共用同一份账本文件。"""
    import types

    captured = _wiring_scaffold(tmp_path, monkeypatch, _agora_cfg(tmp_path))
    monkeypatch.setattr(daemon, "SemanticRouter",
                        lambda **kwargs: types.SimpleNamespace(kwargs=kwargs))
    assert daemon.main() == 0
    assert captured["router"].kwargs["state_path"] == captured["state_path"]
