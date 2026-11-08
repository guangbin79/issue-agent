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
