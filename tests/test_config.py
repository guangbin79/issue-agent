"""config.py tests: numeric validation (F23), the ISSUE_AGENT_SSL_VERIFY
env opt-out (F13), the per-instance canonical_path pin, and the
service_name-derived note_marker. The file default stays ssl_verify=false
— the self-signed LAN Gitea design — so these tests pin both the default
AND the override.
"""

import json
import os


import pytest

import config


def write_config(tmp_path, **overrides):
    payload = {
        "api_base": "https://gitea.example.com:8443",
        "pat": "tok-x",
        "repo_path": str(tmp_path),
        "callback_url": "http://192.0.2.10:8644",
        "canonical_path": str(tmp_path),
    }
    payload.update(overrides)
    path = tmp_path / "config.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def no_ssl_env(monkeypatch):
    monkeypatch.delenv("ISSUE_AGENT_SSL_VERIFY", raising=False)
    monkeypatch.delenv("ISSUE_AGENT_CONFIG", raising=False)
    monkeypatch.delenv("AGORA_HOME", raising=False)


def test_happy_path_defaults(tmp_path):
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.port == 8644
    assert cfg.ssl_verify is False, "shipped default for the self-signed LAN Gitea"
    assert cfg.reconcile_interval == 600
    assert cfg.opencode_timeout == 1800
    assert cfg.review_enabled is True, "task 8: post-push review on by default"
    assert cfg.review_timeout == 3600
    assert cfg.project == ""
    assert cfg.gitea_user == ""
    assert cfg.service_name == "issue-agent", "generic default service name"
    assert cfg.default_branch == "master", "generic default baseline branch"
    assert cfg.identity == ""
    assert cfg.out_of_scope == ()


def test_gitea_fields_from_file(tmp_path):
    cfg = config.load_config(write_config(
        tmp_path, project="acme/widget", gitea_user="dev-b"))
    assert cfg.project == "acme/widget"
    assert cfg.gitea_user == "dev-b"


def test_numeric_fields_from_file(tmp_path):
    cfg = config.load_config(
        write_config(tmp_path, port=9000, reconcile_interval=30, opencode_timeout=60))
    assert (cfg.port, cfg.reconcile_interval, cfg.opencode_timeout) == (9000, 30, 60)


def test_review_fields_from_file(tmp_path):
    """task 8: review_enabled/review_timeout load like the other knobs —
    plain bool(raw.get(...)) + bounded int, mirroring opencode_timeout."""
    cfg = config.load_config(write_config(
        tmp_path, review_enabled=False, review_timeout=900))
    assert cfg.review_enabled is False
    assert cfg.review_timeout == 900


def test_review_timeout_default_missing_key_is_3600(tmp_path):
    """review_timeout absent from the config file loads the 3600 default
    via load_config (not the dataclass)."""
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.review_timeout == 3600


@pytest.mark.parametrize("field,bad", [
    ("port", "not-a-number"),
    ("port", 0),
    ("port", 70000),
    ("reconcile_interval", "abc"),
    ("reconcile_interval", 0),      # 0 would busy-loop a reconcile every poll
    ("reconcile_interval", -5),     # negative breaks queue.get(timeout=...)
    ("opencode_timeout", "soon"),
    ("opencode_timeout", 0),
    ("review_timeout", "never"),
    ("review_timeout", 0),
    ("heartbeat_interval", "often"),
    ("heartbeat_interval", -1),
])
def test_bad_numeric_fields_raise_config_error(tmp_path, field, bad):
    with pytest.raises(config.ConfigError) as excinfo:
        config.load_config(write_config(tmp_path, **{field: bad}))
    assert field in str(excinfo.value)


def test_env_ssl_verify_true_overrides_file_false(tmp_path, monkeypatch):
    monkeypatch.setenv("ISSUE_AGENT_SSL_VERIFY", "true")
    assert config.load_config(write_config(tmp_path)).ssl_verify is True


def test_env_ssl_verify_false_beats_file_true(tmp_path, monkeypatch):
    monkeypatch.setenv("ISSUE_AGENT_SSL_VERIFY", "false")
    cfg = write_config(tmp_path, ssl_verify=True)
    assert config.load_config(cfg).ssl_verify is False


@pytest.mark.parametrize("value", ["1", "TRUE", "Yes", "on"])
def test_env_ssl_verify_truthy_forms(tmp_path, monkeypatch, value):
    monkeypatch.setenv("ISSUE_AGENT_SSL_VERIFY", value)
    assert config.load_config(write_config(tmp_path)).ssl_verify is True


def test_env_ssl_verify_unset_keeps_file_value(tmp_path):
    cfg = write_config(tmp_path, ssl_verify=True)
    assert config.load_config(cfg).ssl_verify is True


def test_default_branch_default_and_from_file(tmp_path):
    assert config.load_config(
        write_config(tmp_path)).default_branch == "master"
    cfg = config.load_config(write_config(tmp_path, default_branch="main"))
    assert cfg.default_branch == "main"


def test_worktree_path_kept_but_deprecated(tmp_path):
    """worktree_path stays loadable (old config files) but is unused now —
    the agent manages its own worktrees; install writes ""."""
    assert config.load_config(write_config(tmp_path)).worktree_path == ""
    cfg = config.load_config(write_config(tmp_path, worktree_path="/repo_agent"))
    assert cfg.worktree_path == "/repo_agent"


def test_note_marker_derived_from_service_name():
    """The marker is a pure function of service_name: same service -> same
    marker -> old bot comments stay recognized across restarts (T5 note
    compatibility lock)."""
    cfg = config.Config(api_base="http://g", pat="p", repo_path="/test",
                        callback_url="http://c", canonical_path="/test",
                        service_name="test-svc")
    assert cfg.note_marker == "<!-- test-svc -->"
    default = config.Config(api_base="http://g", pat="p", repo_path="/test",
                            callback_url="http://c", canonical_path="/test")
    assert default.note_marker == "<!-- issue-agent -->"


def test_note_marker_from_loaded_service_name(tmp_path):
    cfg = config.load_config(write_config(tmp_path, service_name="proj-issue-agent"))
    assert cfg.service_name == "proj-issue-agent"
    assert cfg.note_marker == "<!-- proj-issue-agent -->"


def test_identity_fields_roundtrip(tmp_path):
    cfg = config.load_config(write_config(
        tmp_path, identity="C++/Qt 应用平台", out_of_scope=["兄弟仓", "运维"],
        verify_command="bash run-gates.sh"))
    assert cfg.identity == "C++/Qt 应用平台"
    assert cfg.out_of_scope == ("兄弟仓", "运维")
    assert cfg.verify_command == "bash run-gates.sh"


def test_config_missing_canonical_path_raises(tmp_path):
    """canonical_path is required: a config without the pin would silently
    accept any repo_path."""
    path = write_config(tmp_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    del payload["canonical_path"]
    path.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(config.ConfigError) as excinfo:
        config.load_config(path)
    assert "canonical_path" in str(excinfo.value)


def test_repo_path_outside_canonical_rejected(tmp_path):
    """Layer A: a config pointing anywhere but the pinned canonical repo
    is rejected; the message must name both the expected and actual paths."""
    with pytest.raises(config.ConfigError) as excinfo:
        config.load_config(write_config(tmp_path, repo_path="/tmp/noncanonical"))
    assert str(tmp_path) in str(excinfo.value)
    assert "/tmp/noncanonical" in str(excinfo.value)


def test_repo_path_canonical_via_json_field(tmp_path):
    """The pin follows the config's canonical_path: point it at a tmp
    realpath and that exact path loads fine."""
    cfg = config.load_config(write_config(
        tmp_path, canonical_path=os.path.realpath(tmp_path),
        repo_path=str(tmp_path)))
    assert cfg.repo_path == str(tmp_path)


def test_repo_path_symlink_resolves_to_canonical(tmp_path):
    """A symlink pointing at the canonical dir resolves via realpath and
    loads fine (equality is on realpath, not the literal string)."""
    link = tmp_path / "repo-link"
    link.symlink_to(tmp_path, target_is_directory=True)
    cfg = config.load_config(write_config(
        tmp_path, canonical_path=os.path.realpath(tmp_path), repo_path=str(link)))
    assert cfg.repo_path == str(link)


def test_repo_path_canonical_constant_behind_symlink(tmp_path):
    """B6/ocr#13: the pin resolves BOTH sides — a canonical path that
    itself sits behind a symlink still matches the real checkout."""
    link = tmp_path / "canonical-link"
    link.symlink_to(tmp_path, target_is_directory=True)
    cfg = config.load_config(write_config(
        tmp_path, canonical_path=str(link), repo_path=str(tmp_path)))
    assert cfg.repo_path == str(tmp_path)


# --- max_parallel: bounded concurrent turns -----------------------------------


def test_max_parallel_default_is_serial(tmp_path):
    """Default 1 == today's serial behavior: existing deployments keep their
    semantics until they explicitly opt in."""
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.max_parallel == 1


def test_max_parallel_valid_value_parses(tmp_path):
    cfg = config.load_config(write_config(tmp_path, max_parallel=2))
    assert cfg.max_parallel == 2


@pytest.mark.parametrize("bad", [0, 5, "two"])
def test_max_parallel_out_of_domain_rejected(tmp_path, bad):
    with pytest.raises(config.ConfigError):
        config.load_config(write_config(tmp_path, max_parallel=bad))


def test_heartbeat_interval_default_600_and_parses_from_file(tmp_path):
    """heartbeat: long-turn liveness note interval; 0 is the explicit opt-out
    (unlike reconcile_interval, a busy-loop is harmless here — off is useful)."""
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.heartbeat_interval == 600
    cfg = config.load_config(write_config(tmp_path, heartbeat_interval=90))
    assert cfg.heartbeat_interval == 90


def test_heartbeat_interval_zero_opt_out_accepted(tmp_path):
    cfg = config.load_config(write_config(tmp_path, heartbeat_interval=0))
    assert cfg.heartbeat_interval == 0


# --- agora execution mode (plan Task 1) --------------------------------------


def agora_config(tmp_path, **overrides):
    """A loadable execution_mode=agora config: every agora-required field
    set, overridable per-branch under test."""
    payload = {
        "execution_mode": "agora",
        "verify_command": "python3 -m pytest tests/ -q",
        "router_model": "zai-coding-plan/glm-5.3",
        "agora_home": str(tmp_path / "plaza"),
    }
    payload.update(overrides)
    return write_config(tmp_path, **payload)


def test_agora_defaults_are_legacy_and_inert(tmp_path):
    """缺省全 inert：legacy 模式、空 router/agora 字段、15s 轮询——现有
    部署不写任何 agora 键也照常加载（零 agora 依赖）。"""
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.execution_mode == "legacy"
    assert cfg.router_model == ""
    assert cfg.agora_home == ""
    assert cfg.agora_poll_interval == 15


def test_agora_mode_requires_trusted_verify_and_router_model(tmp_path):
    """计划锚例：agora 模式强校验，ConfigError 点名缺失字段。"""
    with pytest.raises(config.ConfigError, match="verify_command"):
        config.load_config(agora_config(tmp_path, verify_command=""))
    with pytest.raises(config.ConfigError, match="router_model"):
        config.load_config(agora_config(tmp_path, router_model=""))
    with pytest.raises(config.ConfigError, match="agora_home"):
        config.load_config(agora_config(tmp_path, agora_home=""))


def test_agora_mode_full_config_loads(tmp_path):
    cfg = config.load_config(agora_config(tmp_path, agora_poll_interval=5))
    assert cfg.execution_mode == "agora"
    assert cfg.router_model == "zai-coding-plan/glm-5.3"
    assert cfg.agora_home == str(tmp_path / "plaza")
    assert cfg.agora_poll_interval == 5


def test_agora_home_falls_back_to_env_then_file_wins(tmp_path, monkeypatch):
    """缺省回落 $AGORA_HOME 再 ""；文件值非空时压过环境变量。"""
    monkeypatch.setenv("AGORA_HOME", "/tmp/opencode/plaza-env")
    cfg = config.load_config(agora_config(tmp_path, agora_home=""))
    assert cfg.agora_home == "/tmp/opencode/plaza-env"
    cfg = config.load_config(
        agora_config(tmp_path, agora_home="/tmp/opencode/plaza-file"))
    assert cfg.agora_home == "/tmp/opencode/plaza-file"
    # 环境也无 → 字段落 ""（legacy 容忍 ""；agora 模式的非空由强校验管）
    monkeypatch.delenv("AGORA_HOME", raising=False)
    cfg = config.load_config(write_config(tmp_path))
    assert cfg.agora_home == ""


def test_execution_mode_bad_value_rejected_naming_field(tmp_path):
    with pytest.raises(config.ConfigError) as excinfo:
        config.load_config(write_config(tmp_path, execution_mode="turbo"))
    assert "execution_mode" in str(excinfo.value)


def test_legacy_tolerates_empty_agora_fields(tmp_path):
    """legacy 对四字段全容忍：空 verify/router/agora_home 照常加载。"""
    cfg = config.load_config(write_config(
        tmp_path, verify_command="", router_model="", agora_home="",
        agora_poll_interval=15))
    assert cfg.execution_mode == "legacy"


@pytest.mark.parametrize("bad", [0, -3, "often"])
def test_agora_poll_interval_must_be_at_least_one(tmp_path, bad):
    with pytest.raises(config.ConfigError) as excinfo:
        config.load_config(agora_config(tmp_path, agora_poll_interval=bad))
    assert "agora_poll_interval" in str(excinfo.value)
