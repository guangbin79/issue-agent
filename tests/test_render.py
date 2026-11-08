"""daemon._render_protocol tests: the daemon (and ONLY the daemon) loads
PROTOCOL.template.md at startup, substitutes the instance's config fields,
and writes PROTOCOL.rendered.md into the snapshot dir — the orchestrator
and runner keep receiving a plain path (zero interface change).
"""
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import daemon  # noqa: E402
from config import Config  # noqa: E402


def make_config(tmp_path, **overrides):
    base = dict(
        api_base="http://gitea", pat="p", repo_path=str(tmp_path),
        callback_url="http://cb", canonical_path=str(tmp_path),
        service_name="proj-issue-agent", identity="C++/Qt 应用平台",
        out_of_scope=("兄弟仓 Alpha", "运维"), verify_command="pytest -q",
        default_branch="release-1x")
    base.update(overrides)
    return Config(**base)


def git_repo(tmp_path):
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    return tmp_path


def test_render_substitutes_all_placeholders(tmp_path):
    git_repo(tmp_path)
    config = make_config(tmp_path)
    rendered = daemon._render_protocol(daemon._PKG_DIR, config)
    assert rendered.name == "PROTOCOL.rendered.md"
    assert rendered.parent == daemon._PKG_DIR
    text = rendered.read_text(encoding="utf-8")
    assert "{{" not in text and "}}" not in text, "unsubstituted placeholder left"
    assert "C++/Qt 应用平台" in text
    assert "兄弟仓 Alpha" in text and "运维" in text
    assert "refs/remotes/agent-remote/release-1x" in text
    rendered.unlink()


def test_render_git_default_branch_follows_origin_head(tmp_path):
    repo = git_repo(tmp_path)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "--allow-empty",
                    "-m", "init"], check=True,
                   env={"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
                        "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t",
                        "PATH": "/usr/bin:/bin"})
    subprocess.run(["git", "-C", str(repo), "branch", "-M", "trunk"], check=True)
    subprocess.run(["git", "-C", str(repo), "update-ref",
                    "refs/remotes/origin/trunk", "HEAD"], check=True)
    subprocess.run(["git", "-C", str(repo), "symbolic-ref",
                    "refs/remotes/origin/HEAD", "refs/remotes/origin/trunk"],
                   check=True)
    config = make_config(tmp_path)
    text = daemon._render_protocol(daemon._PKG_DIR, config).read_text(
        encoding="utf-8")
    assert "`trunk`" in text, "git default branch derived from origin/HEAD"
    assert "refs/remotes/agent-remote/release-1x" in text, "baseline stays config-pinned"
    Path(daemon._PKG_DIR / "PROTOCOL.rendered.md").unlink()


def test_render_git_default_branch_falls_back_to_baseline(tmp_path):
    git_repo(tmp_path)  # no origin/HEAD at all
    config = make_config(tmp_path)
    text = daemon._render_protocol(daemon._PKG_DIR, config).read_text(
        encoding="utf-8")
    assert "`release-1x`" in text, "fallback = configured baseline branch"
    Path(daemon._PKG_DIR / "PROTOCOL.rendered.md").unlink()


def test_render_empty_identity_uses_generic_wording(tmp_path):
    git_repo(tmp_path)
    config = make_config(tmp_path, identity="", out_of_scope=())
    text = daemon._render_protocol(daemon._PKG_DIR, config).read_text(
        encoding="utf-8")
    assert "本仓库" in text
    assert "其它产品线/兄弟仓的问题" in text
    assert "{{" not in text
    Path(daemon._PKG_DIR / "PROTOCOL.rendered.md").unlink()


def test_git_default_branch_helper_shapes():
    assert daemon._git_default_branch("/nonexistent-repo", "fb") == "fb"
