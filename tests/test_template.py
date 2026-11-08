"""PROTOCOL.template.md substitution contract: the template carries exactly
the four placeholders the daemon renderer replaces, and a plain
str.replace pass with a sandbox config leaves no residue while threading
the instance's identity through — an independent cross-check of
daemon._render_protocol (tests/test_render.py) and the template lock
(tests/test_protocol_consistency.py).
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

PKG_DIR = Path(__file__).resolve().parent.parent
TEMPLATE = PKG_DIR / "PROTOCOL.template.md"
PLACEHOLDERS = ("{{IDENTITY}}", "{{OUT_OF_SCOPE}}",
                "{{BASELINE_BRANCH}}", "{{GIT_DEFAULT_BRANCH}}")


def _template_text():
    return TEMPLATE.read_text(encoding="utf-8")


def _substitute(text, identity, out_of_scope, baseline, git_default):
    return (text
            .replace("{{IDENTITY}}", identity)
            .replace("{{OUT_OF_SCOPE}}", out_of_scope)
            .replace("{{BASELINE_BRANCH}}", baseline)
            .replace("{{GIT_DEFAULT_BRANCH}}", git_default))


def test_template_placeholders_are_exactly_the_agreed_four():
    found = set(re.findall(r"\{\{[A-Z_]+\}\}", _template_text()))
    assert found == set(PLACEHOLDERS), f"placeholder drift: {found}"


def test_substitution_leaves_no_placeholder_residue():
    rendered = _substitute(_template_text(), "C++/Qt 应用平台",
                           "兄弟仓 Alpha、运维", "release-1x", "trunk")
    assert "{{" not in rendered and "}}" not in rendered


def test_substitution_threads_identity_and_scope():
    rendered = _substitute(_template_text(), "C++/Qt 应用平台",
                           "兄弟仓 Alpha、运维", "release-1x", "trunk")
    assert "C++/Qt 应用平台" in rendered
    assert "兄弟仓 Alpha、运维" in rendered


def test_substitution_threads_baseline_into_agent_remote_ref():
    rendered = _substitute(_template_text(), "X", "Y", "release-1x", "trunk")
    assert "refs/remotes/agent-remote/release-1x" in rendered


def test_substitution_threads_git_default_branch_into_forbidden_zone():
    rendered = _substitute(_template_text(), "X", "Y", "release-1x", "trunk")
    assert "`trunk`" in rendered
    assert "release-1x" in rendered


def test_rendered_protocol_matches_independent_substitution(tmp_path):
    """daemon._render_protocol and this file's independent str.replace
    agree on the rendered body (same inputs -> same output)."""
    import daemon
    from config import Config
    config = Config(api_base="http://g", pat="p", repo_path=str(tmp_path),
                    callback_url="http://c", canonical_path=str(tmp_path),
                    identity="C++/Qt 应用平台",
                    out_of_scope=("兄弟仓 Alpha", "运维"),
                    default_branch="release-1x")
    subprocess = __import__("subprocess")
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    rendered_path = daemon._render_protocol(PKG_DIR, config)
    try:
        via_daemon = rendered_path.read_text(encoding="utf-8")
    finally:
        rendered_path.unlink(missing_ok=True)
    via_independent = _substitute(_template_text(), "C++/Qt 应用平台",
                                  "、".join(("兄弟仓 Alpha", "运维")),
                                  "release-1x", "release-1x")
    assert via_daemon == via_independent
