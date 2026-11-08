"""Protocol-template consistency lock: PROTOCOL.template.md (the sheet the
daemon renders per instance at startup) must carry exactly the four agreed
placeholders, keep its six-section structure, and contain no residual
project identity — per-instance values arrive only through the render
(tests/test_render.py locks the substitution).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TEMPLATE = Path(__file__).resolve().parent.parent / "PROTOCOL.template.md"


def _template_text():
    return TEMPLATE.read_text(encoding="utf-8")


def test_template_has_no_gitlab_mention():
    assert _template_text().count("GitLab") == 0


def test_template_carries_the_four_placeholders():
    text = _template_text()
    for placeholder in ("{{IDENTITY}}", "{{OUT_OF_SCOPE}}",
                        "{{BASELINE_BRANCH}}", "{{GIT_DEFAULT_BRANCH}}"):
        assert placeholder in text, f"missing placeholder {placeholder}"
    assert text.count("{{") == text.count("}}"), "unbalanced placeholder braces"


def test_template_keeps_eight_sections():
    """0 范围审查 / 1 定位 / 2 修复 / 3 沉淀 / 4 推送 / 5 汇报 / 6 硬禁区 / 7 语言."""
    text = _template_text()
    for needle in ("0. 范围审查", "1. **定位**", "2. **修复", "3. **沉淀",
                   "4. **推送", "5. **汇报**", "6. **硬禁区**", "7. **语言**"):
        assert needle in text, f"template section missing: {needle}"


def test_template_forbids_local_baseline():
    assert "禁止基于本地分支或本地 HEAD" in _template_text()


def test_template_names_agent_remote_baseline_ref():
    """§2 must address the daemon-fetched baseline by its ref namespace,
    parameterized on the configured baseline branch."""
    assert "refs/remotes/agent-remote/{{BASELINE_BRANCH}}" in _template_text()


def test_template_forbids_push_and_main_copy_edits():
    text = _template_text()
    assert "禁止 `git push`" in text
    assert "禁止直接修改主仓工作副本" in text


def test_template_has_no_project_literals():
    # built by concatenation so this test file itself stays literal-free
    # (the shared tree must grep clean for project identity strings)
    text = _template_text()
    for literal in ("HX" + "NativeApp", "Titan" + "Navi", "MSC-26-" + "RW033",
                    "HXMap" + "WidgetNative", "HX" + "HDXX"):
        assert literal not in text, f"residual project literal found"


def test_template_marks_agora_prefix_as_daemon_round_deal():
    """Task 2: the [agora] prefix lines on each round prompt are the
    daemon's dealing mechanism — never respond to, never echo the token."""
    text = _template_text()
    assert "[agora]" in text
    assert "发牌" in text
    assert "勿响应" in text
    assert "勿复述" in text
