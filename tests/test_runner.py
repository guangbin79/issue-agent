"""TDD tests for runner — the headless opencode CLI executor (plan task 5).

opencode itself is never invoked: FakeRun injects canned outcomes, and the
two real-subprocess tests use python3/sleep stubs only (fast, no network).
Failure-mapping contract consumed by the orchestrator (task 7):
TurnTimeout -> defer, TurnCrash -> blocked, SessionInvalid -> new-session+comment.
"""
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import runner  # noqa: E402  (path setup must precede the import)


NDJSON_STREAM = "\n".join(
    [
        '{"type":"step-start","sessionID":"ses_abc","info":{}}',
        '{"type":"step-update","sessionID":"ses_abc","part":{"type":"status","status":"busy"}}',
        '{"type":"step-update","sessionID":"ses_abc","part":{"type":"text","text":"第一段"}}',
        '{"type":"step-update","sessionID":"ses_abc","part":{"type":"text","text":"最后一段"}}',
        '{"type":"step-end","sessionID":"ses_abc"}',
    ]
)


def completed(returncode=0, stdout="", stderr=""):
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr=stderr)


class FakeRun:
    """Callable subprocess.run stand-in recording every call.

    *results* items are CompletedProcess objects to return or exceptions to
    raise; an exhausted list means the code under test retried too often.
    """

    def __init__(self, results):
        self.results = list(results)
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if not self.results:
            raise AssertionError(f"unexpected extra subprocess call #{len(self.calls)}")
        outcome = self.results.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


# --- parse_ndjson ----------------------------------------------------------


class TestParseNdjson:
    def test_session_id_and_last_text_from_multi_line_stream(self):
        assert runner.parse_ndjson(NDJSON_STREAM) == ("ses_abc", "最后一段")

    def test_multiple_text_parts_last_wins(self):
        stream = "\n".join(
            [
                '{"sessionID":"s","part":{"type":"text","text":"a"}}',
                '{"sessionID":"s","part":{"type":"text","text":"b"}}',
                '{"sessionID":"s","part":{"type":"text","text":"c"}}',
            ]
        )
        assert runner.parse_ndjson(stream) == ("s", "c")

    def test_no_text_part_returns_none(self):
        session_id, text = runner.parse_ndjson('{"type":"step-start","sessionID":"ses_x"}')
        assert session_id == "ses_x"
        assert text is None

    def test_empty_stream_returns_nones(self):
        assert runner.parse_ndjson("") == (None, None)

    def test_truncated_tail_line_keeps_prior_events(self):
        stream = NDJSON_STREAM + '\n{"type":"step-update","sessionID":"ses_abc","part":{"type":"te'
        assert runner.parse_ndjson(stream) == ("ses_abc", "最后一段")

    def test_blank_lines_tolerated(self):
        assert runner.parse_ndjson("\n\n" + NDJSON_STREAM + "\n\n") == ("ses_abc", "最后一段")


# --- is_report / prefer_report / export_last_report -------------------------


def _report(heads=("根因", "改动文件清单", "验证方式"), settlement=True):
    lines = [f"{head}：……" for head in heads]
    if settlement:
        lines.append("沉淀：知识＝issue-agent 模块页；经验＝待人工审核")
    return "\n".join(lines)


def _ndjson(*texts):
    return "\n".join(
        json.dumps({"sessionID": "ses_abc",
                    "part": {"type": "text", "text": text}})
        for text in texts
    )


class TestIsReport:
    def test_full_report_hits(self):
        assert runner.is_report(_report()) is True

    @pytest.mark.parametrize("missing", ["根因", "改动文件清单", "验证方式"])
    def test_missing_any_section_head_misses(self, missing):
        assert runner.is_report(_report().replace(missing, "……")) is False

    def test_missing_settlement_line_misses(self):
        assert runner.is_report(_report(settlement=False)) is False

    def test_progress_fragment_misses(self):
        assert runner.is_report("构建进行中…等待后台完成通知") is False

    def test_out_of_scope_reply_misses(self):
        assert runner.is_report("范围外：此需求属运维事项，建议转对应仓库。") is False

    def test_non_string_misses(self):
        assert runner.is_report(None) is False

    def test_markdown_bold_settlement_hits(self):
        # issue#65: 终审报告沉淀行写「**沉淀**：」曾判负，报告不被识别
        assert runner.is_report(_report().replace("沉淀：", "**沉淀**：")) is True

    @pytest.mark.parametrize("decorated", ["- 沉淀：", "> 沉淀：", "  沉淀："])
    def test_markdown_decorated_settlement_hits(self, decorated):
        assert runner.is_report(_report().replace("沉淀：", decorated)) is True

    def test_short_second_head_hits(self):
        # issue#65: 实际报告写「改动文件」短段头曾判负
        assert runner.is_report(
            _report(heads=("根因", "改动文件", "验证方式"))) is True

    def test_issue65_bold_verify_head_hits(self):
        # issue#65 后续实证判负形态：第三段写「**验证**：构建绿」（粗体
        # 短段头，无「验证方式」子串）＋粗体沉淀行，报告曾整篇不被识别——
        # 第三闸替代表：「验证方式」子串 OR 任一行剥装饰后以「验证」起始
        text = "\n".join([
            "根因：回合结束于汇报产出前",
            "**验证**：构建绿",
            "改动文件清单：- runner.py",
            "**沉淀**：知识＝issue-agent 模块页；经验＝待人工审核",
        ])
        assert runner.is_report(text) is True

    @pytest.mark.parametrize("verify_line", [
        "验证方式：python3 -m pytest tests/ -q 全绿",
        "- 验证：python3 -m pytest tests/ -q 全绿",
        "**验证**：python3 -m pytest tests/ -q 全绿",
    ])
    def test_third_gate_forms_hit(self, verify_line):
        # 第三闸三形态（段头子串/剥饰行首「验证」含列表粗体装饰）任一命中
        text = "\n".join(["根因：……", "改动文件：……", verify_line, "沉淀：……"])
        assert runner.is_report(text) is True

    def test_third_gate_missing_misses(self):
        # 第三闸两形态全缺（既无「验证方式」子串也无行首「验证」）→ 判负
        text = "\n".join(["根因：……", "改动文件：……", "自测通过：全绿", "沉淀：……"])
        assert runner.is_report(text) is False


class TestParseNdjsonPreferReport:
    def test_report_displaced_by_progress_still_wins(self):
        # issue#51: 字面末条是「构建进行中…」，prefer_report 仍回发真汇报
        stream = _ndjson(_report(), "构建进行中…等待后台完成通知")
        session_id, text = runner.parse_ndjson(stream, prefer_report=True)
        assert session_id == "ses_abc"
        assert text == _report()

    def test_no_report_hit_keeps_literal_last(self):
        stream = _ndjson("第一段", "最后一段")
        assert runner.parse_ndjson(stream, prefer_report=True)[1] == "最后一段"

    def test_report_as_last_unchanged(self):
        stream = _ndjson("分析中…", _report())
        assert runner.parse_ndjson(stream, prefer_report=True)[1] == _report()

    def test_multiple_reports_last_wins(self):
        other = _report().replace("……", "——B")
        stream = _ndjson(_report(), "中途进度", other)
        assert runner.parse_ndjson(stream, prefer_report=True)[1] == other

    def test_default_off_literal_last(self):
        # 评审回合（new_session_publishing_sid）语义不动：字面末条
        stream = _ndjson(_report(), "评审总结：无阻塞发现，门禁通过。")
        assert runner.parse_ndjson(stream)[1] == "评审总结：无阻塞发现，门禁通过。"


def _export_payload(messages):
    return json.dumps({"info": {"id": "ses_abc"}, "messages": messages})


def _assistant(*texts):
    return {"type": "assistant",
            "content": [{"type": "text", "text": t} for t in texts]}


class ExportRun:
    """run= stand-in for export_last_report: writes *payload* into the
    caller-supplied stdout sink (the code must redirect stdout to a real
    seekable file — PIPE produces broken JSON) and records every call."""

    def __init__(self, payload="", returncode=0, exc=None):
        self.payload = payload
        self.returncode = returncode
        self.exc = exc
        self.calls = []

    def __call__(self, argv, **kwargs):
        self.calls.append((argv, kwargs))
        if self.exc is not None:
            raise self.exc
        kwargs["stdout"].write(self.payload.encode("utf-8"))
        return subprocess.CompletedProcess(args=argv, returncode=self.returncode,
                                           stdout="", stderr="")


class TestExportLastReport:
    def test_scans_history_tail_to_head_last_report_wins(self):
        messages = [
            {"type": "user", "content": [{"type": "text", "text": "请修复"}]},
            _assistant("分析中…"),
            _assistant(_report()),
            _assistant("继续跟进中…"),
            _assistant("收尾说明", _report().replace("……", "——B")),
        ]
        fake = ExportRun(_export_payload(messages))
        assert runner.export_last_report("ses_abc", "/repo", run=fake) == \
            _report().replace("……", "——B")
        argv, kwargs = fake.calls[0]
        assert argv == ["opencode", "session", "export", "ses_abc"]
        assert kwargs["stdout"] is not subprocess.PIPE
        assert callable(kwargs["stdout"].seek)
        assert kwargs["cwd"] == "/repo"

    def test_broken_json_returns_none(self):
        fake = ExportRun("{not-json")
        assert runner.export_last_report("ses_abc", "/repo", run=fake) is None

    def test_nonzero_exit_returns_none(self):
        fake = ExportRun(_export_payload([_assistant(_report())]), returncode=1)
        assert runner.export_last_report("ses_abc", "/repo", run=fake) is None

    def test_no_report_hit_returns_none(self):
        fake = ExportRun(_export_payload([_assistant("进度 ping"), _assistant("收尾说明")]))
        assert runner.export_last_report("ses_abc", "/repo", run=fake) is None

    def test_user_message_report_ignored_only_assistant_counts(self):
        messages = [
            {"type": "user", "content": [{"type": "text", "text": _report()}]},
            _assistant("普通说明文字"),
        ]
        fake = ExportRun(_export_payload(messages))
        assert runner.export_last_report("ses_abc", "/repo", run=fake) is None

    @pytest.mark.parametrize("exc", [
        subprocess.TimeoutExpired(cmd="opencode", timeout=1),
        OSError("disk gone"),
    ])
    def test_runtime_failures_return_none(self, exc):
        fake = ExportRun(exc=exc)
        assert runner.export_last_report("ses_abc", "/repo", run=fake) is None


# --- argv / env construction ------------------------------------------------


class TestCommandConstruction:
    def test_new_session_argv_and_cwd(self):
        fake = FakeRun([completed(stdout=NDJSON_STREAM)])
        runner.new_session("/repo", "issue#7: 修复登录", "prompt-text", run=fake)
        argv, kwargs = fake.calls[0]
        assert argv == [
            "opencode", "run", "--format", "json",
            "--title", "issue#7: 修复登录", "prompt-text",
        ]
        assert kwargs["cwd"] == "/repo"

    def test_continue_session_argv(self):
        fake = FakeRun([completed()])
        runner.continue_session("/repo", "ses_abc", "next message", run=fake)
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json",
            "--session", "ses_abc", "next message",
        ]

    def test_child_env_is_fixed_and_headless(self, monkeypatch):
        monkeypatch.setenv("PATH", "/usr/bin")
        monkeypatch.setenv("HOME", "/home/agent")
        monkeypatch.setenv("TZ", "Asia/Shanghai")
        monkeypatch.setenv("SECRET_JUNK", "must-not-leak")
        fake = FakeRun([completed()])
        runner.new_session("/repo", "t", "m", run=fake)
        assert fake.calls[0][1]["env"] == {
            "PATH": "/usr/bin",
            "HOME": "/home/agent",
            "TZ": "Asia/Shanghai",
            "GIT_TERMINAL_PROMPT": "0",
        }

    def test_timeout_forwarded_to_run(self):
        fake = FakeRun([completed()])
        runner.new_session("/repo", "t", "m", timeout_s=123, run=fake)
        assert fake.calls[0][1]["timeout"] == 123


class TestModelSelection:
    """Plan Task 2: new/continue 可选 model——仅显式提供才加 --model；
    缺席（含显式 None/空串）时 argv 与历史逐字节一致（回归锚）。"""

    def _ok(self):
        return completed(stdout='{"sessionID":"ses_m","part":{"type":"text","text":"ok"}}')

    def test_new_session_without_model_keeps_historical_argv(self):
        fake = FakeRun([self._ok()])
        runner.new_session("/repo", "t", "m", run=fake)
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json", "--title", "t", "m",
        ]

    def test_new_session_explicit_none_model_keeps_historical_argv(self):
        fake = FakeRun([self._ok()])
        runner.new_session("/repo", "t", "m", run=fake, model=None)
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json", "--title", "t", "m",
        ]

    def test_new_session_with_model_inserts_flag_pair(self):
        fake = FakeRun([self._ok()])
        runner.new_session("/repo", "t", "m", run=fake, model="prov/m")
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json",
            "--model", "prov/m", "--title", "t", "m",
        ]

    def test_continue_session_without_model_keeps_historical_argv(self):
        fake = FakeRun([self._ok()])
        runner.continue_session("/repo", "ses_1", "m", run=fake)
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json", "--session", "ses_1", "m",
        ]

    def test_continue_session_with_model_inserts_flag_pair(self):
        fake = FakeRun([self._ok()])
        runner.continue_session("/repo", "ses_1", "m", run=fake, model="prov/m")
        assert fake.calls[0][0] == [
            "opencode", "run", "--format", "json",
            "--model", "prov/m", "--session", "ses_1", "m",
        ]

    def test_empty_model_string_treated_as_absent(self):
        fake = FakeRun([self._ok()])
        runner.new_session("/repo", "t", "m", run=fake, model="")
        assert "--model" not in fake.calls[0][0]


# --- happy path --------------------------------------------------------------


class TestHappyPath:
    def test_turn_result_fields(self):
        fake = FakeRun([completed(stdout=NDJSON_STREAM)])
        result = runner.new_session("/repo", "t", "m", run=fake)
        assert isinstance(result, runner.TurnResult)
        assert result.session_id == "ses_abc"
        assert result.reply_text == "最后一段"
        assert result.exit_code == 0
        assert result.duration_s >= 0

    def test_one_crash_then_success_after_single_retry(self):
        fake = FakeRun([completed(1, stderr="transient"), completed(stdout=NDJSON_STREAM)])
        result = runner.new_session("/repo", "t", "m", run=fake)
        assert result.reply_text == "最后一段"
        assert len(fake.calls) == 2
        assert fake.calls[0][0] == fake.calls[1][0]  # retry repeats the SAME command


# --- failure mapping ---------------------------------------------------------


class TestFailureMapping:
    def test_crash_retries_once_then_raises_turn_crash(self):
        fake = FakeRun([completed(1, stderr="kaboom"), completed(1, stderr="kaboom")])
        with pytest.raises(runner.TurnCrash):
            runner.new_session("/repo", "t", "m", run=fake)
        assert len(fake.calls) == 2

    def test_timeout_raises_turn_timeout_without_retry(self):
        fake = FakeRun([subprocess.TimeoutExpired(cmd="opencode", timeout=1)])
        with pytest.raises(runner.TurnTimeout):
            runner.new_session("/repo", "t", "m", timeout_s=1, run=fake)
        assert len(fake.calls) == 1

    @pytest.mark.parametrize(
        "stderr",
        [
            'Error: session "ses_x" not found',
            "fatal: invalid session id ses_x",
            "session ses_x does not exist",
        ],
    )
    def test_session_invalid_pattern_raises_immediately(self, stderr):
        fake = FakeRun([completed(1, stderr=stderr)])
        with pytest.raises(runner.SessionInvalid):
            runner.continue_session("/repo", "ses_x", "msg", run=fake)
        assert len(fake.calls) == 1

    def test_session_invalid_pattern_in_stdout_also_detected(self):
        fake = FakeRun([completed(1, stdout='{"error":"session ses_x not found"}')])
        with pytest.raises(runner.SessionInvalid):
            runner.continue_session("/repo", "ses_x", "msg", run=fake)
        assert len(fake.calls) == 1

    def test_plain_crash_is_not_session_invalid(self):
        fake = FakeRun(
            [completed(1, stderr="panic: runtime error"), completed(1, stderr="panic: runtime error")]
        )
        with pytest.raises(runner.TurnCrash):
            runner.continue_session("/repo", "ses_x", "msg", run=fake)

    def test_new_session_killed_midstream_is_turn_crash_not_session_invalid(self):
        # 2026-10-19 incident: a sibling `opencode service restart` killed the
        # shared server; the daemon's headless child died mid-stream. Its NDJSON
        # transcript always carries "sessionID" and model text often contains
        # "missing/invalid" — a NEW session has no session target at all, so
        # this is a crash (retryable), never SessionInvalid. Misclassification
        # blocked fresh issues with a bogus "新建会话也被拒绝" comment.
        transcript = (
            '{"type":"step_start","timestamp":1,"sessionID":"ses_a"}\n'
            '{"type":"step_start","timestamp":2,"sessionID":"ses_a",'
            '"part":{"id":"prt_1","sessionID":"ses_a","messageID":"msg_1",'
            '"text":"fatal: missing header"}}\n'
        )
        fake = FakeRun([completed(1, stdout=transcript),
                        completed(1, stdout=transcript)])
        with pytest.raises(runner.TurnCrash):
            runner.new_session("/repo", "t", "m", run=fake)
        assert len(fake.calls) == 2, "crash still gets its one retry"


# --- real subprocess stubs (no opencode involved) -----------------------------


class TestRealSubprocess:
    def test_real_pipe_ndjson_read(self):
        emit = (
            "import json\n"
            "events = ["
            '{"type":"step-start","sessionID":"ses_real"},'
            '{"type":"step-update","sessionID":"ses_real","part":{"type":"text","text":"管道中"}},'
            '{"type":"step-update","sessionID":"ses_real","part":{"type":"text","text":"真实管道完成"}}'
            "]\n"
            "for event in events:\n"
            "    print(json.dumps(event, ensure_ascii=False))\n"
        )

        def python3_run(argv, **kwargs):
            return subprocess.run(["python3", "-c", emit, *argv[1:]], **kwargs)

        result = runner.new_session("/tmp", "t", "m", run=python3_run)
        assert (result.session_id, result.reply_text, result.exit_code) == (
            "ses_real", "真实管道完成", 0,
        )

    def test_real_timeout_raises_turn_timeout(self):
        def sleep_run(argv, **kwargs):
            return subprocess.run(["sleep", "5"], **kwargs)

        with pytest.raises(runner.TurnTimeout):
            runner.new_session("/tmp", "t", "m", timeout_s=1, run=sleep_run)


# --- build_prompt ------------------------------------------------------------


class TestBuildPrompt:
    def test_points_at_absolute_protocol_and_embeds_issue(self, tmp_path):
        protocol = tmp_path / "PROTOCOL.md"
        protocol.write_text("secret protocol rules", encoding="utf-8")
        prompt = runner.build_prompt(str(protocol), "#7 登录超时\n\n步骤：打开设置")
        assert os.path.abspath(str(protocol)) in prompt
        assert "#7 登录超时" in prompt
        assert "步骤：打开设置" in prompt

    def test_prompt_does_not_inline_protocol_content(self, tmp_path):
        protocol = tmp_path / "PROTOCOL.md"
        protocol.write_text("RULE-DO-NOT-INLINE", encoding="utf-8")
        prompt = runner.build_prompt(str(protocol), "issue body")
        assert "RULE-DO-NOT-INLINE" not in prompt


# --- streaming sid publication (new_session_publishing_sid) ---------------


def python3_popen(script):
    """Popen factory: run *script* with the real python3, forwarding the
    runner's stdout/stderr/env wiring (argv[0] 'opencode' is replaced)."""
    def spawn(argv, **kwargs):
        return subprocess.Popen(["python3", "-c", script, *argv[1:]], **kwargs)
    return spawn


_EMIT_TWO_LINES = (
    "import json\n"
    "print(json.dumps({'type': 'step-start', 'sessionID': 'ses_stream'}))\n"
    "print(json.dumps({'type': 'step-update', 'sessionID': 'ses_stream',"
                       " 'part': {'type': 'text', 'text': '流式完成'}}))\n"
)


class TestNewSessionPublishingSid:
    """Real python3 subprocesses exercise the select loop, the atomic sid
    publication, the deadline kill, and the retry rewrite (no opencode)."""

    def test_streams_sid_and_turn_result(self, tmp_path):
        sid_path = tmp_path / "session-id.txt"
        result = runner.new_session_publishing_sid(
            "/tmp", "t", "m", 30, str(sid_path),
            popen=python3_popen(_EMIT_TWO_LINES))
        assert (result.session_id, result.reply_text, result.exit_code) == (
            "ses_stream", "流式完成", 0,
        )
        assert sid_path.read_text(encoding="utf-8") == "ses_stream\n", (
            "bare first-line sessionID + newline, published mid-flight")

    def test_argv_matches_new_session(self, tmp_path):
        seen = []
        base = python3_popen(_EMIT_TWO_LINES)

        def recording_popen(argv, **kwargs):
            seen.append(list(argv))
            return base(argv, **kwargs)

        runner.new_session_publishing_sid(
            "/tmp", "review#9: 标题", "正文", 30, str(tmp_path / "sid.txt"),
            popen=recording_popen)
        assert seen == [["opencode", "run", "--format", "json",
                         "--title", "review#9: 标题", "正文"]]

    def test_timeout_publishes_sid_then_raises_turn_timeout(self, tmp_path):
        sid_path = tmp_path / "session-id.txt"
        slow = (
            "import json, time\n"
            "print(json.dumps({'sessionID': 'ses_slow'}), flush=True)\n"
            "time.sleep(30)\n"
        )
        with pytest.raises(runner.TurnTimeout):
            runner.new_session_publishing_sid(
                "/tmp", "t", "m", 1, str(sid_path), popen=python3_popen(slow))
        assert sid_path.read_text(encoding="utf-8") == "ses_slow\n", (
            "the sid landed BEFORE the deadline kill fired")

    def test_crash_then_retry_sid_is_final_attempt(self, tmp_path):
        sid_path = tmp_path / "session-id.txt"
        crash = "import sys; sys.stderr.write('transient boom'); sys.exit(1)"
        scripts = iter([crash, _EMIT_TWO_LINES])

        def two_phase_popen(argv, **kwargs):
            return subprocess.Popen(
                ["python3", "-c", next(scripts), *argv[1:]], **kwargs)

        result = runner.new_session_publishing_sid(
            "/tmp", "t", "m", 30, str(sid_path), popen=two_phase_popen)
        assert (result.session_id, result.reply_text, result.exit_code) == (
            "ses_stream", "流式完成", 0,
        )
        assert sid_path.read_text(encoding="utf-8") == "ses_stream\n", (
            "first attempt wrote nothing; the final attempt's ID wins")

    def test_retry_invalidates_stale_sid_from_dead_attempt(self, tmp_path):
        """ocr finding (round 1): if attempt 1 publishes a sessionID and then
        crashes retryably, the file must be INVALIDATED before attempt 2 —
        a present file always belongs to the current attempt, so a reviewer
        can never adopt the dead attempt's ID."""
        sid_path = tmp_path / "session-id.txt"
        sid_then_crash = (
            "import json, sys\n"
            "print(json.dumps({'sessionID': 'ses_dead'}))\n"
            "sys.stderr.write('transient after sid')\n"
            "sys.exit(1)\n"
        )
        silent_crash = "import sys; sys.exit(1)"
        scripts = iter([sid_then_crash, silent_crash])

        def two_phase_popen(argv, **kwargs):
            return subprocess.Popen(
                ["python3", "-c", next(scripts), *argv[1:]], **kwargs)

        with pytest.raises(runner.TurnCrash):
            runner.new_session_publishing_sid(
                "/tmp", "t", "m", 30, str(sid_path), popen=two_phase_popen)
        assert not sid_path.exists(), (
            "attempt 2 started by unlinking attempt 1's dead ID and never "
            "published a new one — no stale ID may survive the turn")


# --- heartbeat: activity extraction + on_event streaming turns ------------


class TestActivityFromEvent:
    """One NDJSON line -> one-line 'what is it doing now', or None. The
    orchestrator's heartbeat note quotes this verbatim; best-effort by
    contract: an unrecognized line must never raise."""

    def test_tool_event_reports_tool_and_first_string_input(self):
        line = json.dumps({"part": {"type": "tool", "tool": "bash",
                                    "state": {"input": {"command": "pytest -q",
                                                        "timeout": 30}}}})
        assert runner.activity_from_event(line) == "bash: pytest -q"

    def test_tool_event_without_string_input_reports_tool_only(self):
        line = json.dumps({"part": {"type": "tool", "tool": "read",
                                    "state": {"input": {}}}})
        assert runner.activity_from_event(line) == "read"

    def test_tool_event_without_state_reports_tool_only(self):
        line = json.dumps({"part": {"type": "tool", "tool": "grep"}})
        assert runner.activity_from_event(line) == "grep"

    def test_text_event_reports_first_line(self):
        line = json.dumps({"part": {"type": "text", "text": "第一行\n第二行"}})
        assert runner.activity_from_event(line) == "第一行"

    def test_overlong_hint_truncated_to_80_chars(self):
        line = json.dumps({"part": {"type": "tool", "tool": "edit",
                                    "state": {"input": {"path": "x" * 200}}}})
        assert runner.activity_from_event(line) == "edit: " + "x" * 80

    def test_unusable_lines_return_none(self):
        assert runner.activity_from_event("not-json") is None
        assert runner.activity_from_event('{"part":{"type":"step-start"}}') is None
        assert runner.activity_from_event('{"no-part": true}') is None
        assert runner.activity_from_event("42") is None


_EMIT_TOOL_THEN_TEXT = (
    "import json\n"
    "print(json.dumps({'sessionID': 'ses_live',"
    " 'part': {'type': 'step-start'}}, ensure_ascii=False))\n"
    "print(json.dumps({'sessionID': 'ses_live', 'part': {'type': 'tool',"
    " 'tool': 'bash', 'state': {'input': {'command': 'pytest -q'}}}}),"
    " flush=True)\n"
    "print(json.dumps({'sessionID': 'ses_live',"
    " 'part': {'type': 'text', 'text': '流式完成'}}, ensure_ascii=False))\n"
)


class TestOnEventLiveTurns:
    """on_event turns stream through the proven select loop: every complete
    NDJSON line reaches the callback mid-flight, failure mapping (timeout)
    is unchanged, and the session-targeted argv keeps its shape."""

    def test_new_session_forwards_every_line(self):
        lines = []
        result = runner.new_session(
            "/tmp", "t", "m", 30, on_event=lines.append,
            popen=python3_popen(_EMIT_TOOL_THEN_TEXT))
        assert (result.session_id, result.reply_text, result.exit_code) == (
            "ses_live", "流式完成", 0)
        assert any('"tool"' in line for line in lines), lines
        assert any("流式完成" in line for line in lines), lines

    def test_continue_session_forwards_lines_and_keeps_argv(self):
        lines = []
        seen_argv = []

        def recording_popen(argv, **kwargs):
            seen_argv.append(list(argv))
            return python3_popen(_EMIT_TOOL_THEN_TEXT)(argv, **kwargs)

        result = runner.continue_session(
            "/tmp", "ses_live", "m", 30, on_event=lines.append,
            popen=recording_popen)
        assert result.session_id == "ses_live"
        assert lines, "callback saw the stream"
        assert seen_argv == [["opencode", "run", "--format", "json",
                              "--session", "ses_live", "m"]]

    def test_publishing_sid_forwards_lines_too(self, tmp_path):
        lines = []
        sid_path = tmp_path / "sid.txt"
        runner.new_session_publishing_sid(
            "/tmp", "t", "m", 30, str(sid_path), on_event=lines.append,
            popen=python3_popen(_EMIT_TOOL_THEN_TEXT))
        assert lines, "callback saw the stream"
        assert sid_path.read_text(encoding="utf-8") == "ses_live\n", (
            "sid publication unchanged when on_event rides along")

    def test_live_timeout_still_raises_turn_timeout(self):
        slow = (
            "import json, time\n"
            "print(json.dumps({'sessionID': 'ses_slow'}), flush=True)\n"
            "time.sleep(30)\n"
        )
        seen = []
        with pytest.raises(runner.TurnTimeout):
            runner.new_session("/tmp", "t", "m", 1, on_event=seen.append,
                               popen=python3_popen(slow))
        assert seen, "lines streamed before the deadline kill"

# --- credential prefix (Task 2: agora gate round dealing) ---------------------


class TestCredentialPrefix:
    """Round dealing: a four-line prefix prepended to every turn prompt so
    the headless session holds the agora write-gate credential (Task 1's
    one-message bootstrap contract — format locked verbatim)."""

    def test_four_line_format_verbatim(self):
        prefix = runner.credential_prefix(7, "/repo")
        lines = prefix.split("\n")
        assert lines[0] == "[agora] task-issue-7: issue-agent round"
        assert lines[1] == f"env: {os.path.abspath('/repo')}"
        assert lines[2] == "[agora] credential task-issue-7"
        assert re.fullmatch(r"token: [0-9a-f]{32}", lines[3]), lines[3]

    def test_token_differs_between_calls(self):
        first = runner.credential_prefix(3, "/repo").splitlines()[-1]
        second = runner.credential_prefix(3, "/repo").splitlines()[-1]
        assert first != second, "each round gets a fresh token"

    def test_new_session_prepends_prefix_with_blank_line(self):
        fake = FakeRun([completed(stdout=NDJSON_STREAM)])
        runner.new_session("/repo", "t", "prompt-text", cred_prefix="L1\nL2", run=fake)
        argv, _ = fake.calls[0]
        assert argv[-1] == "L1\nL2\n\nprompt-text"

    def test_continue_session_prepends_prefix_with_blank_line(self):
        fake = FakeRun([completed(stdout=NDJSON_STREAM)])
        runner.continue_session("/repo", "ses_1", "next message",
                                cred_prefix="L1\nL2", run=fake)
        assert fake.calls[0][0][-1] == "L1\nL2\n\nnext message"

    def test_publishing_sid_prepends_prefix_too(self, tmp_path):
        seen = []
        base = python3_popen(_EMIT_TWO_LINES)

        def recording_popen(argv, **kwargs):
            seen.append(list(argv))
            return base(argv, **kwargs)

        runner.new_session_publishing_sid(
            "/tmp", "t", "正文", 30, str(tmp_path / "sid.txt"),
            cred_prefix="L1\nL2", popen=recording_popen)
        assert seen[0][-1] == "L1\nL2\n\n正文"
