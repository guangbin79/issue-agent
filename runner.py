"""Headless opencode CLI executor (plan task 5).

Runs `opencode run --format json` as a subprocess, parses the NDJSON event
stream for the session id and the final text part, and maps failures into
typed exceptions for the orchestrator (task 7):

- TurnTimeout    -> defer
- TurnCrash      -> blocked
- SessionInvalid -> new session + explanatory comment

The child env is fixed to PATH/HOME/TZ + GIT_TERMINAL_PROMPT=0 so opencode
runs headless regardless of the daemon's ambient environment.
"""
import json
import os
import re
import select
import secrets
import subprocess
import tempfile
import time
from dataclasses import dataclass
from types import SimpleNamespace

DEFAULT_TIMEOUT_S = 1800

# A not-found-like phrase anywhere in the output, case-insensitive. Combined
# with a "session" substring check this matches both "session X not found"
# and "invalid session id" orderings without matching ordinary crashes.
_NOT_FOUND_LIKE = re.compile(
    r"not found|no such|unknown|invalid|deleted|missing|does not exist",
    re.IGNORECASE,
)


class TurnTimeout(RuntimeError):
    """opencode exceeded timeout_s — orchestrator maps to defer."""


class TurnCrash(RuntimeError):
    """opencode exited non-zero twice — orchestrator maps to blocked."""


class SessionInvalid(RuntimeError):
    """--session target is gone — orchestrator maps to new-session+comment."""


@dataclass(frozen=True)
class TurnResult:
    session_id: str | None
    reply_text: str | None
    duration_s: float
    exit_code: int


def _child_env():
    env = {key: os.environ[key] for key in ("PATH", "HOME", "TZ") if key in os.environ}
    env["GIT_TERMINAL_PROMPT"] = "0"
    return env


def credential_prefix(iid, repo_path: str) -> str:
    """Task 2 回合发牌：四行前缀（与 agora 插件的逐字契约，单消息自举），
    headless 会话凭此持有 write 闸凭证；token 每轮一换。"""
    return (
        f"[agora] task-issue-{iid}: issue-agent round\n"
        f"env: {os.path.abspath(repo_path)}\n"
        f"[agora] credential task-issue-{iid}\n"
        f"token: {secrets.token_hex(16)}"
    )


def _with_cred_prefix(message, cred_prefix):
    # 前缀置于 prompt 最前，空行分隔——唯一拼接点
    return f"{cred_prefix}\n\n{message}" if cred_prefix else message


def parse_ndjson(output_text, prefer_report=False):
    """Extract (session_id, last_text) from an NDJSON event stream.

    session_id is the first event's "sessionID" field. last_text is the text
    of the last part carrying a "text" field; None when no such part exists.
    prefer_report=True returns the LAST text part that is_report() hits
    instead — a fix turn's real report survives later progress chatter
    (issue#51/#24); with no report hit the literal-last behaviour stands.
    A truncated tail line (mid-stream crash) is skipped — events parsed so
    far are kept.
    """
    session_id = None
    last_text = None
    last_report = None
    for line in output_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        if session_id is None:
            sid = event.get("sessionID")
            if isinstance(sid, str) and sid:
                session_id = sid
        part = event.get("part")
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            last_text = part["text"]
            if prefer_report and is_report(last_text):
                last_report = last_text
    if prefer_report and last_report is not None:
        last_text = last_report
    return session_id, last_text


# 协议第 5 条汇报形状：三段头＋以「沉淀」开头的末行清单。三闸同过才算
# 汇报——进度 ping、范围外回复至少缺一闸。第二项用子串「改动文件」：
# 「改动文件清单」等清单形式仍命中（issue#65 实证短段头「改动文件」
# 判负致干净终审报告不被识别）。
_REPORT_HEADS = ("根因", "改动文件", "验证方式")

# 沉淀行允许的行首 markdown/缩进装饰（issue#65 实证「**沉淀**：」粗体判负）。
_LINE_DECOR = re.compile(r"^(?:\s|[>#*`-])*")


def is_report(text):
    """True when *text* is a protocol §5 report: all three section heads
    present AND one line starting with 沉淀 after stripping leading
    markdown decoration (bold/bullet/quote/indent). issue#65 实证：粗体
    沉淀行/短段头判负致干净终审报告不被识别。Progress fragments
    and out-of-scope replies miss at least one gate."""
    if not isinstance(text, str):
        return False
    if not all(head in text for head in _REPORT_HEADS):
        return False
    return any(_LINE_DECOR.sub("", line).startswith("沉淀")
               for line in text.splitlines())


def export_last_report(session_id, cwd, timeout_s=60, run=subprocess.run):
    """Last protocol §5 report in a finished session's full history, or None.

    Runs `opencode session export <sid>` and scans messages tail-to-head
    for the last assistant text part that is_report() hits. ANY failure
    (non-zero exit, broken JSON, timeout, OSError) degrades to None — the
    caller posts a not-found note rather than guessing.

    硬约束：stdout 必须重定向到可 seek 的临时文件（stderr 用 DEVNULL），
    禁止 stdout=PIPE——opencode export 在 PIPE 上产废 JSON（实测两次
    偏移不同地解析失败），文件重定向才有效。
    """
    argv = ["opencode", "session", "export", session_id]
    try:
        sink = tempfile.TemporaryFile()
    except OSError:  # TMPDIR 满/不可写：同样按「任何失败→None」契约降级
        return None
    try:
        try:
            proc = run(argv, cwd=cwd, env=_child_env(), stdout=sink,
                       stderr=subprocess.DEVNULL, timeout=timeout_s)
        except (subprocess.TimeoutExpired, OSError):
            return None
        if proc.returncode != 0:
            return None
        sink.seek(0)
        raw = sink.read()
    finally:
        sink.close()
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return None
    if isinstance(payload, dict):
        messages = payload.get("messages")
    elif isinstance(payload, list):
        messages = payload
    else:
        return None
    if not isinstance(messages, list):
        return None
    for message in reversed(messages):
        if not isinstance(message, dict) or message.get("type") != "assistant":
            continue
        content = message.get("content")
        if not isinstance(content, list):
            continue
        for part in reversed(content):
            if isinstance(part, dict) and isinstance(part.get("text"), str) \
                    and is_report(part["text"]):
                return part["text"]
    return None


def _looks_session_invalid(proc):
    blob = f"{proc.stderr or ''}\n{proc.stdout or ''}"
    return "session" in blob.lower() and bool(_NOT_FOUND_LIKE.search(blob))


def _execute(argv, cwd, timeout_s, run):
    try:
        return run(
            argv,
            cwd=cwd,
            env=_child_env(),
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except subprocess.TimeoutExpired as exc:
        raise TurnTimeout(f"opencode exceeded {timeout_s}s: {' '.join(argv[:4])}") from exc


def _turn(argv, cwd, timeout_s, run, session_targeted, prefer_report=False):
    start = time.monotonic()
    last = None
    for _attempt in (1, 2):
        proc = _execute(argv, cwd, timeout_s, run)
        if proc.returncode == 0:
            session_id, reply_text = parse_ndjson(proc.stdout, prefer_report)
            return TurnResult(session_id, reply_text, time.monotonic() - start, proc.returncode)
        # only a run that actually targeted a session can invalidate it: a
        # NEW session's NDJSON transcript always contains "sessionID" and its
        # model text often contains "missing/invalid", so an unscaled match
        # here misread mid-stream kills (e.g. a shared-server restart) as
        # SessionInvalid — those must fall through to the TurnCrash retry.
        # Explicit flag, not argv sniffing: a message could literally be
        # the string "--session".
        if session_targeted and _looks_session_invalid(proc):
            detail = (proc.stderr or proc.stdout or "").strip()[:200]
            raise SessionInvalid(f"session target rejected by opencode: {detail}")
        last = proc
    raise TurnCrash(
        f"opencode exit {last.returncode} after retry: {(last.stderr or '').strip()[:200]}"
    )


def new_session(cwd, title, message, timeout_s=DEFAULT_TIMEOUT_S, run=subprocess.run,
                on_event=None, cred_prefix=None, popen=subprocess.Popen):
    """Start a fresh opencode session titled *title* and wait for the reply.

    on_event (heartbeat): when given, the turn streams through the proven
    select loop and every complete NDJSON line reaches the callback
    mid-flight; without it the historical blocking path runs unchanged.
    cred_prefix (agora 发牌), non-empty: prepended to *message*."""
    message = _with_cred_prefix(message, cred_prefix)
    argv = ["opencode", "run", "--format", "json", "--title", title, message]
    if on_event is not None:
        return _turn_streaming(argv, cwd, timeout_s, None, popen,
                               session_targeted=False, on_event=on_event,
                               prefer_report=True)
    return _turn(argv, cwd, timeout_s, run, session_targeted=False,
                 prefer_report=True)


def _atomic_write_sid(sid_path, sid):
    """Publish *sid* (bare ID + newline) to *sid_path* atomically: temp file
    in the same directory + os.replace, overwriting any previous value —
    a retry attempt must replace the stale ID from the attempt before."""
    fd, tmp_path = tempfile.mkstemp(dir=os.path.dirname(os.path.abspath(sid_path)))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{sid}\n")
        os.replace(tmp_path, sid_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _line_session_id(line_text):
    """The "sessionID" of ONE NDJSON line — same shape test as parse_ndjson's
    first-event extraction; None when the line is not a session-bearing
    JSON object."""
    try:
        event = json.loads(line_text)
    except json.JSONDecodeError:
        return None
    if not isinstance(event, dict):
        return None
    sid = event.get("sessionID")
    return sid if isinstance(sid, str) and sid else None


def _stream_once(argv, cwd, timeout_s, sid_path, popen, on_event=None):
    """One streaming attempt. Returns (returncode, stdout_text, stderr_text);
    raises TurnTimeout once the total deadline expires (kill + wait first).

    stderr goes to a temp file (failure diagnostics kept, its pipe buffer
    can never fill and deadlock). stdout is drained by a single-threaded
    select loop bounded by the deadline: complete lines accumulate as they
    arrive, and the FIRST sessionID-bearing JSON line is published to
    *sid_path* the moment it is seen — seconds into a turn, not after it.
    *on_event* (heartbeat), when given, sees every complete line as it
    arrives (callback exceptions propagate — the orchestrator's callback
    only mutates a dict and cannot raise)."""
    deadline = time.monotonic() + timeout_s
    stderr_sink = tempfile.TemporaryFile(
        mode="w+", encoding="utf-8", errors="replace")
    try:
        proc = popen(argv, cwd=cwd, env=_child_env(),
                     stdout=subprocess.PIPE, stderr=stderr_sink)
    except BaseException:
        stderr_sink.close()
        raise
    try:
        fd = proc.stdout.fileno()
        pending = b""
        lines = []
        sid_seen = False
        eof = False
        while not eof:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                proc.kill()
                proc.wait()
                raise TurnTimeout(
                    f"opencode exceeded {timeout_s}s: {' '.join(argv[:4])}")
            ready, _, _ = select.select([fd], [], [], remaining)
            if not ready:
                continue  # select hit the deadline; re-checked at loop top
            chunk = os.read(fd, 65536)
            if not chunk:
                eof = True
                break
            pending += chunk
            while b"\n" in pending:
                raw, _, pending = pending.partition(b"\n")
                line_text = raw.decode("utf-8", errors="replace")
                lines.append(line_text + "\n")
                if on_event is not None:
                    on_event(line_text)
                if sid_path is not None and not sid_seen:
                    sid = _line_session_id(line_text)
                    if sid is not None:
                        _atomic_write_sid(sid_path, sid)
                        sid_seen = True
        if pending:
            # truncated tail without a newline — parse_ndjson skips it, but
            # keep it so failure diagnostics see everything the child said
            lines.append(pending.decode("utf-8", errors="replace"))
        try:
            returncode = proc.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
            raise TurnTimeout(
                f"opencode exceeded {timeout_s}s: {' '.join(argv[:4])}") from None
        stderr_sink.seek(0)
        return returncode, "".join(lines), stderr_sink.read()
    finally:
        if proc.poll() is None:  # raise paths (e.g. sid write failure)
            proc.kill()
            proc.wait()
        try:
            proc.stdout.close()
        except OSError:
            pass
        stderr_sink.close()


def _turn_streaming(argv, cwd, timeout_s, sid_path, popen, session_targeted,
                    on_event=None, prefer_report=False):
    """_turn's two-attempt retry + failure mapping over streaming attempts:
    each attempt re-accumulates stdout, reopens its stderr sink, invalidates
    any sid published by an earlier attempt before starting (a PRESENT sid
    file always belongs to the CURRENT attempt — a reviewer reading it
    mid-retry can never adopt a dead attempt's ID), and may publish its own
    sid mid-flight. sid_path=None skips publication entirely (heartbeat-only
    live turns); on_event (optional) sees every complete NDJSON line."""
    start = time.monotonic()
    last = None
    for _attempt in (1, 2):
        if sid_path is not None:
            try:
                os.unlink(sid_path)  # a present file must belong to THIS attempt:
            except OSError:          # a retried turn must never expose a dead attempt's ID
                pass
        returncode, out_text, err_text = _stream_once(
            argv, cwd, timeout_s, sid_path, popen, on_event)
        if returncode == 0:
            session_id, reply_text = parse_ndjson(out_text, prefer_report)
            return TurnResult(
                session_id, reply_text, time.monotonic() - start, returncode)
        proc_like = SimpleNamespace(
            returncode=returncode, stderr=err_text, stdout=out_text)
        if session_targeted and _looks_session_invalid(proc_like):
            detail = (err_text or out_text or "").strip()[:200]
            raise SessionInvalid(f"session target rejected by opencode: {detail}")
        last = proc_like
    raise TurnCrash(
        f"opencode exit {last.returncode} after retry: "
        f"{(last.stderr or '').strip()[:200]}"
    )


def new_session_publishing_sid(cwd, title, message, timeout_s, sid_path,
                               popen=subprocess.Popen, on_event=None,
                               cred_prefix=None):
    """new_session, streaming: same argv and exception semantics, but the
    session ID is atomically published to *sid_path* the moment it first
    appears on stdout — so a waiting reviewer can learn its own session ID
    seconds into a multi-hour turn instead of only after it ends. on_event
    (heartbeat) additionally sees every complete NDJSON line mid-flight.
    cred_prefix (agora 发牌), non-empty: prepended to *message*."""
    message = _with_cred_prefix(message, cred_prefix)
    return _turn_streaming(
        ["opencode", "run", "--format", "json", "--title", title, message],
        cwd, timeout_s, sid_path, popen, session_targeted=False,
        on_event=on_event)

def continue_session(cwd, session_id, message, timeout_s=DEFAULT_TIMEOUT_S, run=subprocess.run,
                     on_event=None, cred_prefix=None, popen=subprocess.Popen):
    """Send *message* to session *session_id* and wait for the reply.
    cred_prefix (agora 发牌), non-empty: prepended to *message*."""
    message = _with_cred_prefix(message, cred_prefix)
    argv = ["opencode", "run", "--format", "json", "--session", session_id, message]
    if on_event is not None:
        return _turn_streaming(argv, cwd, timeout_s, None, popen,
                               session_targeted=True, on_event=on_event,
                               prefer_report=True)
    return _turn(argv, cwd, timeout_s, run, session_targeted=True,
                 prefer_report=True)


def activity_from_event(line_text):
    """One NDJSON line -> one-line 'what is it doing now' for the heartbeat
    note, or None when the line says nothing describable. Best-effort by
    contract: an unrecognized line never raises, the heartbeat just falls
    back to elapsed time (schema from a real `opencode run --format json`
    stream: tool parts carry part.tool + part.state.input, text parts
    carry part.text)."""
    try:
        event = json.loads(line_text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(event, dict):
        return None
    part = event.get("part")
    if not isinstance(part, dict):
        return None
    ptype = part.get("type")
    if ptype == "tool":
        name = str(part.get("tool") or "tool")
        state = part.get("state")
        hint = ""
        if isinstance(state, dict) and isinstance(state.get("input"), dict):
            for value in state["input"].values():
                if isinstance(value, str) and value.strip():
                    hint = value.strip().splitlines()[0][:80]
                    break
        return f"{name}: {hint}" if hint else name
    if ptype == "text" and isinstance(part.get("text"), str):
        stripped = part["text"].strip()
        if stripped:
            return stripped.splitlines()[0][:80]
    return None


def build_prompt(protocol_path, issue_block):
    """Thin prompt: point at the protocol file, embed the issue block.

    The real instructions live in PROTOCOL.md ("协议即文档") — never inline them.
    """
    absolute = os.path.abspath(protocol_path)
    return (
        f"Read and follow the protocol at {absolute} to handle the issue below.\n"
        f"\n"
        f"<issue>\n{issue_block}\n</issue>"
    )
