"""Event state machine for the Gitea issue agent — pure functions, zero I/O.

``decide(event, ledger)`` maps ONE Gitea webhook payload plus a read-only
ledger view to a decision object. It never raises, never mutates its inputs,
never touches files/network/logging. All side effects belong to the
orchestrator that consumes the decision.

Payload contract (Gitea 1.21; events registered: "issues" + "issue_comment")
----------------------------------------------------------------------
Event kind: a top-level ``comment`` key → comment (issue_comment) event;
otherwise → issue event. There is no object_kind / object_attributes /
noteable_type / system flag — those were GitLab.

    issues payload:        {"action": "opened"|"closed"|"reopened"|"edited",
                            "issue": {"number", "title", "body", "state"},
                            "sender": {"login"}}
    issue_comment payload: {"action": "created"|"edited",
                            "issue": {"number", "state", ...},
                            "comment": {"id", "body", "user": {"login"}},
                            "sender": {"login"}}

Confidential guard: DELETED. Gitea has no confidential issues, so the plan's
"confidential never triggers" Must-NOT is trivially satisfied with no code.

Decision schema (STABLE — orchestrator contract)
------------------------------------------------
    {"action": <str>, "iid": <int|None>, "reason": <short str>}

Actions and what the orchestrator must do for each:

    "new"      start a NEW opencode session for iid; ledger status -> active
               (also the wake-up path for an "adopted" issue receiving a
               qualifying comment)
    "continue" continue the existing session for iid (one turn)
    "repost"   「重发汇报」lightweight repost: the daemon re-posts the last
               report (or a not-found note) from the existing session —
               NO opencode turn; falls back to a normal continue turn when
               its preconditions (fix branch pushed + review loop ran) fail
    "pause"    ledger status -> paused; no turn; do NOT interrupt an
               in-flight turn
    "unpause"  ledger status -> active; send the 🤖 ack note; no turn
    "close"    ledger status -> done, unconditionally overriding any
               in-flight status (active/paused/defer/pending/blocked/adopted)
    "reopen"   ledger status -> active; no session created (the next
               qualifying comment continues)
    "redispatch" panel disposal of a BLOCKED agora task (ag-1c7315, whole
               line 重试/解决冲突/重派): the orchestrator re-dispatches
               the SAME task; anything else -> journal-only ignore
    "cancel_task" panel disposal (ag-1c7315, whole line 取消任务/取消):
               task_cancel on the blocked task; same ignore rule
    "approve"   panel approval (ag-72646a, whole line 批准 <8-hex 裁定id>):
                the comment-approval executor validates (whitelist/comment
                read-back/ruling store/consumed/idempotent board recheck)
                and executes the published task_update ruling with root
                credentials — pure code, zero LLM
    "ignore"   nothing

Inputs
------
``event`` is the parsed webhook JSON (issues / issue_comment / garbage).
``ledger`` is a plain dict ``{iid: {"status": ..., "processed_note_ids": [...]}}``
read from state.json; keys may be int or str (JSON round-trip) — lookup
tolerates both. Statuses: adopted|active|done|paused|blocked|defer|pending.

Rules (in evaluation order)
---------------------------
1.  Malformed payload (non-dict, missing required field) -> ignore, no raise.
    Required for issue events: action, issue.number. Required for comment
    events: action, issue.number, comment.body. The comment id is optional
    (dedup is best-effort). issue.number must be a real int (bool/float/
    str/None/containers -> "malformed issue number" ignore) — gated once in
    decide() so downstream code can assume a genuine int.
2.  Issue events by action:
    opened + untracked -> "new"; opened + already tracked -> ignore
    (idempotent skip of duplicate delivery); closed -> "close"; reopened ->
    "reopen"; edited (or anything else) -> ignore.
3.  Comment events qualify only when: action == "created" ("edited" is
    ignored), body carries no agent marker (universal 🤖 plus the injected
    instance markers), issue state "open", iid tracked
    (adopted counts as tracked), comment id not already processed.
    Then, before the general rule, whole-line commands (body.strip() equal):
    暂停 / pause (case-insensitive) -> "pause"; 恢复 / resume
    (case-insensitive) -> "unpause"; 重发汇报 -> "repost"; 重试 / 解决冲突 /
    重派 -> "redispatch"; 取消任务 / 取消 -> "cancel_task"; 批准 <8 位小写
    hex> -> "approve" (ag-72646a). Containment
    ("这个暂停逻辑有 bug") must NOT trigger.
    General rule: ledger status paused -> ignore; adopted -> "new";
    anything else -> "continue".

Accepted risk: marker filtering is containment-based, so a HUMAN comment
that quotes an agent note (🤖 or the instance's <!-- ... --> marker
anywhere in the body) is filtered too. Locked by test.
"""

# Universal marker: every instance prefixes notes with the bot emoji, so it
# filters here unconditionally. The instance-specific HTML-comment marker
# (config.note_marker, service_name-derived) is INJECTED by the orchestrator
# via decide(..., markers=(marker,)) — None keeps only the universal set.
_AGENT_MARKERS = ("🤖",)

# 双机认领协议 v2：认领评论嵌的机器标记（8 位 hex machine_id）。
# 纯字符串解析（本模块零 import 契约）：<!--claim:machine=<8+位hex>-->
_CLAIM_TAG = "<!--claim:machine="
_HEX = set("0123456789abcdef")
# 三态扫描（协议第 5 条；叠加不替换既有 status 语义）
IN_FLIGHT, DORMANT, TRIGGERED = "in_flight", "dormant", "triggered"


def claim_machine(body):
    """认领评论里的 machine_id；非认领评论返回 None。纯函数。

    长度帽 16（install 生成 8 位 hex；超长 hex＝敌意 body 的持久化
    DoS 放大面，C 册终闸判例）——超帽按无标记处理。"""
    if not isinstance(body, str):
        return None
    start = body.find(_CLAIM_TAG)
    if start < 0:
        return None
    rest = body[start + len(_CLAIM_TAG):]
    end = rest.find("-->")
    machine = rest[:end].strip().lower() if end > 0 else ""
    return machine if machine and len(machine) <= 16 \
        and set(machine) <= _HEX else None


def claim_comments(comments):
    """[(comment_id, machine_id)] — 墙上全部认领评论，按 id 升序。"""
    out = []
    for c in comments or []:
        if not isinstance(c, dict):
            continue
        machine = claim_machine(c.get("body"))
        cid = c.get("id")
        if machine and isinstance(cid, int):
            out.append((cid, machine))
    return sorted(out)


def triage(entry, fresh_note_ids):
    """我名下 issue 的三态分类（协议第 5 条）。

    in_flight＝本地 pending 在飞（无未消评论、非停靠）；dormant＝终报后
    水位未动（永不重开）／paused 永不自动唤醒／blocked 人工出口／done；
    triggered＝水位新动或 pending 未消（停靠/崩溃恢复），占槽。
    """
    if not isinstance(entry, dict):
        return DORMANT
    status = entry.get("status")
    if status == "pending":
        if fresh_note_ids or entry.get("agora_pending"):
            return TRIGGERED  # pending 未消（崩溃恢复）／停靠待派
        return IN_FLIGHT
    if status == "defer":
        return TRIGGERED  # 待重试（回收/停靠/超时）——现有 defer-retry 车道
    if status in ("active", "adopted"):
        return TRIGGERED if fresh_note_ids else DORMANT
    return DORMANT  # paused / blocked / done：永不自动唤醒


def decide(event, ledger, markers=None):
    """Map one webhook payload + ledger view to a decision dict. Pure.

    *markers* (optional iterable of strings) adds instance-specific agent
    note markers to the universal _AGENT_MARKERS filter set.
    """
    if not isinstance(event, dict):
        return _ignore()
    # Single choke point for the issue number: a hostile (valid-signature)
    # payload can carry ANY JSON type in issue.number — anything but a real
    # int (bool excluded) is dropped here, so raw values never reach journal
    # strings, the ledger, or fix_branch.
    issue = event.get("issue")
    iid = issue.get("number") if isinstance(issue, dict) else None
    if isinstance(iid, bool) or not isinstance(iid, int):
        return _ignore("malformed issue number")
    if "comment" in event:
        return _decide_comment(event, ledger, _effective_markers(markers))
    return _decide_issue(event, ledger)


def _effective_markers(markers):
    """Universal emoji markers + the injected instance markers (deduped)."""
    extra = tuple(m for m in (markers or ()) if m)
    return tuple(dict.fromkeys(_AGENT_MARKERS + extra))


def _decide_issue(event, ledger):
    issue = event.get("issue")
    if not isinstance(issue, dict):
        return _ignore("malformed issue payload")
    action = event.get("action")
    iid = issue.get("number")
    if not action or iid is None:
        return _ignore("malformed issue payload")
    if action == "edited":
        return _ignore("issue edited", iid)
    if action == "opened":
        if _entry(ledger, iid) is not None:
            return _ignore("duplicate open delivery", iid)
        return {"action": "new", "iid": iid, "reason": "new issue opened"}
    if action == "closed":
        return {"action": "close", "iid": iid, "reason": "issue closed"}
    if action == "reopened":
        return {"action": "reopen", "iid": iid, "reason": "issue reopened"}
    return _ignore("unsupported issue action: %r" % (action,), iid)


def _decide_comment(event, ledger, agent_markers):
    comment = event.get("comment")
    issue = event.get("issue")
    if not isinstance(comment, dict) or not isinstance(issue, dict):
        return _ignore("malformed comment payload")
    action = event.get("action")
    iid = issue.get("number")
    body = comment.get("body")
    if not action or iid is None or not isinstance(body, str):
        return _ignore("malformed comment payload", iid)
    if action != "created":
        return _ignore("comment not created: %r" % (action,), iid)
    if any(marker in body for marker in agent_markers):
        return _ignore("agent marker", iid)
    state = issue.get("state")
    if not state:
        return _ignore("malformed comment payload", iid)
    if state != "open":
        return _ignore("issue not open", iid)
    entry = _entry(ledger, iid)
    if entry is None:
        return _ignore("untracked issue", iid)
    note_id = comment.get("id")
    if note_id is not None and note_id in entry.get("processed_note_ids", ()):
        return _ignore("duplicate comment delivery", iid)
    command = _command(body)
    if command:
        return {"action": command, "iid": iid, "reason": "%s command" % command}
    status = entry.get("status")
    if status == "paused":
        return _ignore("paused", iid)
    if status == "adopted":
        return {"action": "new", "iid": iid, "reason": "adopted issue woken by comment"}
    return {"action": "continue", "iid": iid, "reason": "new comment"}


def _command(body):
    """Whole-line command match only; english forms case-insensitive."""
    line = body.strip()
    if line == "暂停" or line.lower() == "pause":
        return "pause"
    if line == "恢复" or line.lower() == "resume":
        return "unpause"
    if line == "重发汇报":
        return "repost"
    if line in ("重试", "解决冲突", "重派"):
        return "redispatch"
    if line in ("取消任务", "取消"):
        return "cancel_task"
    # ag-72646a：评论批准指令「批准 <8 位小写 hex 裁定 id>」——严格全行
    # （T3 处置卡同源 id）；rid 由执行器从回读正文提取，fsm 只分类不携参
    if line.startswith("批准 ") and len(line) == 11 \
            and set(line[3:]) <= _HEX:
        return "approve"
    return None


def _entry(ledger, iid):
    """Ledger lookup tolerating int or str keys (JSON round-trip)."""
    if not isinstance(ledger, dict):
        return None
    for key in (iid, str(iid)):
        entry = ledger.get(key)
        if isinstance(entry, dict):
            return entry
    return None


def _ignore(reason="ignored event", iid=None):
    return {"action": "ignore", "iid": iid, "reason": reason}
