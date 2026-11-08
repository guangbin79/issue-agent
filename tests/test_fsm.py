"""TDD tests for fsm.decide — the event→action state machine (Gitea payloads).

Every row of the event table is parametrized against Gitea 1.21 webhook
shapes ("issues" and "issue_comment" events only). fsm must stay pure:
no I/O, no mutation of inputs, no exceptions on malformed payloads.

Event-kind detection (binding): presence of a top-level "comment" key →
comment (issue_comment) event; otherwise → issue event. Gitea sends no
object_kind / object_attributes / noteable_type / system / confidential
fields — those were GitLab.
"""
import copy
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import fsm  # noqa: E402  (path setup must precede the import)

VALID_ACTIONS = {"new", "continue", "pause", "unpause", "close", "reopen",
                 "repost", "ignore"}

IN_FLIGHT_STATUSES = ["active", "paused", "defer", "pending", "blocked", "adopted"]

# Instance marker injected by the orchestrator (config.note_marker derived
# from service_name); the fsm only sees what it is handed.
TEST_MARKER = "<!-- test-svc -->"
TEST_MARKERS = (TEST_MARKER,)


def issue_event(action="opened", number=5):
    return {
        "action": action,
        "issue": {"number": number, "title": "build fails", "body": "logs attached",
                  "state": "open", "user": {"login": "baiguangbin"}},
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "baiguangbin"},
    }


def comment_event(body="please fix", number=5, state="open", comment_id=None,
                  action="created"):
    comment = {"body": body, "user": {"login": "someone"}}
    if comment_id is not None:
        comment["id"] = comment_id
    return {
        "action": action,
        "issue": {"number": number, "title": "build fails", "body": "logs attached",
                  "state": state, "user": {"login": "baiguangbin"}},
        "comment": comment,
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "someone"},
    }


def ledger_with(status, number=5, processed=None):
    return {number: {"status": status, "processed_note_ids": processed or []}}


CASES = [
    # -- issue events ---------------------------------------------------
    pytest.param(issue_event("opened"), {}, "new", id="open-untracked-new"),
    pytest.param(issue_event("opened"), ledger_with("active"), "ignore",
                 id="open-duplicate-delivery-active"),
    pytest.param(issue_event("opened"), ledger_with("done"), "ignore",
                 id="open-duplicate-delivery-done"),
    pytest.param(issue_event("closed"), {}, "close", id="close-untracked"),
    pytest.param(issue_event("reopened"), ledger_with("done"), "reopen",
                 id="reopen-tracked-done"),
    pytest.param(issue_event("reopened"), {}, "reopen", id="reopen-untracked"),
    pytest.param(issue_event("edited"), {}, "ignore", id="edited-ignored-untracked"),
    pytest.param(issue_event("edited"), ledger_with("active"), "ignore",
                 id="edited-ignored-active"),
    pytest.param(issue_event("assigned"), ledger_with("active"), "ignore",
                 id="unknown-issue-action-ignored"),

    # -- comment events: general rule --------------------------------------
    pytest.param(comment_event(), ledger_with("active"), "continue",
                 id="note-active-continue"),
    pytest.param(comment_event(), ledger_with("pending"), "continue",
                 id="note-pending-continue"),
    pytest.param(comment_event(), ledger_with("defer"), "continue",
                 id="note-defer-continue"),
    pytest.param(comment_event(), ledger_with("blocked"), "continue",
                 id="note-blocked-continue"),
    pytest.param(comment_event(), ledger_with("adopted"), "new",
                 id="note-adopted-wakes-new-session"),
    pytest.param(comment_event(), ledger_with("paused"), "ignore",
                 id="note-while-paused-ignored"),
    pytest.param(comment_event(), {}, "ignore", id="note-untracked-ignored"),
    pytest.param(comment_event(state="closed"), ledger_with("active"), "ignore",
                 id="note-on-closed-issue-ignored"),
    pytest.param(comment_event(action="edited"), ledger_with("active"), "ignore",
                 id="edited-comment-ignored"),
    pytest.param(comment_event(body="🤖 已接手 issue#5"), ledger_with("active"),
                 "ignore", id="agent-marker-emoji-ignored"),
    pytest.param(comment_event(body=f"done {TEST_MARKER}"),
                 ledger_with("active"), "ignore",
                 id="agent-marker-html-comment-ignored"),
    # accepted risk: a human comment quoting/replying with the marker is filtered
    pytest.param(comment_event(body="> 🤖 已接手 issue#5\n我的补充意见"),
                 ledger_with("active"), "ignore",
                 id="human-quote-with-marker-filtered-accepted-risk"),

    # -- comment events: per-comment-id idempotency -------------------------
    pytest.param(comment_event(comment_id=42), ledger_with("active", processed=[42]),
                 "ignore", id="note-id-already-processed-ignored"),
    pytest.param(comment_event(comment_id=42), ledger_with("active", processed=[41]),
                 "continue", id="note-id-not-yet-processed-continues"),

    # -- command comments: whole-line 暂停/pause, 恢复/resume ----------------
    pytest.param(comment_event(body="暂停"), ledger_with("active"), "pause",
                 id="cmd-pause-chinese"),
    pytest.param(comment_event(body="  暂停 \n"), ledger_with("active"), "pause",
                 id="cmd-pause-chinese-whitespace-stripped"),
    pytest.param(comment_event(body="pause"), ledger_with("active"), "pause",
                 id="cmd-pause-english"),
    pytest.param(comment_event(body="PAUSE"), ledger_with("active"), "pause",
                 id="cmd-pause-english-uppercase"),
    pytest.param(comment_event(body="Pause"), ledger_with("active"), "pause",
                 id="cmd-pause-english-mixedcase"),
    pytest.param(comment_event(body="暂停"), ledger_with("paused"), "pause",
                 id="cmd-pause-idempotent-while-paused"),
    pytest.param(comment_event(body="暂停"), ledger_with("adopted"), "pause",
                 id="cmd-pause-on-adopted-tracked"),
    pytest.param(comment_event(body="恢复"), ledger_with("paused"), "unpause",
                 id="cmd-resume-chinese-while-paused"),
    pytest.param(comment_event(body="resume"), ledger_with("paused"), "unpause",
                 id="cmd-resume-english-while-paused"),
    pytest.param(comment_event(body=" RESUME "), ledger_with("paused"), "unpause",
                 id="cmd-resume-english-whitespace-case"),
    pytest.param(comment_event(body="恢复"), ledger_with("active"), "unpause",
                 id="cmd-resume-while-active-idempotent"),
    pytest.param(comment_event(body="重发汇报"), ledger_with("active"), "repost",
                 id="cmd-repost-chinese"),
    pytest.param(comment_event(body=" 重发汇报\n"), ledger_with("active"), "repost",
                 id="cmd-repost-whitespace-stripped"),
    pytest.param(comment_event(body="重发汇报"), ledger_with("paused"), "repost",
                 id="cmd-repost-while-paused-same-discipline-as-pause-resume"),

    # -- command near-misses: containment must NOT trigger ------------------
    pytest.param(comment_event(body="这个暂停逻辑有 bug"), ledger_with("active"),
                 "continue", id="containment-pause-chinese-not-triggered"),
    pytest.param(comment_event(body="暂停一下"), ledger_with("active"),
                 "continue", id="partial-command-pause-not-triggered"),
    pytest.param(comment_event(body="please pause it"), ledger_with("active"),
                 "continue", id="containment-pause-english-not-triggered"),
    pytest.param(comment_event(body="恢复之前的讨论吧"), ledger_with("active"),
                 "continue", id="partial-command-resume-not-triggered"),
    pytest.param(comment_event(body="能不能重发汇报一下？"), ledger_with("active"),
                 "continue", id="containment-repost-not-triggered"),
    pytest.param(comment_event(body="重发汇报吧"), ledger_with("active"),
                 "continue", id="partial-command-repost-not-triggered"),
]

# close overrides ANY in-flight ledger status
CASES += [
    pytest.param(issue_event("closed"), ledger_with(status), "close",
                 id=f"close-overrides-{status}")
    for status in IN_FLIGHT_STATUSES
]


@pytest.mark.parametrize("event,ledger,expected_action", CASES)
def test_event_table(event, ledger, expected_action):
    decision = fsm.decide(event, ledger, markers=TEST_MARKERS)
    assert decision["action"] == expected_action
    assert set(decision) == {"action", "iid", "reason"}
    assert isinstance(decision["reason"], str) and decision["reason"]
    assert decision["action"] in VALID_ACTIONS


MALFORMED = [
    pytest.param(None, {}, id="none-payload"),
    pytest.param("not-a-dict", {}, id="string-payload"),
    pytest.param([], {}, id="list-payload"),
    pytest.param({}, {}, id="empty-dict"),
    pytest.param({"ref": "refs/heads/master", "commits": []}, {},
                 id="non-issue-payload"),
    pytest.param({"action": "opened"}, {}, id="issue-missing-issue-block"),
    pytest.param({"action": "opened", "issue": "junk"}, {},
                 id="issue-block-not-a-dict"),
    pytest.param({"issue": {"number": 5}}, {}, id="issue-missing-action"),
    pytest.param({"action": "opened", "issue": {"title": "x"}}, {},
                 id="issue-missing-number"),
    pytest.param({"action": "created", "comment": "junk",
                  "issue": {"number": 5, "state": "open"}}, {},
                 id="comment-block-not-a-dict"),
    pytest.param({"action": "created", "comment": {"body": "x"}}, {},
                 id="comment-missing-issue-block"),
    pytest.param({"comment": {"body": "x"},
                  "issue": {"number": 5, "state": "open"}}, {},
                 id="comment-missing-action"),
    pytest.param({"action": "created", "comment": {"id": 9},
                  "issue": {"number": 5, "state": "open"}}, {},
                 id="comment-missing-body"),
    pytest.param({"action": "created", "comment": {"body": "x"},
                  "issue": {"number": 5, "state": "open", "title": "t"}}, {},
                 id="comment-body-not-a-string"),
    pytest.param({"action": "created", "comment": {"body": "x"}, "issue": {}}, {},
                 id="comment-missing-number"),
    pytest.param({"action": "created", "comment": {"body": "x"},
                  "issue": {"number": 5}}, {},
                 id="comment-missing-state"),
    pytest.param({"action": "created", "comment": None,
                  "issue": {"number": 5, "state": "open"}}, {},
                 id="comment-block-none"),
]


@pytest.mark.parametrize("event,ledger", MALFORMED)
def test_malformed_payload_is_ignored_not_raised(event, ledger):
    decision = fsm.decide(event, ledger)
    assert decision["action"] == "ignore"
    assert isinstance(decision["reason"], str)


# -- hostile issue.number types: gated once in decide(), never raise --------


@pytest.mark.parametrize("number", [5.0, "5", "5\nforged journal line", None, True,
                                   [5], {"n": 5}])
def test_non_int_issue_number_is_ignored_for_issue_and_comment_events(number):
    # R2: raw iid values used to flow into journal f-strings and ledger
    # lookups (unhashable dict -> TypeError, violating never-raise).
    assert fsm.decide(issue_event(number=number), {})["action"] == "ignore"
    assert fsm.decide(comment_event(number=number),
                      ledger_with("active"))["action"] == "ignore"


def test_actionable_decisions_carry_the_iid():
    assert fsm.decide(issue_event("opened", number=7), {})["iid"] == 7
    assert fsm.decide(comment_event(number=9),
                      ledger_with("active", number=9))["iid"] == 9


def test_ledger_keys_tolerate_string_iids():
    # state.json round-trips dict keys as strings; the view may use either form
    ledger = {"5": {"status": "active", "processed_note_ids": []}}
    assert fsm.decide(comment_event(number=5), ledger)["action"] == "continue"


def test_decide_does_not_mutate_its_inputs():
    event = comment_event(body="暂停", comment_id=42)
    ledger = ledger_with("active", processed=[1])
    event_before, ledger_before = copy.deepcopy(event), copy.deepcopy(ledger)
    fsm.decide(event, ledger)
    assert event == event_before
    assert ledger == ledger_before


def test_fsm_module_stays_pure_no_io_imports():
    source = Path(fsm.__file__).read_text(encoding="utf-8")
    assert not re.findall(r"^\s*(import|from)\s+\w", source, re.MULTILINE), \
        "fsm.py must not import anything (pure stdlib-free decision logic)"
