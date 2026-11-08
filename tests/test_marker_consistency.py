"""Marker-consistency lock: the api client's note_marker and the fsm's
injected marker set must stay in lockstep — notes built by the client must
be filtered by the fsm given the SAME marker, and the default marker is a
stable constant (instances override it via config.note_marker, derived
from service_name). Drift between the two sites fails here first.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import api  # noqa: E402  (path setup must precede the import)
import fsm  # noqa: E402


def test_default_note_marker_is_stable_and_generic():
    assert api.NOTE_MARKER == "<!-- issue-agent -->"
    assert api.NOTE_MARKER in fsm._effective_markers((api.NOTE_MARKER,))


def test_client_default_marker_is_the_module_constant():
    client = api.GitLabClient("http://gitea", "pat")
    assert client.note_marker == api.NOTE_MARKER


def test_fsm_ignores_notes_built_with_client_marker():
    client = api.GitLabClient("http://gitea", "pat")
    event = {
        "action": "created",
        "issue": {"number": 5, "state": "open"},
        "comment": {"id": 111, "body": client.build_note_body("done, pushed branch")},
        "sender": {"login": "someone"},
    }
    ledger = {5: {"status": "active", "processed_note_ids": []}}
    decision = fsm.decide(event, ledger, markers=(client.note_marker,))
    assert decision["action"] == "ignore"


def test_fsm_ignores_notes_built_with_injected_marker():
    marker = "<!-- my-svc -->"
    client = api.GitLabClient("http://gitea", "pat", note_marker=marker)
    event = {
        "action": "created",
        "issue": {"number": 5, "state": "open"},
        "comment": {"id": 111, "body": client.build_note_body("done")},
        "sender": {"login": "someone"},
    }
    ledger = {5: {"status": "active", "processed_note_ids": []}}
    assert fsm.decide(event, ledger, markers=(marker,))["action"] == "ignore"


def test_universal_emoji_marker_filters_without_injection():
    event = {
        "action": "created",
        "issue": {"number": 5, "state": "open"},
        "comment": {"id": 111, "body": "🤖 quoted agent note"},
        "sender": {"login": "someone"},
    }
    ledger = {5: {"status": "active", "processed_note_ids": []}}
    assert fsm.decide(event, ledger)["action"] == "ignore"


def test_plain_comment_passes_with_injection_active():
    event = {
        "action": "created",
        "issue": {"number": 5, "state": "open"},
        "comment": {"id": 111, "body": "please fix the build"},
        "sender": {"login": "someone"},
    }
    ledger = {5: {"status": "active", "processed_note_ids": []}}
    assert fsm.decide(event, ledger, markers=("<!-- my-svc -->",))["action"] == "continue"
