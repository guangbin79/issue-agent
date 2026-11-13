"""Ledger (state.py) tests: atomic writes, .bak fallback, bootstrap baseline,
per-note-id idempotency, claim/recycle.

Run: python3 -m pytest tests/test_state.py -q   (from scripts/issue-agent/)
"""

import json
from datetime import datetime, timedelta

import state as state_mod
from state import (
    adopt_existing,
    claim,
    claim_timeout_recycle,
    DEFAULT_CLAIM_TIMEOUT_S,
    is_note_processed,
    load_state,
    mark_processed,
    save_state,
)

T0 = "2026-09-26T12:00:00+00:00"


def test_roundtrip_save_load(tmp_path):
    path = tmp_path / "state.json"
    state = {"issues": {}}
    mark_processed(state, 7, [1, 2])
    save_state(path, state)
    loaded = load_state(path)
    assert is_note_processed(loaded, 7, 1)
    assert loaded["issues"]["7"]["last_note_id"] == 2


def test_bak_holds_previous_good_state(tmp_path):
    path = tmp_path / "state.json"
    save_state(path, {"issues": {"1": {"status": "done"}}})
    save_state(path, {"issues": {"1": {"status": "active"}}})
    bak = json.loads((tmp_path / "state.json.bak").read_text())
    assert bak["issues"]["1"]["status"] == "done"
    assert json.loads(path.read_text())["issues"]["1"]["status"] == "active"
    assert not list(tmp_path.glob("*.tmp"))  # no tmp leftovers


def test_interrupted_write_falls_back_to_bak(tmp_path):
    path = tmp_path / "state.json"
    good = {"issues": {"3": {"status": "active", "processed_note_ids": [9]}}}
    save_state(path, good)
    save_state(path, good)  # second write: .bak now holds the previous good state
    # simulate a crash mid-write: truncated JSON lands on the primary
    path.write_text('{"issues": {"3": {"stat')
    warnings = []
    loaded = load_state(path, warn=warnings.append)
    assert is_note_processed(loaded, 3, 9)
    assert loaded["issues"]["3"]["status"] == "active"
    assert warnings  # journal warning fired even though .bak saved us


def test_corrupt_primary_and_bak_returns_empty_ledger_with_warning(tmp_path):
    path = tmp_path / "state.json"
    save_state(path, {"issues": {"1": {"status": "done"}}})
    save_state(path, {"issues": {"1": {"status": "active"}}})  # bak now exists
    path.write_text("not json at all")
    (tmp_path / "state.json.bak").write_text("{ also broken")
    warnings = []
    loaded = load_state(path, warn=warnings.append)
    assert loaded == {"issues": {}}
    assert len(warnings) == 1


def test_fresh_install_missing_files_returns_empty_ledger(tmp_path):
    warnings = []
    loaded = load_state(tmp_path / "state.json", warn=warnings.append)
    assert loaded == {"issues": {}}


def test_adopt_existing_snapshots_all_note_ids(tmp_path):
    state = {"issues": {}}
    adopt_existing(state, {11: [101, 102, 103], 12: []})
    assert state["issues"]["11"]["status"] == "adopted"
    assert state["issues"]["11"]["processed_note_ids"] == [101, 102, 103]
    assert state["issues"]["11"]["last_note_id"] == 103
    assert state["issues"]["11"]["session_id"] is None
    assert state["issues"]["12"]["processed_note_ids"] == []
    assert state["issues"]["12"]["last_note_id"] is None


def test_second_full_reconcile_triggers_zero_turns(tmp_path):
    """Anti-replay: after the first-install baseline, a full replay of the same
    open issues must produce zero unprocessed notes (zero turns)."""
    path = tmp_path / "state.json"
    open_issues = {11: [101, 102], 12: [201]}
    state = load_state(path)
    adopt_existing(state, open_issues)
    save_state(path, state)

    # second full reconcile: same data, fresh process
    state2 = load_state(path)
    adopt_existing(state2, open_issues)
    save_state(path, state2)
    unprocessed = [
        (iid, note)
        for iid, notes in open_issues.items()
        for note in notes
        if not is_note_processed(state2, iid, note)
    ]
    assert unprocessed == []


def test_adopt_does_not_clobber_tracked_issues(tmp_path):
    state = {"issues": {}}
    adopt_existing(state, {11: [1]})
    state["issues"]["11"]["status"] = "active"  # human commented, agent running
    adopt_existing(state, {11: [1, 2], 12: [5]})
    assert state["issues"]["11"]["status"] == "active"  # not reset to adopted
    assert state["issues"]["12"]["status"] == "adopted"


def test_mark_processed_dedups_and_derives_last_note_id():
    state = {"issues": {}}
    mark_processed(state, 5, [7, 3, 3, 9])
    mark_processed(state, 5, [3, 11])
    assert state["issues"]["5"]["processed_note_ids"] == [3, 7, 9, 11]
    assert state["issues"]["5"]["last_note_id"] == 11
    assert is_note_processed(state, 5, 3)
    assert not is_note_processed(state, 5, 4)
    assert not is_note_processed(state, 999, 3)  # unknown issue is unprocessed


def test_claim_persists_pending_and_claimed_at_in_single_write(tmp_path):
    path = tmp_path / "state.json"
    state = {"issues": {}}
    adopt_existing(state, {21: [1]})
    save_state(path, state)

    claim(path, 21, T0)

    # one on-disk read proves both fields landed together
    on_disk = json.loads(path.read_text())
    assert on_disk["issues"]["21"]["status"] == "pending"
    assert on_disk["issues"]["21"]["claimed_at"] == T0
    assert not list(tmp_path.glob("*.tmp"))


def test_claim_timeout_recycle_moves_stale_claims_to_defer(tmp_path):
    path = tmp_path / "state.json"
    state = {"issues": {}}
    adopt_existing(state, {31: [1], 32: [1], 33: [1]})
    save_state(path, state)
    claim(path, 31, T0)  # stale claim
    now = (datetime.fromisoformat(T0)
           + timedelta(seconds=DEFAULT_CLAIM_TIMEOUT_S + 1)).isoformat()
    claim(path, 32, now)  # fresh claim, must survive
    state = load_state(path)
    state["issues"]["33"]["status"] = "active"  # not claimed, must be untouched
    save_state(path, state)

    recycled = claim_timeout_recycle(path, now)  # 1s past the configured claim timeout

    assert recycled == ["31"]
    final = load_state(path)
    assert final["issues"]["31"]["status"] == "defer"
    assert final["issues"]["31"]["claimed_at"] is None
    assert final["issues"]["31"]["defer_count"] == 1
    assert final["issues"]["32"]["status"] == "pending"
    assert final["issues"]["33"]["status"] == "active"


def test_load_normalizes_derived_fields(tmp_path):
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"issues": {"8": {
        "status": "paused", "session_id": "s1", "branch": "issue/8-fix",
        "processed_note_ids": [4, 2, 2], "last_note_id": 999, "claimed_at": None,
        "defer_count": 0,
    }}}))
    entry = load_state(path)["issues"]["8"]
    assert entry["processed_note_ids"] == [2, 4]
    assert entry["last_note_id"] == 4  # derived wins over stored drift
    assert entry["branch"] == "issue/8-fix"


def test_review_push_pending_at_survives_roundtrip(tmp_path):
    """The delayed-review-push mark is part of the stable entry schema:
    _normalize must keep it, or every load/save cycle would silently disarm
    the reconcile grace check (gt-9f24)."""
    path = tmp_path / "state.json"
    st = {"issues": {}}
    adopt_existing(st, {51: [1]})
    st["issues"]["51"]["status"] = "active"
    st["issues"]["51"]["review_push_pending_at"] = "2026-10-20T09:46:04+00:00"
    save_state(path, st)
    loaded = load_state(path)
    assert (loaded["issues"]["51"]["review_push_pending_at"]
            == "2026-10-20T09:46:04+00:00")


def test_report_probe_fields_survive_roundtrip(tmp_path):
    """The report-probe fields are part of the stable entry schema (ag-a9af5e):
    _normalize must keep them across a save->load cycle, or every cycle would
    silently disarm an armed probe and erase its consumed budget."""
    path = tmp_path / "state.json"
    st = {"issues": {}}
    adopt_existing(st, {52: [1]})
    st["issues"]["52"]["status"] = "active"
    st["issues"]["52"]["report_probe_pending"] = True
    st["issues"]["52"]["report_probe_count"] = 1
    save_state(path, st)
    loaded = load_state(path)
    assert loaded["issues"]["52"]["report_probe_pending"] is True
    assert loaded["issues"]["52"]["report_probe_count"] == 1
    fresh = adopt_existing({"issues": {}}, {53: [1]})["issues"]["53"]
    assert fresh["report_probe_pending"] is False
    assert fresh["report_probe_count"] == 0


# --- agora ledger fields (plan Task 3, 设计 §9.6 六字段) ---------------------


def test_agora_fields_survive_save_load_and_legacy_session_is_untouched(tmp_path):
    """计划锚例：agora 六字段随 save/load 往返；legacy session_id 原样保留
    （agora worker sid 不落 session_id——设计 §9.4 每回合一新会话）。"""
    path = tmp_path / "state.json"
    st = {"issues": {}}
    adopt_existing(st, {7: [3]})
    entry = st["issues"]["7"]
    entry["status"] = "active"
    entry["session_id"] = "legacy-sid"
    entry["agora_task_id"] = "ag-123"
    entry["agora_status"] = "awaiting_external"
    entry["delivery_commit"] = "a" * 40
    entry["agora_pending"] = True
    entry["router_goal"] = "g"
    entry["router_model"] = "m"
    save_state(path, st)
    loaded = load_state(path)["issues"]["7"]
    assert loaded["session_id"] == "legacy-sid"
    assert loaded["agora_task_id"] == "ag-123"
    assert loaded["agora_status"] == "awaiting_external"
    assert loaded["delivery_commit"] == "a" * 40
    assert loaded["agora_pending"] is True
    assert loaded["router_goal"] == "g"
    assert loaded["router_model"] == "m"


def test_agora_fields_default_in_new_issue_and_normalize(tmp_path):
    """六字段进 _new_issue（新条目默认）与 _normalize（legacy 时代旧账加载
    补默认）——缺键旧账不得在往返中凭空丢字段或报错。"""
    fresh = adopt_existing({"issues": {}}, {61: [1]})["issues"]["61"]
    assert fresh["agora_task_id"] is None
    assert fresh["agora_status"] is None
    assert fresh["delivery_commit"] is None
    assert fresh["agora_pending"] is False
    assert fresh["router_goal"] is None
    assert fresh["router_model"] is None

    path = tmp_path / "state.json"
    path.write_text(json.dumps({"issues": {"62": {"status": "active"}}}))
    normed = load_state(path)["issues"]["62"]
    assert normed["agora_task_id"] is None
    assert normed["agora_status"] is None
    assert normed["delivery_commit"] is None
    assert normed["agora_pending"] is False
    assert normed["router_goal"] is None
    assert normed["router_model"] is None


def test_state_docstring_documents_the_agora_fields():
    """四处同步之四：模块 docstring 的 schema 文档必须列出六字段，否则
    schema 说明与实现漂移。"""
    for key in ("agora_task_id", "agora_status", "delivery_commit",
                "agora_pending", "router_goal", "router_model"):
        assert key in state_mod.__doc__, f"docstring missing {key}"


def test_atomic_write_survives_concurrent_writers(tmp_path, monkeypatch):
    """verify 红（ag-8f3133）：共享单一 .tmp 暂存名在并发写者下互噬——
    后写者 truncate 掉前写者暂存，先 replace 者消费掉暂存名，后者
    os.replace 抛 FileNotFoundError，原子写契约破灭。暂存名必须按写者
    唯一，两路并发 save 都得成功、终值是二者之一。"""
    import itertools
    import os
    import threading

    path = str(tmp_path / "state.json")
    fsync_calls = itertools.count()
    gate = threading.Barrier(2, timeout=5)
    real_fsync = os.fsync

    def gated_fsync(fd):
        if next(fsync_calls) < 2:  # 恰好拦住双写者的首次暂存 fsync
            gate.wait()            # 两边都暂存完毕才放行 replace
        real_fsync(fd)

    monkeypatch.setattr(state_mod.os, "fsync", gated_fsync)
    errors = []

    def writer(tag):
        try:
            for _ in range(20):
                state_mod._atomic_write(
                    path, json.dumps({"tag": tag}).encode("utf-8"))
        except OSError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=writer, args=(tag,))
               for tag in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert json.loads((tmp_path / "state.json").read_text())["tag"] in "ab"
