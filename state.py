"""Issue-agent ledger: state.json with atomic writes, .bak fallback, and
per-note-id idempotency.

The ledger is the single source of truth for which issues are tracked and
which note ids have already been consumed. Layout:

    {"router_session_id": str|null,        # root: persistent Router opencode
    #   session id (agora mode, plan Task 2); None/absent in legacy.
     "issues": {"<iid>": {
        "status": "adopted"|"active"|"done"|"paused"|"blocked"|"defer"|"pending",
        "session_id": str|null,
        "branch": str|null,
        "processed_note_ids": [int, ...],
        "last_note_id": int|null,          # derived: max(processed_note_ids)
        "claimed_at": ISO-8601 str|null,
        "defer_count": int,
        "review_push_pending_at": ISO-8601 str|null,
        #   ^ set ONLY by the review-phase abnormal-terminal path (gt-9f24):
        #     reconcile owes the branch ONE delayed re-push after the grace
        #     period; cleared by that push attempt or the next turn's claim.
        "report_probe_pending": bool,      # ag-a9af5e: armed when a fix reply
        #   posted without a §5 report (turn ended before the report was
        #   produced); the sweep probes the session within a 2-probe budget
        #   and re-posts the late report; cleared by delivery/claim/close.
        "report_probe_count": int,         # probe budget consumed (max 2)
        # --- agora fields (plan Task 3, 设计 §9.6) ---
        "agora_task_id": str|null,         # 最近一次 create 成功的 agora 任务
        "agora_status": str|null,          # monitor 同步的 agora 任务态 (T4/T5)
        "delivery_commit": str|null,       # 外部交付 commit (Task 4)
        "agora_pending": bool,             # 待派发标记：容量满/agora 不可用/
        #   建单-派发间中断；claim 留存，重试复用已持久化 goal/model
        "router_goal": str|null,           # 首次 create 前持久化（同 payload
        #   重试才能取回原任务，§8.1）
        "router_model": str|null,
        #   ^ legacy session_id 永不被 agora 覆盖（§9.4 每回合一新会话）。
    }}}

Write path is atomic (tmp + fsync + rename + dir fsync) and every successful
write promotes the previous good primary to .bak, so a crash mid-write never
loses the last good state. Read path falls back to .bak and finally to an
empty ledger with a warning callback (wired to the daemon journal).
"""

import json
import os
import threading
from datetime import datetime, timezone
from typing import Callable, Optional, Union

STATUSES = ("adopted", "active", "done", "paused", "blocked", "defer", "pending")
DEFAULT_CLAIM_TIMEOUT_S = 5500
PathLike = Union[str, "os.PathLike[str]"]

# 账本根字段（agora 模式，计划 Task 2）：持久语义 Router 的 opencode
# session id。放在根级而非任何 issue 条目——Router 跨 issue 共享一个会话
# （设计 §9.6）；issue 级的 agora 六字段扩展见上方条目 schema（Task 3）。
ROUTER_SESSION_KEY = "router_session_id"


def router_session_id(state: dict) -> Optional[str]:
    """Root-level persistent Router session id, or None.

    Garbage (missing/non-string/empty) degrades to None — a fresh session
    is cheaper than trusting a corrupt id."""
    value = state.get(ROUTER_SESSION_KEY) if isinstance(state, dict) else None
    return value if isinstance(value, str) and value else None


def _to_iso(value) -> str:
    if isinstance(value, str):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _parse_ts(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _new_issue(status: str) -> dict:
    return {
        "status": status,
        "session_id": None,
        "branch": None,
        "processed_note_ids": [],
        "last_note_id": None,
        "claimed_at": None,
        "defer_count": 0,
        "review_push_pending_at": None,
        "report_probe_pending": False,
        "report_probe_count": 0,
        "agora_task_id": None,
        "agora_status": None,
        "delivery_commit": None,
        "agora_pending": False,
        "router_goal": None,
        "router_model": None,
    }


def _normalize(state: dict) -> dict:
    """Validate and rebuild an in-memory ledger; ValueError on garbage."""
    if not isinstance(state, dict) or not isinstance(state.get("issues"), dict):
        raise ValueError("ledger root must be {'issues': {...}}")
    issues = {}
    for iid, raw in state["issues"].items():
        entry = raw if isinstance(raw, dict) else {}
        if entry.get("status") not in STATUSES:
            raise ValueError(f"issue {iid}: unknown status {entry.get('status')!r}")
        note_ids = sorted({int(n) for n in entry.get("processed_note_ids") or []})
        issues[str(int(iid))] = {
            "status": entry["status"],
            "session_id": entry.get("session_id"),
            "branch": entry.get("branch"),
            "processed_note_ids": note_ids,
            "last_note_id": note_ids[-1] if note_ids else None,
            "claimed_at": entry.get("claimed_at"),
            "defer_count": int(entry.get("defer_count") or 0),
            "review_push_pending_at": entry.get("review_push_pending_at"),
            "report_probe_pending": bool(entry.get("report_probe_pending")),
            "report_probe_count": int(entry.get("report_probe_count") or 0),
            # agora 六字段（设计 §9.6）：legacy 时代旧账缺键 → 默认值，
            # 不得因往返丢字段或报错。
            "agora_task_id": entry.get("agora_task_id"),
            "agora_status": entry.get("agora_status"),
            "delivery_commit": entry.get("delivery_commit"),
            "agora_pending": bool(entry.get("agora_pending")),
            "router_goal": entry.get("router_goal"),
            "router_model": entry.get("router_model"),
        }
    state["issues"] = issues
    return state


def load_state(path: PathLike, warn: Optional[Callable[[str], None]] = None) -> dict:
    """Load the ledger from *path*, falling back to *path*.bak.

    Fires *warn* whenever the primary is unusable (corrupt or missing);
    returns an empty ledger only when both copies are unusable.
    """
    primary, backup = os.fspath(path), os.fspath(path) + ".bak"
    errors = []
    loaded = None
    for candidate in (primary, backup):
        try:
            with open(candidate, encoding="utf-8") as fh:
                loaded = _normalize(json.load(fh))
            break
        except (OSError, ValueError) as exc:
            errors.append(f"{candidate}: {exc}")
    if loaded is not None:
        if errors and warn:  # primary unusable, backup saved us
            warn(f"primary ledger unreadable ({errors[0]}), restored from {backup}")
        return loaded
    if warn:
        warn("ledger unreadable (" + "; ".join(errors) + "), starting from empty ledger")
    return {"issues": {}}




def save_state(path: PathLike, state: dict) -> None:
    """Atomically persist *state*; promote the previous good primary to .bak."""
    path = os.fspath(path)
    payload = json.dumps(_normalize(dict(state)), ensure_ascii=False, indent=2)
    previous = _read_if_valid(path)
    _atomic_write(path, payload.encode("utf-8"))
    if previous is not None:
        _atomic_write(path + ".bak", previous)


def _read_if_valid(path: str) -> Optional[bytes]:
    """Previous primary bytes, but only if they parse as a valid ledger.

    A corrupt primary must never overwrite the last good .bak.
    """
    try:
        with open(path, "rb") as fh:
            raw = fh.read()
        _normalize(json.loads(raw.decode("utf-8")))
    except (OSError, ValueError):
        return None
    return raw


def _atomic_write(path: str, data: bytes) -> None:
    # 暂存名按写者唯一：共享单名在并发写者下互噬（后者 truncate 前者
    # 暂存，先 replace 者消费掉暂存名，后者 os.replace 抛
    # FileNotFoundError——os.replace 的原子性只覆盖单一名下的单写者）
    tmp = f"{path}.tmp.{os.getpid()}.{threading.get_ident()}"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, path)
    dir_fd = os.open(os.path.dirname(path) or ".", os.O_DIRECTORY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def adopt_existing(state: dict, open_issues_with_notes: dict) -> dict:
    """First-install baseline: mark untracked open issues as "adopted" and
    snapshot ALL their current note ids into processed_note_ids, so the next
    full reconcile replays zero historical notes.

    open_issues_with_notes: {iid: [note_id, ...]}  (int or str keys both fine)
    Issues already in the ledger are left untouched (they may be active).
    """
    issues = state.setdefault("issues", {})
    for iid, note_ids in open_issues_with_notes.items():
        key = str(int(iid))
        if key in issues:
            continue
        entry = _new_issue("adopted")
        entry["processed_note_ids"] = sorted({int(n) for n in note_ids})
        entry["last_note_id"] = entry["processed_note_ids"][-1] if entry["processed_note_ids"] else None
        issues[key] = entry
    return state


def mark_processed(state: dict, iid, note_ids) -> dict:
    """Record *note_ids* as consumed for issue *iid* (dedup, derives last)."""
    entry = state.setdefault("issues", {}).setdefault(str(int(iid)), _new_issue("active"))
    merged = set(entry["processed_note_ids"]) | {int(n) for n in note_ids}
    entry["processed_note_ids"] = sorted(merged)
    entry["last_note_id"] = entry["processed_note_ids"][-1] if merged else entry["last_note_id"]
    return state


def is_note_processed(state: dict, iid, note_id) -> bool:
    entry = state.get("issues", {}).get(str(int(iid)))
    return entry is not None and int(note_id) in entry["processed_note_ids"]


def claim(path: PathLike, iid, now) -> dict:
    """Claim-before-run: set status="pending" + claimed_at in a single write."""
    path = os.fspath(path)
    state = load_state(path)
    entry = state.setdefault("issues", {}).setdefault(str(int(iid)), _new_issue("pending"))
    entry["status"] = "pending"
    entry["claimed_at"] = _to_iso(now)
    save_state(path, state)
    return state


def claim_timeout_recycle(path: PathLike, now, timeout_s: int = DEFAULT_CLAIM_TIMEOUT_S) -> list:
    """Recycle claims older than *timeout_s* back to "defer".

    Returns the list of recycled iids; persists only when something changed.
    """
    path = os.fspath(path)
    state = load_state(path)
    now_dt = _parse_ts(_to_iso(now))
    recycled = []
    for iid, entry in state["issues"].items():
        if entry["status"] != "pending":
            continue
        try:
            age = (now_dt - _parse_ts(entry["claimed_at"])).total_seconds()
        except (TypeError, ValueError, AttributeError):
            # missing (None) / undecodable claim time: treat as stale
            age = timeout_s + 1
        if age > timeout_s:
            entry["status"] = "defer"
            entry["claimed_at"] = None
            entry["defer_count"] += 1
            recycled.append(iid)
    if recycled:
        save_state(path, state)
    return recycled
