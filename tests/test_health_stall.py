"""health_digest 僵滞三查＋守门自动重启 tests（ag-754139）。
注入缝：零真实 systemctl/journalctl。Run: python3 -m pytest tests/test_health_stall.py -q
"""

import io
import json
import time
from datetime import datetime, timezone

import health_digest

NOW = 1_800_000_000.0  # 固定 epoch；账本时间戳由 iso() 派生


def iso(epoch):
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def jline(epoch, msg="tick"):
    """journalctl short 行（英文月缩写、空格补日；epoch 恒落 1 月）。"""
    lt = time.localtime(epoch)
    return (f"Jan {lt.tm_mday:2d} {lt.tm_hour:02d}:"
            f"{lt.tm_min:02d}:{lt.tm_sec:02d} host demo[1]: {msg}")


def make_instance(root, entries, cfg_extra=None, name="demo-issue-agent"):
    cfg_dir = root / ".config" / name
    cfg_dir.mkdir(parents=True, exist_ok=True)
    (cfg_dir / "config.json").write_text(json.dumps(
        {"execution_mode": "legacy", **(cfg_extra or {})}), encoding="utf-8")
    state_dir = root / ".local" / "state" / name
    state_dir.mkdir(parents=True, exist_ok=True)
    (state_dir / "state.json").write_text(
        json.dumps({"issues": entries}), encoding="utf-8")


def make_restarter(ok=True):
    calls = []
    def restarter(svc):
        calls.append(svc)
        return ok
    return restarter, calls


def run_digest(root, **kw):
    restarter, calls = make_restarter()
    kw.setdefault("restarter", restarter)
    kw.setdefault("service_state", lambda svc: "active")
    kw.setdefault("journal_reader", lambda svc: "")
    out = io.StringIO()
    rc = health_digest.run(out=out, now=NOW, config_root=root / ".config",
                           state_root=root / ".local" / "state", repos=(),
                           **kw)
    return rc, out.getvalue(), calls


def p1_entry(age_s=7 * 3600, status="pending"):
    return {str(7): {"status": status, "claimed_at": iso(NOW - age_s)}}


def test_p1_stall_threshold_and_terminal(tmp_path):
    """7h 非终态 claim＝点名计问题；1h 在办不报；终态旧 claim 是残留不报。"""
    make_instance(tmp_path, p1_entry(age_s=7 * 3600))
    rc, text, _ = run_digest(tmp_path)
    assert rc == 1 and "issue 7" in text and "7.0 小时" in text
    assert "status=pending" in text and "【需要处理】" in text
    assert "回合僵滞" in text.split("行动清单", 1)[-1]
    make_instance(tmp_path, p1_entry(age_s=3600))
    rc, text, _ = run_digest(tmp_path)
    assert rc == 0 and "全部正常" in text
    make_instance(tmp_path, p1_entry(age_s=7 * 3600, status="done"))
    rc, text, _ = run_digest(tmp_path)
    assert rc == 0 and "僵滞" not in text


def test_p2_review_debt_threshold(tmp_path):
    """置位 2h＝欠账计问题；10min 前刚置位不报；终态留置标（有意不清，
    归人处置）不报——否则永久误报。"""
    make_instance(tmp_path, {str(8): {
        "status": "active", "review_push_pending_at": iso(NOW - 7200)}})
    rc, text, _ = run_digest(tmp_path)
    assert rc == 1
    assert "欠账滞留" in text and "issue 8" in text and "2.0 小时" in text
    make_instance(tmp_path, {str(8): {
        "status": "active", "review_push_pending_at": iso(NOW - 600)}})
    rc, _, _ = run_digest(tmp_path)
    assert rc == 0
    make_instance(tmp_path, {str(8): {
        "status": "done", "review_push_pending_at": iso(NOW - 7200)}})
    rc, text, _ = run_digest(tmp_path)
    assert rc == 0 and "欠账滞留" not in text


# --- P3 守门自动重启（autorestart×journal 矩阵＋边界） -------------------------

def test_p3_restarts_when_all_gates_pass(tmp_path):
    """矩阵格 on×journal 旧：四条全满足 → 恰一次重启并计问题；重启失败
    亦如实计问题；journalctl「-- No entries --」哨兵＝真静默，照样重启。"""
    make_instance(tmp_path, p1_entry(), cfg_extra={"autorestart": True})
    rc, text, calls = run_digest(
        tmp_path, journal_reader=lambda svc: jline(NOW - 7200) + "\n")
    assert rc == 1 and calls == ["demo-issue-agent"]
    assert "已自动重启 demo-issue-agent（原因：P1/P2 僵滞）" in text
    assert "已自动重启" in text.split("行动清单", 1)[-1]
    make_instance(tmp_path, p1_entry(), cfg_extra={"autorestart": True})
    rc, _, calls = run_digest(
        tmp_path, journal_reader=lambda svc: "-- No entries --\n")
    assert rc == 1 and calls == ["demo-issue-agent"]
    restarter, calls = make_restarter(ok=False)
    rc, text, _ = run_digest(tmp_path, restarter=restarter)
    assert rc == 1 and calls == ["demo-issue-agent"]
    assert "自动重启失败 demo-issue-agent（原因：P1/P2 僵滞）" in text


def test_p3_no_restart_when_journal_fresh(tmp_path):
    """矩阵格 on×journal 新：daemon 还在说话，不许杀（P1 照常计问题）。"""
    make_instance(tmp_path, p1_entry(), cfg_extra={"autorestart": True})
    rc, text, calls = run_digest(
        tmp_path, journal_reader=lambda svc: jline(NOW - 60) + "\n")
    assert rc == 1 and calls == [] and "已自动重启" not in text


def test_p3_no_restart_without_autorestart(tmp_path):
    """矩阵格 off×journal 新旧：键缺失/false/非严格 true 均与现状一致。"""
    journals = (lambda svc: jline(NOW - 7200) + "\n",
                lambda svc: jline(NOW - 60) + "\n")
    for extra in (None, {"autorestart": False}, {"autorestart": "true"}):
        for journal in journals:
            make_instance(tmp_path, p1_entry(), cfg_extra=extra)
            rc, text, calls = run_digest(
                tmp_path, journal_reader=journal)
            assert rc == 1 and calls == []
            assert "已自动重启" not in text


def test_p3_gate_blocked_by_inactive_or_unreadable_journal(tmp_path):
    """inactive 挂死归 systemd；journal 不可读/全不可解＝无法确认静默——均不杀。"""
    make_instance(tmp_path, p1_entry(), cfg_extra={"autorestart": True})
    rc, text, calls = run_digest(
        tmp_path, service_state=lambda svc: "inactive",
        journal_reader=lambda svc: jline(NOW - 7200) + "\n")
    assert rc == 1 and calls == [] and "服务状态 inactive" in text
    rc, _, calls = run_digest(tmp_path, journal_reader=lambda svc: None)
    assert rc == 1 and calls == []
    rc, _, calls = run_digest(tmp_path, journal_reader=lambda svc: "乱码\n")
    assert rc == 1 and calls == []


def test_p3_restart_exactly_once_with_multiple_stalls(tmp_path):
    entries = {str(i): {"status": "pending", "claimed_at": iso(NOW - 7 * 3600)}
               for i in (7, 8, 9)}
    make_instance(tmp_path, entries, cfg_extra={"autorestart": True})
    rc, _, calls = run_digest(tmp_path)
    assert rc == 1 and calls == ["demo-issue-agent"]  # 每实例每 run 至多一次


def test_p3_healthy_with_autorestart_never_restarts(tmp_path):
    """回归：无僵滞时 autorestart 开着也零重启，rc 契约不变。"""
    make_instance(tmp_path, p1_entry(age_s=3600, status="active"),
                  cfg_extra={"autorestart": True})
    rc, text, calls = run_digest(
        tmp_path, journal_reader=lambda svc: jline(NOW - 60) + "\n")
    assert rc == 0 and calls == [] and "全部正常" in text
