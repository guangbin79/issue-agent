#!/usr/bin/env python3
"""issue-agent 每日健康巡检（ag-723860）。

被 dotfiles 侧定时任务调用（定时与发邮件不归本脚本）：无参数执行，
stdout 输出中文可读摘要（每实例一节），退出码 0＝全部正常、
1＝发现任何问题——dotfiles 侧据此定邮件主题前缀。

实例发现：~/.config/*-issue-agent/config.json（服务名＝目录名）。
每实例检查：① systemctl --user is-active；② 近 24h journal 中
CRITICAL/Traceback/blocked 计数；③ 账本 ~/.local/state/<svc>/state.json
blocked/pending 计数（pending＝claim 后在办占位，只计数不计问题）；
④ execution_mode=agora 实例：agorad 可达性（POST /doctor，root 牌）
＋本服务任务（client_key 前缀 <svc>:issue-，按账本 agora_task_id 对账）
非终态任务数与最老 awaiting_external 年龄（均为信息项，不计问题）。

三项外部读取面（systemd/journal/agora）皆为注入 seam——测试零真实
systemctl/journalctl/agorad 依赖。纯 stdlib；零外网（agora 仅 localhost）。
"""

import json
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# agorad 任务终态口径（agorad report._TERMINAL）：merged/closed＝已合并，
# blocked/cancelled＝已拍人处置；todo/dispatched/running/done/
# verify_failed/reopened 均算在飞（done＝待并/待外部交付）。
_TERMINAL_STATES = frozenset({"merged", "closed", "blocked", "cancelled"})
_JOURNAL_KEYWORDS = ("CRITICAL", "Traceback", "blocked")


# ---- 缺省 seam 实现（真实环境） ----------------------------------------------

def _systemd_state(svc):
    """systemctl --user is-active <svc> 的状态词（active/inactive/failed/…）。"""
    try:
        proc = subprocess.run(["systemctl", "--user", "is-active", svc],
                              capture_output=True, text=True, timeout=10)
        return proc.stdout.strip() or f"unknown(rc={proc.returncode})"
    except (OSError, subprocess.SubprocessError) as exc:
        return f"unknown({type(exc).__name__})"


def _journal_text(svc):
    """近 24h 的 user journal 文本；不可读返回 None（信息项，不算问题）。"""
    try:
        proc = subprocess.run(["journalctl", "--user", "-u", svc,
                               "--since", "24 hours ago"],
                              capture_output=True, text=True, timeout=30)
        return proc.stdout if proc.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _agora_probe(agora_home, service_name, task_ids, timeout_s=3.0):
    """agorad 可达性＋任务面快照（root 牌，仅 localhost，超时短）。

    返回 {"ok": True, "tasks": {tid: {client_key, state, created,
    awaiting_external}}} 或 {"ok": False, "error": …}。错误文本只带
    异常类名/值——root 牌值永不入文本（服务端回显也照抹不了：根本不拼）。
    """
    try:
        agora_dir = Path(agora_home) / ".agora"
        service = json.loads(
            (agora_dir / "service.json").read_text(encoding="utf-8"))
        port = int(service["port"])
        if not 1 <= port <= 65535:
            raise ValueError(f"port out of range: {port}")
        token = (agora_dir / "root.token").read_text(
            encoding="utf-8").strip()
        if not token or not all(0x21 <= ord(c) <= 0x7E for c in token):
            raise ValueError("root token missing or illegal")
    except (OSError, ValueError, KeyError, TypeError) as exc:
        return {"ok": False, "error": f"service discovery failed: {exc}"}

    def call(path, body):
        request = urllib.request.Request(
            f"http://127.0.0.1:{port}{path}",
            data=json.dumps(body).encode("utf-8"), method="POST",
            headers={"Content-Type": "application/json",
                     "X-Agora-Token": token})
        with urllib.request.urlopen(request, timeout=timeout_s) as resp:
            return json.loads(resp.read().decode("utf-8"))

    try:
        call("/doctor", {})
        tasks = {}
        for tid in task_ids:
            row = call("/board", {"task": tid}).get("task")
            if not isinstance(row, dict):
                raise ValueError(f"/board {tid}: missing task row")
            delivery = row.get("delivery") or {}
            tasks[tid] = {
                "client_key": row.get("client_key") or "",
                "state": row.get("state") or "",
                "created": row.get("created") or 0.0,
                "awaiting_external":
                    delivery.get("status") == "awaiting_external",
            }
        return {"ok": True, "tasks": tasks}
    except (OSError, ValueError) as exc:  # HTTPError/JSONDecodeError 均是其子类
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}


# ---- 检查逻辑 ----------------------------------------------------------------

def _read_json(path):
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, ValueError):
        return None


def check_instance(svc, config_root, state_root, now, service_state,
                   journal_reader, agora_probe, emit):
    """检查一个实例：emit(line) 输出一节；返回该实例的问题描述列表。"""
    problems = []
    cfg = _read_json(config_root / svc / "config.json")
    mode = (cfg or {}).get("execution_mode") or "legacy"
    emit(f"== {svc}（{mode} 模式）==")

    state_word = service_state(svc)
    emit(f"服务状态: {state_word}")
    if state_word != "active":
        problems.append(f"服务状态 {state_word}")

    journal = journal_reader(svc)
    if journal is None:
        emit("近24h日志: 不可读")
    else:
        counts = {kw: sum(1 for line in journal.splitlines() if kw in line)
                  for kw in _JOURNAL_KEYWORDS}
        emit("近24h日志: " + " / ".join(f"{kw} {counts[kw]}"
                                        for kw in _JOURNAL_KEYWORDS))
        hot = {kw: n for kw, n in counts.items() if n}
        if hot:
            problems.append("近24h日志异常: "
                            + " / ".join(f"{kw} {n}" for kw, n in hot.items()))

    ledger_path = state_root / svc / "state.json"
    if not ledger_path.exists():
        blocked = pending = 0
        task_ids = []
        emit("账本: 无（尚未写过）")
    else:
        ledger = _read_json(ledger_path)
        issues = (ledger or {}).get("issues")
        if not isinstance(issues, dict):
            emit("账本: 不可读")
            blocked = pending = 0
            task_ids = []
            problems.append("账本不可读")
        else:
            entries = [e for e in issues.values() if isinstance(e, dict)]
            blocked = sum(1 for e in entries if e.get("status") == "blocked")
            pending = sum(1 for e in entries if e.get("status") == "pending")
            task_ids = sorted({e["agora_task_id"] for e in entries
                               if e.get("agora_task_id")})
            emit(f"账本: blocked {blocked} / pending {pending}")
    if blocked:
        problems.append(f"账本 blocked issue {blocked} 个")

    if mode == "agora":
        agora_home = (cfg or {}).get("agora_home") or ""
        result = agora_probe(agora_home, svc, task_ids)
        if not result.get("ok"):
            emit(f"agora: 不可达（{result.get('error', 'unknown')}）")
            problems.append(f"agora 不可达: {result.get('error', 'unknown')}")
        else:
            rows = [r for r in result["tasks"].values()
                    if r["client_key"].startswith(f"{svc}:issue-")]
            inflight = [r for r in rows
                        if r["state"] not in _TERMINAL_STATES]
            awaiting = [r for r in rows
                        if r["awaiting_external"] and r["created"] > 0]
            age = f"最老 awaiting_external {(now - min(r['created'] for r in awaiting)) / 3600:.1f} 小时" if awaiting else "无 awaiting_external"
            emit(f"agora: 可达；在飞任务 {len(inflight)}；{age}")
    return problems


def run(out=None, config_root=None, state_root=None, now=None,
        service_state=None, journal_reader=None, agora_probe=None):
    """巡检全部实例；返回进程退出码（0=全部正常，1=发现任何问题）。"""
    out = out if out is not None else sys.stdout
    now = time.time() if now is None else now
    service_state = service_state or _systemd_state
    journal_reader = journal_reader or _journal_text
    agora_probe = agora_probe or _agora_probe
    config_root = Path(config_root or Path.home() / ".config")
    state_root = Path(state_root or Path.home() / ".local" / "state")
    emit = lambda line: print(line, file=out, flush=True)  # noqa: E731

    services = sorted(p.parent.name
                      for p in config_root.glob("*-issue-agent/config.json"))
    problems = []
    print(f"issue-agent 每日健康巡检（{len(services)} 个实例）", file=out)
    if not services:
        problems.append("未发现任何 issue-agent 实例")
    for svc in services:
        try:  # 单实例失败不中断其余实例的检查（汇总后统一 rc）
            problems.extend(f"{svc}: {p}"
                            for p in check_instance(
                                svc, config_root, state_root, now,
                                service_state, journal_reader,
                                agora_probe, emit))
        except Exception as exc:  # noqa: BLE001 -- 巡检脚本须永远出全量报告
            emit(f"== {svc}（巡检异常 {type(exc).__name__}）==")
            problems.append(f"{svc}: 巡检异常 {type(exc).__name__}: {exc}")
        print(file=out)

    if problems:
        print("问题清单:", file=out)
        for p in problems:
            print(f"- {p}", file=out)
        print(f"共 {len(problems)} 项问题", file=out)
        return 1
    print("全部正常", file=out)
    return 0


if __name__ == "__main__":
    sys.exit(run())
