#!/usr/bin/env python3
"""issue-agent 每日健康巡检（ag-723860）。

被 dotfiles 侧定时任务调用（定时与发邮件不归本脚本）：无参数执行，
stdout 输出中文可读摘要（每实例一节），退出码 0＝全部正常、
1＝发现任何问题——dotfiles 侧据此定邮件主题前缀。

实例发现：~/.config/*-issue-agent/config.json（服务名＝目录名）。
每实例检查：① systemctl --user is-active；② 近 24h journal 中
CRITICAL/Traceback/blocked/dropped 计数；③ 账本 ~/.local/state/<svc>/state.json
blocked/pending 计数（pending＝claim 后在办占位，只计数不计问题）；
④ execution_mode=agora 实例：agorad 可达性（POST /doctor，root 牌）
＋本服务任务（client_key 前缀 <svc>:issue-，按账本 agora_task_id 对账）
在飞分桶计数（健康/排队/卡死）与最老 awaiting_external 年龄；blocked
卡死逐单点名（tid/标题/delivery.error/年龄）计问题。
末尾全局检查⑤：固定清单本地消费仓（cron/daemon 读本地文件，agora
合并只推远端不拉＝功能静默空转）逐仓 git fetch 后比 HEAD..@{u}，
落后或 fetch 失败＝问题；dirty 只附注。

外部读取面（systemd/journal/agora/git）皆为注入 seam 或 tmp 仓可测——
测试零真实 systemctl/journalctl/agorad 依赖。纯 stdlib；agora 仅
localhost；fetch 仅 GitHub 远端。
"""

import http.client
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

# agorad 任务终态口径（agorad report._TERMINAL）：merged/closed＝已合并，
# blocked/cancelled＝已拍人处置。本巡检故意只认 merged/closed/cancelled
# 为终态：blocked 仍需人处置，入「在飞·卡死」桶逐单点名计问题——
# 与 agora_monitor._TERMINAL_STATES（blocked 仍占轮询位）口径不同。
_TERMINAL_STATES = frozenset({"merged", "closed", "cancelled"})
_JOURNAL_KEYWORDS = ("CRITICAL", "Traceback", "blocked", "dropped")

# 本地消费仓固定清单（cron/daemon 读本地工作副本；agora 合并只推远端，
# 本地不拉＝改动静默空转）——加仓只改这里一处。
_LOCAL_CONSUMER_REPOS = (
    "~/.dotfiles",
    "~/issue-agent",
    "~/.dotfiles/.tmux/plugins/tmux-agent-indicator",
)


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
                              capture_output=True, text=True,
                              errors="replace", timeout=30)
        return proc.stdout if proc.returncode == 0 else None
    except (OSError, subprocess.SubprocessError):
        return None


def _agora_probe(agora_home, service_name, task_ids, timeout_s=3.0):
    """agorad 可达性＋任务面快照（root 牌，仅 localhost，超时短）。

    返回 {"ok": True, "tasks": {tid: {client_key, state, created,
    title, error, awaiting_external}}} 或 {"ok": False, "error": …}。错误文本只带
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
            payload = call("/board", {"task": tid})
            if not isinstance(payload, dict):  # 200 但协议外响应体（null/[]）
                raise ValueError(f"/board {tid}: non-dict response")
            row = payload.get("task")
            if not isinstance(row, dict):
                raise ValueError(f"/board {tid}: missing task row")
            delivery = row.get("delivery") or {}
            tasks[tid] = {
                "client_key": row.get("client_key") or "",
                "state": row.get("state") or "",
                "created": row.get("created") or 0.0,
                "title": row.get("title") or "",
                "error": delivery.get("error") or "",
                "awaiting_external":
                    delivery.get("status") == "awaiting_external",
            }
        return {"ok": True, "tasks": tasks}
    except (OSError, ValueError,
            http.client.HTTPException) as exc:  # IncompleteRead 非 OSError 子类
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
    if cfg is None:  # glob 命中却读不出＝真实健康问题，不能静默降级 legacy
        emit("config: 不可读")
        problems.append("config.json 不可读")
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
        agora_home = ((cfg or {}).get("agora_home")
                      or os.environ.get("AGORA_HOME", ""))  # 对齐 config.py 口径
        result = agora_probe(agora_home, svc, task_ids)
        if not result.get("ok"):
            emit(f"agora: 不可达（{result.get('error', 'unknown')}）")
            problems.append(f"agora 不可达: {result.get('error', 'unknown')}")
        else:
            rows = {tid: r for tid, r in result["tasks"].items()
                    if r["client_key"].startswith(f"{svc}:issue-")}
            inflight = {tid: r for tid, r in rows.items()
                        if r["state"] not in _TERMINAL_STATES}
            blocked = sorted(
                ((tid, r) for tid, r in inflight.items()
                 if r["state"] == "blocked"),
                key=lambda kv: kv[1]["created"])
            queued = sum(1 for r in inflight.values() if r["state"] == "done")
            healthy = len(inflight) - len(blocked) - queued
            awaiting = [r for r in rows.values()
                        if r["awaiting_external"] and r["created"] > 0]
            age = (f"最老 awaiting_external "
                   f"{max(0.0, now - min(r['created'] for r in awaiting)) / 3600:.1f} 小时"
                   if awaiting else "无 awaiting_external")
            emit(f"agora: 可达；在飞任务 {len(inflight)}"
                 f"（健康 {healthy} / 排队 {queued} / 卡死 {len(blocked)}）；{age}")
            for tid, r in blocked:  # 卡死逐单点名——混在计数里不可辨是事故根源
                title = r["title"][:20]
                err = r["error"] or "无 delivery.error"
                task_age = (f"年龄 {max(0.0, now - r['created']) / 3600:.1f} 小时"
                            if r["created"] > 0 else "年龄未知")
                emit(f"  - {tid} {title}：{err}；{task_age}")
                problems.append(f"agora blocked: {tid} {title}：{err}；{task_age}")
    return problems


def _git(repo, *args, timeout_s=30):
    """在 repo 跑 git；返回 (rc, stdout)。OSError/超时归一为 rc≠0。"""
    try:
        proc = subprocess.run(["git", "-C", str(repo), *args],
                              capture_output=True, text=True, timeout=timeout_s)
        return proc.returncode, proc.stdout
    except (OSError, subprocess.SubprocessError):
        return 1, ""


def check_local_repos(repos, emit):
    """检查本地消费仓滞后：fetch 后比 HEAD..@{u}；返回问题描述列表。

    落后或 fetch 失败＝问题（单仓失败不中断其余）；dirty 只附注。
    """
    problems = []
    for entry in repos:
        repo = Path(entry).expanduser()
        rc, _ = _git(repo, "fetch", "-q", timeout_s=120)
        if rc != 0:
            emit(f"{entry}: fetch 失败")
            problems.append(f"本地消费仓 {entry}: fetch 失败")
            continue
        dirty = ("（dirty）"
                 if _git(repo, "status", "--porcelain")[1].strip() else "")
        rc, out = _git(repo, "rev-list", "--count", "HEAD..@{u}")
        if rc != 0:
            emit(f"{entry}: 上游对比失败{dirty}")
            problems.append(f"本地消费仓 {entry}: 上游对比失败")
        elif out.strip() == "0":
            emit(f"{entry}: 同步{dirty}")
        else:
            emit(f"{entry}: 本地落后 {out.strip()} 提交{dirty}")
            problems.append(
                f"本地消费仓 {entry}: 本地落后 {out.strip()} 提交")
    return problems


def run(out=None, config_root=None, state_root=None, now=None,
        service_state=None, journal_reader=None, agora_probe=None, repos=None):
    """巡检全部实例；返回进程退出码（0=全部正常，1=发现任何问题）。"""
    out = out if out is not None else sys.stdout
    now = time.time() if now is None else now
    service_state = service_state or _systemd_state
    journal_reader = journal_reader or _journal_text
    agora_probe = agora_probe or _agora_probe
    repos = tuple(_LOCAL_CONSUMER_REPOS if repos is None else repos)
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

    if repos:
        print("本地消费仓:", file=out)
        problems.extend(check_local_repos(repos, emit))
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
