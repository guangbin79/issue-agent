#!/usr/bin/env python3
"""issue-agent 每日健康邮（ag-723860；行动分级重设计 ag-0f95bb）。

被 dotfiles 侧定时任务调用（定时与发邮件不归本脚本）：无参数执行，
stdout 输出中文可读摘要——首行执行摘要「结论：…」一眼定调，随后是
带行动分级标签的行动清单，原始计数详情降级到清单之后的详情区。
退出码 0＝无「需要处理」项、1＝有任何「需要处理」——dotfiles 侧据此
定邮件主题前缀（「知悉即可/无需处理」不计 rc）。

行动分级（ag-0f95bb，可测试判据）：
- 无需处理＝自愈类：journal 异常计数而账本/agora 面无现行问题→历史
  余音，24h 窗口滑出即自动消失（最迟明晨）；
- 知悉即可＝正常业务态：账本 blocked 且 delivery_commit 在案＝修复
  已就绪等 PR 合并（等人工合并不是故障，不计 rc——有意变更）；
- 需要处理＝真故障：服务非 active、config/账本不可读、blocked 无
  delivery_commit、agora 不可达、在飞卡死、本地消费仓落后/fetch 失败，
  每项附具体建议动作。

僵滞三查（ag-754139，查「活着但僵了」）：P1 非终态 claimed_at 超 6h
（shortcut: 心跳断流无外部 API 面，并入 P1 静默语义）；P2
review_push_pending_at 置位超 1h（2×缺省宽限）；P3 守门自动重启＝
autorestart==true＋服务 active＋P1/P2 命中＋journal 近 30 分钟静默时
systemctl --user restart（每实例每 run 至多一次）。

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
from datetime import datetime, timezone
from pathlib import Path

# agorad 任务终态口径（agorad report._TERMINAL）：merged/closed＝已合并，
# blocked/cancelled＝已拍人处置。本巡检故意只认 merged/closed/cancelled
# 为终态：blocked 仍需人处置，入「在飞·卡死」桶逐单点名计问题
# （口径与 agora_monitor._TERMINAL_STATES 一致，动机不同——那是轮询
# 名额释放，这是巡检可见性）。
_TERMINAL_STATES = frozenset({"merged", "closed", "cancelled"})
_JOURNAL_KEYWORDS = ("CRITICAL", "Traceback", "blocked", "dropped")

# 行动分级标签（ag-0f95bb）：rc 只认需要处理。
_NEED, _KNOW, _AUTO = "需要处理", "知悉即可", "无需处理"

# 账本终态口径（state.py docstring schema）：done/paused/blocked/defer＝
# 回合已收口或已拍人处置；adopted/active/pending＝回合可能在办。
_LEDGER_TERMINAL = frozenset({"done", "paused", "blocked", "defer"})
_STALL_CLAIM_S = 6 * 3600     # P1：非终态 claimed_at 年龄阈值
_REVIEW_DEBT_S = 2 * 1800     # P2：review_push_pending_at＝2×缺省宽限
_RESTART_SILENCE_S = 30 * 60  # P3：journal 静默窗口（还在说话不许杀）
# journalctl short 格式行首固定英文月缩写（_journal_text 钉 LC_ALL=C）
_EN_MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


def _iid_sort_key(iid):
    """账本 issue 键排序：数字键数值序，异形键（isdigit 真而 int() 抛、
    超长数字串）殿后按字典序——排序键不得抛异常中断该实例检查。"""
    try:
        return (0, int(iid))
    except ValueError:
        return (1, str(iid))

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


def _systemd_restart(svc):
    """systemctl --user restart <svc>；返回是否成功（P3 注入缝）。

    timeout 120s ≥ systemd 缺省 TimeoutStopSec=90s——慢停单元的 stop
    阶段走完才算结论，避免在途重启被误报失败。"""
    try:
        proc = subprocess.run(["systemctl", "--user", "restart", svc],
                              capture_output=True, timeout=120)
        return proc.returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


def _journal_text(svc):
    """近 24h 的 user journal 文本；不可读返回 None（信息项，不算问题）。

    LC_ALL=C 钉死英文时间戳/月名——journalctl 行首随进程 locale 本地化
    （zh_CN 下输出「11月 15 …」，实测 systemd 249），下游 _journal_quiet
    按英文口径解析；消息体本身不受影响，关键词计数口径不变。"""
    try:
        proc = subprocess.run(["journalctl", "--user", "-u", svc,
                               "--since", "24 hours ago"],
                              capture_output=True, text=True,
                              errors="replace", timeout=30,
                              env={**os.environ, "LC_ALL": "C"})
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

def _parse_iso(value):
    """ISO-8601 时间串 → epoch 秒；不可解返回 None（对齐 state._parse_ts）。"""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (TypeError, ValueError):
        return None


def _journal_quiet(journal, now, window_s=_RESTART_SILENCE_S):
    """近 window_s 内 journal 是否无新行（P3 条件④）。

    None（不可读）＝无法确认静默，返回 False——守门不放行。「--」前缀
    行是 journalctl 哨兵/分隔（-- No entries -- / -- Reboot --），先滤掉：
    零条目的全静默 daemon 恰是 P3 首要目标，不得被哨兵行挡住。行首按
    journalctl short 口径解析（_journal_text 已钉英文月名；缺年份按当前
    年补——跨年旧行算成未来→判不静默，方向安全）；滤后为空＝确证
    静默，非空却全不可解＝无法确认，同 None 不放行。
    """
    if journal is None:
        return False
    lines = [ln for ln in journal.splitlines()
             if ln.strip() and not ln.startswith("--")]
    year = time.localtime(now).tm_year
    parsed = False
    for line in lines:
        try:
            mon = _EN_MONTHS.index(line[:3]) + 1
            dt = datetime(year, mon, int(line[4:6]), int(line[7:9]),
                          int(line[10:12]), int(line[13:15]))
        except ValueError:  # 无时间戳行（多行消息续行等）不计新
            continue
        parsed = True
        if time.mktime(dt.timetuple()) > now - window_s:
            return False
    return parsed or not lines


def _read_json(path):
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        return raw if isinstance(raw, dict) else None
    except (OSError, ValueError):
        return None


def check_instance(svc, config_root, state_root, now, service_state,
                   journal_reader, agora_probe, emit, restarter):
    """检查一个实例：emit(line) 写详情节；返回行动分级项 [(标签, 一行说明)]。"""
    items = []
    stalled = False  # P1/P2 任一命中（P3 守门输入＋journal 同源判据）
    cfg = _read_json(config_root / svc / "config.json")
    if cfg is None:  # glob 命中却读不出＝真实健康问题，不能静默降级 legacy
        items.append((_NEED, f"{svc}: config.json 不可读——对照可读实例恢复"
                       f" ~/.config/{svc}/config.json"))
    mode = (cfg or {}).get("execution_mode") or "legacy"
    emit(f"== {svc}（{mode} 模式）==")

    state_word = service_state(svc)
    emit(f"服务状态: {state_word}")
    if state_word != "active":
        items.append((_NEED, f"{svc}: 服务状态 {state_word}"
                       f"——systemctl --user status {svc} 查原因，"
                       f"必要时 systemctl --user restart {svc}"))

    journal = journal_reader(svc)
    journal_hot = {}
    if journal is None:
        emit("近24h日志: 不可读")
    else:
        counts = {kw: sum(1 for line in journal.splitlines() if kw in line)
                  for kw in _JOURNAL_KEYWORDS}
        emit("近24h日志: " + " / ".join(f"{kw} {counts[kw]}"
                                        for kw in _JOURNAL_KEYWORDS))
        journal_hot = {kw: n for kw, n in counts.items() if n}

    # 账本/agora 面条目（journal 分级依赖其结果，统一拼在 journal 项之后）
    tail_items = []
    ledger_path = state_root / svc / "state.json"
    ledger_unreadable = False
    blocked_nodc, blocked_dc, pending, task_ids = [], [], 0, []
    if not ledger_path.exists():
        emit("账本: 无（尚未写过）")
    else:
        ledger = _read_json(ledger_path)
        issues = (ledger or {}).get("issues")
        if not isinstance(issues, dict):
            emit("账本: 不可读")
            ledger_unreadable = True
        else:
            entries = [(iid, e) for iid, e in issues.items()
                       if isinstance(e, dict)]
            pending = sum(1 for _, e in entries
                          if e.get("status") == "pending")
            blocked_ids = [iid for iid, e in entries
                           if e.get("status") == "blocked"]
            blocked_dc = sorted(
                (iid for iid, e in entries
                 if e.get("status") == "blocked" and e.get("delivery_commit")),
                key=_iid_sort_key)
            blocked_nodc = sorted(set(blocked_ids) - set(blocked_dc),
                                  key=_iid_sort_key)
            task_ids = sorted({e["agora_task_id"] for _, e in entries
                               if e.get("agora_task_id")})
            emit(f"账本: blocked {len(blocked_ids)} / pending {pending}")

            # P1 回合僵滞（非终态 claim>6h＝收口方僵死）＋P2 欠账滞留
            # （review_push>2×宽限未清）；时间不可解视同超期
            def stale(value, limit_s, head, tail=""):
                if not value:
                    return False
                ts = _parse_iso(value)
                age_s = now - ts if ts is not None else limit_s + 1
                if age_s <= limit_s:
                    return False
                when = f"{age_s / 3600:.1f} 小时" if ts is not None else "年龄未知"
                msg = f"{head} {when}{tail}"
                emit(f"  - {msg}")
                tail_items.append((_NEED, msg))
                return True

            for iid, e in sorted(issues.items(), key=lambda kv: _iid_sort_key(kv[0])):
                # 终态跳过两查：旧 claim 是残留；review_push 标在终态条目
                # 由 _delayed_review_push 有意留置（归人处置），报＝永久误报
                if not isinstance(e, dict) or e.get("status") in _LEDGER_TERMINAL:
                    continue
                stalled |= stale(e.get("claimed_at"), _STALL_CLAIM_S,
                                 f"{svc}: 回合僵滞 issue {iid} 认领",
                                 f" 未收口（status={e.get('status')}）"
                                 "——daemon 僵滞，可开 autorestart 自愈")
                stalled |= stale(e.get("review_push_pending_at"), _REVIEW_DEBT_S,
                                 f"{svc}: 欠账滞留 issue {iid} review_push"
                                 " 置位", " 未推——查 daemon reconcile 是否停摆")
    if ledger_unreadable:
        tail_items.append((_NEED, f"{svc}: 账本不可读——检查 {ledger_path}"
                          " 是否损坏，必要时以 .bak 恢复"))
    if blocked_nodc:
        nums = " ".join(f"#{i}" for i in blocked_nodc)
        tail_items.append((_NEED, f"{svc}: blocked issue {len(blocked_nodc)} 个"
                          f"无交付在案（{nums}）——需人工处置：见各 issue 评论"
                          "中的求助说明，合并已推分支/重试/关单"))
    if blocked_dc:
        nums = " ".join(f"#{i}" for i in blocked_dc)
        tail_items.append((_KNOW, f"{svc}: blocked issue {len(blocked_dc)} 个"
                          f"修复已就绪等 PR 合并（{nums}）——正常业务态，"
                          "人工合并后消失"))

    agora_face_problem = False
    if mode == "agora":
        agora_home = ((cfg or {}).get("agora_home")
                      or os.environ.get("AGORA_HOME", ""))  # 对齐 config.py 口径
        result = agora_probe(agora_home, svc, task_ids)
        if not result.get("ok"):
            emit(f"agora: 不可达（{result.get('error', 'unknown')}）")
            agora_face_problem = True
            tail_items.append((_NEED, f"{svc}: agora 不可达"
                          f"（{result.get('error', 'unknown')}）——查 "
                          f"{agora_home or '~/plaza'}/.agora/service.json"
                          " 进程是否存活"))
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
                tail_items.append((_NEED, f"{svc}: agora 卡死 {tid} {title}："
                              f"{err}；{task_age}——面板评论『重试』重派"
                              "或『取消任务』归档"))
            agora_face_problem = bool(blocked)

    # journal 分级判据：账本/agora 面有现行问题＝同源「需要处理」，
    # 否则 24h 窗口余音＝「无需处理」（自愈时点：最迟明晨）。
    if journal_hot:
        hot_text = " / ".join(f"{kw} {n}" for kw, n in journal_hot.items())
        if ledger_unreadable or blocked_nodc or agora_face_problem or stalled:
            items.append((_NEED, f"{svc}: 近24h日志异常（{hot_text}）——与现行"
                          "账本/agora 问题同源，处置对应「需要处理」项后"
                          "随之消失"))
        else:
            items.append((_AUTO, f"{svc}: 近24h日志历史余音（{hot_text}）——24h"
                          " 窗口滑出即自动消失，最迟明晨不再出现"))
    items.extend(tail_items)

    # P3 守门自动重启（结构上单点执行＝每实例每 run 至多一次）：四条全
    # 满足才动——①autorestart 显式 true ②active（inactive 归 systemd）
    # ③P1/P2 命中 ④journal 近 30 分钟静默（还在说话＝在办合法回合不许杀）。
    if ((cfg or {}).get("autorestart") is True and state_word == "active"
            and stalled and _journal_quiet(journal, now)):
        word = "已自动重启" if restarter(svc) else "自动重启失败"
        emit(f"{word} {svc}（原因：P1/P2 僵滞）")
        items.append((_NEED, f"{svc}: {word}（原因：P1/P2 僵滞）"
                      "——僵滞已处置，观察明晨报告是否复现"))
    return items


def _git(repo, *args, timeout_s=30):
    """在 repo 跑 git；返回 (rc, stdout, stderr)。

    OSError/超时归一为 rc≠0；decode 容错（quotepath=false＋非 UTF-8
    文件名不得让巡检崩溃，对齐 _journal_text 的 errors="replace"）。
    """
    try:
        proc = subprocess.run(["git", "-C", str(repo), *args],
                              capture_output=True, text=True,
                              errors="replace", timeout=timeout_s)
        return proc.returncode, proc.stdout, proc.stderr
    except (OSError, subprocess.SubprocessError):
        return 1, "", ""


def _err_tail(err):
    """stderr 首个非空行（git 病因行，截 80 字符，带括号）；空则空串。"""
    lines = [ln.strip() for ln in err.splitlines() if ln.strip()]
    return f"（{lines[0][:80]}）" if lines else ""


def check_local_repos(repos, emit):
    """检查本地消费仓滞后：fetch 后比 HEAD..@{u}；返回行动分级项列表。

    落后或 fetch 失败＝「需要处理」（单仓失败不中断其余）；dirty 只附注。
    """
    items = []
    for entry in repos:
        repo = Path(entry).expanduser()
        rc, _, err = _git(repo, "fetch", "-q", timeout_s=120)
        if rc != 0:
            hint = _err_tail(err)
            emit(f"{entry}: fetch 失败{hint}")
            items.append((_NEED, f"本地消费仓 {entry}: fetch 失败{hint}"
                          f"——查网络/凭证：git -C {repo} fetch -v"))
            continue
        dirty = ("（dirty）"
                 if _git(repo, "status", "--porcelain")[1].strip() else "")
        rc, out, err = _git(repo, "rev-list", "--count", "HEAD..@{u}")
        if rc != 0:
            hint = _err_tail(err)
            emit(f"{entry}: 上游对比失败{hint}")
            items.append((_NEED, f"本地消费仓 {entry}: 上游对比失败{hint}"
                          f"——查上游配置：git -C {repo} branch -vv"))
        elif out.strip() == "0":
            emit(f"{entry}: 同步{dirty}")
        else:
            emit(f"{entry}: 本地落后 {out.strip()} 提交{dirty}")
            items.append((_NEED, f"本地消费仓 {entry}: 本地落后 {out.strip()}"
                          f" 提交——git -C {repo} pull（本地不拉，"
                          "cron/daemon 读旧版＝改动静默空转）"))
    return items


def run(out=None, config_root=None, state_root=None, now=None,
        service_state=None, journal_reader=None, agora_probe=None, repos=None,
        restarter=None):
    """巡检全部实例；返回进程退出码（0=无「需要处理」项，1=有）。

    输出结构（ag-0f95bb）：首行执行摘要「结论：…」→ 行动清单（分级
    标签行）→ 详情区（各实例节与本地消费仓原始计数）。
    """
    out = out if out is not None else sys.stdout
    now = time.time() if now is None else now
    service_state = service_state or _systemd_state
    journal_reader = journal_reader or _journal_text
    agora_probe = agora_probe or _agora_probe
    restarter = restarter or _systemd_restart
    repos = tuple(_LOCAL_CONSUMER_REPOS if repos is None else repos)
    config_root = Path(config_root or Path.home() / ".config")
    state_root = Path(state_root or Path.home() / ".local" / "state")

    services = sorted(p.parent.name
                      for p in config_root.glob("*-issue-agent/config.json"))
    items = []          # (标签, 一行说明) —— 行动清单
    details = []        # 详情区行（emit 面向详情缓冲）
    if not services:
        items.append((_NEED, "未发现任何 issue-agent 实例——确认 "
                      "~/.config/*-issue-agent/ 部署是否还在"))
    for svc in services:
        try:  # 单实例失败不中断其余实例的检查（汇总后统一 rc）
            items.extend(check_instance(
                svc, config_root, state_root, now,
                service_state, journal_reader, agora_probe, details.append,
                restarter))
        except Exception as exc:  # noqa: BLE001 -- 巡检脚本须永远出全量报告
            details.append(f"== {svc}（巡检异常 {type(exc).__name__}）==")
            items.append((_NEED, f"{svc}: 巡检异常 {type(exc).__name__}: {exc}"
                          "——把该节输出发给维护者"))
        details.append("")

    if repos:
        details.append("本地消费仓:")
        items.extend(check_local_repos(repos, details.append))
        details.append("")

    need = [text for grade, text in items if grade == _NEED]
    know = [text for grade, text in items if grade == _KNOW]
    auto = [text for grade, text in items if grade == _AUTO]
    verdict = (f"结论：{len(need)} 项需要处理（首项："
               f"{need[0].split('——', 1)[0]}）") if need else "结论：无需处理"
    print(verdict, file=out)
    print(f"issue-agent 每日健康巡检（{len(services)} 个实例）", file=out)
    if items:
        print(file=out)
        print("行动清单:", file=out)
        for grade, text in items:
            print(f"- 【{grade}】{text}", file=out)
        print(f"共需要处理 {len(need)}、知悉 {len(know)}、无需 {len(auto)}",
              file=out)
    print(file=out)
    for line in details:
        print(line, file=out)
    if not items:
        print("全部正常", file=out)
    return 1 if need else 0


if __name__ == "__main__":
    sys.exit(run())
