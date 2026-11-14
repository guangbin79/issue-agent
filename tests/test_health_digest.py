# ruff: noqa: E402 -- imports are ordered around the stdlib-http preload shim below
"""health_digest.py tests: fake instances under tmp roots, injected seams.

Seams (per 任务页 ag-723860 contract): service-state probe, journal reader,
agora probe — tests never touch real systemctl/journalctl/agorad. The one
real-path test spins the repo's standard recording HTTP server (same pattern
as tests/test_agora_client.py) to exercise the DEFAULT agora probe end to
end: service.json/root.token discovery, X-Agora-Token header, /doctor +
/board detail parsing.

Run: python3 -m pytest tests/test_health_digest.py -q
"""

import io
import json
import os
import sys

# Same stdlib-http preload shim as tests/test_agora_client.py: the sibling
# http.py shadows the stdlib ``http`` package whenever this repo is first on
# sys.path. Preload the real stdlib http with the shadowing entries
# temporarily removed.
_SHADOW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_poisoned = sys.modules.get("http")
if _poisoned is not None and os.path.abspath(
        getattr(_poisoned, "__file__", "")) == os.path.join(_SHADOW_DIR, "http.py"):
    del sys.modules["http"]  # someone already imported the sibling as 'http'
_saved = [p for p in sys.path if os.path.abspath(p or os.curdir) == _SHADOW_DIR]
for _p in _saved:
    sys.path.remove(_p)
import http.server  # noqa: F401 -- preload stdlib http; used indirectly
for _p in _saved:
    sys.path.insert(0, _p)

import contextlib
import subprocess
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import health_digest

ROOT_TOKEN = "root-token-secret-1"


# --- fixtures -----------------------------------------------------------------

def make_instance(root, name, mode="legacy", agora_home="", issues=None):
    """Write ~/.config/<name>/config.json (+ optional state ledger) under root."""
    cfg_dir = root / ".config" / name
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.json").write_text(json.dumps(
        {"execution_mode": mode, "agora_home": agora_home}), encoding="utf-8")
    if issues is not None:
        state_dir = root / ".local" / "state" / name
        state_dir.mkdir(parents=True)
        entries = {iid: {"status": st, "agora_task_id": tid}
                   for iid, (st, tid) in issues.items()}
        (state_dir / "state.json").write_text(
            json.dumps({"issues": entries}), encoding="utf-8")


def run_digest(root, repos=(), **kw):
    out = io.StringIO()
    rc = health_digest.run(out=out, config_root=root / ".config",
                           state_root=root / ".local" / "state", repos=repos, **kw)
    return rc, out.getvalue()


def ok_probe(tasks):
    return lambda home, svc, tids: {"ok": True, "tasks": tasks}


def dead_probe(home, svc, tids):
    return {"ok": False, "error": "service discovery failed: OSError"}


# --- 全绿路径 ------------------------------------------------------------------

def test_all_green_legacy_rc0(tmp_path):
    make_instance(tmp_path, "demo-issue-agent",
                  issues={"7": ("active", None), "8": ("done", None)})
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "ok: nothing to report")
    assert rc == 0
    assert "全部正常" in text
    assert "demo-issue-agent" in text
    assert "legacy 模式" in text


def test_all_green_agora_mode_rc0(tmp_path):
    now = 1_000_000.0
    make_instance(tmp_path, "demo-issue-agent", mode="agora", agora_home="/ah",
                  issues={"7": ("active", "ag-1"), "8": ("done", "ag-2")})
    tasks = {
        "ag-1": {"client_key": "demo-issue-agent:issue-7:note-1",
                 "state": "done", "created": now - 7200,
                 "awaiting_external": True, "title": "t7", "error": ""},
        "ag-2": {"client_key": "demo-issue-agent:issue-8:note-1",
                 "state": "merged", "created": now - 9999,
                 "awaiting_external": False, "title": "t8", "error": ""},
    }
    rc, text = run_digest(tmp_path, now=now, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", agora_probe=ok_probe(tasks))
    assert rc == 0
    assert "agora 模式" in text
    assert "在飞任务 1" in text          # done 待并＝非终态；merged 不计
    assert "2.0 小时" in text            # 最老 awaiting_external 年龄
    assert "全部正常" in text


def test_foreign_client_key_not_counted(tmp_path):
    """非本服务 client_key 前缀的任务不入在飞计数（<svc>:issue- 前缀口径）。"""
    make_instance(tmp_path, "demo-issue-agent", mode="agora", agora_home="/ah",
                  issues={"7": ("active", "ag-1")})
    tasks = {"ag-1": {"client_key": "other-svc:issue-9:note-1",
                      "state": "running", "created": 1.0,
                      "awaiting_external": False, "title": "t", "error": ""}}
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", agora_probe=ok_probe(tasks))
    assert rc == 0
    assert "在飞任务 0" in text


# --- 四类 rc=1 触发 ------------------------------------------------------------

def test_service_dead_rc1_names_instance(tmp_path):
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "failed",
                          journal_reader=lambda svc: "")
    assert rc == 1
    assert "共 1 项问题" in text
    assert "demo-issue-agent" in text          # 摘要点名
    assert "服务状态 failed" in text
    assert text.rstrip().endswith("共 1 项问题")  # 末行总结


def test_ledger_blocked_rc1(tmp_path):
    make_instance(tmp_path, "demo-issue-agent",
                  issues={"7": ("blocked", None), "8": ("active", None)})
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "")
    assert rc == 1
    assert "demo-issue-agent" in text
    assert "blocked 1" in text
    assert "blocked" in text.split("问题清单", 1)[-1]  # 摘要点名问题


def test_pending_only_is_not_a_problem(tmp_path):
    """pending＝claim 后在办占位，巡检只计数不计问题（问题面只认 blocked）。"""
    make_instance(tmp_path, "demo-issue-agent",
                  issues={"7": ("pending", None)})
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "")
    assert rc == 0
    assert "pending 1" in text
    assert "全部正常" in text


def test_journal_critical_rc1(tmp_path):
    make_instance(tmp_path, "demo-issue-agent")
    journal = ("CRITICAL: boom\nnormal line\nTraceback (most recent call last):\n"
               "issue 7 blocked\nstill blocked\n")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: journal)
    assert rc == 1
    assert "CRITICAL 1" in text
    assert "Traceback 1" in text
    assert "blocked 2" in text


def test_journal_dropped_rc1(tmp_path):
    """monitor "mail for unknown task dropped" 类孤儿症状行计入异常。"""
    make_instance(tmp_path, "demo-issue-agent")
    journal = "agora_monitor: mail for unknown task ag-9 dropped\nfine\n"
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: journal)
    assert rc == 1
    assert "dropped 1" in text


def test_agora_blocked_bucketed_and_named(tmp_path):
    """在飞分桶（健康/排队/卡死）＋blocked 逐单点名（tid/标题/error/年龄）。"""
    now = 1_000_000.0
    make_instance(tmp_path, "demo-issue-agent", mode="agora", agora_home="/ah",
                  issues={"7": ("active", "ag-1"), "8": ("active", "ag-2"),
                          "9": ("active", "ag-3")})
    tasks = {
        "ag-1": {"client_key": "demo-issue-agent:issue-7:note-1",
                 "state": "running", "created": now - 3600,
                 "awaiting_external": False, "title": "健康活任务",
                 "error": ""},
        "ag-2": {"client_key": "demo-issue-agent:issue-8:note-1",
                 "state": "done", "created": now - 7200,
                 "awaiting_external": True, "title": "待并任务",
                 "error": ""},
        "ag-3": {"client_key": "demo-issue-agent:issue-9:note-1",
                 "state": "blocked", "created": now - 10800,
                 "awaiting_external": False,
                 "title": "卡死任务标题" + "长" * 30,
                 "error": "delivery failed: non_ff"},
    }
    rc, text = run_digest(tmp_path, now=now, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", agora_probe=ok_probe(tasks))
    assert rc == 1
    assert "在飞任务 3" in text
    assert "健康 1" in text and "排队 1" in text and "卡死 1" in text
    assert "ag-3" in text                       # tid 点名
    assert "卡死任务标题" in text                # 标题点名
    assert "长" * 15 not in text                # 标题截断（≤20 字符）
    assert "non_ff" in text                     # delivery.error 点名
    assert "3.0 小时" in text                   # 年龄
    summary = text.split("问题清单", 1)[1]
    assert "ag-3" in summary and "non_ff" in summary  # 卡死进问题清单


def test_agora_unreachable_rc1(tmp_path):
    make_instance(tmp_path, "demo-issue-agent", mode="agora", agora_home="/ah",
                  issues={"7": ("active", "ag-1")})
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", agora_probe=dead_probe)
    assert rc == 1
    assert "demo-issue-agent" in text
    assert "agora 不可达" in text


# --- 汇总与鲁棒性 ---------------------------------------------------------------

def test_multi_instance_partial_failure_keeps_other_sections(tmp_path):
    make_instance(tmp_path, "bad-issue-agent")
    make_instance(tmp_path, "good-issue-agent")
    rc, text = run_digest(
        tmp_path, service_state=lambda svc: "inactive" if svc.startswith("bad") else "active",
        journal_reader=lambda svc: "")
    assert rc == 1
    assert "bad-issue-agent" in text and "good-issue-agent" in text
    # 只有失败实例进问题清单；正常实例的节照常输出
    summary = text.split("问题清单", 1)[1]
    assert "bad-issue-agent" in summary
    assert "good-issue-agent" not in summary


def test_no_instances_rc1(tmp_path):
    (tmp_path / ".config").mkdir()
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "")
    assert rc == 1
    assert "未发现" in text


def test_config_unreadable_is_problem(tmp_path):
    """glob 命中却读不出的 config＝真实问题，不得静默降级 legacy。"""
    cfg_dir = tmp_path / ".config" / "demo-issue-agent"
    cfg_dir.mkdir(parents=True)
    (cfg_dir / "config.json").write_text("{ not json", encoding="utf-8")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "")
    assert rc == 1
    assert "config.json 不可读" in text
    assert "服务状态: active" in text      # 其余检查照常执行


# --- 本地消费仓滞后 -------------------------------------------------------------

def _git(*args, cwd=None):
    subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.com",
                    *args], cwd=cwd, check=True, capture_output=True, text=True)


def make_git_pair(tmp):
    """bare origin＋已推 main 的 seed 工作仓；返回 (origin, seed)。"""
    origin = tmp / "origin.git"
    _git("init", "--bare", "-b", "main", str(origin))
    seed = tmp / "seed"
    _git("clone", str(origin), str(seed))
    _git("checkout", "-B", "main", cwd=seed)
    (seed / "f.txt").write_text("1\n", encoding="utf-8")
    _git("add", "f.txt", cwd=seed)
    _git("commit", "-m", "c1", cwd=seed)
    _git("push", "-q", "origin", "main", cwd=seed)
    return origin, seed


def clone_consumer(tmp, name="consumer"):
    clone = tmp / name
    _git("clone", str(tmp / "origin.git"), str(clone))
    return clone


def test_local_consumer_repo_list_pinned():
    """固定清单常量＝部署事实（一处加仓）；路径 ~ 展开由实现负责。"""
    assert health_digest._LOCAL_CONSUMER_REPOS == (
        "~/.dotfiles",
        "~/issue-agent",
        "~/.dotfiles/.tmux/plugins/tmux-agent-indicator",
    )


def test_repo_behind_is_problem(tmp_path):
    origin, seed = make_git_pair(tmp_path)
    clone = clone_consumer(tmp_path)
    (seed / "f.txt").write_text("2\n", encoding="utf-8")
    _git("commit", "-am", "c2", cwd=seed)
    _git("push", "-q", "origin", "main", cwd=seed)
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", repos=(str(clone),))
    assert rc == 1
    assert "本地落后 1 提交" in text
    summary = text.split("问题清单", 1)[1]
    assert str(clone) in summary


def test_repo_sync_is_green(tmp_path):
    make_git_pair(tmp_path)
    clone = clone_consumer(tmp_path)
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", repos=(str(clone),))
    assert rc == 0
    assert "同步" in text
    assert "全部正常" in text


def test_repo_dirty_noted_not_problem(tmp_path):
    make_git_pair(tmp_path)
    clone = clone_consumer(tmp_path)
    (clone / "f.txt").write_text("dirty\n", encoding="utf-8")
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", repos=(str(clone),))
    assert rc == 0
    assert "dirty" in text
    assert "全部正常" in text


def test_repo_fetch_failure_is_problem_and_not_fatal(tmp_path):
    """fetch 失败计问题，但不中断其余仓的检查。"""
    make_git_pair(tmp_path)
    bad = clone_consumer(tmp_path, "bad")
    _git("remote", "set-url", "origin", str(tmp_path / "gone"), cwd=bad)
    good = clone_consumer(tmp_path, "good")
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "",
                          repos=(str(bad), str(good)))
    assert rc == 1
    assert "fetch 失败（fatal" in text        # 带 stderr 原因，可直接定位
    assert "同步" in text                      # 其余仓照常出结论


def test_repo_weird_filename_does_not_crash(tmp_path):
    """quotepath=false＋非 UTF-8 文件名：decode 容错，不得整份巡检崩溃。"""
    make_git_pair(tmp_path)
    clone = clone_consumer(tmp_path)
    _git("config", "core.quotepath", "false", cwd=clone)
    (clone / os.fsdecode(b"bad\xe9.txt")).write_text("x", encoding="utf-8")
    make_instance(tmp_path, "demo-issue-agent")
    rc, text = run_digest(tmp_path, service_state=lambda svc: "active",
                          journal_reader=lambda svc: "", repos=(str(clone),))
    assert rc == 0
    assert "同步" in text


# --- 默认 agora 探针（真实 HTTP，本机假服务） -----------------------------------

class _RecordingHandler(BaseHTTPRequestHandler):
    def _handle(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b""
        record = {"path": parsed.path,
                  "token": self.headers.get("X-Agora-Token"),
                  "json": json.loads(raw_body) if raw_body else None}
        self.server.requests.append(record)
        if parsed.path == "/doctor":
            status, payload = 200, {"models": ["m"]}
        elif parsed.path == "/board":
            tid = record["json"]["task"]
            status, payload = 200, {"task": {
                "client_key": f"demo-issue-agent:issue-7:note-{tid[-1]}",
                "state": "done", "created": 1234.5, "title": "demo task",
                "delivery": {"status": "awaiting_external",
                             "error": "boom"}}}
        else:
            status, payload = 404, {"error": "not found"}
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = _handle

    def log_message(self, *args):
        pass


@contextlib.contextmanager
def fake_agora():
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.requests = []
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def write_agora_home(tmp_path, port):
    home = tmp_path / "plaza"
    (home / ".agora").mkdir(parents=True)
    (home / ".agora" / "service.json").write_text(json.dumps({"port": port}))
    (home / ".agora" / "root.token").write_text(ROOT_TOKEN + "\n")
    return str(home)


def test_default_probe_http_roundtrip(tmp_path):
    with fake_agora() as server:
        home = write_agora_home(tmp_path, server.server_address[1])
        result = health_digest._agora_probe(home, "demo-issue-agent",
                                            ["ag-1", "ag-2"], timeout_s=5)
    assert result["ok"] is True
    assert set(result["tasks"]) == {"ag-1", "ag-2"}
    row = result["tasks"]["ag-1"]
    assert row["state"] == "done"
    assert row["awaiting_external"] is True
    assert row["title"] == "demo task"
    assert row["error"] == "boom"
    assert row["client_key"].startswith("demo-issue-agent:issue-")
    paths = [r["path"] for r in server.requests]
    assert paths[0] == "/doctor"
    assert sorted(paths[1:]) == ["/board", "/board"]
    assert all(r["token"] == ROOT_TOKEN for r in server.requests)


def test_default_probe_discovery_failure_no_leak(tmp_path):
    result = health_digest._agora_probe(str(tmp_path / "nowhere"),
                                        "demo-issue-agent", [])
    assert result["ok"] is False
    assert ROOT_TOKEN not in result["error"]


def test_default_probe_non_dict_board_response(tmp_path):
    """/board 200 但顶层非 dict（null/[]）→ ok:False 归类返回，不逃异常。"""
    class _NullBoard(_RecordingHandler):
        def _handle(self):
            length = int(self.headers.get("Content-Length") or 0)
            self.rfile.read(length)
            body = b"null" if self.path == "/board" else b"{}"
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        do_POST = _handle

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), _NullBoard)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        home = write_agora_home(tmp_path, server.server_address[1])
        result = health_digest._agora_probe(home, "demo-issue-agent",
                                            ["ag-1"], timeout_s=5)
    finally:
        server.shutdown()
        server.server_close()
    assert result["ok"] is False
    assert "non-dict" in result["error"]
