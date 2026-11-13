# ruff: noqa: E402 -- imports are ordered around the stdlib-http preload shim below
"""真实 agora e2e（计划 Task 6）：起**真实 agorad 临时实例**测 issue-agent 全链。

夹具（参照 opencode-agora tests/e2e/multi_instance_delivery.py 的
assemble/make_server 模式）：独立临时 AGORA_HOME＋端口 0 的真实 agorad 服务
（StubOpencode 顶替 worker 的 opencode 服务）、临时 bare git 远端＋项目克隆
（真 git）、fake Gitea API（tests/test_api.py 模式：真 urllib 往返＋本地
scripted HTTP 服务）。被测方是 issue-agent——orchestrator/agora_client/
agora_monitor/state/fsm/gitop 全真码；worker 侧行为由 agora F102 九场景证明、
Router LLM 决策由 tests/test_router.py 证明，此处分别以测试替身 worker（HTTP
面动作）与脚本化 Router 代役。

场景（计划 Task 6 Step 1 原文）：
  1. 新 issue→Router→agora worker done→评审锚/verify→awaiting_external→
     issue-agent 精确推送→task_deliver(delivered)→Gitea 回评（坑位行原样
     保留）；评审恰一次（worker done 前的锚×1）、legacy _run_review_phase
     零调用；默认分支 SHA 全程不动；worker 零 push（远端引用快照）、零嵌套
     worktree。
  2. non_ff：远端前推→issue-agent 携 base 换基→门体重排重验出新 handoff→
     二次推送交付。
  3. daemon 重启恢复：awaiting_external 中重启不重复建单，恢复轮询完成交付。
  4. create 后崩溃（dispatch 前中断）：client_key 幂等取回原任务，恢复后
     走完全链。

零外网：origin/agorad/Gitea 全 127.0.0.1 或本地路径；不触任何生产实例。

Run: python3 -m pytest tests/test_agora_e2e.py -q
"""

import os
import sys

# stdlib-http preload shim（T1 遗留纪律，同 tests/test_api.py）：PKG_DIR 的
# http.py 会遮蔽 stdlib http 包，先在遮蔽路径移除态下预载真 http，再恢复。
_SHADOW_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_poisoned = sys.modules.get("http")
if _poisoned is not None and os.path.abspath(
        getattr(_poisoned, "__file__", "")) == os.path.join(_SHADOW_DIR, "http.py"):
    del sys.modules["http"]
_saved = [p for p in sys.path if os.path.abspath(p or os.curdir) == _SHADOW_DIR]
for _p in _saved:
    sys.path.remove(_p)
import http.client  # noqa: F401 -- preload stdlib http (urllib depends on it)
import http.server  # noqa: F401
import urllib.error  # noqa: F401
import urllib.request  # noqa: F401
for _p in _saved:
    sys.path.insert(0, _p)

import importlib.util
import json
import subprocess
import threading
import time
from pathlib import Path

import pytest

import orchestrator
import state
from agora_client import AgoraClient, AgoraUnavailable
from agora_monitor import AgoraMonitor
from api import GitLabClient
from router import RouterDecision

PKG_DIR = Path(__file__).resolve().parent.parent
AGORAD_SRC = Path("/home/guangbin/opencode-agora")
if not AGORAD_SRC.is_dir():
    pytest.skip("agorad source not available at "
                f"{AGORAD_SRC} (real-instance e2e)", allow_module_level=True)

# agorad 包（真实服务实例源码）；stub_opencode 以文件路径直载——pytest 把本
# 仓 tests/ 也注册为包名 tests，同名包注入会撞车。
sys.path.insert(0, str(AGORAD_SRC))
from agorad.main import assemble  # noqa: E402
from agorad.transport.httpd import make_server  # noqa: E402

_spec = importlib.util.spec_from_file_location(
    "agora_e2e_stub_opencode", AGORAD_SRC / "tests" / "stub_opencode.py")
_stub_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_stub_mod)
StubOpencode = _stub_mod.StubOpencode

SERVICE = "issue-agent-e2e"
PROJECT = "acme/widget"
DEFAULT_BRANCH = "main"
# worker 模型须过 agorad 的 providerID/modelID 校验；StubOpencode 的 /agent
# 面恰好登记了它（doctor models 含此条）。
WORKER_MODEL = "zai-coding-plan/glm-5.3"
ROUTER_GOAL = ("修复 issue#7 的构建失败：补齐缺失测试并让门禁全绿。\n"
               "沉淀：涉及知识/经验时按仓内 docs/agents 契约归档。\n"
               "最终报告末行附「坑位：」提示（无则省略整行）。\n"
               "Issue: #7")


def wait_until(condition, timeout=90.0, interval=0.05, what=""):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = condition()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"等待超时（{timeout}s）：{what or condition}")


def git(args, cwd, check=True):
    proc = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                          text=True, encoding="utf-8", errors="replace")
    if check and proc.returncode != 0:
        raise AssertionError(f"git {args}: {proc.stderr.strip()}")
    return proc


def branch_sha(repo, branch):
    return git(["rev-parse", f"refs/heads/{branch}"], repo).stdout.strip()


def remote_refs(url, cwd):
    out = git(["ls-remote", str(url)], cwd).stdout
    return {line.split()[1]: line.split()[0]
            for line in out.splitlines() if line}


def http_call(port, path, body, token):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "X-Agora-Token": token},
        method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"{}")


class FakeGitea:
    """本地 scripted Gitea（test_api.py 模式）：内存 issues/comments＋请求
    记录；真 GitLabClient 走真 urllib 往返。只实现 daemon 面用到的端点。"""

    def __init__(self):
        self.issues = {}      # iid -> {"title","body","state"}
        self.comments = {}    # iid -> [{"id","body","user"}]
        self.posts = []       # (iid, body) —— POST /comments 记录面
        self._next_comment_id = 1000
        self.httpd = None

    def start(self):
        gitea = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _json(self, code, payload):
                body = json.dumps(payload).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def _body(self):
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(length) if length else b""
                return json.loads(raw) if raw.strip() else {}

            def do_GET(self):
                path = self.path.split("?")[0]
                if path == f"/api/v1/repos/{PROJECT}/issues":
                    self._json(200, [
                        {"number": iid, "title": rec["title"],
                         "body": rec["body"], "state": rec["state"]}
                        for iid, rec in sorted(gitea.issues.items())
                        if rec["state"] == "open"])
                elif path == f"/api/v1/repos/{PROJECT}/issues/{iid(path)}/comments":
                    self._json(200, gitea.comments.get(iid(path), []))
                elif path == f"/api/v1/repos/{PROJECT}/issues/{iid(path)}":
                    rec = gitea.issues.get(iid(path))
                    self._json(200, rec or {"state": "open"})
                else:
                    self._json(404, {"message": "not found"})

            def do_POST(self):
                path = self.path.split("?")[0]
                if path == f"/api/v1/repos/{PROJECT}/issues/{iid(path)}/comments":
                    gitea._next_comment_id += 1
                    note = {"id": gitea._next_comment_id,
                            "body": self._body().get("body", ""),
                            "user": {"login": "issue-agent-e2e"}}
                    gitea.comments.setdefault(iid(path), []).append(note)
                    gitea.posts.append((iid(path), note["body"]))
                    self._json(201, note)
                else:
                    self._json(404, {"message": "not found"})

            def do_PATCH(self):
                path = self.path.split("?")[0]
                if path == f"/api/v1/repos/{PROJECT}/issues/{iid(path)}":
                    rec = gitea.issues.setdefault(
                        iid(path), {"title": "", "body": "", "state": "open"})
                    rec.update(self._body())
                    self._json(200, rec)
                else:
                    self._json(404, {"message": "not found"})

        def iid(path):
            return int(path.rstrip("/").split("/")[-2])

        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.httpd.serve_forever,
                         kwargs={"poll_interval": 0.05}, daemon=True).start()
        return self.httpd.server_address[1]

    def stop(self):
        if self.httpd is not None:
            self.httpd.shutdown()
            self.httpd.server_close()
            self.httpd = None


class ScriptedRouter:
    """SemanticRouter 替身：恒返回同一 dispatch 决策（goal 契约含沉淀/坑位/
    Issue trailer——断言面在 agorad 建单行上核对原文入账）。"""

    def __init__(self):
        self.calls = 0

    def decide(self, *, iid, event, issue_context, available_models):
        self.calls += 1
        return RouterDecision(
            action="dispatch", title=f"issue#{iid}: e2e"[:40],
            goal=ROUTER_GOAL.replace("issue#7", f"issue#{iid}"),
            model=WORKER_MODEL, reason="e2e scripted", raw={})

    def answer(self, *, iid, task, question):
        return "e2e 答复（scripted）"


class FakeListener:
    """queue-only listener：事件直接入队（webhook HMAC 层归 test_listener）。"""

    def __init__(self):
        import queue
        self.queue = queue.Queue()

    def start(self):
        return 4242

    def stop(self):
        return None


class AgoradInstance:
    """真实 agorad 服务实例（同 opencode-agora e2e 的装配模式）＋
    AgoraClient 服务发现文件（service.json/root.token，对齐生产 agorad 启动
    的落盘面）。"""

    def __init__(self, home):
        self.home = Path(home)
        self.home.mkdir(parents=True, exist_ok=True)
        self.stub = StubOpencode()
        self.stub.start()
        try:
            self.app = assemble(self.home, oc_url=self.stub.base_url)
            self.root_token = self.app.auth.issue("main", "root")
            self.httpd = make_server(self.app)
            threading.Thread(target=self.httpd.serve_forever,
                             kwargs={"poll_interval": 0.02}, daemon=True
                             ).start()
            self.port = self.httpd.server_address[1]
            agora_dir = self.home / ".agora"
            agora_dir.mkdir(exist_ok=True)
            (agora_dir / "service.json").write_text(
                json.dumps({"pid": os.getpid(), "port": self.port}), "utf-8")
            (agora_dir / "root.token").write_text(
                self.root_token + "\n", "utf-8")
        except Exception:
            self.stub.stop()
            raise

    def call(self, path, body, token=None):
        return http_call(self.port, path, body, token or self.root_token)

    def row(self, tid):
        status, payload = self.call("/board", {"task": tid})
        assert status == 200, payload
        return payload["task"]

    def wait_board(self, tid, predicate, what="", timeout=90):
        return wait_until(lambda: predicate(self.row(tid)) or None,
                          timeout=timeout, what=f"{tid} {what}")

    def worker_token(self, tid):
        path = self.home / "credentials" / f"{tid}.token"
        return json.loads(path.read_text(encoding="utf-8"))["token"]

    def task_count(self):
        """agorad 账本在飞任务数（state/tasks.jsonl 末行快照）。"""
        tasks_file = self.home / "state" / "tasks.jsonl"
        if not tasks_file.exists():
            return 0
        snap = None
        for line in tasks_file.read_text(encoding="utf-8").splitlines():
            try:
                snap = json.loads(line)
            except ValueError:
                continue
        return len((snap or {}).get("tasks", {}))

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.stub.stop()


class E2EEnv:
    """一个场景的全套夹具：bare origin＋项目克隆＋真实 agorad＋fake Gitea。"""

    def __init__(self, root: Path):
        self.root = root
        (root / "layout").mkdir(parents=True, exist_ok=True)
        # bare origin＋种子基线（main）
        self.origin = root / "layout" / "origin.git"
        self.origin.mkdir()
        git(["init", "--bare", "-b", DEFAULT_BRANCH], self.origin)
        seed = root / "layout" / "seed"
        seed.mkdir()
        git(["init", "-b", DEFAULT_BRANCH], seed)
        git(["config", "user.email", "e2e@t"], seed)
        git(["config", "user.name", "e2e"], seed)
        (seed / "base.txt").write_text("base\n", encoding="utf-8")
        git(["add", "-A"], seed)
        git(["commit", "-m", "base"], seed)
        git(["push", str(self.origin), "main:refs/heads/main"], seed)
        self.base_sha = branch_sha(seed, DEFAULT_BRANCH)
        # 项目克隆＝issue-agent 的 repo_path（agora env 以其 worktree 形态挂靠）
        self.repo = root / "layout" / "project"
        git(["clone", str(self.origin), "project"], root / "layout")
        git(["config", "user.email", "e2e@t"], self.repo)
        git(["config", "user.name", "e2e"], self.repo)
        # 真实 agorad 实例＋fake Gitea
        self.agorad = AgoradInstance(root / "agora-home")
        self.gitea = FakeGitea()
        self.gitea_port = self.gitea.start()
        self.journal = []
        self.state_path = root / "state.json"
        self.router = ScriptedRouter()
        self.review_phase_calls = []
        self.orch = None
        self.worker_anchor_writes = []
        self.worker_env_worktrees = {}  # tid -> env 存活期 worktree 注册快照

    # ---- issue-agent 侧 --------------------------------------------------

    def build_orchestrator(self):
        client = AgoraClient(service_name=SERVICE, home=str(self.agorad.home),
                             poll_interval=1, timeout_s=15)
        monitor = AgoraMonitor(client=client, journal=self.journal.append,
                               max_active=2)
        note_marker = f"<!-- {SERVICE} -->"
        api_client = GitLabClient(
            f"http://127.0.0.1:{self.gitea_port}", "e2e-pat",
            ssl_verify=False, note_marker=note_marker)
        return orchestrator.Orchestrator(
            project=PROJECT, api=api_client, journal=self.journal.append,
            listener=FakeListener(), state_path=self.state_path,
            repo_path=str(self.repo), remote_url=str(self.origin),
            protocol_path=str(PKG_DIR / "PROTOCOL.template.md"),
            ssl_verify=True, default_branch=DEFAULT_BRANCH,
            service_name=SERVICE, verify_command="true",
            reconcile_interval=99999,
            agora_client=client, router=self.router, agora_monitor=monitor,
            ledger_lock=threading.RLock())

    def start_orchestrator(self):
        assert self.orch is None
        self.orch = self.build_orchestrator()
        # 计划锚例的 legacy_run_review_phase_calls 计数：agora 全链必须为零
        # （替身直接短路——真件会拉起 opencode 评审进程，绝不允许发生）。
        def spying_phase(*args, **kwargs):
            self.review_phase_calls.append(args)
            return True
        self.orch._run_review_phase = spying_phase  # noqa: SLF001——e2e 间谍面
        self.orch.start()
        return self.orch

    def stop_orchestrator(self):
        if self.orch is not None:
            self.orch.stop()
            self.orch = None

    def open_issue(self, iid, title="构建失败", body="main 上跑不过测试"):
        event = {"action": "opened",
                 "issue": {"number": iid, "title": title, "body": body,
                           "state": "open"},
                 "repository": {"full_name": PROJECT},
                 "sender": {"login": "someone"}}
        self.gitea.issues[iid] = {"title": title, "body": body, "state": "open"}
        self.orch.listener.queue.put(event)

    def entry(self, iid):
        return state.load_state(self.state_path)["issues"].get(
            str(int(iid)), {})

    def turn_done(self, iid):
        entry = self.entry(iid)
        return entry.get("status") == "active" \
            and entry.get("claimed_at") is None

    # ---- worker 侧（替身 worker：HTTP 面动作，同 F102 e2e 的 worker_done） --

    def worker_complete(self, tid, files, msg, report):
        """真实 worker 流程：env 内真提交→恰写一次评审锚→note 留结构化报告
        →HTTP done（六闸受理）。返回 (handoff sha, env_dir)。"""
        token = self.agorad.worker_token(tid)
        env_dir = Path(self.agorad.row(tid)["env_dir"])
        git(["config", "user.email", "worker@e2e"], env_dir)
        git(["config", "user.name", "worker"], env_dir)
        for name, content in files.items():
            path = env_dir / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        git(["add", "-A"], env_dir)
        git(["commit", "-m", msg], env_dir)
        sha = git(["rev-parse", "HEAD"], env_dir).stdout.strip()
        anchor = env_dir / ".review-loop" / ".last-reviewed"
        anchor.parent.mkdir(exist_ok=True)
        anchor.write_text(sha, encoding="utf-8")
        self.worker_anchor_writes.append(sha)  # 评审锚恰一次的证据面
        # worker 零嵌套 worktree 的证据面：env 在交付收口后即被服务回收，
        # 快照须在存活期采集。
        porcelain = git(["worktree", "list", "--porcelain"], env_dir).stdout
        self.worker_env_worktrees[tid] = [
            line.split(" ", 1)[1] for line in porcelain.splitlines()
            if line.startswith("worktree ")]
        status, payload = self.agorad.call(
            "/post", {"task": tid, "kind": "note", "body": report}, token)
        assert status == 200, payload
        status, payload = self.agorad.call(
            "/post", {"task": tid, "kind": "done", "body": sha}, token)
        assert status == 200 and payload.get("result") == "accepted", payload
        return sha, env_dir

    def stop(self):
        self.stop_orchestrator()
        self.agorad.stop()
        self.gitea.stop()


@pytest.fixture()
def env(tmp_path):
    e = E2EEnv(tmp_path)
    try:
        yield e
    finally:
        e.stop()


WORKER_REPORT = ("根因：base.txt 缺少边界用例。\n"
                 "改动：tests/test_sample.py 新增 3 例。\n"
                 "验证：python3 -m pytest tests/ -q 全绿。\n"
                 "坑位：tests/fake_data.py 是生成器替身，勿手改")


# --- 场景 1：新 issue → 交付 → 回评 全链 ---------------------------------------


def test_full_chain_delivers_and_comments(env):
    before_refs = remote_refs(env.origin, env.repo)  # 仅 refs/heads/main
    env.start_orchestrator()
    env.open_issue(7)
    wait_until(lambda: env.turn_done(7), what="agora 回合建单派发收尾",
               timeout=30)
    assert any("已接手 issue#7" in body for _, body in env.gitea.posts), \
        "接手评论必须落到 Gitea"
    assert env.agorad.task_count() == 1
    tid = env.entry(7)["agora_task_id"]
    row = env.agorad.row(tid)
    assert "Issue: #7" in row["goal"], "goal 契约保留 Issue trailer"
    assert "沉淀" in row["goal"], "goal 契约保留知识/经验沉淀要求"
    assert row["target_branch"] == "issue/7-fix"
    assert row["deliverable"] == "external_branch"

    handoff, env_dir = env.worker_complete(
        tid, {"fix.txt": "fix\n", "tests/test_sample.py": "def test_ok():\n"},
        "fix issue#7", WORKER_REPORT)
    env.agorad.wait_board(
        tid, lambda r: r["state"] == "done"
        and r["delivery"]["status"] == "awaiting_external",
        what="done+awaiting_external（评审锚/verify 过闸）")

    wait_until(lambda: env.entry(7).get("delivery_commit") == handoff,
               what="monitor 精确推送＋delivered 收口＋报告回评")
    row = env.agorad.row(tid)
    assert row["state"] == "closed"
    assert row["delivery"]["status"] == "delivered"

    # 评审恒一次：worker 锚×1＋legacy review phase×0（计划锚例）
    assert len(env.worker_anchor_writes) == 1
    assert env.review_phase_calls == []
    # 默认分支 SHA 全程不动；远端引用只新增 issue/7-fix==handoff
    assert branch_sha(env.origin, DEFAULT_BRANCH) == env.base_sha
    assert remote_refs(env.origin, env.repo) == {
        **before_refs, f"refs/heads/issue/7-fix": handoff}
    # worker 零 push：env 不推远端（上面引用快照已证）；零嵌套 worktree
    # （快照采集于 env 存活期——收口后服务回收 env 属正常语义）：worktree
    # 家族只含主检出与 env 自身，env 目录内部不得再有 worker 建的 worktree）
    assert env.worker_env_worktrees[tid] == [str(env.repo), str(env_dir)], \
        f"worker 不得建嵌套 worktree：{env.worker_env_worktrees[tid]}"
    # Gitea 回评：报告原样上墙，坑位行逐字保留
    report_notes = [body for _, body in env.gitea.posts
                    if "根因" in body and WORKER_REPORT in body]
    assert report_notes, f"最终报告必须回评 Gitea：{env.gitea.posts}"
    assert "坑位：tests/fake_data.py 是生成器替身，勿手改" in report_notes[-1]
    assert env.entry(7)["agora_status"] == "closed/delivered"


# --- 场景 2：non_ff 换基重排 ------------------------------------------------------


def test_non_ff_rotates_base_and_redelivers(env):
    env.start_orchestrator()
    env.open_issue(7)
    wait_until(lambda: env.turn_done(7), what="建单派发", timeout=30)
    tid = env.entry(7)["agora_task_id"]

    # 远端前推（确定性：在首个 handoff 产生前就把 issue/7-fix 推到 origin，
    # monitor 首推必 non_ff）：ext 克隆从 main 建 issue/7-fix＋一笔外部提交。
    ext = env.root / "layout" / "ext"
    git(["clone", str(env.origin), "ext"], env.root / "layout")
    git(["config", "user.email", "ext@e2e"], ext)
    git(["config", "user.name", "ext"], ext)
    git(["checkout", "-b", "issue/7-fix"], ext)
    (ext / "external.txt").write_text("external advance\n", encoding="utf-8")
    git(["add", "-A"], ext)
    git(["commit", "-m", "external advance"], ext)
    external_base = git(["rev-parse", "HEAD"], ext).stdout.strip()
    git(["push", "origin", "issue/7-fix"], ext)

    handoff, _ = env.worker_complete(tid, {"fix.txt": "fix\n"},
                                     "fix issue#7", WORKER_REPORT)
    env.agorad.wait_board(
        tid, lambda r: r["state"] == "done"
        and r["delivery"]["status"] == "awaiting_external",
        what="首个 handoff")
    # monitor：首推 non_ff → 携实际远端 tip 换基 → 门体重排重验出新 handoff
    # → 二次推送 → delivered 收口。
    wait_until(lambda: env.entry(7).get("delivery_commit")
               not in (None, handoff), what="non_ff 换基后新 handoff 交付")
    row = env.agorad.row(tid)
    assert row["state"] == "closed"
    assert row["delivery"]["status"] == "delivered"
    assert row["delivery"]["attempt"] == 2, f"恰一次换基重试：{row['delivery']}"
    new_commit = row["delivery"]["commit"]
    assert new_commit != handoff
    assert git(["merge-base", "--is-ancestor", external_base, new_commit],
               env.repo, check=False).returncode == 0, \
        "新 handoff 必须落在外部推进基之上"
    assert remote_refs(env.origin, env.repo)[
        f"refs/heads/issue/7-fix"] == new_commit
    assert branch_sha(env.origin, DEFAULT_BRANCH) == env.base_sha
    assert any("根因" in body for _, body in env.gitea.posts), "报告照常回评"


# --- 场景 3：daemon 重启恢复（awaiting_external 中重启）--------------------------


def test_restart_recovers_awaiting_external_without_duplicate(env):
    env.start_orchestrator()
    env.open_issue(7)
    wait_until(lambda: env.turn_done(7), what="建单派发", timeout=30)
    tid = env.entry(7)["agora_task_id"]
    handoff, _ = env.worker_complete(tid, {"fix.txt": "fix\n"},
                                     "fix issue#7", WORKER_REPORT)
    env.agorad.wait_board(
        tid, lambda r: r["state"] == "done"
        and r["delivery"]["status"] == "awaiting_external",
        what="重启前 awaiting_external")

    env.stop_orchestrator()  # daemon 重启：monitor 线程/注册表全部清零
    env.worker_anchor_writes.clear()
    env.start_orchestrator()
    assert env.agorad.task_count() == 1, "重启恢复不得重复建单"
    assert env.entry(7)["agora_task_id"] == tid

    wait_until(lambda: env.entry(7).get("delivery_commit") == handoff,
               what="恢复后 monitor 完成交付")
    assert env.agorad.task_count() == 1, "全程恰一个任务"
    assert env.agorad.row(tid)["state"] == "closed"
    assert branch_sha(env.origin, DEFAULT_BRANCH) == env.base_sha
    assert remote_refs(env.origin, env.repo)[
        f"refs/heads/issue/7-fix"] == handoff
    assert any("根因" in body for _, body in env.gitea.posts)
    assert env.worker_anchor_writes == [], "重启不重跑 worker 侧任何动作"


# --- 场景 4：create 后崩溃（client_key 幂等取回）--------------------------------


def test_crash_after_create_recovers_same_task(env):
    env.start_orchestrator()
    # 脚本化故障窗：create 已达服务端、dispatch 未达（崩溃模拟）。
    client = env.orch.agora_client
    real_dispatch = client.dispatch
    crashed = threading.Event()

    def crashing_dispatch(tid):
        crashed.set()
        raise AgoraUnavailable("dispatch crashed (scripted fault)")
    client.dispatch = crashing_dispatch

    env.open_issue(7)
    wait_until(lambda: env.entry(7).get("agora_task_id")
               and env.entry(7)["agora_pending"] is True,
               what="create 落账、dispatch 中断、保留待派发", timeout=30)
    assert env.agorad.task_count() == 1
    tid = env.entry(7)["agora_task_id"]
    assert env.agorad.row(tid)["state"] == "todo", "dispatch 未达"

    # 恢复（生产面＝claim-timeout 回收→requeue；此处直驱同一路径）
    client.dispatch = real_dispatch
    ledger = state.load_state(env.state_path)
    ledger["issues"]["7"]["status"] = "defer"
    state.save_state(env.state_path, ledger)
    env.orch._handle_requeue(7)  # noqa: SLF001——直驱恢复通道（同 test_orch）
    wait_until(lambda: env.turn_done(7), what="幂等取回原任务并派发")

    assert env.agorad.task_count() == 1, "client_key 幂等取回，绝不建新任务"
    assert env.entry(7)["agora_task_id"] == tid
    assert env.entry(7)["agora_pending"] is False
    assert env.agorad.row(tid)["state"] != "todo", "原任务已被派发"

    handoff, _ = env.worker_complete(tid, {"fix.txt": "fix\n"},
                                     "fix issue#7", WORKER_REPORT)
    wait_until(lambda: env.entry(7).get("delivery_commit") == handoff,
               what="恢复后的原任务走完全链交付")
    assert env.agorad.task_count() == 1
    assert remote_refs(env.origin, env.repo)[
        f"refs/heads/issue/7-fix"] == handoff
    assert branch_sha(env.origin, DEFAULT_BRANCH) == env.base_sha
