# ruff: noqa: E402 -- imports are ordered around the stdlib-http preload shim below
"""agora_client.py tests against a real local HTTP server.

Mirrors tests/test_api.py's recording fake: a ThreadingHTTPServer bound to
127.0.0.1 on an ephemeral port records every request (method/path/
X-Agora-Token/body) and serves scripted JSON; the agora home is a tmp dir
holding .agora/service.json (the server's port) and .agora/root.token.
Real urllib round-trips run end to end — zero production-agora contact.

Security anchors pinned here: the root token appears ONLY on
/cred_for_sid, the dispatcher credential is issued exactly once per client
instance, and no exception text ever carries a token value (a hostile
server echoing the received token back in its error text included).

Run: python3 -m pytest tests/test_agora_client.py -q
"""

import os
import sys

# Same stdlib-http preload shim as tests/test_api.py: the sibling http.py
# shadows the stdlib ``http`` package whenever this repo is first on
# sys.path, which breaks urllib.request (it imports http.client). Preload
# the real stdlib http with the shadowing entries temporarily removed.
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
import json
import socket
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from agora_client import (AgoraClient, AgoraProtocolError, AgoraRejected,
                          AgoraUnavailable)

ROOT_TOKEN = "root-token-secret-1"
DISPATCHER_TOKEN = "dispatcher-token-secret-2"


class _RecordingHandler(BaseHTTPRequestHandler):
    """Record every incoming request, then dispatch to the scripted route."""

    def _handle(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b""
        record = {
            "method": self.command,
            "path": parsed.path,
            "token": self.headers.get("X-Agora-Token"),
            "json": json.loads(raw_body) if raw_body else None,
        }
        self.server.requests.append(record)
        route = self.server.routes.get((self.command, parsed.path))
        status, payload = route(record) if route else (404, {"error": "not found"})
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self.send_response(status)
        if body:
            self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    do_POST = _handle

    def log_message(self, *args):  # keep pytest output pristine
        pass


@contextlib.contextmanager
def fake_agora(routes):
    """Start the fake agora service on 127.0.0.1:<ephemeral>; yield it."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.requests = []
    server.routes = routes
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def default_routes(task="ag-123"):
    """A full happy-path agorad: cred issuance + every task-plane endpoint."""
    return {
        ("POST", "/cred_for_sid"): lambda rec: (
            200, {"token": DISPATCHER_TOKEN, "role": "dispatcher"}),
        ("POST", "/task_create"): lambda rec: (200, {"task": task}),
        ("POST", "/task_dispatch"): lambda rec: (200, {"task": task}),
        ("POST", "/board"): lambda rec: (200, {"buckets": {}}),
        ("POST", "/post"): lambda rec: (200, {"message": 7}),
        ("POST", "/check_mail"): lambda rec: (200, {"messages": [{"id": 1}]}),
        ("POST", "/doctor"): lambda rec: (200, {"models": ["m"]}),
        ("POST", "/task_deliver"): lambda rec: (200, {"task": task, "delivery": {}}),
        ("POST", "/task_update"): lambda rec: (200, {"ok": True}),
        ("POST", "/task_cancel"): lambda rec: (200, {"ok": True}),
    }


def agora_home(tmp_path, port):
    home = tmp_path / "plaza"
    (home / ".agora").mkdir(parents=True)
    (home / ".agora" / "service.json").write_text(json.dumps({"port": port}))
    (home / ".agora" / "root.token").write_text(ROOT_TOKEN + "\n")
    return str(home)


def make_client(home, poll_interval=15, timeout_s=10):
    return AgoraClient(service_name="demo", home=home,
                       poll_interval=poll_interval, timeout_s=timeout_s)


def task_payload():
    return dict(goal="修 bug", project_dir="/repo", target_branch="issue/7-fix",
                verify_cmd="python3 -m pytest tests/ -q", model="zai/glm",
                title="修 #7", client_key="issue-agent:issue-7:note-9")


def with_server(routes, tmp_path):
    """Combined context: fake service + home dir + client (test-local sugar)."""
    class _Combo:
        def __enter__(self):
            self._ctx = fake_agora(routes)
            self.server = self._ctx.__enter__()
            self.client = make_client(agora_home(tmp_path, self.server.server_address[1]))
            return self
        def __exit__(self, *exc):
            return self._ctx.__exit__(*exc)
    return _Combo()


# --- credential plane ------------------------------------------------------


def test_client_uses_service_dispatcher_not_root_for_tasks(tmp_path):
    """计划锚例：root 牌只在 /cred_for_sid 出现；任务面全部携 dispatcher 牌。"""
    with with_server(default_routes(), tmp_path) as combo:
        tid = combo.client.create_task(**task_payload())
    assert tid == "ag-123"
    requests = combo.server.requests
    assert requests[0]["path"] == "/cred_for_sid"  # issuance precedes the task call
    cred = requests[0]
    assert cred["token"] == ROOT_TOKEN
    assert cred["json"] == {"sid": "issue-agent:demo"}
    task_reqs = [r for r in requests if r["path"] != "/cred_for_sid"]
    assert task_reqs, "the task call itself must have happened"
    assert all(r["token"] == DISPATCHER_TOKEN for r in task_reqs)
    assert all(r["token"] != ROOT_TOKEN for r in task_reqs)


def test_create_task_verify_base_exempt_wire_shape(tmp_path):
    """F107：verify_base_exempt 仅显式设置才上送（None 不含键）。"""
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.create_task(**task_payload())
        combo.client.create_task(**task_payload(), verify_base_exempt=True)
    bodies = [r["json"] for r in combo.server.requests
              if r["path"] == "/task_create"]
    assert "verify_base_exempt" not in bodies[0]
    assert bodies[1]["verify_base_exempt"] is True


def test_task_cancel_force_wire_shape(tmp_path):
    """F28 忙碌闸应答直透：force 仅 True 时上送（缺省不含键）。"""
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.task_cancel("ag-123")
        combo.client.task_cancel("ag-123", force=True)
    bodies = [r["json"] for r in combo.server.requests
              if r["path"] == "/task_cancel"]
    assert "force" not in bodies[0]
    assert bodies[1]["force"] is True


def test_task_update_wire_shape(tmp_path):
    """ag-fb9e5a：verify 提案执行回路——/task_update 携 dispatcher 牌，
    body 恰为 {task, verify_cmd}。"""
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.task_update("ag-123",
                                 verify_cmd="HX_TIDY_BASE=0f7b145e "
                                            "./container/build.sh check")
    reqs = [r for r in combo.server.requests if r["path"] == "/task_update"]
    assert len(reqs) == 1
    assert reqs[0]["token"] == DISPATCHER_TOKEN
    assert reqs[0]["json"] == {
        "task": "ag-123",
        "verify_cmd": "HX_TIDY_BASE=0f7b145e ./container/build.sh check"}


def test_dispatcher_token_signed_exactly_once_across_calls(tmp_path):
    with with_server(default_routes(), tmp_path) as combo:
        client = combo.client
        client.create_task(**task_payload())
        client.dispatch("ag-123")
        client.board("ag-123")
        client.board()
        client.post("ag-123", "note", "hi")
        client.message_pull()
        client.doctor()
        client.deliver("ag-123", "delivered", "a" * 40)
    requests = combo.server.requests
    cred = [r for r in requests if r["path"] == "/cred_for_sid"]
    assert len(cred) == 1, "一个客户端实例恰签发一次"
    assert len(requests) == 9  # 1 issuance + 8 task-plane calls, no re-issue
    task_reqs = [r for r in requests if r["path"] != "/cred_for_sid"]
    assert all(r["token"] == DISPATCHER_TOKEN for r in task_reqs)


# --- body shapes ------------------------------------------------------------


def test_create_task_body_shape_and_after_omitted_when_none(tmp_path):
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.create_task(**task_payload())
        combo.client.create_task(**task_payload(), after="ag-001")
    bodies = [r["json"] for r in combo.server.requests
              if r["path"] == "/task_create"]
    assert bodies[0] == {**task_payload(), "deliverable": "external_branch"}
    assert "after" not in bodies[0]
    assert bodies[1]["after"] == "ag-001"


def test_board_bodies_with_and_without_task(tmp_path):
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.board("ag-123")
        combo.client.board()
    bodies = [r["json"] for r in combo.server.requests
              if r["path"] == "/board"]
    assert bodies == [{"task": "ag-123"}, {}]


def test_deliver_omits_base_key_when_none_and_always_sends_reason(tmp_path):
    with with_server(default_routes(), tmp_path) as combo:
        combo.client.deliver("ag-123", "non_ff", "b" * 40,
                             base="c" * 40, reason="tip moved")
        combo.client.deliver("ag-123", "delivered", "b" * 40)
    bodies = [r["json"] for r in combo.server.requests
              if r["path"] == "/task_deliver"]
    assert bodies[0] == {"task": "ag-123", "outcome": "non_ff",
                         "commit": "b" * 40, "base": "c" * 40,
                         "reason": "tip moved"}
    assert "base" not in bodies[1], "base=None 时 body 不含 base 键（服务端字符串校验拒 null）"
    assert bodies[1]["reason"] == "", "reason 恒传（默认空串）"


def test_message_pull_unwraps_messages_and_post_returns_none(tmp_path):
    """F160 二段⑦：message_pull 走 v2 面 /check_mail（dispatcher 牌），
    unwrap messages；post/dispatch 恒返 None。Req: F160-compat-2"""
    with with_server(default_routes(), tmp_path) as combo:
        assert combo.client.message_pull() == [{"id": 1}]
        assert combo.client.post("ag-123", "note", "hi") is None
        assert combo.client.dispatch("ag-123") is None
    pulls = [r for r in combo.server.requests if r["path"] == "/check_mail"]
    assert len(pulls) == 1
    assert pulls[0]["token"] == DISPATCHER_TOKEN
    assert pulls[0]["json"] == {}


def test_message_pull_missing_messages_list_is_protocol_error(tmp_path):
    """F160 二段⑦：200 非 messages list（契约外载荷）＝AgoraProtocolError，
    不静默吞收件。Req: F160-compat-2"""
    routes = dict(default_routes())
    routes[("POST", "/check_mail")] = lambda rec: (200, {"ok": True})
    with pytest.raises(AgoraProtocolError):
        with with_server(routes, tmp_path) as combo:
            combo.client.message_pull()


def test_wiki_write_wire_shape(tmp_path):
    """F160 二段⑧：/wiki_write 携 dispatcher 牌，body 恰为 {path, text}。
    Req: F160-compat-2"""
    routes = dict(default_routes())
    routes[("POST", "/wiki_write")] = lambda rec: (200, {"ok": True})
    with with_server(routes, tmp_path) as combo:
        combo.client.wiki_write("issue-specs/demo-repo-7.md", "# 规格\n正文")
    reqs = [r for r in combo.server.requests if r["path"] == "/wiki_write"]
    assert len(reqs) == 1
    assert reqs[0]["token"] == DISPATCHER_TOKEN
    assert reqs[0]["json"] == {"path": "issue-specs/demo-repo-7.md",
                               "text": "# 规格\n正文"}


def test_wiki_write_rejection_surfaces_as_agora_rejected(tmp_path):
    """F160 二段⑧：服务端 4xx（如 wiki/tasks/ 前缀拒写、旧版服务 404）
    映射 AgoraRejected——调用方按回退处置。Req: F160-compat-2"""
    routes = dict(default_routes())
    routes[("POST", "/wiki_write")] = lambda rec: (
        403, {"error": "wiki/tasks/ 前缀拒写（服务独占）"})
    with pytest.raises(AgoraRejected) as excinfo:
        with with_server(routes, tmp_path) as combo:
            combo.client.wiki_write("tasks/ag-1.md", "x")
    assert "服务独占" in str(excinfo.value)


def test_task_cancel_carries_dispatcher_plane_and_task_body(tmp_path):
    """ag-1c7315：task_cancel 走任务面（dispatcher 牌）＋最小 body；
    F28 忙碌闸应答直透（2026-11-16：返回 payload 供调用方判 busy）。"""
    with with_server(default_routes(), tmp_path) as combo:
        assert combo.client.task_cancel("ag-123") == {"ok": True}
    reqs = [r for r in combo.server.requests if r["path"] == "/task_cancel"]
    assert len(reqs) == 1
    assert reqs[0]["json"] == {"task": "ag-123"}
    assert reqs[0]["token"] == DISPATCHER_TOKEN


def test_poll_interval_returns_configured_float():
    client = AgoraClient(service_name="demo", home="/nonexistent",
                         poll_interval=30)
    assert client.poll_interval() == 30.0
    assert isinstance(client.poll_interval(), float)


def test_constructor_is_lazy(tmp_path):
    """构造不触网不读盘：home 不存在也能构造，首个调用才失败。"""
    client = make_client(str(tmp_path / "no-such-home"))
    assert client.poll_interval() == 15.0


# --- error mapping -----------------------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 404, 409])
def test_4xx_is_rejected_with_server_error_text(tmp_path, status):
    routes = default_routes()
    routes[("POST", "/task_create")] = lambda rec: (
        status, {"error": f"denied-{status}"})
    with with_server(routes, tmp_path) as combo:
        with pytest.raises(AgoraRejected) as excinfo:
            combo.client.create_task(**task_payload())
    assert f"denied-{status}" in str(excinfo.value)
    assert str(status) in str(excinfo.value)


@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_is_unavailable(tmp_path, status):
    routes = default_routes()
    routes[("POST", "/task_create")] = lambda rec: (status, {"error": "boom"})
    with with_server(routes, tmp_path) as combo:
        with pytest.raises(AgoraUnavailable):
            combo.client.create_task(**task_payload())


def test_connection_refused_is_unavailable(tmp_path):
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead_port = sock.getsockname()[1]
    sock.close()
    client = make_client(agora_home(tmp_path, dead_port))
    with pytest.raises(AgoraUnavailable):
        client.create_task(**task_payload())


def test_read_timeout_is_unavailable(tmp_path):
    def slow(rec):
        time.sleep(1.0)
        return 200, {"task": "ag-123"}

    routes = default_routes()
    routes[("POST", "/task_create")] = slow
    with with_server(routes, tmp_path) as combo:
        combo.client.timeout_s = 0.3
        with pytest.raises(AgoraUnavailable):
            combo.client.create_task(**task_payload())


@pytest.mark.parametrize("payload", [
    b"<html>some other app</html>",  # non-JSON body
    b"[1, 2]",                       # JSON, not an object
    b'"text"',                       # JSON, not an object
    b"null",                         # JSON, not an object
], ids=["html", "array", "string", "null"])
def test_200_non_object_is_protocol_error(tmp_path, payload):
    routes = default_routes()
    routes[("POST", "/task_create")] = lambda rec: (200, payload)
    with with_server(routes, tmp_path) as combo:
        with pytest.raises(AgoraProtocolError):
            combo.client.create_task(**task_payload())


def test_exception_texts_never_leak_tokens(tmp_path):
    """服务端 error 文本回显收到的牌值时，异常消息也必须零牌泄漏。"""
    routes = default_routes()
    routes[("POST", "/task_create")] = lambda rec: (
        400, {"error": f"bad token {rec['token']}"})
    routes[("POST", "/board")] = lambda rec: (
        502, {"error": f"gateway saw {rec['token']}"})
    with with_server(routes, tmp_path) as combo:
        with pytest.raises(AgoraRejected) as rejected:
            combo.client.create_task(**task_payload())
        with pytest.raises(AgoraUnavailable) as unavailable:
            combo.client.board()
    assert ROOT_TOKEN not in str(rejected.value)
    assert DISPATCHER_TOKEN not in str(rejected.value)
    assert DISPATCHER_TOKEN not in str(unavailable.value)


def test_service_discovery_failures_are_unavailable(tmp_path):
    """service.json 缺失/坏、root.token 缺失——一律 AgoraUnavailable。"""
    with pytest.raises(AgoraUnavailable):
        make_client(str(tmp_path / "nowhere")).doctor()

    home = tmp_path / "plaza"
    (home / ".agora").mkdir(parents=True)
    (home / ".agora" / "root.token").write_text(ROOT_TOKEN)
    for bad in ["{}", "not json", '{"port": "x"}', '{"port": 70000}',
                '{"port": -1}']:
        (home / ".agora" / "service.json").write_text(bad)
        with pytest.raises(AgoraUnavailable):
            make_client(str(home)).doctor()
    (home / ".agora" / "service.json").write_text(json.dumps({"port": 1}))
    (home / ".agora" / "root.token").unlink()
    with pytest.raises(AgoraUnavailable):
        make_client(str(home)).doctor()


# --- F160: root.token two forms (v2 plaintext / v3 authsvc JSON) ---------------


def test_root_token_v3_authsvc_json_form(tmp_path):
    """F160：v3 authsvc 把 root.token 写成 JSON 对象 {"token": ...}——两形
    读等价：解析出的 token 仍只出现在 /cred_for_sid（其余走 v2 用例）。"""
    with fake_agora(default_routes()) as server:
        home = tmp_path / "plaza"
        (home / ".agora").mkdir(parents=True)
        (home / ".agora" / "service.json").write_text(
            json.dumps({"port": server.server_address[1]}))
        (home / ".agora" / "root.token").write_text(
            json.dumps({"token": ROOT_TOKEN}))
        client = make_client(str(home))
        assert client.message_pull() == [{"id": 1}]
    assert server.requests[0]["path"] == "/cred_for_sid"
    assert server.requests[0]["token"] == ROOT_TOKEN


@pytest.mark.parametrize("bad", [
    json.dumps({"token": 5}),      # v3 形但 token 非 str
    json.dumps({"token": None}),
    json.dumps({}),                # v3 形但缺 token 键
    json.dumps({"other": "x"}),
], ids=["non-string", "null-token", "empty-object", "wrong-key"])
def test_root_token_v3_bad_json_object_fails_closed(tmp_path, bad):
    """F160：dict 形而坏 v3 形（缺 token 键/非 str）＝发现失败
    AgoraUnavailable（fail-closed，不回退误读为明文）。"""
    home = tmp_path / "plaza"
    (home / ".agora").mkdir(parents=True)
    (home / ".agora" / "service.json").write_text(json.dumps({"port": 1}))
    (home / ".agora" / "root.token").write_text(bad)
    with pytest.raises(AgoraUnavailable):
        make_client(str(home)).doctor()


def test_root_token_numeric_plaintext_is_v2_not_v3(tmp_path):
    """F160 判形规则锚：仅 dict 形按 v3 解析——纯数字 v2 明文（恰是合法
    JSON 标量）原样作为明文 token 使用。"""
    with fake_agora(default_routes()) as server:
        home = tmp_path / "plaza"
        (home / ".agora").mkdir(parents=True)
        (home / ".agora" / "service.json").write_text(
            json.dumps({"port": server.server_address[1]}))
        (home / ".agora" / "root.token").write_text("123456\n")
        client = make_client(str(home))
        assert client.message_pull() == [{"id": 1}]
    assert server.requests[0]["token"] == "123456"


def test_illegal_token_characters_rejected_before_header(tmp_path):
    """牌值含 header 非法字符（内嵌换行）在客户端入口即拒：putheader 的
    ValueError repr 含牌值，穿透 _scrub 泄牌（终闸 C 册 finding）。"""
    routes = default_routes()
    routes[("POST", "/cred_for_sid")] = lambda rec: (
        200, {"token": "dispatcher-tok\nEvil: x", "role": "dispatcher"})
    with with_server(routes, tmp_path) as combo:
        with pytest.raises(AgoraProtocolError) as excinfo:
            combo.client.create_task(**task_payload())
    assert "Evil" not in str(excinfo.value)

    # root.token 内嵌换行：发现层即 AgoraUnavailable
    home = tmp_path / "plaza2"
    (home / ".agora").mkdir(parents=True)
    (home / ".agora" / "service.json").write_text(
        json.dumps({"port": combo.server.server_address[1]}))
    (home / ".agora" / "root.token").write_text("root-tok\nEvil: y")
    with pytest.raises(AgoraUnavailable):
        make_client(str(home)).doctor()


# --- port/base discovery cache invalidation (ag-ac5771 缺陷2) -----------------


def test_client_follows_service_json_port_change_after_refused(tmp_path):
    """ag-ac5771 缺陷2：agorad 重启换端口（service.json 变更）后，长跑
    daemon 客户端持旧端口缓存曾经终身制（Connection refused 无限打旧端口）。
    拒连必须失效端口/base 发现缓存并重发现一次：下次调用自动跟随新端口
    成功；dispatcher 牌缓存不动（零重签）。"""
    with fake_agora(default_routes()) as old_server:
        home = agora_home(tmp_path, old_server.server_address[1])
        client = make_client(home)
        assert client.message_pull() == [{"id": 1}]  # 首连成功（发现+签发）
    old_port = old_server.server_address[1]
    # 占死旧端口：新 server 必拿不同端口（确定性）。SO_REUSEADDR——旧 server
    # 已关连接的服务端 socket 残留 TIME_WAIT 时不致 EADDRINUSE（对活监听仍拒绑）。
    blocker = socket.socket()
    blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    blocker.bind(("127.0.0.1", old_port))
    try:
        with fake_agora(default_routes()) as new_server:
            assert new_server.server_address[1] != old_port
            with open(os.path.join(home, ".agora", "service.json"),
                      "w", encoding="utf-8") as f:
                json.dump({"port": new_server.server_address[1]}, f)
            assert client.message_pull() == [{"id": 1}], \
                "拒连后自动失效端口缓存、跟随 service.json 新端口"
            assert [r["path"] for r in new_server.requests] == ["/check_mail"], \
                "dispatcher 牌缓存不动：拒连只失效端口/base 发现缓存"
    finally:
        blocker.close()


def test_vanished_service_json_on_reconnect_is_unavailable(tmp_path):
    """ag-ac5771 缺陷2测试锚：拒连触发重发现时 service.json 已消失——仍按
    AgoraUnavailable（发现失败口径不变）。"""
    with fake_agora(default_routes()) as server:
        home = agora_home(tmp_path, server.server_address[1])
        client = make_client(home)
        client.message_pull()
    os.unlink(os.path.join(home, ".agora", "service.json"))
    with pytest.raises(AgoraUnavailable):
        client.message_pull()
