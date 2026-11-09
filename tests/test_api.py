# ruff: noqa: E402 -- imports are ordered around the stdlib-http preload shim below
"""Gitea API client (api.py) tests against a real local HTTP server.

The fake Gitea is a ThreadingHTTPServer bound to 127.0.0.1 on an ephemeral
port; it records every request (method/path/query/headers/body) and serves
scripted JSON. Real urllib round-trips are exercised end to end, and all
assertions run against what the server actually captured.

Gitea v1 specifics pinned here: ``Authorization: token <PAT>`` (never
PRIVATE-TOKEN), ``type=issues`` on the issues listing (Gitea mixes PRs into
that endpoint), ``state=open`` (not "opened"), and short-page pagination
(loop pages while the response fills the requested limit).

TLS behavior is covered twice: with a mock urlopen (exact context/args) and
with a real self-signed HTTPS server (openssl-generated certificate).

Run: PYTHONDONTWRITEBYTECODE=1 python3 -m pytest tests/test_api.py -q
     (from scripts/issue-agent/)
"""

import os
import sys

# The sibling http.py (task 4's listener) shadows the stdlib ``http`` package
# whenever scripts/issue-agent is first on sys.path, which breaks urllib.request
# (it imports http.client). Preload the real stdlib http with the shadowing
# entries temporarily removed, then restore them so ``from api import ...``
# and everything else still resolves. Contained to this file: we do not touch
# the other worker's module or shared conftest infrastructure.
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
import shutil
import socket
import ssl
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

import api
import api
from api import ApiAuthError, ApiTransientError, GitLabClient

PROJ = "acme/widget"
ISSUES_PATH = f"/api/v1/repos/{PROJ}/issues"
HOOKS_PATH = f"/api/v1/repos/{PROJ}/hooks"
CALLBACK = "http://127.0.0.1:8650/gitea"

# Hardcoded independently of api.py's hook body so the two can drift and the
# test still fails.
EXPECTED_HOOK_PAYLOAD = {
    "type": "gitea",
    "active": True,
    "branch_filter": "*",
    "events": ["issues", "issue_comment"],
    "config": {"url": CALLBACK, "content_type": "json", "secret": "dummy-secret"},
}


class _RecordingHandler(BaseHTTPRequestHandler):
    """Record every incoming request, then dispatch to the scripted route."""

    def _handle(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length") or 0)
        raw_body = self.rfile.read(length) if length else b""
        record = {
            "method": self.command,
            "path": parsed.path,
            "query": {k: v[0] for k, v in urllib.parse.parse_qs(parsed.query).items()},
            "auth": self.headers.get("Authorization"),
            "content_type": self.headers.get("Content-Type"),
            "json": json.loads(raw_body) if raw_body else None,
        }
        self.server.requests.append(record)
        route = self.server.routes.get((self.command, parsed.path))
        status, extra_headers, payload = (
            route(record) if route else (404, [], {"message": "not found"})
        )
        body = b"" if status == 204 else (
            payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        )
        self.send_response(status)
        for name, value in extra_headers:
            self.send_header(name, value)
        if body:
            self.send_header(
                "Content-Type", "text/html" if isinstance(payload, bytes) else "application/json"
            )
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    do_GET = do_POST = do_PATCH = do_PUT = do_DELETE = _handle

    def log_message(self, *args):  # keep pytest output pristine
        pass


@contextlib.contextmanager
def fake_gitea(routes, tls_cert=None):
    """Start the fake Gitea on 127.0.0.1:<ephemeral port>; yield the server."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
    server.requests = []
    server.routes = routes
    if tls_cert is not None:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(*tls_cert)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def make_client(server, **overrides):
    kwargs = {"api_base": f"http://127.0.0.1:{server.server_address[1]}", "pat": "dummy-pat"}
    kwargs.update(overrides)
    return GitLabClient(**kwargs)


def self_signed_cert(tmp_path):
    cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
    subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
         "-subj", "/CN=127.0.0.1", "-keyout", str(key), "-out", str(cert)],
        check=True, capture_output=True,
    )
    return str(cert), str(key)


# --- note body -----------------------------------------------------------


def test_build_note_body_format_default_marker():
    client = GitLabClient("http://gitea", "pat")
    assert client.build_note_body("hello") == f"🤖 hello\n{api.NOTE_MARKER}"


def test_build_note_body_injected_marker():
    client = GitLabClient("http://gitea", "pat", note_marker="<!-- my-svc -->")
    assert client.build_note_body("hi") == "🤖 hi\n<!-- my-svc -->"


# --- pagination + normalization ------------------------------------------


def test_list_open_issues_paginates_until_short_page_and_normalizes():
    pages = {
        1: [{"number": i, "title": f"t{i}", "body": f"b{i}", "state": "open",
             "labels": [], "user": {"login": "alice"}} for i in range(1, 51)],
        2: [{"number": i, "title": f"t{i}", "body": None, "state": "open"}
            for i in range(51, 64)],
    }

    def route(rec):
        return 200, [], pages[int(rec["query"].get("page", "1"))]

    with fake_gitea({("GET", ISSUES_PATH): route}) as server:
        issues = make_client(server).list_open_issues(PROJ)
    assert len(issues) == 63
    assert [r["query"]["page"] for r in server.requests] == ["1", "2"]  # short page stops
    for r in server.requests:
        assert r["path"] == ISSUES_PATH
        assert r["query"]["state"] == "open"  # Gitea state value, not "opened"
        assert r["query"]["type"] == "issues"  # MUST filter PRs out
        assert r["query"]["limit"] == "50"
        assert r["auth"] == "token dummy-pat"  # Gitea auth header form
    assert issues[0] == {"number": 1, "title": "t1", "body": "b1", "state": "open"}
    assert issues[62] == {"number": 63, "title": "t63", "body": "", "state": "open"}
    assert all(set(i) == {"number", "title", "body", "state"} for i in issues)


def test_list_issue_comments_normalizes_shape():
    comments_path = f"{ISSUES_PATH}/42/comments"

    def route(rec):
        return 200, [], [
            {"id": 11, "body": "first", "user": {"login": "alice", "id": 2},
             "created_at": "2026-09-26T10:00:00Z"},
            {"id": 12, "body": None, "user": {"login": "bob"}},
        ]

    with fake_gitea({("GET", comments_path): route}) as server:
        comments = make_client(server).list_issue_comments(PROJ, 42)
    rec = server.requests[0]
    assert rec["path"] == comments_path
    assert rec["auth"] == "token dummy-pat"
    assert comments == [
        {"id": 11, "body": "first", "user": {"login": "alice"}},
        {"id": 12, "body": "", "user": {"login": "bob"}},
    ]


def test_list_issue_comments_paginates_until_short_page():
    comments_path = f"{ISSUES_PATH}/42/comments"
    pages = {1: [{"id": i, "body": "x", "user": {"login": "u"}} for i in range(50)],
             2: [{"id": 100, "body": "y", "user": {"login": "u"}}]}

    def route(rec):
        return 200, [], pages[int(rec["query"].get("page", "1"))]

    with fake_gitea({("GET", comments_path): route}) as server:
        comments = make_client(server).list_issue_comments(PROJ, 42)
    assert len(comments) == 51
    assert [r["query"]["page"] for r in server.requests] == ["1", "2"]


def test_list_issue_comments_since_forwarded_when_set():
    """issue#65 后续：since_iso 非空 → since 查询参数原样直通分页基参
    （服务端不支持则忽略该参数，无回归）；缺省不带 since（全量零回归）。"""
    client = GitLabClient("http://gitea.lan:3000", "dummy-pat")
    captured = {}

    def stub_all_pages(path, base_params):
        captured["path"], captured["base_params"] = path, base_params
        return []

    client._get_all_pages = stub_all_pages
    client.list_issue_comments(PROJ, 42, since_iso="2026-11-05T00:00:00Z")
    assert captured["base_params"] == {"since": "2026-11-05T00:00:00Z"}

    captured.clear()
    client.list_issue_comments(PROJ, 42)
    assert captured["base_params"] == {}


def test_pagination_stops_at_hard_page_cap():
    """R2: a stub that always returns a full page stops at MAX_PAGES and
    raises ApiTransientError (defer-later), never a silent partial list."""
    comments_path = f"{ISSUES_PATH}/42/comments"
    full_page = [{"id": i, "body": "x", "user": {"login": "u"}}
                 for i in range(api.PAGE_LIMIT)]

    with fake_gitea({("GET", comments_path): lambda rec: (200, [], full_page)}) as server:
        with pytest.raises(ApiTransientError):
            make_client(server).list_issue_comments(PROJ, 42)
    assert len(server.requests) == api.MAX_PAGES


# --- post note -----------------------------------------------------------


def test_post_note_posts_verbatim_json_body():
    comments_path = f"{ISSUES_PATH}/42/comments"

    def route(rec):
        return 201, [], {"id": 55, "body": rec["json"]["body"]}

    with fake_gitea({("POST", comments_path): route}) as server:
        client = make_client(server)
        note = client.post_note(PROJ, 42, client.build_note_body("done, pushed branch"))
    rec = server.requests[0]
    assert rec["method"] == "POST"
    assert rec["path"] == comments_path
    assert rec["auth"] == "token dummy-pat"
    assert rec["content_type"] == "application/json"
    assert rec["json"] == {"body": f"🤖 done, pushed branch\n{api.NOTE_MARKER}"}
    assert note["id"] == 55


def test_post_note_transient_failure_is_not_retried(monkeypatch):
    """F22: POSTs are not idempotent — a 5xx note post must surface
    ApiTransientError after exactly ONE attempt (no auto-retry that could
    double-post a landed-but-unread note)."""
    monkeypatch.setattr(api, "RETRY_BACKOFF_S", 0)
    comments_path = f"{ISSUES_PATH}/42/comments"
    routes = {("POST", comments_path): lambda rec: (502, [], {"error": "bad gateway"})}
    with fake_gitea(routes) as server:
        with pytest.raises(ApiTransientError):
            client = make_client(server)
            client.post_note(PROJ, 42, client.build_note_body("reply"))
    assert [r["method"] for r in server.requests] == ["POST"], (
        "a retried POST could create a duplicate user-visible note")


# --- close issue ---------------------------------------------------------


def test_close_issue_patches_state_closed():
    """B6: close_issue PATCHes {"state": "closed"} on the issue endpoint
    (idempotent, so the transport's transient retry applies as-is)."""
    issue_path = f"{ISSUES_PATH}/42"

    def route(rec):
        return 200, [], {"number": 42, "state": "closed"}

    with fake_gitea({("PATCH", issue_path): route}) as server:
        make_client(server).close_issue(PROJ, 42)
    rec = server.requests[0]
    assert rec["method"] == "PATCH"
    assert rec["path"] == issue_path
    assert rec["auth"] == "token dummy-pat"
    assert rec["json"] == {"state": "closed"}


# --- current user --------------------------------------------------------


def test_get_user_returns_login():
    routes = {("GET", "/api/v1/user"): lambda rec: (200, [], {"login": "baiguangbin", "id": 1})}
    with fake_gitea(routes) as server:
        assert make_client(server).get_user() == "baiguangbin"
    assert server.requests[0]["auth"] == "token dummy-pat"


# --- webhook upsert ------------------------------------------------------


def test_upsert_hook_posts_when_no_hook_exists():
    created = []

    def post_hooks(rec):
        created.append(rec["json"])
        return 201, [], {"id": 5, **rec["json"]}

    with fake_gitea({
        ("GET", HOOKS_PATH): lambda rec: (200, [], []),
        ("POST", HOOKS_PATH): post_hooks,
    }) as server:
        hook = make_client(server).upsert_hook(PROJ, CALLBACK, "dummy-secret")
    assert [r["method"] for r in server.requests] == ["GET", "POST"]
    assert created == [EXPECTED_HOOK_PAYLOAD]  # events exactly issues+issue_comment
    assert created[0]["events"] == ["issues", "issue_comment"]
    assert hook["id"] == 5


def test_upsert_hook_patches_existing_and_never_stacks():
    # No POST route on purpose: a stray POST would 404 and fail the client.
    patched = []
    routes = {
        ("GET", HOOKS_PATH): lambda rec: (200, [], [
            {"id": 7, "config": {"url": CALLBACK}, "events": ["push"]}]),
        ("PATCH", f"{HOOKS_PATH}/7"): lambda rec: (patched.append(rec["json"]) or (200, [], {"id": 7})),
    }
    with fake_gitea(routes) as server:
        client = make_client(server)
        client.upsert_hook(PROJ, CALLBACK, "dummy-secret")
        client.upsert_hook(PROJ, CALLBACK, "dummy-secret")  # re-run: still idempotent
    assert [(r["method"], r["path"]) for r in server.requests] == [
        ("GET", HOOKS_PATH), ("PATCH", f"{HOOKS_PATH}/7"),
    ] * 2
    assert all(r["method"] != "POST" for r in server.requests)
    assert patched == [EXPECTED_HOOK_PAYLOAD, EXPECTED_HOOK_PAYLOAD]  # config.url match


def test_delete_hook_hits_delete_endpoint():
    routes = {("DELETE", f"{HOOKS_PATH}/9"): lambda rec: (204, [], None)}
    with fake_gitea(routes) as server:
        make_client(server).delete_hook(PROJ, 9)
    rec = server.requests[0]
    assert rec["method"] == "DELETE"
    assert rec["path"] == f"{HOOKS_PATH}/9"


# --- error mapping -------------------------------------------------------


@pytest.mark.parametrize("status", [401, 403])
def test_auth_error_is_immediate_no_retry(status):
    routes = {("GET", ISSUES_PATH): lambda rec: (status, [], {"message": "denied"})}
    with fake_gitea(routes) as server:
        with pytest.raises(ApiAuthError):
            make_client(server).list_open_issues(PROJ)
    assert len(server.requests) == 1  # never retried


def test_502_gets_exactly_one_retry_then_transient(monkeypatch):
    monkeypatch.setattr(api, "RETRY_BACKOFF_S", 0)
    routes = {("GET", ISSUES_PATH): lambda rec: (502, [], {"error": "bad gateway"})}
    with fake_gitea(routes) as server:
        with pytest.raises(ApiTransientError):
            make_client(server).list_open_issues(PROJ)
    assert [r["method"] for r in server.requests] == ["GET", "GET"]  # exactly 2 attempts


def test_unreachable_host_retries_once_then_transient(monkeypatch):
    # grab a port that nothing listens on
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    dead_port = sock.getsockname()[1]
    sock.close()
    sleeps = []
    monkeypatch.setattr(api.time, "sleep", lambda s: sleeps.append(s))
    client = GitLabClient(f"http://127.0.0.1:{dead_port}", "dummy-pat")
    with pytest.raises(ApiTransientError) as excinfo:
        client.list_hooks(PROJ)
    assert isinstance(excinfo.value.__cause__, urllib.error.URLError)
    assert len(sleeps) == 1  # exactly one retry


def test_200_non_json_body_retries_once_then_transient(monkeypatch):
    """D2/ocr#17: 200 with a non-JSON body (some other app on that port)
    is transient — retried once, then ApiTransientError, never an
    unhandled JSONDecodeError."""
    monkeypatch.setattr(api, "RETRY_BACKOFF_S", 0)
    routes = {("GET", ISSUES_PATH): lambda rec: (200, [], b"<html>not json</html>")}
    with fake_gitea(routes) as server:
        with pytest.raises(ApiTransientError) as excinfo:
            make_client(server).list_open_issues(PROJ)
    assert isinstance(excinfo.value.__cause__, json.JSONDecodeError)
    assert [r["method"] for r in server.requests] == ["GET", "GET"]  # exactly 2 attempts


# --- TLS fallback --------------------------------------------------------


class _FakeResponse:
    def __init__(self):
        self.headers = {}

    def read(self):
        return b"[]"

    def close(self):
        pass


def test_tls_fallback_uses_unverified_context_and_warns_once(monkeypatch):
    state = {"fail_next": True}
    contexts = []

    def fake_urlopen(req, timeout=None, context=None):
        contexts.append(context)
        if state["fail_next"]:
            state["fail_next"] = False
            raise urllib.error.URLError(ssl.SSLCertVerificationError("certificate verify failed"))
        return _FakeResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    warns = []
    client = GitLabClient("https://gitea.example", "dummy-pat",
                          ssl_verify=False, warn=warns.append)
    assert client.list_hooks(PROJ) == []
    assert client.list_hooks(PROJ) == []  # second call: fallback again, no new warn
    assert contexts[0] is None  # first attempt tries a verified context
    assert contexts[1].check_hostname is False
    assert contexts[1].verify_mode is ssl.CERT_NONE
    assert len(warns) == 1
    assert "unverified" in warns[0]


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl binary not available")
def test_tls_selfsigned_server_falls_back_and_warns_once(tmp_path):
    cert, key = self_signed_cert(tmp_path)
    routes = {("GET", "/api/v1/repos/own/x/hooks"): lambda rec: (200, [], [])}
    with fake_gitea(routes, tls_cert=(cert, key)) as server:
        warns = []
        client = GitLabClient(f"https://127.0.0.1:{server.server_address[1]}",
                              "dummy-pat", ssl_verify=False, warn=warns.append)
        assert client.list_hooks("own/x") == []
        assert client.list_hooks("own/x") == []
    assert len(warns) == 1
    assert "unverified" in warns[0]


@pytest.mark.skipif(shutil.which("openssl") is None, reason="openssl binary not available")
def test_tls_no_fallback_when_ssl_verify_true(tmp_path):
    cert, key = self_signed_cert(tmp_path)
    routes = {("GET", "/api/v1/repos/own/x/hooks"): lambda rec: (200, [], [])}
    with fake_gitea(routes, tls_cert=(cert, key)) as server:
        warns = []
        client = GitLabClient(f"https://127.0.0.1:{server.server_address[1]}",
                              "dummy-pat", ssl_verify=True, warn=warns.append)
        with pytest.raises((ssl.SSLCertVerificationError, urllib.error.URLError)) as excinfo:
            client.list_hooks("own/x")
    assert api._is_cert_error(excinfo.value)  # escaped as a cert error, not transient
    assert warns == []


# --- health probe --------------------------------------------------------


@pytest.mark.parametrize(
    "status,payload,expected",
    [
        (200, {"version": "1.21.11"}, True),
        (401, {"message": "401 Unauthorized"}, False),
        (404, {"message": "404 Not Found"}, False),
        (200, b"<html><body>Portal app</body></html>", False),
    ],
    ids=["ok-200", "unauthorized-401", "not-found-404", "html-is-not-gitea"],
)
def test_check_version_probe(status, payload, expected):
    routes = {("GET", "/api/v1/version"): lambda rec: (status, [], payload)}
    with fake_gitea(routes) as server:
        assert make_client(server).check_version() is expected
