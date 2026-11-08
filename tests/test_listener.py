"""TDD tests for the webhook listener — http.py's WebhookListener (Gitea HMAC).

Real sockets on 127.0.0.1:0 and a real HTTP client (urllib.request); the
server is never mocked. Every test tears its listener down.

Gitea 1.21 signs webhooks as X-Gitea-Signature: lowercase hex of
HMAC-SHA256(secret, RAW request body) — the signature must be verified
over the exact bytes received, before any JSON parsing.

Import note: scripts/issue-agent/http.py shadows the stdlib ``http`` package
whenever that directory sits on sys.path (pytest inserts it as the tests
package rootdir; ``python -m pytest`` also adds the cwd). So before touching
any http-dependent import we strip the directory from sys.path, pre-import
the stdlib http bits, then load our module under an unambiguous name.
"""

import hashlib
import hmac
import importlib.util
import json
import socket
import sys
import threading
import time
from pathlib import Path

import pytest

PKG_DIR = Path(__file__).resolve().parent.parent

# Strip the issue-agent dir from sys.path BEFORE any http-dependent import,
# or our http.py shadows the stdlib http package (pytest puts PKG_DIR on
# sys.path as the tests package rootdir; `python -m pytest` adds the cwd).
sys.path[:] = [p for p in sys.path if p not in ("", str(PKG_DIR))]

import http.client  # noqa: E402,F401  (stdlib http must win before our http.py can shadow it)
import http.server  # noqa: E402,F401
import urllib.error  # noqa: E402  (safe now: stdlib http is cached in sys.modules)
import urllib.request  # noqa: E402

_spec = importlib.util.spec_from_file_location("issue_agent_http", PKG_DIR / "http.py")
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
WebhookListener = _mod.WebhookListener

SECRET = "test-secret-token"


def sign(secret, data):
    return hmac.new(secret.encode("utf-8"), data, hashlib.sha256).hexdigest()


def issue_payload(number=5, title="build fails on master"):
    return {
        "action": "opened",
        "issue": {"number": number, "title": title, "body": "logs", "state": "open"},
        "repository": {"full_name": "acme/widget"},
        "sender": {"login": "baiguangbin"},
    }


def comment_payload(number=5, body="please fix"):
    return {
        "action": "created",
        "issue": {"number": number, "title": "t", "body": "b", "state": "open"},
        "comment": {"id": 77, "body": body, "user": {"login": "someone"}},
        "sender": {"login": "someone"},
    }


def post(port, body, secret=SECRET, path="/webhook"):
    """POST *body* (dict → JSON, or raw bytes) with an HMAC signature derived
    from *secret* over the exact bytes sent, and return the HTTP status.
    secret=None sends no signature header at all."""
    data = body if isinstance(body, bytes) else json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if secret is not None:
        headers["X-Gitea-Signature"] = sign(secret, data)
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


def get(port, path="/webhook"):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status
    except urllib.error.HTTPError as exc:
        return exc.code


@pytest.fixture()
def listener():
    lst = WebhookListener(0, SECRET, host="127.0.0.1")
    yield lst
    lst.stop()


# -- happy path: valid payloads are parsed and enqueued ------------------------


def test_issue_payload_200_and_enqueued(listener):
    port = listener.start()
    assert post(port, issue_payload()) == 200
    event = listener.queue.get(timeout=2)
    assert event["action"] == "opened"
    assert event["issue"]["number"] == 5
    assert event["issue"]["title"] == "build fails on master"


def test_comment_payload_200_and_enqueued(listener):
    port = listener.start()
    assert post(port, comment_payload()) == 200
    event = listener.queue.get(timeout=2)
    assert event["action"] == "created"
    assert event["comment"]["body"] == "please fix"
    assert event["issue"]["number"] == 5


def test_utf8_body_signed_over_raw_bytes(listener):
    port = listener.start()
    data = json.dumps(comment_payload(body="请修复构建"),
                      ensure_ascii=False).encode("utf-8")
    assert post(port, data) == 200
    event = listener.queue.get(timeout=2)
    assert event["comment"]["body"] == "请修复构建"


def test_gitlab_alias_path_still_accepted(listener):
    port = listener.start()
    assert post(port, issue_payload(), path="/gitlab") == 200
    event = listener.queue.get(timeout=2)
    assert event["issue"]["number"] == 5

def test_root_path_accepted(listener):
    # Gitea delivers to bare "/" when the callback URL is registered
    # without a path; the HMAC signature is the gate, not the route.
    port = listener.start()
    assert post(port, issue_payload(), path="/") == 200
    event = listener.queue.get(timeout=2)
    assert event["issue"]["number"] == 5


def test_arbitrary_path_accepted(listener):
    port = listener.start()
    assert post(port, issue_payload(), path="/anything/else") == 200
    event = listener.queue.get(timeout=2)
    assert event["issue"]["number"] == 5

# -- rejection paths ------------------------------------------------------------


def test_wrong_signature_404_and_nothing_enqueued(listener):
    port = listener.start()
    assert post(port, issue_payload(), secret="wrong-secret") == 404
    assert listener.queue.empty()


def test_missing_signature_404(listener):
    port = listener.start()
    assert post(port, issue_payload(), secret=None) == 404
    assert listener.queue.empty()


def test_signature_is_over_the_raw_bytes_sent(listener):
    # sign the secret correctly but over DIFFERENT bytes than are sent
    port = listener.start()
    data = json.dumps(issue_payload()).encode()
    headers = {"Content-Type": "application/json",
               "X-Gitea-Signature": sign(SECRET, b'{"different": "body"}')}
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/webhook", data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
    except urllib.error.HTTPError as exc:
        status = exc.code
    assert status == 404
    assert listener.queue.empty()




def test_get_404(listener):
    port = listener.start()
    assert get(port) == 404
    assert get(port, path="/") == 404


def test_non_json_body_with_valid_signature_200_drop_and_warn_fired():
    warnings = []
    lst = WebhookListener(0, SECRET, warn=warnings.append, host="127.0.0.1")
    port = lst.start()
    try:
        assert post(port, b"<<definitely not json>>") == 200
        assert lst.queue.empty()
        assert len(warnings) == 1
    finally:
        lst.stop()


def test_non_json_body_without_signature_404():
    lst = WebhookListener(0, SECRET, host="127.0.0.1")
    port = lst.start()
    try:
        assert post(port, b"<<definitely not json>>", secret=None) == 404
        assert lst.queue.empty()
    finally:
        lst.stop()


def test_oversized_content_length_413_without_reading_body(listener):
    # Pre-auth body cap: the declared Content-Length alone triggers 413
    # BEFORE any body byte is read — headers claim ~1 GB, no body is ever
    # sent, and the 413 still arrives (a read-first handler would block
    # waiting for a body that never comes).
    port = listener.start()
    with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
        sock.sendall(
            b"POST /webhook HTTP/1.0\r\n"
            b"Content-Length: 999999999\r\n"
            b"X-Gitea-Signature: " + b"0" * 64 + b"\r\n"
            b"\r\n"
        )
        status_line = sock.recv(1024).split(b"\r\n", 1)[0]
    assert b" 413 " in status_line
    assert listener.queue.empty()


# -- fast-ACK under load --------------------------------------------------------


def test_ack_under_2s_while_consumer_busy(listener):
    port = listener.start()
    got_first = threading.Event()

    def slow_consumer():
        listener.queue.get()  # takes the first event...
        got_first.set()
        time.sleep(3)  # ...then stays busy for a long worker turn

    threading.Thread(target=slow_consumer, daemon=True).start()
    assert post(port, issue_payload()) == 200
    assert got_first.wait(timeout=2)

    start = time.monotonic()
    assert post(port, comment_payload()) == 200
    assert time.monotonic() - start < 2.0


# -- lifecycle ------------------------------------------------------------------


def test_stop_then_post_connection_refused():
    lst = WebhookListener(0, SECRET, host="127.0.0.1")
    port = lst.start()
    lst.stop()
    with pytest.raises(urllib.error.URLError) as excinfo:
        post(port, issue_payload())
    assert isinstance(excinfo.value.reason, ConnectionRefusedError)
