"""Fast-ACK Gitea webhook listener (plan task 4, Gitea pivot).

``WebhookListener`` binds a ThreadingHTTPServer, verifies the
``X-Gitea-Signature`` header (lowercase hex of HMAC-SHA256 over the RAW
request body) in constant time on ANY POST path — the path adds no
security, the HMAC signature is the gate (Gitea may deliver to bare
``POST /`` when the callback URL is registered without a path), parses
the JSON body, hands the event to ``on_event``
(default: an in-process ``queue.Queue``, exposed as ``listener.queue`` —
task 7's worker thread consumes it serially), and answers 200
immediately. Gitea retries timed-out deliveries, so the handler does
nothing beyond the signature check, the JSON parse and the enqueue: no
fsync, no network, no worker logic. Slow processing belongs in the
worker thread, never here. The signature MUST be computed over the raw
bytes exactly as received, BEFORE any JSON parsing.

Failure policy: wrong or missing signature → 404; valid signature +
non-JSON body → 200 + drop with a message
through the injectable ``warn`` callable (never show Gitea an error that
triggers a retry storm); any non-POST method → 404.

Import note: this file is ``http.py`` and shadows the stdlib ``http``
package whenever its directory is first on sys.path (e.g. running
``python3 daemon.py`` from inside scripts/issue-agent). Load it via
``importlib.util.spec_from_file_location`` under a distinct name — see
tests/test_listener.py for the reference pattern.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import queue
import sys
import threading

# --- stdlib shadow guard ---------------------------------------------------
# This file is http.py. Whenever its directory is first on sys.path, any
# stdlib consumer (urllib & friends) imports THIS FILE as the ``http``
# package. If that happens, hand the name back to the real stdlib package
# (so http.client/http.server keep working process-wide) and park this
# module object under the unambiguous alias ``issue_agent_http``.
# The supported way to load this module is a file-location import under
# that alias — see tests/test_listener.py for the reference pattern.
_PKG_DIR = os.path.dirname(os.path.abspath(__file__))
if __name__ == "http":
    _self = sys.modules["http"]
    _saved_path = sys.path[:]
    sys.path[:] = [
        p for p in sys.path
        if isinstance(p, str) and os.path.abspath(p or ".") != _PKG_DIR
    ]
    del sys.modules["http"]
    import http as _stdlib_http  # noqa: F401  (fresh load: the real stdlib)
    sys.path[:] = _saved_path
    sys.modules.setdefault("issue_agent_http", _self)
# --- end stdlib shadow guard -----------------------------------------------

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

__all__ = ["WebhookListener"]

# Pre-auth body cap: any request whose declared Content-Length exceeds this
# is rejected (413) BEFORE the body is read — a same-segment peer needs no
# secret to otherwise grow RSS without bound (Gitea payloads stay far below
# 1 MiB).
MAX_BODY_BYTES = 4 << 20


class WebhookListener:
    """Webhook listener: any POST path, constant-time HMAC check, fast ACK.

    Authentication is the ``X-Gitea-Signature`` header (HMAC-SHA256 over
    the raw request body); the request path is irrelevant — any POST path
    is accepted.

    on_event: called with each parsed JSON event inline in the handler
        thread — must be non-blocking (enqueue-only); slow work belongs to
        the worker consuming ``self.queue`` (task 7's wiring).
    warn: receives one message string per dropped payload.
    host: bind address — tests use "127.0.0.1", production binds "0.0.0.0"
        so Gitea can reach the callback URL from the network.
    """

    def __init__(self, port, secret_token, on_event=None, warn=None, host="0.0.0.0"):
        self._host = host
        self._port = port
        self._secret = secret_token.encode("utf-8")
        self.queue = queue.Queue()
        self._on_event = on_event if on_event is not None else self.queue.put
        self._warn = warn if warn is not None else (lambda message: None)
        self._server = None
        self._thread = None

    @property
    def port(self):
        """Actual bound port (meaningful when constructed with port 0)."""
        return self._server.server_address[1] if self._server else self._port

    def start(self):
        """Bind, serve in a background thread, return the actual port."""
        listener = self

        class Handler(BaseHTTPRequestHandler):
            # Drop stalled connections: a slow-drip peer must not hold a
            # handler thread indefinitely (ThreadingHTTPServer spawns one
            # per connection with no cap).
            timeout = 30

            def do_POST(self):
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                except ValueError:
                    length = 0
                if length > MAX_BODY_BYTES:
                    self._too_large()
                    return
                body = self.rfile.read(min(length, MAX_BODY_BYTES)) if length > 0 else b""
                # HMAC over the RAW bytes, before any parsing
                signature = self.headers.get("X-Gitea-Signature", "")
                expected = hmac.new(listener._secret, body, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(signature.encode("utf-8"),
                                           expected.encode("utf-8")):
                    self._reject()
                    return
                try:
                    event = json.loads(body)
                except ValueError:
                    listener._warn(f"dropping non-JSON webhook body: {body[:64]!r}")
                    self._ok()
                    return
                listener._on_event(event)
                self._ok()

            def do_GET(self):
                self._reject()

            do_HEAD = do_PUT = do_DELETE = do_PATCH = do_OPTIONS = do_GET

            def _reject(self):
                self.send_response(404)
                self.end_headers()

            def _ok(self):
                self.send_response(200)
                self.end_headers()

            def _too_large(self):
                self.send_response(413)
                self.end_headers()

            def log_message(self, format, *args):  # noqa: A002 (stdlib signature)
                pass  # keep per-request stderr noise out of the journal

        self._server = ThreadingHTTPServer((self._host, self._port), Handler)
        self._port = self._server.server_address[1]
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="webhook-listener",
            daemon=True,
        )
        self._thread.start()
        return self._port

    def stop(self):
        """Shut down, close the listening socket, join the thread. Idempotent."""
        server, self._server = self._server, None
        thread, self._thread = self._thread, None
        if server is not None:
            server.shutdown()
            server.server_close()
        if thread is not None:
            thread.join(timeout=5)
