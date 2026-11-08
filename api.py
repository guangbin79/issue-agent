"""Gitea REST API client for the issue agent — python3 stdlib only (urllib).

Config values (api_base/pat/ssl_verify/note_marker) are injected via the
GitLabClient constructor; this module never imports config.py, so it stays
testable. The class keeps its GitLab-era name: it is referenced by the
daemon and the e2e tooling, and churning callers for a rename buys nothing.
api_base stores the server root WITHOUT /api/v1 (e.g.
http://gitea.example.com:3000); this client appends /api/v1 to every path.
Gitea auth is ``Authorization: token <PAT>`` (never PRIVATE-TOKEN, and the
git push URL uses ``https://{user}:{PAT}@host/...`` — not oauth2:PAT).

Error mapping (callers decide what to do):
  - HTTP 401/403 -> ApiAuthError: the PAT is broken; the caller can only
    journal CRITICAL (posting a comment for help is unavailable).
  - other 4xx/5xx and network failures -> exactly one retry, then
    ApiTransientError (the caller defers the turn) — EXCEPT POSTs, which
    are never auto-retried: a request that landed but whose response was
    lost would post a second user-visible note (F22).

Pagination: Gitea paginates with limit/page query params and signals the
last page by returning fewer items than the requested limit; pages are
fetched until one comes back short — bounded by a hard page cap
(MAX_PAGES): a server that keeps returning full pages past the cap
raises ApiTransientError instead of looping (and allocating) forever;
callers already map that to defer/retry-later, and a partial list is
never returned silently (the reconcile's set-difference close would
mistake unlisted issues for closed ones).

TLS: the first attempt always verifies the certificate. On a certificate
verification failure the request is retried with an unverified SSL context
only when ssl_verify=False (self-hosted Gitea with a self-signed cert); the
warn callback fires once per client. With ssl_verify=True the certificate
error escapes unmapped and is never retried. Deployment note (F13): a
plain-HTTP LAN Gitea never exercises this path; it stays for https Giteas.
"""

import contextlib
import json
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Callable, Optional

PAGE_LIMIT = 50
MAX_PAGES = 50
RETRY_BACKOFF_S = 0.2
# Default self-identification marker (per-instance value arrives via the
# GitLabClient constructor — config.note_marker derived from service_name).
NOTE_MARKER = "<!-- issue-agent -->"


# Project-hook trigger events: ONLY issues + issue comments.
HOOK_EVENTS = ["issues", "issue_comment"]


class ApiAuthError(RuntimeError):
    """HTTP 401/403 — credentials rejected; do not retry, defer is pointless."""


class ApiTransientError(RuntimeError):
    """Other 4xx/5xx or network failure that persisted after one retry."""


def _repo_path(project) -> str:
    """'owner/repo' -> URL-encoded path keeping the slash as the separator."""
    return "/".join(urllib.parse.quote(part, safe="")
                    for part in str(project).strip("/").split("/"))


def _unverified_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context


def _is_cert_error(exc: BaseException) -> bool:
    """True for ssl.SSLCertVerificationError, raw or wrapped in URLError.reason."""
    if isinstance(exc, ssl.SSLCertVerificationError):
        return True
    return isinstance(getattr(exc, "reason", None), ssl.SSLCertVerificationError)


class GitLabClient:
    """Minimal Gitea v1 REST client for one repository at a time.

    Normalized return shapes (the contract consumed by the orchestrator and
    the webhook lane — keep exact):
      list_open_issues(project) ->
          [{"number": N, "title": str, "body": str, "state": "open"}]
      list_issue_comments(project, number) ->
          [{"id": M, "body": str, "user": {"login": str}}]
    with *project* an "owner/repo" string.
    """

    def __init__(self, api_base: str, pat: str, ssl_verify: bool = False,
                 warn: Optional[Callable[[str], None]] = None, timeout: float = 10,
                 note_marker: str = NOTE_MARKER):
        self.api_base = api_base.rstrip("/")
        self.pat = pat
        self.ssl_verify = ssl_verify
        self.timeout = timeout
        self.note_marker = note_marker
        self._warn = warn or (lambda message: None)
        self._tls_warned = False

    def build_note_body(self, text: str) -> str:
        """Wrap an agent reply with the bot prefix and the instance's
        self-identification marker (fsm filters notes carrying it)."""
        return f"🤖 {text}\n{self.note_marker}"

    # --- endpoints -------------------------------------------------------

    def list_open_issues(self, project):
        """All open issues (never PRs) of *project*, aggregated across pages.

        Gitea mixes pull requests into the issues endpoint — the mandatory
        type=issues query param filters them out.
        """
        raw = self._get_all_pages(f"/api/v1/repos/{_repo_path(project)}/issues",
                                  {"state": "open", "type": "issues"})
        return [{"number": i.get("number"), "title": i.get("title") or "",
                 "body": i.get("body") or "", "state": i.get("state") or "open"}
                for i in raw]

    def list_issue_comments(self, project, number):
        """All comments of issue *number*, aggregated across pages."""
        raw = self._get_all_pages(
            f"/api/v1/repos/{_repo_path(project)}/issues/{int(number)}/comments", {})
        return [{"id": c.get("id"), "body": c.get("body") or "",
                 "user": {"login": (c.get("user") or {}).get("login") or ""}}
                for c in raw]

    def issue_state(self, project, number):
        """State of one issue ("open"/"closed"/...) — the sweep's closed-skip
        probe; a transient failure raises and the caller treats it as open."""
        payload, _ = self._request_json(
            "GET", f"/api/v1/repos/{_repo_path(project)}/issues/{int(number)}")
        return (payload or {}).get("state") or "open"

    def post_note(self, project, number, body):
        """POST *body* verbatim as a comment (use build_note_body to wrap it)."""
        path = f"/api/v1/repos/{_repo_path(project)}/issues/{int(number)}/comments"
        note, _ = self._request_json("POST", path, body={"body": body})
        return note

    def list_hooks(self, project):
        return self._get_all_pages(f"/api/v1/repos/{_repo_path(project)}/hooks", {})

    def upsert_hook(self, project, callback_url, secret):
        """Create or update the repository webhook at *callback_url* — idempotent."""
        body = {
            "type": "gitea",
            "active": True,
            "branch_filter": "*",
            "events": list(HOOK_EVENTS),
            "config": {"url": callback_url, "content_type": "json", "secret": secret},
        }
        base = f"/api/v1/repos/{_repo_path(project)}/hooks"
        for hook in self.list_hooks(project):
            if (hook.get("config") or {}).get("url") == callback_url:
                updated, _ = self._request_json(
                    "PATCH", f"{base}/{hook['id']}", body=body)
                return updated
        created, _ = self._request_json("POST", base, body=body)
        return created

    def delete_hook(self, project, hook_id):
        self._request_json(
            "DELETE", f"/api/v1/repos/{_repo_path(project)}/hooks/{int(hook_id)}")

    def check_version(self) -> bool:
        """Health probe: True iff the Gitea API answers /api/v1/version.

        200 with a JSON {"version": ...} body counts as found — the endpoint
        is unauthenticated on Gitea, so no PAT is needed. Anything else —
        401/403/404, 5xx, an HTML page from some unrelated app on that port,
        or an unreachable host after one retry — counts as not found.
        """
        req = self._build_request("GET", "/api/v1/version")
        for attempt in (1, 2):
            try:
                resp = self._open(req)
            except urllib.error.HTTPError:
                return False
            except (urllib.error.URLError, TimeoutError) as exc:
                if _is_cert_error(exc):
                    raise  # same policy as every request: fatal when verified
                if attempt == 2:
                    return False
                time.sleep(RETRY_BACKOFF_S)
                continue
            with contextlib.closing(resp):
                raw = resp.read()
            try:
                payload = json.loads(raw.decode("utf-8"))
            except ValueError:
                return False  # 200 with HTML body: some other app lives here
            return isinstance(payload, dict) and "version" in payload
        return False

    def get_user(self) -> str:
        """Login name of the PAT owner (GET /api/v1/user) — PAT validation."""
        payload, _ = self._request_json("GET", "/api/v1/user")
        return (payload or {}).get("login") or ""

    # --- transport -------------------------------------------------------

    def _get_all_pages(self, path, base_params):
        params = dict(base_params)
        params["limit"] = PAGE_LIMIT
        results = []
        page = 1
        while True:
            params["page"] = page
            data, _ = self._request_json("GET", path, params=params)
            chunk = data or []
            results.extend(chunk)
            if len(chunk) < PAGE_LIMIT:
                return results  # short page: Gitea's last-page signal
            page += 1
            if page > MAX_PAGES:
                # A hostile/compromised Gitea (or a LAN MITM on the accepted
                # plain-HTTP callback) returning full pages forever must not
                # hang the worker or grow memory without bound.
                raise ApiTransientError(
                    f"gitea api pagination exceeded {MAX_PAGES} pages: GET {path}")

    def _request_json(self, method, path, params=None, body=None):
        """One logical request: auth errors raise immediately; transient
        failures get exactly one retry before ApiTransientError. POSTs are
        not idempotent (a lost response may still have landed the note),
        so they skip the retry and surface ApiTransientError at once —
        the caller defers and the note comes back as a deliberate, visible
        retry instead of a silent double post."""
        last_exc = None
        for attempt in (1, 2):
            try:
                return self._attempt(method, path, params, body)
            except ApiAuthError:
                raise
            except urllib.error.URLError as exc:
                if _is_cert_error(exc):
                    raise  # certificate failure under ssl_verify=True is fatal
                last_exc = exc
            except TimeoutError as exc:
                last_exc = exc
            except json.JSONDecodeError as exc:
                # 200 with a non-JSON body (some other app on that port):
                # transient like any other bad response, so it retries once
                last_exc = exc
            if method == "POST":
                break  # non-idempotent: never auto-retry (F22)
            if attempt == 1:
                time.sleep(RETRY_BACKOFF_S)
        raise ApiTransientError(
            f"gitea api unreachable after retry: {method} {path}") from last_exc

    def _attempt(self, method, path, params, body):
        req = self._build_request(method, path, params, body)
        try:
            resp = self._open(req)
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise ApiAuthError(
                    f"gitea api rejected credentials: HTTP {exc.code} "
                    f"({method} {path})") from exc
            raise
        with contextlib.closing(resp):
            raw = resp.read()
            headers = resp.headers
        data = json.loads(raw.decode("utf-8")) if raw.strip() else None
        return data, headers

    def _build_request(self, method, path, params=None, body=None):
        url = self.api_base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        data = None
        headers = {"Authorization": f"token {self.pat}"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        return urllib.request.Request(url, data=data, headers=headers, method=method)

    def _open(self, req):
        try:
            return urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.URLError as exc:
            if not _is_cert_error(exc):
                raise
            if self.ssl_verify:
                raise
            if not self._tls_warned:
                self._tls_warned = True
                self._warn(
                    f"TLS certificate verification failed for {self.api_base}; "
                    "continuing with an unverified SSL context (ssl_verify=false)")
            return urllib.request.urlopen(
                req, timeout=self.timeout, context=_unverified_context())
