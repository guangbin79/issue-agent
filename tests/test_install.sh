#!/usr/bin/env bash
# Offline tests for scripts/issue-agent/{install,uninstall}.sh — run: bash tests/test_install.sh
# Self-contained: stubs curl/loginctl/systemctl on PATH; real git/python3/socket.
# Exit 0 = all assertions pass.
set -euo pipefail

PKG_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PASS=0
FAIL=0
SANDBOXES=()

ok()   { PASS=$((PASS + 1)); echo "  ok: $*"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $*" >&2; }
assert_eq() { [ "$1" = "$2" ] && ok "$3" || fail "$3: got '$1' want '$2'"; }
assert_file() { [ -f "$1" ] && ok "file exists: $1" || fail "missing file: $1"; }
assert_no() { [ ! -e "$1" ] && ok "absent: $1" || fail "should not exist: $1"; }
assert_grep() { grep -q -e "$2" "$1" && ok "match in $1: $2" || fail "no match in $1: $2"; }
assert_nogrep() { ! grep -q -e "$2" "$1" && ok "no match in $1: $2" || fail "UNEXPECTED match in $1: $2"; }

cleanup() {
  for s in ${SANDBOXES[@]+"${SANDBOXES[@]}"}; do rm -rf "$s"; done
}
trap cleanup EXIT

# --- syntax -----------------------------------------------------------------
for s in "$PKG_DIR/install.sh" "$PKG_DIR/uninstall.sh" "${BASH_SOURCE[0]}"; do
  bash -n "$s" && ok "bash -n $(basename "$s")" || fail "bash -n $(basename "$s")"
done

# --- sandbox with stateful stubs ----------------------------------------------

new_sandbox() {
  SAN="$(mktemp -d /tmp/issue-agent-install-test.XXXXXX)"
  SANDBOXES+=("$SAN")
  BIN="$SAN/bin"
  FAKE_HOME="$SAN/home"
  REPO="$SAN/repo"
  STUB_STATE="$SAN/stub"
  mkdir -p "$BIN" "$FAKE_HOME" "$REPO"

  cat > "$BIN/curl" <<'STUB'
#!/usr/bin/env bash
# Stateful offline Gitea stub. Tunables via env: STUB_STATE (required),
# STUB_USER_CODE (default 200), STUB_VERSION_CODE (default 200).
set -u
STATE="${STUB_STATE:?STUB_STATE not set}"
mkdir -p "$STATE"
method=GET url= out= wfmt= data=
while [ $# -gt 0 ]; do
  case "$1" in
    -H) shift 2 ;;
    -d) data="${2:-}"; shift 2 ;;
    -o) out="${2:-}"; shift 2 ;;
    -w) wfmt="${2:-}"; shift 2 ;;
    -X) method="${2:-GET}"; shift 2 ;;
    -s|-k|-f|-S|--silent|--insecure|--fail) shift ;;
    *) url="$1"; shift ;;
  esac
done
case "$data" in
  # B2: install.sh sends request bodies as 0600 temp files (-d @file)
  @*) data="$(cat "${data#@}")"; echo ok > "$STATE/body_via_file" ;;
esac
url="${url%%\?*}"
echo "${method} ${url}" >> "$STATE/curl.log"

emit() {
  if [ -n "$out" ]; then printf '%s' "$2" > "$out"; else printf '%s' "$2"; fi
  case "$wfmt" in
    *http_code*) printf '%b' "${wfmt//%\{http_code\}/$1}" ;;
  esac
  return 0
}

check_hook_flags() {
  python3 - "$data" "$STATE/hook_flags_ok" <<'PY' || true
import json, sys
body = json.loads(sys.argv[1])
cfg = body.get("config") or {}
required = {"type": "gitea", "active": True, "branch_filter": "*"}
if (all(body.get(k) == v for k, v in required.items())
        and body.get("events") == ["issues", "issue_comment"]
        and cfg.get("content_type") == "json"
        and cfg.get("url") and cfg.get("secret")):
    open(sys.argv[2], "w").write("ok")
PY
}

case "$url" in
  */api/v1/version)
    emit "${STUB_VERSION_CODE:-200}" '{"version":"1.21.11-stub"}' ;;
  */api/v1/user)
    emit "${STUB_USER_CODE:-200}" '{"id":1,"login":"stub-user"}' ;;
  */api/v1/repos/*/hooks/*/tests)
    emit 204 '' ;;
  */api/v1/repos/*/hooks/[0-9]*)
    id="${url##*/}"
    case "$method" in
      PATCH)
        check_hook_flags
        python3 - "$STATE/hooks.json" "$id" "$data" <<'PY' || true
import json, sys
path, i, body = sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3])
hooks = json.load(open(path))
for h in hooks:
    if h["id"] == i:
        h["config"] = body.get("config", h["config"])
json.dump(hooks, open(path, "w"))
PY
        emit 200 '{"updated":true}' ;;
      DELETE)
        python3 - "$STATE/hooks.json" "$id" <<'PY' || true
import json, sys
path, i = sys.argv[1], int(sys.argv[2])
hooks = json.load(open(path))
json.dump([h for h in hooks if h["id"] != i], open(path, "w"))
PY
        emit 204 '' ;;
      *)
        emit 404 '{}' ;;
    esac ;;
  */api/v1/repos/*/hooks)
    case "$method" in
      GET)
        if [ -f "$STATE/hooks.json" ]; then
          emit 200 "$(cat "$STATE/hooks.json")"
        else
          emit 200 '[]'
        fi ;;
      POST)
        check_hook_flags
        nid=$(( $(cat "$STATE/next_id" 2>/dev/null || echo 0) + 1 ))
        echo "$nid" > "$STATE/next_id"
        created="$(python3 - "$STATE/hooks.json" "$nid" "$data" <<'PY'
import json, sys
path, i, body = sys.argv[1], int(sys.argv[2]), json.loads(sys.argv[3])
try:
    hooks = json.load(open(path))
except Exception:
    hooks = []
hooks.append({"id": i, "config": body.get("config")})
json.dump(hooks, open(path, "w"))
print(json.dumps({"id": i, "config": body.get("config")}))
PY
)"
        emit 201 "$created" ;;
      *)
        emit 405 '{}' ;;
    esac ;;
  */api/v1/repos/*/branch_protections)
    if [ "${STUB_MASTER_PROTECTED:-true}" = "false" ]; then
      emit 200 '[]'
    else
      emit 200 '[{"branch_name":"master"}]'
    fi ;;
  */api/v1/repos/*)
    emit 200 '{"default_branch":"master"}' ;;
  *)
    emit 404 '{}' ;;
esac
exit 0
STUB

  cat > "$BIN/loginctl" <<'STUB'
#!/usr/bin/env bash
set -u
if [ "${1:-}" = "show-user" ]; then
  echo "Linger=${STUB_LINGER:-yes}"
fi
exit 0
STUB

  # Stateful systemctl stub: is-active answers from $STATE/svc.state
  # (written by enable/restart/stop) unless STUB_ACTIVE overrides; default
  # inactive so the port-bind preflight runs on fresh installs (F0 tests).
  cat > "$BIN/systemctl" <<'STUB'
#!/usr/bin/env bash
set -u
STATE="${STUB_STATE:?STUB_STATE not set}"
mkdir -p "$STATE"
echo "$*" >> "$STATE/systemctl.log"
sub=""
for a in "$@"; do
  case "$a" in
    -*) ;;
    *) sub="$a"; break ;;
  esac
done
case "$sub" in
  enable)
    if [ "${STUB_ENABLE_FAIL:-0}" = "1" ]; then exit 1; fi
    echo active > "$STATE/svc.state" ;;
  restart)
    echo active > "$STATE/svc.state" ;;
  stop|disable)
    echo inactive > "$STATE/svc.state" ;;
  is-active)
    if [ -n "${STUB_ACTIVE:-}" ]; then
      echo "$STUB_ACTIVE"
    elif [ -f "$STATE/svc.state" ]; then
      cat "$STATE/svc.state"
    else
      echo inactive
    fi ;;
  is-enabled) echo enabled ;;
esac
exit 0
STUB

  chmod +x "$BIN/curl" "$BIN/loginctl" "$BIN/systemctl"
  git -C "$REPO" init -q
  git -C "$REPO" remote add origin http://192.168.1.100:3000/acme/widget.git
  printf '{"canonical_path": "%s", "identity": "沙箱测试项目", "out_of_scope": ["兄弟仓", "运维"]}\n' \
    "$REPO" > "$REPO/.issue-agent.json"
}

run_install() {
  RC=0
  env -i HOME="$FAKE_HOME" USER="$(id -un)" PATH="$BIN:/usr/bin:/bin" \
    STUB_STATE="$STUB_STATE" STUB_LINGER="${STUB_LINGER:-yes}" \
    STUB_USER_CODE="${STUB_USER_CODE:-200}" STUB_VERSION_CODE="${STUB_VERSION_CODE:-200}" \
    STUB_ACTIVE="${STUB_ACTIVE:-}" STUB_ENABLE_FAIL="${STUB_ENABLE_FAIL:-0}" \
    STUB_MASTER_PROTECTED="${STUB_MASTER_PROTECTED:-true}" \
    ISSUE_AGENT_API_BASE="${UIA_API_BASE_ENV-http://192.168.1.100:3000}" \
    ISSUE_AGENT_PAT="${UIA_PAT_ENV-gitea-stub-token}" \
    ISSUE_AGENT_REPO="$REPO" \
    ISSUE_AGENT_CALLBACK_URL="${UIA_CB_ENV-http://192.0.2.10:8644}" \
    ISSUE_AGENT_SSL_VERIFY="${ISSUE_AGENT_SSL_VERIFY:-}" \
    ISSUE_AGENT_MAX_PARALLEL="${ISSUE_AGENT_MAX_PARALLEL:-}" \
    ISSUE_AGENT_SERVICE_NAME="${ISSUE_AGENT_SERVICE_NAME:-}" \
    ISSUE_AGENT_PORT="${ISSUE_AGENT_PORT:-}" \
    ISSUE_AGENT_DEFAULT_BRANCH="${ISSUE_AGENT_DEFAULT_BRANCH:-}" \
    ISSUE_AGENT_IDENTITY="${ISSUE_AGENT_IDENTITY:-}" \
    ISSUE_AGENT_OUT_OF_SCOPE="${ISSUE_AGENT_OUT_OF_SCOPE:-}" \
    ISSUE_AGENT_CANONICAL_REPO="${ISSUE_AGENT_CANONICAL_REPO-$REPO}" \
    bash "$PKG_DIR/install.sh" >"$SAN/install.out" 2>&1 || RC=$?
}

run_uninstall() {
  RC=0
  env -i HOME="$FAKE_HOME" USER="$(id -un)" PATH="$BIN:/usr/bin:/bin" \
    STUB_STATE="$STUB_STATE" ISSUE_AGENT_SERVICE_NAME="repo-issue-agent" \
    bash "$PKG_DIR/uninstall.sh" >"$SAN/uninstall.out" 2>&1 || RC=$?
}

config_secret() {
  python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["secret_token"])' \
    "$FAKE_HOME/.config/repo-issue-agent/config.json"
}

echo "== 1. env non-interactive install, twice (idempotency) =="
new_sandbox
run_install
assert_eq "$RC" "0" "install run 1 exit code"
CFG="$FAKE_HOME/.config/repo-issue-agent/config.json"
UNIT="$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
SNAP="$FAKE_HOME/.local/lib/repo-issue-agent"
assert_file "$SNAP/daemon.py"
assert_file "$SNAP/PROTOCOL.template.md"
assert_file "$CFG"
assert_eq "$(stat -c %a "$CFG")" "600" "config.json mode 0600"
assert_file "$UNIT"
assert_grep "$UNIT" '^ExecStart=/usr/bin/python3 %h/\.local/lib/repo-issue-agent/daemon\.py$'
assert_grep "$UNIT" '^Restart=on-failure$'
assert_grep "$UNIT" '^Environment="PATH=%h/\.opencode/bin:/usr/local/bin:/usr/bin:/bin"$'
assert_grep "$STUB_STATE/systemctl.log" 'enable --now repo-issue-agent\.service'
assert_file "$STUB_STATE/hook_flags_ok"  # webhook: gitea type, issues+issue_comment only
assert_file "$STUB_STATE/body_via_file"  # B2: hook body sent via -d @file, never on argv
assert_grep "$STUB_STATE/curl.log" 'GET .*/api/v1/user'  # PAT validated via Gitea user endpoint
python3 - "$CFG" <<'PY' && ok "config fields match config.py schema" || fail "config fields wrong"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["api_base"] == "http://192.168.1.100:3000"
assert c["pat"] == "gitea-stub-token"
assert c["gitea_user"] == "stub-user"
assert c["project"] == "acme/widget"
assert c["repo_path"].endswith("/repo")
assert c["callback_url"] == "http://192.0.2.10:8644"
assert c["port"] == 8644
assert c["ssl_verify"] is False
assert c["default_branch"] == "master"
assert len(c["secret_token"]) > 0
PY
assert_nogrep "$SAN/install.out" 'gitea-stub-token'  # PAT never echoed
assert_nogrep "$STUB_STATE/curl.log" 'gitea-stub-token'  # PAT never logged
SEC1="$(config_secret)"
run_install
assert_eq "$RC" "0" "install run 2 exit code (idempotent)"
SEC2="$(config_secret)"
assert_eq "$SEC2" "$SEC1" "secret_token reused across runs"
POSTS="$(grep -c '^POST .*/hooks$' "$STUB_STATE/curl.log" || true)"
assert_eq "$POSTS" "1" "webhook POST called exactly once across two runs"
assert_grep "$STUB_STATE/curl.log" '^PATCH .*/hooks/[0-9]'  # run 2 patched, not stacked

echo "== 2. wrong PAT -> abort exit 1, nothing written =="
new_sandbox
STUB_USER_CODE=401 run_install
assert_eq "$RC" "1" "wrong PAT exit code"
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
assert_no "$FAKE_HOME/.config/repo-issue-agent/config.json"
assert_no "$FAKE_HOME/.local/lib/repo-issue-agent"

echo "== 2b. version gate not 200 -> abort exit 1, nothing written =="
new_sandbox
STUB_VERSION_CODE=404 run_install
assert_eq "$RC" "1" "version gate exit code"
assert_grep "$SAN/install.out" 'api/v1/version'
assert_no "$FAKE_HOME/.config/repo-issue-agent/config.json"
assert_no "$FAKE_HOME/.local/lib/repo-issue-agent"

echo "== 3. listener port occupied -> exit 2 (ephemeral port, no real-host bind) =="
new_sandbox
EPH="$(python3 -c 'import socket; s=socket.socket(); s.bind(("0.0.0.0",0)); print(s.getsockname()[1]); s.close()')"
python3 - "$EPH" <<'PY' &
import socket, sys, time
s = socket.socket()
s.bind(("0.0.0.0", int(sys.argv[1])))
s.listen(1)
time.sleep(60)
PY
BLOCKER=$!
sleep 0.5
ISSUE_AGENT_PORT="$EPH" run_install
assert_eq "$RC" "2" "port occupied exit code"
assert_grep "$SAN/install.out" "端口 ${EPH} 已被占用"
kill "$BLOCKER" 2>/dev/null || true
wait "$BLOCKER" 2>/dev/null || true

echo "== 4. linger missing -> exit 1 with sudo hint =="
new_sandbox
STUB_LINGER=no run_install
assert_eq "$RC" "1" "linger missing exit code"
assert_grep "$SAN/install.out" 'sudo loginctl enable-linger'
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"

echo "== 5. uninstall: stop+unit+webhook+snapshot gone; config+state kept =="
new_sandbox
run_install
assert_eq "$RC" "0" "install before uninstall"
run_uninstall
assert_eq "$RC" "0" "uninstall exit code"
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
assert_no "$FAKE_HOME/.local/lib/repo-issue-agent"
assert_file "$FAKE_HOME/.config/repo-issue-agent/config.json"  # kept
assert_grep "$STUB_STATE/systemctl.log" 'disable --now repo-issue-agent\.service'
assert_grep "$STUB_STATE/curl.log" '^DELETE .*/hooks/[0-9]*$'
assert_grep "$SAN/uninstall.out" '已保留'

echo "== 6. rollback on non-active service: unit+snapshot+webhook removed, config kept =="
new_sandbox
STUB_ACTIVE=failed run_install
assert_eq "$RC" "1" "rollback exit code"
assert_grep "$SAN/install.out" '回滚'
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
assert_no "$FAKE_HOME/.local/lib/repo-issue-agent"
assert_file "$FAKE_HOME/.config/repo-issue-agent/config.json"  # kept by design
assert_grep "$STUB_STATE/curl.log" '^DELETE .*/hooks/[0-9]*$'  # F5: webhook deleted
assert_grep "$SAN/install.out" 'webhook'  # manual-removal hint printed

echo "== 7. unprotected default branch: loud warning, install still succeeds =="
new_sandbox
STUB_MASTER_PROTECTED=false run_install
assert_eq "$RC" "0" "unprotected-branch install exit code"
assert_grep "$SAN/install.out" '未受保护'

echo "== 8. re-run while service active: port check skipped, unit restarted =="
new_sandbox
EPH="$(python3 -c 'import socket; s=socket.socket(); s.bind(("0.0.0.0",0)); print(s.getsockname()[1]); s.close()')"
ISSUE_AGENT_PORT="$EPH" run_install
assert_eq "$RC" "0" "first install"
python3 - "$EPH" <<'PY' &
import socket, sys, time
s = socket.socket()
s.bind(("0.0.0.0", int(sys.argv[1])))
s.listen(1)
time.sleep(60)
PY
BLOCKER=$!
sleep 0.5
ISSUE_AGENT_PORT="$EPH" run_install
assert_eq "$RC" "0" "re-run while active succeeds despite the port being held"
kill "$BLOCKER" 2>/dev/null || true
wait "$BLOCKER" 2>/dev/null || true
assert_grep "$STUB_STATE/systemctl.log" 'restart repo-issue-agent\.service'
assert_grep "$SAN/install.out" '重启'  # new snapshot actually loaded

echo "== 9. ISSUE_AGENT_SSL_VERIFY=true lands in config.json =="
new_sandbox
ISSUE_AGENT_SSL_VERIFY=true run_install
assert_eq "$RC" "0" "ssl-verify install exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "ssl_verify=true written to config" || fail "ssl_verify override missing"
import json, sys
assert json.load(open(sys.argv[1]))["ssl_verify"] is True
PY

echo "== 10. detect_lan_ip prefers the route-to-Gitea-host source IP (dual-NIC) =="
new_sandbox
mkdir -p "$SAN/bin"
cat > "$SAN/bin/ip" <<'STUB'
#!/usr/bin/env bash
# Dual-NIC landscape: default route (wifi) and Gitea route (wired) differ.
set -u
STATE="${STUB_STATE:?STUB_STATE not set}"
mkdir -p "$STATE"
echo "$*" >> "$STATE/ip.log"
case "$*" in
  *"route get 192.168.1.100"*) echo "192.168.1.100 via 192.168.1.1 dev eth1 src 192.168.1.50" ;;
  *) echo "1.1.1.1 via 192.168.0.1 dev wlan0 src 192.168.0.122" ;;
esac
STUB
chmod +x "$SAN/bin/ip"
sed -n '/^detect_lan_ip()/,/^}/p' "$PKG_DIR/install.sh" > "$SAN/func.sh"
detected="$(API_BASE="http://192.168.1.100:3000" STUB_STATE="$STUB_STATE" \
  PATH="$SAN/bin:/usr/bin:/bin" bash -c 'source "$0"; detect_lan_ip' "$SAN/func.sh")"
assert_eq "$detected" "192.168.1.50" "detect_lan_ip uses route-to-Gitea-host source IP"
assert_grep "$STUB_STATE/ip.log" 'route get 192.168.1.100'

echo "== 11. instance naming: ISSUE_AGENT_SERVICE_NAME isolates unit/config/snapshot =="
new_sandbox
ISSUE_AGENT_SERVICE_NAME=issue-agent-foo ISSUE_AGENT_PORT=8644 run_install
assert_eq "$RC" "0" "named-instance install exit code"
UNIT_FOO="$FAKE_HOME/.config/systemd/user/issue-agent-foo.service"
CFG_FOO="$FAKE_HOME/.config/issue-agent-foo/config.json"
SNAP_FOO="$FAKE_HOME/.local/lib/issue-agent-foo"
assert_file "$UNIT_FOO"
assert_file "$CFG_FOO"
assert_file "$SNAP_FOO/daemon.py"
assert_grep "$UNIT_FOO" '^Environment=ISSUE_AGENT_SERVICE_NAME=issue-agent-foo$'
assert_grep "$UNIT_FOO" '^Environment=ISSUE_AGENT_PORT=8644$'
assert_grep "$STUB_STATE/systemctl.log" 'enable --now issue-agent-foo\.service'
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
assert_no "$FAKE_HOME/.config/repo-issue-agent/config.json"
python3 - "$CFG_FOO" <<'PY' && ok "named instance config fields" || fail "named instance config wrong"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["port"] == 8644
assert c["default_branch"] == "master"
PY
# per-instance idempotency: re-run patches the same hook, secret reused
SEC_A="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["secret_token"])' "$CFG_FOO")"
ISSUE_AGENT_SERVICE_NAME=issue-agent-foo ISSUE_AGENT_PORT=8644 run_install
assert_eq "$RC" "0" "named-instance re-run exit code (idempotent)"
SEC_B="$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["secret_token"])' "$CFG_FOO")"
assert_eq "$SEC_B" "$SEC_A" "named-instance secret reused across runs"
POSTS_FOO="$(grep -c '^POST .*/hooks$' "$STUB_STATE/curl.log" || true)"
assert_eq "$POSTS_FOO" "1" "webhook POST called exactly once across named-instance runs"

echo "== 12. ISSUE_AGENT_DEFAULT_BRANCH override lands in config.json =="
new_sandbox
ISSUE_AGENT_DEFAULT_BRANCH=main run_install
assert_eq "$RC" "0" "default-branch override install exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "default_branch=main written to config" || fail "default_branch override missing"
import json, sys
assert json.load(open(sys.argv[1]))["default_branch"] == "main"
PY
assert_grep "$SAN/install.out" '默认分支 main'


echo "== 13. worktree delegation: legacy env ignored, worktree_path empty, no exclude written =="
new_sandbox
ISSUE_AGENT_WORKTREE=1 ISSUE_AGENT_WORKTREE_PATH=/nonexistent/wt run_install
assert_eq "$RC" "0" "install exit code with legacy worktree env"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "worktree_path empty (agent manages its own worktrees)" || fail "worktree_path should be empty"
import json, sys
assert json.load(open(sys.argv[1]))["worktree_path"] == ""
PY
# no local-exclude line may be written anymore
if [ -f "$REPO/.git/info/exclude" ]; then
  assert_nogrep "$REPO/.git/info/exclude" "agent-worktree"
else
  ok "no info/exclude created at all"
fi
assert_nogrep "$SAN/install.out" 'worktree'  # summary no longer mentions it

echo "== 14. canonical pin: no pin source at all -> rejected offline =="
new_sandbox
printf '{"identity": "沙箱测试项目", "out_of_scope": ["兄弟仓"]}\n' > "$REPO/.issue-agent.json"
ISSUE_AGENT_CANONICAL_REPO= run_install
assert_eq "$RC" "1" "missing-pin exit code"
assert_grep "$SAN/install.out" '缺少必填项 canonical_path'
assert_nogrep "$SAN/install.out" 'Gitea API base'  # died before any prompt
assert_no "$STUB_STATE/curl.log"                   # died before any network probe
assert_no "$FAKE_HOME/.config/repo-issue-agent/config.json"

echo "== 15. canonical pin: project json canonical_path mismatch -> rejected offline =="
new_sandbox
printf '{"canonical_path": "/tmp/elsewhere-entirely"}\n' > "$REPO/.issue-agent.json"
ISSUE_AGENT_CANONICAL_REPO= run_install
assert_eq "$RC" "1" "json-pin-mismatch exit code"
assert_grep "$SAN/install.out" '仓库路径不在钉定目录'
assert_grep "$SAN/install.out" 'elsewhere-entirely'
assert_no "$STUB_STATE/curl.log"

echo "== 15b. service name derivation: canonical basename lowercased =="
new_sandbox
PROJ_DIR="$SAN/TestProject"
mkdir -p "$PROJ_DIR"
git -C "$PROJ_DIR" init -q
git -C "$PROJ_DIR" remote add origin http://192.168.1.100:3000/acme/TestProject.git
printf '{"canonical_path": "%s", "identity": "混合大小写项目", "out_of_scope": ["其它仓库"]}\n' "$PROJ_DIR" > "$PROJ_DIR/.issue-agent.json"
REPO="$PROJ_DIR" ISSUE_AGENT_CANONICAL_REPO= run_install
assert_eq "$RC" "0" "mixed-case project install exit code"
assert_file "$FAKE_HOME/.config/testproject-issue-agent/config.json"
assert_grep "$FAKE_HOME/.config/testproject-issue-agent/config.json" '"service_name": "testproject-issue-agent"'
python3 - "$FAKE_HOME/.config/testproject-issue-agent/config.json" <<'PY' && ok "derived service name lowercased into config" || fail "service_name derivation wrong"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["service_name"] == "testproject-issue-agent", c.get("service_name")
assert c["canonical_path"] == "/tmp" + "/TestProject" or c["canonical_path"].endswith("/TestProject")
PY

echo "== 16. canonical pin: existing foreign config for this service -> anti-clobber =="
new_sandbox
mkdir -p "$FAKE_HOME/.config/repo-issue-agent"
printf '{"repo_path": "/tmp/other-instance-repo"}\n' \
  > "$FAKE_HOME/.config/repo-issue-agent/config.json"
run_install
assert_eq "$RC" "1" "anti-clobber exit code"
assert_grep "$SAN/install.out" '拒绝覆盖'
assert_grep "$FAKE_HOME/.config/repo-issue-agent/config.json" 'other-instance-repo'  # untouched
assert_no "$STUB_STATE/curl.log"

echo "== 16b. legacy config (no repo_path/canonical_path) + default-derived service -> update proceeds, schema migrated =="
new_sandbox
run_install
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
for k in ("repo_path", "canonical_path"):
    c.pop(k, None)
json.dump(c, open(p, "w"))
PY
run_install
assert_eq "$RC" "0" "legacy config in-place update exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "legacy config schema migrated on update" || fail "legacy config schema not migrated"
import json, sys
c = json.load(open(sys.argv[1]))
assert c.get("repo_path", "").endswith("/repo"), c.get("repo_path")
assert c.get("canonical_path", "").endswith("/repo"), c.get("canonical_path")
PY

echo "== 16c. legacy config + service name mismatched to canonical -> refuse =="
new_sandbox
mkdir -p "$FAKE_HOME/.config/repo-issue-agent" "$SAN/other-repo"
printf '{"service_name": "repo-issue-agent", "project": "acme/repo", "port": 8642}
' \
  > "$FAKE_HOME/.config/repo-issue-agent/config.json"
REPO="$SAN/other-repo" ISSUE_AGENT_SERVICE_NAME="repo-issue-agent" run_install
assert_eq "$RC" "1" "legacy anti-clobber exit code"
assert_grep "$SAN/install.out" '拒绝覆盖'

echo "== 17. bare install never touches another instance's config =="
new_sandbox
SENTINEL="$FAKE_HOME/.config/other-svc-issue-agent/config.json"
mkdir -p "$(dirname "$SENTINEL")"
printf '{"sentinel": true}\n' > "$SENTINEL"
SHA_BEFORE="$(sha256sum "$SENTINEL" | cut -d' ' -f1)"
run_install
assert_eq "$RC" "0" "install exit code with other-instance config present"
SHA_AFTER="$(sha256sum "$SENTINEL" | cut -d' ' -f1)"
assert_eq "$SHA_AFTER" "$SHA_BEFORE" "other-instance config sha256 unchanged"

echo "== 18. origin/HEAD probe deleted: env-less install pins master =="
new_sandbox
# Realistic trap: sandbox origin/HEAD resolves to master — the OLD probe-
# based code would have derived master from exactly this layout.
git -C "$REPO" -c user.email=stub@test -c user.name=stub \
  commit -q --allow-empty -m init
git -C "$REPO" branch -M master
git -C "$REPO" update-ref refs/remotes/origin/master "$(git -C "$REPO" rev-parse HEAD)"
git -C "$REPO" symbolic-ref refs/remotes/origin/HEAD refs/remotes/origin/master
probe="$(git -C "$REPO" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null | sed 's#^origin/##')"
assert_eq "$probe" "master" "precondition: sandbox origin/HEAD resolves to master"
ISSUE_AGENT_DEFAULT_BRANCH= run_install
assert_eq "$RC" "0" "env-less install exit code with origin/HEAD -> master"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "default_branch=master despite origin/HEAD resolving elsewhere (probe deleted)" || fail "origin/HEAD leaked back into default_branch"
import json, sys
assert json.load(open(sys.argv[1]))["default_branch"] == "master"
PY
assert_grep "$SAN/install.out" '默认分支 master'

echo "== 19. STUB_ENABLE_FAIL: enable --now failure triggers full rollback =="
new_sandbox
STUB_ENABLE_FAIL=1 run_install
assert_eq "$RC" "1" "enable-failure exit code"
assert_grep "$SAN/install.out" '回滚'
assert_no "$FAKE_HOME/.config/systemd/user/repo-issue-agent.service"
assert_no "$FAKE_HOME/.local/lib/repo-issue-agent"
assert_file "$FAKE_HOME/.config/repo-issue-agent/config.json"  # kept by design
assert_grep "$STUB_STATE/curl.log" '^DELETE .*/hooks/[0-9]*$'  # F5: webhook deleted
assert_grep "$SAN/install.out" 'webhook'  # manual-removal hint printed

echo "== 20. stale same-port hook swept on install (LAN IP churn) =="
new_sandbox
mkdir -p "$STUB_STATE"
python3 - "$STUB_STATE/hooks.json" <<'PY'
import json, sys
json.dump([{"id": 41, "config": {"url": "http://192.0.2.99:8644"}}], open(sys.argv[1], "w"))
PY
run_install
assert_eq "$RC" "0" "install with stale same-port hook"
assert_grep "$STUB_STATE/curl.log" '^DELETE .*/hooks/41$'  # B3: stale swept
python3 - "$STUB_STATE/hooks.json" <<'PY' && ok "only the fresh callback remains" || fail "stale hook survived the sweep"
import json, sys
hooks = json.load(open(sys.argv[1]))
urls = [(h.get("config") or {}).get("url") for h in hooks]
assert urls == ["http://192.0.2.10:8644"], urls
PY

echo "== 21. invalid PORT / SERVICE_NAME rejected offline =="
new_sandbox
ISSUE_AGENT_PORT=8644x run_install
assert_eq "$RC" "1" "non-numeric port exit code"
assert_grep "$SAN/install.out" 'ISSUE_AGENT_PORT'
assert_no "$STUB_STATE/curl.log"  # died before any network probe
# 10# 对超 int64 字面量按 mod 2^64 回绕为 1（可骗过区间检查）——长度封死
ISSUE_AGENT_PORT=18446744073709551617 run_install
assert_eq "$RC" "1" "wraparound port rejected exit code"
assert_grep "$SAN/install.out" 'ISSUE_AGENT_PORT'
assert_no "$STUB_STATE/curl.log"
ISSUE_AGENT_SERVICE_NAME="bad/name" run_install
assert_eq "$RC" "1" "illegal service name exit code"
assert_grep "$SAN/install.out" '服务名含非法字符'
assert_no "$STUB_STATE/curl.log"

echo "== 22. required field: json without identity -> exit 1 offline =="
new_sandbox
printf '{"canonical_path": "%s"}\n' "$REPO" > "$REPO/.issue-agent.json"
ISSUE_AGENT_CANONICAL_REPO= run_install
assert_eq "$RC" "1" "missing-identity exit code"
assert_grep "$SAN/install.out" '缺少必填项 identity'
assert_no "$STUB_STATE/curl.log"

echo "== 23. port auto-allocation: 8644 first, next service gets 8645 =="
new_sandbox
run_install
assert_eq "$RC" "0" "alloc run 1 exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "first service allocated 8644" || fail "alloc base port wrong"
import json, sys
assert json.load(open(sys.argv[1]))["port"] == 8644
PY
ISSUE_AGENT_SERVICE_NAME=other-svc run_install
assert_eq "$RC" "0" "alloc run 2 (second service) exit code"
python3 - "$FAKE_HOME/.config/other-svc/config.json" <<'PY' && ok "second service allocated 8645" || fail "alloc skipped taken port"
import json, sys
assert json.load(open(sys.argv[1]))["port"] == 8645
PY
python3 - "$FAKE_HOME/.config/issue-agent-infra/ports.json" <<'PY' && ok "ports.json registry holds both" || fail "registry wrong"
import json, sys
r = json.load(open(sys.argv[1]))
assert r == {"8644": "repo-issue-agent", "8645": "other-svc"}, r
PY
# idempotent re-run of service 1 reuses ITS registered port (no drift to 8646)
run_install
assert_eq "$RC" "0" "alloc re-run exit code (idempotent)"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "re-run reused registered port" || fail "alloc not idempotent"
import json, sys
assert json.load(open(sys.argv[1]))["port"] == 8644
PY

echo "== 24. uninstall releases the auto-allocated port =="
new_sandbox
run_install
assert_eq "$RC" "0" "install before release-test"
python3 - "$FAKE_HOME/.config/issue-agent-infra/ports.json" <<'PY'
import json, sys
assert "8644" in json.load(open(sys.argv[1]))
PY
ok "port registered before uninstall"
run_uninstall
assert_eq "$RC" "0" "uninstall exit code"
python3 - "$FAKE_HOME/.config/issue-agent-infra/ports.json" <<'PY' && ok "port released from registry" || fail "port not released"
import json, sys
assert json.load(open(sys.argv[1])) == {}, json.load(open(sys.argv[1]))
PY

echo "== 25. PAT/api_base reuse from an existing same-canonical config =="
new_sandbox
run_install
assert_eq "$RC" "0" "seed install exit code"
UIA_API_BASE_ENV= UIA_PAT_ENV= run_install
assert_eq "$RC" "0" "reuse install exit code (no env api/pat)"
assert_grep "$SAN/install.out" '复用既有 config 的 pat/api_base'
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "reused values survive into config" || fail "reuse lost values"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["api_base"] == "http://192.168.1.100:3000"
assert c["pat"] == "gitea-stub-token"
PY

echo "== 26. env overriding json fields is logged to stderr, never silent =="
new_sandbox
printf '{"canonical_path": "%s", "identity": "沙箱测试项目", "out_of_scope": ["兄弟仓"], "default_branch": "master"}\n' \
  "$REPO" > "$REPO/.issue-agent.json"
ISSUE_AGENT_DEFAULT_BRANCH=dev run_install
assert_eq "$RC" "0" "env-override install exit code"
assert_grep "$SAN/install.out" '环境变量 ISSUE_AGENT_DEFAULT_BRANCH 覆盖 .issue-agent.json'
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "env value won" || fail "env did not win"
import json, sys
assert json.load(open(sys.argv[1]))["default_branch"] == "dev"
PY

echo "== 27. env passthrough: ISSUE_AGENT_IDENTITY/OUT_OF_SCOPE win over json =="
new_sandbox
ISSUE_AGENT_IDENTITY="env 身份" ISSUE_AGENT_OUT_OF_SCOPE="env范围外,运维" run_install
assert_eq "$RC" "0" "env identity install exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "env identity/out_of_scope landed in config" || fail "env passthrough clobbered by json"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["identity"] == "env 身份", c.get("identity")
assert c["out_of_scope"] == ["env范围外", "运维"], c.get("out_of_scope")
PY
assert_grep "$SAN/install.out" '环境变量 ISSUE_AGENT_IDENTITY 覆盖'

echo "== 28. reinstall merge: env-less re-run keeps existing config fields =="
new_sandbox
run_install
assert_eq "$RC" "0" "merge-test seed install exit code"
CFG="$FAKE_HOME/.config/repo-issue-agent/config.json"
# 模拟部署后的真实 config：max_parallel 已调、daemon 侧自有字段、显式 port
python3 - "$CFG" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
c["max_parallel"] = 4
c["ssl_verify"] = True
c["default_branch"] = "main"
c["port"] = 9001
c["callback_url"] = "http://10.9.9.9:7777"   # 下一步 env 显式重装必须覆盖它
c["reconcile_interval"] = 300                # daemon 侧自定义字段，install.sh 不写
c["user_custom"] = {"keep": "me"}            # 未知自定义键
json.dump(c, open(p, "w"), indent=2)
PY
run_install   # 合并字段 env 未提供；callback_url env 显式
assert_eq "$RC" "0" "merge reinstall exit code"
python3 - "$CFG" <<'PY' && ok "merge kept defaults-only + custom fields, env input won" || fail "reinstall clobbered existing config"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["max_parallel"] == 4, c["max_parallel"]
assert c["ssl_verify"] is True, c["ssl_verify"]
assert c["default_branch"] == "main", c["default_branch"]
assert c["port"] == 9001, c["port"]
assert c["reconcile_interval"] == 300, c.get("reconcile_interval")
assert c["user_custom"] == {"keep": "me"}, c.get("user_custom")
assert c["callback_url"] == "http://192.0.2.10:8644", c["callback_url"]
assert c["identity"] == "沙箱测试项目"
PY
assert_grep "$SAN/install.out" '保留既有 config 字段'
assert_grep "$SAN/install.out" '复用既有 config 的监听端口 9001'
# F-1 回归：复用端口同步注册表（首装 8644 行被替换回收，不留孤儿）
python3 - "$FAKE_HOME/.config/issue-agent-infra/ports.json" <<'PY' && ok "registry synced to reused port" || fail "registry orphaned"
import json, sys
assert json.load(open(sys.argv[1])) == {"9001": "repo-issue-agent"}, json.load(open(sys.argv[1]))
PY
# 显式端口重装：调用者自管——旧册行清掉、显式端口不入册
ISSUE_AGENT_PORT=9002 run_install
assert_eq "$RC" "0" "explicit-port reinstall exit code"
python3 - "$FAKE_HOME/.config/repo-issue-agent/config.json" <<'PY' && ok "explicit port landed" || fail "explicit port lost"
import json, sys
assert json.load(open(sys.argv[1]))["port"] == 9002
PY
python3 - "$FAKE_HOME/.config/issue-agent-infra/ports.json" <<'PY' && ok "explicit port not registered, old row cleared" || fail "registry stale after explicit port"
import json, sys
assert json.load(open(sys.argv[1])) == {}, json.load(open(sys.argv[1]))
PY
# 显式输入照常覆盖
ISSUE_AGENT_MAX_PARALLEL=2 run_install
assert_eq "$RC" "0" "explicit max_parallel reinstall exit code"
python3 - "$CFG" <<'PY' && ok "explicit ISSUE_AGENT_MAX_PARALLEL overrode config" || fail "explicit input did not win"
import json, sys
assert json.load(open(sys.argv[1]))["max_parallel"] == 2
PY
# callback_url env 未提供时复用既有值（不再回到派生默认）
UIA_CB_ENV= run_install
assert_eq "$RC" "0" "callback reuse reinstall exit code"
python3 - "$CFG" <<'PY' && ok "env-less callback kept existing value" || fail "callback not reused"
import json, sys
assert json.load(open(sys.argv[1]))["callback_url"] == "http://192.0.2.10:8644"
PY
assert_grep "$SAN/install.out" '复用既有 config 的 callback_url'

echo "== 28b. merge type guard: wrong-typed existing values fall back to defaults =="
new_sandbox
run_install
assert_eq "$RC" "0" "type-guard seed install exit code"
CFG="$FAKE_HOME/.config/repo-issue-agent/config.json"
# F-2 回归：手编失误（布尔写成字符串、数字写成字符串）不得被保留后由
# daemon 强转静默翻转——类型不符当缺失，走内置默认。
python3 - "$CFG" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
c["ssl_verify"] = "false"    # 字符串：daemon bool("false")=True 会静默反转
c["max_parallel"] = "4"      # 字符串：非真 int
c["reconcile_interval"] = 300  # 合法自定义字段，应存活
json.dump(c, open(p, "w"), indent=2)
PY
run_install
assert_eq "$RC" "0" "type-guard reinstall exit code"
python3 - "$CFG" <<'PY' && ok "wrong-typed values fell back to defaults, custom key survived" || fail "type guard leaked wrong types"
import json, sys
c = json.load(open(sys.argv[1]))
assert c["ssl_verify"] is False, c["ssl_verify"]
assert c["max_parallel"] == 1, c["max_parallel"]
assert c["reconcile_interval"] == 300, c.get("reconcile_interval")
PY
assert_grep "$SAN/install.out" '类型不符'

echo "== 28c. port registry conflict + failure-path semantics =="
new_sandbox
run_install
assert_eq "$RC" "0" "28c seed install exit code"
CFG="$FAKE_HOME/.config/repo-issue-agent/config.json"
REG="$FAKE_HOME/.config/issue-agent-infra/ports.json"
assert_eq "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["8644"])' "$REG")" \
  "repo-issue-agent" "28c seed registry row present"
# (a) 复用端口已注册给其它服务：拒绝抢占，注册表原样
printf '{"9001": "other-svc"}\n' > "$REG"
python3 - "$CFG" <<'PY'
import json, sys
p = sys.argv[1]
c = json.load(open(p))
c["port"] = 9001
json.dump(c, open(p, "w"), indent=2)
PY
run_install
assert_eq "$RC" "1" "foreign-owned reuse port rejected exit code"
assert_grep "$SAN/install.out" '拒绝抢占'
assert_eq "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])))' "$REG")" \
  "{'9001': 'other-svc'}" "registry untouched on ownership conflict"
# (b) 失败路径不释放复用行：同步后的册行在 PAT 校验失败后保留
printf '{"9001": "repo-issue-agent"}\n' > "$REG"
STUB_USER_CODE=401 run_install
assert_eq "$RC" "1" "28c pat-failure reinstall exit code"
assert_eq "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1])))' "$REG")" \
  "{'9001': 'repo-issue-agent'}" "reuse row kept across failed install"
# (c) 损坏 ports.json 不阻断显式端口安装（best-effort 清理面）
printf 'not-json{{{\n' > "$REG"
ISSUE_AGENT_PORT=9002 run_install
assert_eq "$RC" "0" "corrupt registry explicit-port install exit code"
assert_grep "$SAN/install.out" 'ports.json 无法解析'
assert_eq "$(cat "$REG")" '{}' "corrupt registry treated as empty"
# (d) 回绕端口长度封死（失败发生在动注册表之前，册保持干净）
ISSUE_AGENT_PORT=18446744073709551617 run_install
assert_eq "$RC" "1" "28c wraparound port rejected exit code"
assert_grep "$SAN/install.out" 'ISSUE_AGENT_PORT'
assert_eq "$(cat "$REG")" '{}' "registry untouched by rejected port"

echo
echo "passed=$PASS failed=$FAIL"
[ "$FAIL" -eq 0 ] || exit 1
exit 0
