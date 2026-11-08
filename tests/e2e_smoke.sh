#!/usr/bin/env bash
# issue-agent — end-to-end smoke against a REAL Gitea.
#
# Required env (missing -> listed + exit 2, works fully offline):
#   ISSUE_AGENT_API_BASE   e.g. http://192.168.1.100:3000 (Gitea web URL)
#   ISSUE_AGENT_PAT        api-scope PAT; only ever read from env, sent via a
#                          0600 header file (never echoed, logged, or on argv)
# Optional env:
#   ISSUE_AGENT_CALLBACK_URL  default: auto-detected LAN IP:<port> (as install.sh)
#   ISSUE_AGENT_REPO          default: git toplevel nearest this script
#   ISSUE_AGENT_PROJECT       default: parsed from the repo's origin remote
#   ISSUE_AGENT_STATE         default: ~/.local/state/<service>/state.json
#   ISSUE_AGENT_CA_BUNDLE     curl --cacert; otherwise -k (self-signed LAN)
#   E2E_LOG                   default: main-repo .omo/evidence/.../task-9-e2e.log
#
# Exit codes: 0 all assertions passed; 1 any failed; 2 required env missing.
#
# Footprint: one throwaway issue (closed at the end), one issue/<number>-fix
# branch (deleted at the end), and the local user install (uninstall+reinstall
# is itself the step-10 replay assertion). master is never pushed.
set -u

# --- required env (first, so the offline check needs nothing else) -----------
missing=()
[ -z "${ISSUE_AGENT_API_BASE:-}" ] && missing+=(ISSUE_AGENT_API_BASE)
[ -z "${ISSUE_AGENT_PAT:-}" ] && missing+=(ISSUE_AGENT_PAT)
if [ "${#missing[@]}" -gt 0 ]; then
  echo "e2e-smoke: 缺少必需环境变量：${missing[*]}" >&2
  echo "e2e-smoke: export ISSUE_AGENT_API_BASE=<gitea-api-base> ISSUE_AGENT_PAT=<token> 后重跑" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_TOP="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
# Instance identity must match install.sh's derivation (canonical basename
# lowercased, non-[a-z0-9._-] folded to "-", + "-issue-agent") so this
# smoke test targets the very unit install.sh creates; an explicit
# ISSUE_AGENT_SERVICE_NAME still wins. Exported together with the port so
# install.sh/uninstall.sh see the same instance instead of re-deriving
# (and auto-allocating a different port).
if [ -n "${ISSUE_AGENT_SERVICE_NAME:-}" ]; then
  SERVICE_NAME="${ISSUE_AGENT_SERVICE_NAME}"
else
  _E2E_CANON="${ISSUE_AGENT_REPO:-${REPO_TOP:-.}}"
  SERVICE_NAME="$(printf '%s' "$(basename "${_E2E_CANON%/}")" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9._-' '-')-issue-agent"
fi
PORT="${ISSUE_AGENT_PORT:-8644}"
export ISSUE_AGENT_SERVICE_NAME="$SERVICE_NAME"
export ISSUE_AGENT_PORT="$PORT"
API_ROOT="${ISSUE_AGENT_API_BASE%/}"
# E2: derive the evidence log from the git toplevel — a hardcoded absolute
# path breaks on any other machine (ocr#19).
DEFAULT_E2E_LOG="${REPO_TOP:-$SCRIPT_DIR}/.omo/evidence/${SERVICE_NAME}/task-9-e2e.log"
E2E_LOG="${E2E_LOG:-$DEFAULT_E2E_LOG}"
STATE_FILE="${ISSUE_AGENT_STATE:-${HOME}/.local/state/${SERVICE_NAME}/state.json}"
USER_NAME="${USER:-$(id -un)}"

CURL_TLS=(-k)
if [ -n "${ISSUE_AGENT_CA_BUNDLE:-}" ]; then
  CURL_TLS=(--cacert "${ISSUE_AGENT_CA_BUNDLE}")
fi

# PAT -> 0600 header file read by curl -H @file (never on a world-readable argv).
HDR_DIR="${XDG_RUNTIME_DIR:-${HOME}/.local/state/${SERVICE_NAME}}"
mkdir -p "$HDR_DIR" 2>/dev/null || true
HDR_FILE="$(mktemp -p "$HDR_DIR" 2>/dev/null || mktemp)"
chmod 600 "$HDR_FILE"
printf 'Authorization: token %s\n' "$ISSUE_AGENT_PAT" > "$HDR_FILE"

FAILED=0
CLEANUP_DONE=0
IID=""

log()  { echo "[e2e] $*"; }
step() { echo; echo "=== STEP $1: $2 ==="; }
pass() { echo "[PASS] step $1"; }
fail() { echo "[FAIL] step $1: $2"; FAILED=1; }
skip() { echo "[SKIP] step $1: $2"; }

# --- log: everything to stdout AND appended to the evidence log --------------
mkdir -p "$(dirname "$E2E_LOG")"
exec > >(tee -a "$E2E_LOG") 2>&1

restore_test_interval() {
  # E1: step 2c shrinks reconcile_interval to 15s for the test window —
  # put the pre-test value back (and restart to load it) so the user's
  # install keeps its cadence once the smoke run ends (ocr#9).
  local cfg changed
  [ -n "${ORIG_INTERVAL:-}" ] || return 0
  cfg="${HOME}/.config/${SERVICE_NAME}/config.json"
  changed="$(python3 - "$cfg" "$ORIG_INTERVAL" <<'EOF' 2>/dev/null || true
import json, sys
p, v = sys.argv[1], int(sys.argv[2])
try:
    cfg = json.load(open(p))
except Exception:
    sys.exit(0)
if cfg.get("reconcile_interval") == v:
    print("no")
else:
    cfg["reconcile_interval"] = v
    with open(p, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print("yes")
EOF
)"
  if [ "$changed" = "yes" ]; then
    systemctl --user restart "${SERVICE_NAME}.service" 2>/dev/null || true
    log "reconcile_interval 已恢复为 ${ORIG_INTERVAL}s（服务已重启加载）"
  fi
}

on_exit() {
  rc=$?
  if [ "${CLEANUP_DONE:-0}" -eq 0 ]; then do_cleanup; fi
  restore_test_interval
  if [ "$rc" -ne 0 ]; then FAILED=1; fi
  if [ "$FAILED" -eq 0 ]; then
    echo "E2E RESULT: PASS"; final=$rc
  else
    echo "E2E RESULT: FAIL"; final=1
  fi
  sleep 1  # let the tee process substitution drain to the log file
  rm -f "$HDR_FILE"
  exit "$final"
}
trap on_exit EXIT

# --- helpers -------------------------------------------------------------------

gitea() { # gitea METHOD PATH [JSON_BODY] -> sets REPLY_BODY + HTTP_CODE
  [ -s "$HDR_FILE" ] || { printf 'Authorization: token %s\n' "$ISSUE_AGENT_PAT" > "$HDR_FILE"; chmod 600 "$HDR_FILE"; }
  local method="$1" path="$2" body="${3:-}" out
  if [ -n "$body" ]; then
    out="$(curl -s "${CURL_TLS[@]}" -X "$method" -H @"$HDR_FILE" \
           -H 'Content-Type: application/json' -d "$body" --max-time 60 \
           -w '\n%{http_code}' "${API_ROOT}${path}" 2>/dev/null || true)"
  else
    out="$(curl -s "${CURL_TLS[@]}" -X "$method" -H @"$HDR_FILE" --max-time 60 \
           -w '\n%{http_code}' "${API_ROOT}${path}" 2>/dev/null || true)"
  fi
  HTTP_CODE="${out##*$'\n'}"
  REPLY_BODY="${out%$'\n'*}"
}

field() { # field <name> — extract top-level field from $REPLY_BODY
  printf '%s' "$REPLY_BODY" | python3 -c '
import json, os, sys
try:
    v = json.load(sys.stdin).get(sys.argv[1])
    print(v if v is not None else "")
except Exception:
    pass' "$1" 2>/dev/null || true
}

issue_state() {
  printf '%s' "$REPLY_BODY" | python3 -c '
import json, os, sys
try:
    print(json.load(sys.stdin).get("state", ""))
except Exception:
    pass' 2>/dev/null || true
}

ledger_get() { # ledger_get <key> — status / session_id of $IID ("" if absent)
  [ -f "$STATE_FILE" ] || return 0
  python3 - "$STATE_FILE" "$IID" "$1" <<'EOF' 2>/dev/null || true
import json, os, sys
try:
    d = json.load(open(sys.argv[1]))
except Exception:
    sys.exit(0)
entry = (d.get("issues") or {}).get(sys.argv[2]) or {}
v = entry.get(sys.argv[3])
print(v if v is not None else "")
EOF
}

post_note() { # post_note <body> — human comment on the test issue
  local body
  body="$(python3 -c 'import json, sys; print(json.dumps({"body": sys.argv[1]}, ensure_ascii=False))' "$1")"
  gitea POST "/api/v1/repos/${PROJECT}/issues/${IID}/comments" "$body"
  [ "$HTTP_CODE" = 201 ]
}

# Refresh REPLY_BODY from the comments API, then set (via eval):
#   AGENT_COUNT / MAX_AGENT_ID / ACK_SEEN / RESUME_SEEN
# Agent comments are identified by the self marker (api.NOTE_MARKER).
fetch_notes() {
  gitea GET "/api/v1/repos/${PROJECT}/issues/${IID}/comments?limit=50&direction=asc"
  eval "$(printf '%s' "$REPLY_BODY" | SERVICE_NAME="$SERVICE_NAME" python3 -c '
import json, os, sys
try:
    notes = json.load(sys.stdin)
except Exception:
    notes = []
marker = "<!-- " + os.environ.get("SERVICE_NAME", "") + " -->"
agent = [n for n in notes if marker in (n.get("body") or "")]
print("AGENT_COUNT=%d" % len(agent))
print("MAX_AGENT_ID=%d" % max((n.get("id", 0) for n in agent), default=0))
print("ACK_SEEN=%d" % any("已接手" in (n.get("body") or "") for n in agent))
print("RESUME_SEEN=%d" % any("恢复" in (n.get("body") or "") for n in agent))
' 2>/dev/null || echo 'AGENT_COUNT=0
MAX_AGENT_ID=0
ACK_SEEN=0
RESUME_SEEN=0')"
}

# poll_until <desc> <timeout_s> <interval_s> <condition_fn>
# The condition fn runs right after each fetch_notes(); globals survive the
# extra gitea() calls a condition may make.
poll_until() {
  local desc="$1" timeout="$2" interval="$3" fn="$4" start=$SECONDS
  log "poll: ${desc}（≤${timeout}s）"
  while :; do
    fetch_notes
    if "$fn"; then
      log "poll: ${desc} — 满足（耗时 $((SECONDS - start))s）"
      return 0
    fi
  if (( SECONDS - start >= timeout )); then break; fi
    sleep "$interval"
  done
  log "poll: ${desc} — 超时（${timeout}s）"
  return 1
}

cond_ack()          { [ "${ACK_SEEN:-0}" -eq 1 ]; }
cond_followup()     { [ "${MAX_AGENT_ID:-0}" -gt "$FOLLOWUP_BASE" ]; }
cond_resume()       { [ "${RESUME_SEEN:-0}" -eq 1 ] && [ "${MAX_AGENT_ID:-0}" -gt "$RESUME_BASE" ]; }
cond_ledger_done()  { [ "$(ledger_get status)" = "done" ]; }
cond_ledger_active() { [ "$(ledger_get status)" = "active" ]; }
branch_exists() {
  gitea GET "/api/v1/repos/${PROJECT}/branches/${BRANCH_ENC}"
  [ "$HTTP_CODE" = 200 ]
}
cond_first_reply() {
  [ "${AGENT_COUNT:-0}" -ge 2 ] || return 1
  branch_exists
}

# detect_lan_ip — same fallback chain as install.sh (route toward the
# Gitea host FIRST: on dual-NIC machines the callback must advertise the
# NIC Gitea can actually reach, not the default-route one)
detect_lan_ip() {
  local ip="" host
  host="$(printf '%s' "${ISSUE_AGENT_API_BASE:-}" | sed -E 's#^[a-zA-Z][a-zA-Z0-9+.-]*://##; s#[/:].*##')"
  if [ -n "$host" ]; then
    ip="$(ip -4 route get "$host" 2>/dev/null \
          | awk '{for (i = 1; i <= NF; i++) if ($i == "src") {print $(i+1); exit}}' || true)"
  fi
  if [ -z "$ip" ]; then
    ip="$(ip -4 route get 1.1.1.1 2>/dev/null \
          | awk '{for (i = 1; i <= NF; i++) if ($i == "src") {print $(i+1); exit}}' || true)"
  fi
  if [ -z "$ip" ]; then
    ip="$( (hostname -I 2>/dev/null || true) | awk '{print $1; exit}')"
  fi
  echo "${ip:-127.0.0.1}"
}

# --- defaults (computed pre-install; install.sh needs them exported) ----------

REPO_PATH="${ISSUE_AGENT_REPO:-$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)}"
CALLBACK_URL="${ISSUE_AGENT_CALLBACK_URL:-http://$(detect_lan_ip):${PORT}}"
# headline properties under test: (a) the agent manages its own worktrees
# (named <repo>__wt-* beside the repo) — none may be LEFT OVER when the
# test window closes; (b) the main working copy stays on its pre-turn branch.
PRE_TURN_BRANCH="$(git -C "${REPO_PATH:-/nonexistent}" branch --show-current 2>/dev/null || true)"
WT_GLOB="$(dirname "${REPO_PATH:-/nonexistent}")/$(basename "${REPO_PATH:-/nonexistent}")__wt-*"
WT_BEFORE="$(ls -d ${WT_GLOB} 2>/dev/null | sort || true)"

if [ -n "${ISSUE_AGENT_PROJECT:-}" ]; then
  PROJECT="$ISSUE_AGENT_PROJECT"
else
  origin="$(git -C "${REPO_PATH:-/nonexistent}" remote get-url origin 2>/dev/null || true)"
  PROJECT="$(python3 - "$origin" <<'EOF' 2>/dev/null || true
import sys
from urllib.parse import urlparse
url = sys.argv[1].strip()
tail = url.split("://", 1)[-1]
path = tail.split(":", 1)[-1] if "://" not in url else urlparse(url).path
path = path.strip("/")
if path.endswith(".git"):
    path = path[:-4]
print(path)
EOF
)"
fi
# Gitea addresses a repo as two path segments (owner/repo); keep the slash.


export ISSUE_AGENT_API_BASE ISSUE_AGENT_PAT ISSUE_AGENT_CALLBACK_URL ISSUE_AGENT_REPO
ISSUE_AGENT_REPO="$REPO_PATH"; export ISSUE_AGENT_REPO
ISSUE_AGENT_CALLBACK_URL="$CALLBACK_URL"; export ISSUE_AGENT_CALLBACK_URL

log "e2e-smoke 开始：api=${ISSUE_AGENT_API_BASE} project=${PROJECT:-?} repo=${REPO_PATH:-?}"
log "callback=${CALLBACK_URL} ledger=${STATE_FILE} log=${E2E_LOG}"

# --- step 1: preflight ---------------------------------------------------------

preflight() {
  for cmd in curl python3 git systemctl loginctl; do
    command -v "$cmd" >/dev/null 2>&1 || { echo "缺少命令：$cmd"; return 1; }
  done
  gitea GET /api/v1/version
  case "$HTTP_CODE" in
    200) log "API 可达（HTTP 200；Gitea 版本接口无需认证）" ;;
    *) echo "API 探测失败：${ISSUE_AGENT_API_BASE}/api/v1/version 返回 ${HTTP_CODE:-无响应}（期望 200）；请确认 Gitea web URL"; return 1 ;;
  esac
  if [ -z "$REPO_PATH" ] || ! git -C "$REPO_PATH" rev-parse --git-dir >/dev/null 2>&1; then
    echo "仓库路径不可用：${REPO_PATH:-<空>}；export ISSUE_AGENT_REPO=<项目路径> 后重跑"; return 1
  fi
  if [ -z "$PROJECT" ]; then
    echo "无法从 origin 解析项目路径（origin=${origin:-无}）；export ISSUE_AGENT_PROJECT=<owner/repo> 后重跑"; return 1
  fi
  linger="$(loginctl show-user "$USER_NAME" -p Linger 2>/dev/null || true)"
  if [ "$linger" != "Linger=yes" ]; then
    echo "systemd linger 未启用（${linger:-无响应}）——请先执行: sudo loginctl enable-linger ${USER_NAME} 然后重跑"; return 1
  fi
  log "linger 已启用；预检通过"
}

# --- step 11 / trap: cleanup (always attempted) --------------------------------

do_cleanup() {
  CLEANUP_DONE=1
  if [ -z "$IID" ]; then
    log "cleanup: 无测试 issue（未创建或创建失败），仅输出回执"
    echo "cleanup 回执：issue=none branch=none（无可清理对象）"
    return 0
  fi
  local close_code="" del_code="" already=closed
  gitea GET "/api/v1/repos/${PROJECT}/issues/${IID}"
  if [ "$HTTP_CODE" != 200 ]; then
    echo "cleanup: 状态查询异常 URL=${API_ROOT}/api/v1/repos/${PROJECT}/issues/${IID} HTTP=${HTTP_CODE} body=${REPLY_BODY:-<empty>}（仍尝试 close）"
  fi
  if [ "$(issue_state)" != "closed" ]; then
    already=no
    gitea PATCH "/api/v1/repos/${PROJECT}/issues/${IID}" \
      '{"state": "closed"}'
    close_code="$HTTP_CODE"
    case "$close_code" in
      200|201) : ;;
      *) echo "cleanup: close 失败 URL=${API_ROOT}/api/v1/repos/${PROJECT}/issues/${IID} HTTP=${close_code} body=${REPLY_BODY:-<empty>}" ;;
    esac
  fi
  gitea DELETE "/api/v1/repos/${PROJECT}/branches/${BRANCH_ENC}"
  del_code="$HTTP_CODE"
  echo "cleanup 回执：issue #${IID} close=${already}(${close_code:-skip}) branch=${BRANCH_ENC} delete=${del_code:-无响应}"
  case "$del_code" in
    200|204|404) return 0 ;;
    *) return 1 ;;
  esac
}

# --- steps 3-9: verification chain (stops at first failure) --------------------

run_verify() {
  local TS T0 FOLLOWUP_BASE="" PAUSE_BASE="" RESUME_BASE=""

  # step 3: create the throwaway test issue
  step 3 "创建测试 issue"
  TS="$(date +%Y%m%d-%H%M%S)"
  local title body
  title="[e2e-smoke] 端到端验证 ${TS}"
  body="e2e 冒烟测试 issue（用后即关）。请在对应的 issue/<iid>-fix 分支上新增一个仅用于验证的空文件 e2e_probe.txt 并提交；除此以外请勿做任何真实修改。" # iid=issue number on Gitea
  body="$(t="$title" b="$body" python3 <<'EOF'
import json, os
print(json.dumps({"title": os.environ["t"], "body": os.environ["b"]}, ensure_ascii=False))
EOF
)"
  gitea POST "/api/v1/repos/${PROJECT}/issues" "$body"
  if [ "$HTTP_CODE" != 201 ]; then fail 3 "创建 issue 失败（HTTP ${HTTP_CODE}）"; return; fi
  IID="$(field number)"
  BRANCH_ENC="$(python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "issue/${IID}-fix")"
  T0=$SECONDS
  log "issue #${IID} 已创建：${title}；分支断言目标 issue/${IID}-fix（t0 已记录）"
  pass 3

  # step 4: ack note (🤖 已接手) within 60s
  step 4 "≤60s 收到「已接手」确认 note"
  if poll_until "已接手 note" 60 5 cond_ack; then
    log "确认 note 到达，距 t0 $((SECONDS - T0))s"
    pass 4
  else
    fail 4 "60s 内未见 agent 确认 note（AGENT_COUNT=${AGENT_COUNT:-0}）——检查 journalctl --user -u ${SERVICE_NAME}"; return
  fi

  # step 5: substantive first reply (2nd agent note) + pushed fix branch, ≤35min
  step 5 "≤35min 实质首评 + issue/${IID}-fix 已推"
  if poll_until "实质首评+分支" 2100 15 cond_first_reply; then
    log "首评与分支均就绪，距 t0 $((SECONDS - T0))s"
    pass 5
  else
    fetch_notes
    gitea GET "/api/v1/repos/${PROJECT}/branches/${BRANCH_ENC}"
    fail 5 "超时：AGENT_COUNT=${AGENT_COUNT:-0}（需≥2），branch HTTP=${HTTP_CODE}（需 200）"; return
  fi

  # step 5b: the user's main working copy was never touched — still on its
  # pre-turn branch, which is the repo's default branch
  local now_branch def_br
  now_branch="$(git -C "$REPO_PATH" branch --show-current 2>/dev/null || true)"
  def_br="$(service_name="$SERVICE_NAME" python3 -c '
import json, os
from pathlib import Path
try:
    p = Path.home() / ".config" / os.environ["service_name"] / "config.json"
    print(json.loads(p.read_text()).get("default_branch") or "master")
except Exception:
    print("")' 2>/dev/null || true)"
  if [ "$now_branch" = "$PRE_TURN_BRANCH" ] && { [ -z "$def_br" ] || [ "$now_branch" = "$def_br" ]; }; then
    log "主仓零接触确认：branch=${now_branch:-<detached>}（default=${def_br:-?}）"
  else
    fail 5 "主仓工作副本被改动：pre=${PRE_TURN_BRANCH:-?} now=${now_branch:-?} default=${def_br:-?}"; return
  fi

  # step 5c: no leftover agent worktree dirs beside the repo (the agent
  # creates AND removes its own worktrees; the daemon never creates any).
  # Zero new __wt-* dirs is the expected outcome; any NEW dir is a fail.
  local wt_now
  wt_now="$(ls -d ${WT_GLOB} 2>/dev/null | sort || true)"
  if [ "$wt_now" = "$WT_BEFORE" ]; then
    log "无遗留 worktree 目录（${WT_GLOB}）"
  else
    fail 5 "出现遗留 worktree 目录：before=[${WT_BEFORE}] now=[${wt_now}]"; return
  fi

  # step 6: follow-up comment -> continued conversation reply
  step 6 "续聊：追问后获得新回复"
  FOLLOWUP_BASE="$MAX_AGENT_ID"
  if ! post_note "请补充说明你的修改思路"; then fail 6 "追问评论发送失败（HTTP ${HTTP_CODE}）"; return; fi
  log "追问已发送（基线 note id=${FOLLOWUP_BASE}）"
  if poll_until "续聊新回复" 2100 15 cond_followup; then
    pass 6
  else
    fail 6 "35min 内未见续聊回复（MAX_AGENT_ID=${MAX_AGENT_ID:-0} ≤ 基线 ${FOLLOWUP_BASE}）"; return
  fi

  # step 7: pause -> bait comment -> 90s silence
  step 7 "暂停静默：暂停后 90s 无新 agent note"
  # 暂停不中断在途回合：若 step 6 的续聊回合仍在 flight（status=pending），
  # 其回复 note 会落在 90s 静默窗口内造成假失败——先排空再取基线
  if ! poll_until "在途回合排空（ledger→active）" 2100 15 cond_ledger_active; then
    fail 7 "在途回合未结束（status=$(ledger_get status)），无法开始暂停测试"; return
  fi
  fetch_notes
  PAUSE_BASE="$MAX_AGENT_ID"
  if ! post_note "暂停"; then fail 7 "「暂停」评论发送失败（HTTP ${HTTP_CODE}）"; return; fi
  if ! post_note "这条不应被回复"; then fail 7 "诱饵评论发送失败（HTTP ${HTTP_CODE}）"; return; fi
  log "已发「暂停」+诱饵评论（基线 note id=${PAUSE_BASE}），静默观察 90s"
  sleep 90
  fetch_notes
  if [ "${MAX_AGENT_ID:-0}" -le "$PAUSE_BASE" ]; then
    log "静默确认：MAX_AGENT_ID=${MAX_AGENT_ID} ≤ 基线 ${PAUSE_BASE}"
    pass 7
  else
    fail 7 "暂停后出现新 agent note（id=${MAX_AGENT_ID} > 基线 ${PAUSE_BASE}）"; return
  fi

  # step 8: resume -> 🤖 恢复 confirmation within 60s
  step 8 "≤60s 收到「恢复」确认 note"
  RESUME_BASE="$MAX_AGENT_ID"
  if ! post_note "恢复"; then fail 8 "「恢复」评论发送失败（HTTP ${HTTP_CODE}）"; return; fi
  if poll_until "恢复确认 note" 60 5 cond_resume; then
    pass 8
  else
    fail 8 "60s 内未见恢复确认 note（RESUME_SEEN=${RESUME_SEEN:-0}）"; return
  fi

  # step 9: close via API -> ledger status done within 60s
  step 9 "close 后账本状态 done"
  gitea PATCH "/api/v1/repos/${PROJECT}/issues/${IID}" '{"state": "closed"}'
  case "$HTTP_CODE" in
    200|201) : ;;  # Gitea EditIssue 对状态变更返回 201（200 亦接受）
    *) fail 9 "关闭 issue 失败（HTTP ${HTTP_CODE}）URL=${API_ROOT}/api/v1/repos/${PROJECT}/issues/${IID} body=${REPLY_BODY:-<empty>}"; return ;;
  esac
  if poll_until "账本 done" 60 5 cond_ledger_done; then
    log "账本 ${STATE_FILE} 中 issue #${IID} 状态为 done"
    pass 9
  else
    fail 9 "60s 内账本未到 done（当前: $(ledger_get status || echo '?')）"; return
  fi
}

# replay (step 10): uninstall + reinstall, then 120s watching for any replay
run_replay_check() {
  step 10 "重装不重放：uninstall+install 后 120s 无新会话/新 note"
  local replay_base replay_session_pre start
  fetch_notes
  replay_base="$MAX_AGENT_ID"
  replay_session_pre="$(ledger_get session_id)"
  log "重装前基线：MAX_AGENT_ID=${replay_base} session_id=${replay_session_pre:-<无>}"
  if ! bash "${SCRIPT_DIR}/../uninstall.sh"; then fail 10 "uninstall.sh 非零退出"; return; fi
  if ! bash "${SCRIPT_DIR}/../install.sh"; then fail 10 "重装 install.sh 非零退出"; return; fi
  local state
  state="$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
  if [ "$state" != "active" ]; then fail 10 "重装后服务非 active（${state:-unknown}）"; return; fi
  local violated=0
  start=$SECONDS
  while (( SECONDS - start < 120 )); do
    fetch_notes
    if [ "${MAX_AGENT_ID:-0}" -gt "$replay_base" ]; then violated=1; break; fi
    sleep 10
  done
  local session_post
  session_post="$(ledger_get session_id)"
  if [ "$violated" -eq 1 ]; then
    fail 10 "重装后出现新 agent note（id=${MAX_AGENT_ID} > 基线 ${replay_base}）——历史被重放"
  elif [ "$session_post" != "$replay_session_pre" ]; then
    fail 10 "账本 session_id 变化：${replay_session_pre:-<无>} -> ${session_post:-<无>}"
  else
    log "120s 观察完毕：无新 note，session_id 未变（${session_post:-<无>}）"
    pass 10
  fi
}

# --- main ----------------------------------------------------------------------

step 1 "预检：API 可达 / 仓库 / linger"
if preflight; then
  pass 1
else
  fail 1 "预检失败（上方含修复提示）"
fi

if [ "$FAILED" -eq 0 ]; then
  step 2 "env 非交互安装 + 服务 active"
  if bash "${SCRIPT_DIR}/../install.sh" && \
     [ "$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)" = "active" ]; then
    pass 2
  else
    fail 2 "install.sh 失败或服务未 active——检查上方 install 输出与 journalctl --user -u ${SERVICE_NAME}"
  fi
else
  skip 2 "上游预检失败"
fi

if [ "$FAILED" -eq 0 ]; then
  # 2b: 等 daemon 首轮 baseline reconcile 跑完再建 issue——否则刚建的 issue 会被
  # 误认作存量（ledger status=adopted、零回复），step 4 必然超时。
  step 2b "等待 daemon 就绪（daemon: ready）"
  ready=0
  for ((try = 0; try < 20; try++)); do
    if journalctl --user -u "${SERVICE_NAME}.service" --no-pager -n 20 2>/dev/null | grep -q "daemon: ready"; then
      ready=1
      break
    fi
    sleep 1
  done
  if [ "$ready" -eq 1 ]; then
    sleep 2  # reconcile 落账缓冲
    log "daemon ready confirmed"
    pass 2b
  else
    fail 2b "20s 内未见 'daemon: ready'——检查 journalctl --user -u ${SERVICE_NAME}"
  fi
else
  skip 2b "上游失败"
fi

if [ "$FAILED" -eq 0 ]; then
  # 2c: polling-mode readiness — Gitea 1.21's server-side webhook policy
  # (ALLOWED_HOSTS) blocks delivery to LAN callbacks; shrink reconcile_interval
  # so the flow completes via reconcile alone. Step 4's ≤60s ack stays valid:
  # ≤15s poll + fast ack note; webhook delivery, when allowed, only accelerates.
  step 2c "切换 polling 模式（reconcile_interval=15s）"
  AGENT_CFG="${HOME}/.config/${SERVICE_NAME}/config.json"
  ORIG_INTERVAL="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("reconcile_interval", 600))' "$AGENT_CFG" 2>/dev/null || echo 600)"
  RESTART_TS="$(date '+%Y-%m-%d %H:%M:%S')"
  if python3 - "$AGENT_CFG" <<'EOF'
import json, sys
p = sys.argv[1]
cfg = json.load(open(p))
cfg["reconcile_interval"] = 15
with open(p, "w", encoding="utf-8") as f:
    json.dump(cfg, f, ensure_ascii=False, indent=2)
EOF
  then
    systemctl --user restart "${SERVICE_NAME}.service"
    # --since guards against stale 'daemon: ready' lines from the pre-restart
    # run (the journal persists across restarts)
    ready=0
    for ((try = 0; try < 20; try++)); do
      if journalctl --user -u "${SERVICE_NAME}.service" --no-pager --since "$RESTART_TS" 2>/dev/null | grep -q "daemon: ready"; then
        ready=1
        break
      fi
      sleep 1
    done
    if [ "$ready" -eq 1 ]; then
      log "polling interval=15s for test"
      pass 2c
    else
      fail 2c "重启后 20s 内未见 'daemon: ready'"
    fi
  else
    fail 2c "写入 reconcile_interval=15 失败（${AGENT_CFG}）"
  fi
else
  skip 2c "上游失败"
fi

if [ "$FAILED" -eq 0 ]; then
  run_verify
else
  skip 3 "上游失败（步骤 3-9 一并跳过）"
fi

if [ "$FAILED" -eq 0 ]; then
  run_replay_check
elif [ -n "$IID" ]; then
  skip 10 "上游失败，跳过重装重放检查"
fi

step 11 "清理：确认 issue 已关 + 删除远端分支 + 回执"
if do_cleanup; then
  pass 11
else
  fail 11 "清理未完全成功（见上方回执）"
fi

exit 0  # verdict + exit code decided by the EXIT trap
