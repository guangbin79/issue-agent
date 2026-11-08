#!/usr/bin/env bash
# issue-agent — e2e reject: an out-of-scope issue must get a
# 「范围外」 agent reply and NO issue/<n>-fix branch (PROTOCOL.md step 0).
# Required env (missing -> exit 2): ISSUE_AGENT_API_BASE, ISSUE_AGENT_PAT
# Optional: ISSUE_AGENT_REPO, ISSUE_AGENT_PROJECT, ISSUE_AGENT_CA_BUNDLE, E2E_LOG
# Exit codes: 0 pass; 1 fail; 2 env missing. Footprint: one issue (closed).
set -u

for v in ISSUE_AGENT_API_BASE ISSUE_AGENT_PAT; do
  [ -n "${!v:-}" ] || { echo "e2e-reject: 缺少必需环境变量 $v；export ISSUE_AGENT_API_BASE=<gitea-api-base> ISSUE_AGENT_PAT=<token> 后重跑" >&2; exit 2; }
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_TOP="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)"
# Mirror install.sh's service-name derivation (canonical basename
# lowercased + "-issue-agent") and export it so the unit under test is the
# one install.sh actually created; explicit ISSUE_AGENT_SERVICE_NAME wins.
if [ -n "${ISSUE_AGENT_SERVICE_NAME:-}" ]; then
  SERVICE_NAME="${ISSUE_AGENT_SERVICE_NAME}"
else
  _E2E_CANON="${ISSUE_AGENT_REPO:-${REPO_TOP:-.}}"
  SERVICE_NAME="$(printf '%s' "$(basename "${_E2E_CANON%/}")" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9._-' '-')-issue-agent"
fi
export ISSUE_AGENT_SERVICE_NAME="$SERVICE_NAME"
API_ROOT="${ISSUE_AGENT_API_BASE%/}"
# E2: derive the evidence log from the git toplevel — a hardcoded absolute
# path breaks on any other machine (ocr#19).
E2E_LOG="${E2E_LOG:-${REPO_TOP:-$SCRIPT_DIR}/.omo/evidence/${SERVICE_NAME}/e2e-reject.log}"
REPO_PATH="${ISSUE_AGENT_REPO:-$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || true)}"
PROJECT="${ISSUE_AGENT_PROJECT:-$(git -C "${REPO_PATH:-/nonexistent}" remote get-url origin 2>/dev/null \
  | python3 -c '
import sys
from urllib.parse import urlparse
url = sys.stdin.read().strip()
tail = url.split("://", 1)[-1]
path = (tail.split(":", 1)[-1] if "://" not in url else urlparse(url).path).strip("/")
print(path[:-4] if path.endswith(".git") else path)' 2>/dev/null || true)}"
# E5: an empty PROJECT would point every API call at /api/ — refuse
# before anything is sent (ocr#21).
[ -n "$PROJECT" ] || { echo "e2e-reject: 无法解析 PROJECT（origin 缺失或仓库不可用）——export ISSUE_AGENT_PROJECT=<owner/repo> 后重跑" >&2; exit 2; }

CURL_TLS=(-k)
[ -n "${ISSUE_AGENT_CA_BUNDLE:-}" ] && CURL_TLS=(--cacert "${ISSUE_AGENT_CA_BUNDLE}")
HDR_FILE="$(mktemp)"; chmod 600 "$HDR_FILE"
printf 'Authorization: token %s\n' "$ISSUE_AGENT_PAT" > "$HDR_FILE"

FAILED=0; IID=""
log()  { echo "[e2e-reject] $*"; }
pass() { echo "[PASS] step $1"; }
fail() { echo "[FAIL] step $1: $2"; FAILED=1; }

mkdir -p "$(dirname "$E2E_LOG")"
exec > >(tee -a "$E2E_LOG") 2>&1

gitea() { # gitea METHOD PATH [JSON_BODY] -> sets REPLY_BODY + HTTP_CODE
  local method="$1" path="$2" body="${3:-}" out extra=()
  [ -n "$body" ] && extra=(-H 'Content-Type: application/json' -d "$body")
  out="$(curl -s "${CURL_TLS[@]}" -X "$method" -H @"$HDR_FILE" "${extra[@]}" \
         --max-time 60 -w '\n%{http_code}' "${API_ROOT}${path}" 2>/dev/null || true)"
  HTTP_CODE="${out##*$'\n'}"
  REPLY_BODY="${out%$'\n'*}"
}

fetch_notes() { # sets AGENT_COUNT / REJECT_SEEN (agent notes carry the self marker)
  gitea GET "/api/v1/repos/${PROJECT}/issues/${IID}/comments?limit=50&direction=asc"
  eval "$(printf '%s' "$REPLY_BODY" | SERVICE_NAME="$SERVICE_NAME" python3 -c '
import json, os, sys
try: notes = json.load(sys.stdin)
except Exception: notes = []
marker = "<!-- " + os.environ.get("SERVICE_NAME", "") + " -->"
agent = [n for n in notes if marker in (n.get("body") or "")]
print("AGENT_COUNT=%d" % len(agent))
print("REJECT_SEEN=%d" % any("范围外" in (n.get("body") or "") for n in agent))
' 2>/dev/null || echo 'AGENT_COUNT=0
REJECT_SEEN=0')"
}

poll_until() { # poll_until <desc> <timeout_s> <interval_s> <condition_fn>
  local desc="$1" timeout="$2" interval="$3" fn="$4" start=$SECONDS
  while :; do
    fetch_notes
    "$fn" && { log "poll: ${desc} — 满足（耗时 $((SECONDS - start))s）"; return 0; }
    (( SECONDS - start >= timeout )) && break
    sleep "$interval"
  done
  log "poll: ${desc} — 超时（${timeout}s）"; return 1
}

cond_reject()   { [ "${REJECT_SEEN:-0}" -eq 1 ]; }
branch_exists() { gitea GET "/api/v1/repos/${PROJECT}/branches/${BRANCH_ENC}"; [ "$HTTP_CODE" = 200 ]; }

on_exit() { # close the issue, print receipt, decide the verdict
  local rc=$? final
  if [ -n "$IID" ]; then
    gitea PATCH "/api/v1/repos/${PROJECT}/issues/${IID}" '{"state": "closed"}'
    echo "cleanup 回执：issue #${IID} closed=HTTP ${HTTP_CODE}（分支 issue/${IID}-fix 按断言不应存在）"
  else
    echo "cleanup 回执：issue=none（未创建或创建失败）"
  fi
  [ "$rc" -ne 0 ] && FAILED=1
  if [ "$FAILED" -eq 0 ]; then echo "REJECT RESULT: PASS"; final=$rc
  else echo "REJECT RESULT: FAIL"; final=1; fi
  sleep 1  # let the tee process substitution drain to the log file
  rm -f "$HDR_FILE"
  exit "$final"
}
trap on_exit EXIT

# --- step 1: preflight: API reachable + service active -----------------------------
log "e2e-reject 开始：api=${ISSUE_AGENT_API_BASE} project=${PROJECT:-?} repo=${REPO_PATH:-?}"
gitea GET /api/v1/version
if [ "$HTTP_CODE" = 200 ]; then log "API 可达（HTTP 200）"; else fail 1 "API 探测失败：/api/v1/version 返回 ${HTTP_CODE:-无响应}（期望 200）"; fi
if [ "$FAILED" -eq 0 ]; then
  state="$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
  if [ "$state" != "active" ]; then
    fail 1 "服务非 active（${state:-unknown}）——先运行 install.sh"
  else
    # E4: Type=simple flips active immediately while the startup baseline
    # reconcile may still be running — gate on the daemon's own ready
    # line before any POST (ocr#8).
    ready=0
    for ((try = 0; try < 30; try++)); do
      if journalctl --user -u "${SERVICE_NAME}.service" --no-pager -n 20 2>/dev/null | grep -q "daemon: ready"; then
        ready=1
        break
      fi
      sleep 1
    done
    if [ "$ready" -eq 1 ]; then pass 1; else fail 1 "30s 内未见 'daemon: ready'——检查 journalctl --user -u ${SERVICE_NAME}"; fi
  fi
fi

# --- step 2: create the out-of-scope test issue -------------------------------------
if [ "$FAILED" -eq 0 ]; then
  payload="$(python3 -c 'import json; print(json.dumps({"title": "[e2e-reject] 隔壁仓库问题：请修复 TTSPlayer 播放卡顿", "body": "这是 TTSPlayer 仓库的问题，误投到这里，请处理"}, ensure_ascii=False))')"
  gitea POST "/api/v1/repos/${PROJECT}/issues" "$payload"
  if [ "$HTTP_CODE" = 201 ]; then
    IID="$(printf '%s' "$REPLY_BODY" | python3 -c 'import json, sys; print(json.load(sys.stdin).get("number", ""))' 2>/dev/null || true)"
    BRANCH_ENC="$(python3 -c 'import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=""))' "issue/${IID}-fix")"
    log "issue #${IID} 已创建（断言分支 issue/${IID}-fix 不应出现）"
    pass 2
  else
    fail 2 "创建 issue 失败（HTTP ${HTTP_CODE}）"
  fi
fi

# --- step 3: ≤2100s agent note containing 「范围外」 ----------------------------------
if [ "$FAILED" -eq 0 ]; then
  if poll_until "「范围外」 agent note" 2100 15 cond_reject; then pass 3
  else fail 3 "35min 内未见含「范围外」的 agent note（AGENT_COUNT=${AGENT_COUNT:-0}）——检查 journalctl --user -u ${SERVICE_NAME}"; fi
fi

# --- step 4: assert NO issue/<n>-fix branch was pushed -------------------------------
if [ "$FAILED" -eq 0 ]; then
  # E3: a single immediate GET can pass while a push is still in flight —
  # poll the full window so a late push cannot slip past (ocr#20).
  branch_clean=1
  b_start=$SECONDS
  while :; do
    if branch_exists; then branch_clean=0; break; fi
    (( SECONDS - b_start >= 10 )) && break
    sleep 2
  done
  if [ "$branch_clean" -eq 1 ]; then
    log "分支不存在确认（${SECONDS - b_start}s 轮询窗口）：GET issue/${IID}-fix 返回 HTTP ${HTTP_CODE}"
    pass 4
  else
    fail 4 "远端分支 issue/${IID}-fix 不应存在（GET 返回 200）"
  fi
fi

exit 0  # verdict + exit code decided by the EXIT trap
