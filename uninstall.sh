#!/usr/bin/env bash
# issue-agent uninstaller (shared infrastructure).
#   uninstall.sh [project-path]
# Stops the service, removes the unit, deletes the Gitea webhook (needs a
# readable config), removes the snapshot, releases the auto-allocated port
# from ~/.config/issue-agent-infra/ports.json — and KEEPS the XDG config +
# state (~/.config/<service>/, ~/.local/state/<service>/) so a re-install
# does not replay history. Instance name: ISSUE_AGENT_SERVICE_NAME env, or
# derived from <project>/.issue-agent.json (service_name field, else
# canonical_path basename lowercased + "-issue-agent").
set -euo pipefail

# service name: env > project json (service_name, else derived from
# canonical_path basename lowercased + "-issue-agent") > generic default.
if [ -n "${ISSUE_AGENT_SERVICE_NAME:-}" ]; then
  SERVICE_NAME="${ISSUE_AGENT_SERVICE_NAME}"
elif [ -n "${1:-}" ] && [ -f "${1%/}/.issue-agent.json" ]; then
  # Derivation mirrors install.sh exactly: canonical basename lowercased,
  # non-[a-z0-9._-] folded to "-", + "-issue-agent".
  SERVICE_NAME="$(proj_json="${1%/}/.issue-agent.json" python3 - <<'EOF'
import json, os, re
try:
    raw = json.load(open(os.environ["proj_json"], encoding="utf-8"))
except Exception:
    raw = {}
svc = raw.get("service_name")
if not svc:
    canonical = str(raw.get("canonical_path") or "")
    base = canonical.rstrip("/").rsplit("/", 1)[-1] if canonical else ""
    svc = (re.sub(r"[^a-z0-9._-]", "-", base.lower()) + "-issue-agent") if base else ""
print(str(svc))
EOF
  )"
fi
SERVICE_NAME="${SERVICE_NAME:-issue-agent}"
# The name feeds rm -rf paths below — validate the charset exactly like
# install.sh's B5 guard BEFORE any path is built from it (a hand-edited or
# cloned .issue-agent.json must never turn into a traversal).
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || {
  echo "uninstall: 错误：服务名含非法字符（仅允许字母数字开头，后随 . _ -）：${SERVICE_NAME}" >&2
  exit 1
}
PORTS_JSON="${HOME}/.config/issue-agent-infra/ports.json"
CONFIG_FILE="${HOME}/.config/${SERVICE_NAME}/config.json"
STATE_DIR="${HOME}/.local/state/${SERVICE_NAME}"
SNAPSHOT_DIR="${HOME}/.local/lib/${SERVICE_NAME}"
UNIT_FILE="${HOME}/.config/systemd/user/${SERVICE_NAME}.service"

note() { echo "uninstall: $*"; }

release_port() { # release_port <ports_json> <port> [service_name]
  # Exclusive sidecar lock, same as install.sh alloc_port: an uninstall
  # racing an install must not resurrect a just-released row.
  ports_json="$1" port="$2" svc="${3:-}" python3 - <<'EOF'
import fcntl, json, os
from pathlib import Path
p = Path(os.environ["ports_json"])
p.parent.mkdir(parents=True, exist_ok=True)
with open(str(p) + ".lock", "w") as lockfh:
    fcntl.flock(lockfh, fcntl.LOCK_EX)
    try:
        registry = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise SystemExit(0)
    if not isinstance(registry, dict):
        raise SystemExit(0)
    port, svc = os.environ["port"], os.environ.get("svc", "")
    if port in registry and (not svc or registry[port] == svc):
        del registry[port]
        tmp = p.with_name(p.name + ".tmp")
        tmp.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp, p)
EOF
}

# Same TLS opt-out as install.sh (F1): CA bundle switches to --cacert.
CURL_TLS=(-k)
if [ -n "${ISSUE_AGENT_CA_BUNDLE:-}" ]; then
  CURL_TLS=(--cacert "${ISSUE_AGENT_CA_BUNDLE}")
fi

# --- 1. stop service + remove unit --------------------------------------------

if systemctl --user is-enabled "${SERVICE_NAME}.service" >/dev/null 2>&1 || [ -f "$UNIT_FILE" ]; then
  systemctl --user disable --now "${SERVICE_NAME}.service" 2>/dev/null || true
fi
rm -f "$UNIT_FILE"
systemctl --user daemon-reload 2>/dev/null || true
note "服务已停止，unit 已移除"

# --- 2. delete webhook via API (best-effort) -----------------------------------

if [ -f "$CONFIG_FILE" ]; then
  eval "$(uia_cfg="$CONFIG_FILE" python3 - 2>/dev/null <<'EOF' || true
import json, os, shlex
from pathlib import Path
p = Path(os.environ.get("ISSUE_AGENT_CONFIG") or os.environ["uia_cfg"])
try:
    c = json.loads(p.read_text())
except Exception:
    c = {}
for key, field in (("UIA_API_BASE", "api_base"), ("UIA_PAT", "pat"),
                   ("UIA_CALLBACK", "callback_url"), ("UIA_REPO", "repo_path")):
    print(f"{key}={shlex.quote(str(c.get(field, '')))}")
EOF
)"
  if [ -n "${UIA_API_BASE:-}" ] && [ -n "${UIA_PAT:-}" ] && [ -n "${UIA_CALLBACK:-}" ]; then
    # F11: the PAT goes into a 0600 header file read by curl -H @file,
    # never onto the curl argv (world-readable via /proc). Gitea auth form.
    uia_hdr="$(mktemp)"
    chmod 600 "$uia_hdr"
    printf 'Authorization: token %s\n' "$UIA_PAT" > "$uia_hdr"
    trap 'rm -f "$uia_hdr"' EXIT
    origin="$(git -C "${UIA_REPO:-/nonexistent}" remote get-url origin 2>/dev/null || true)"
    if [ -n "$origin" ]; then
      PROJECT="$(python3 - "$origin" <<'EOF'
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
      # owner/repo are separate path segments in Gitea — encode each, keep the slash.
      PROJECT_API="$(python3 - "$PROJECT" <<'EOF'
import sys, urllib.parse
print("/".join(urllib.parse.quote(p, safe="") for p in sys.argv[1].strip("/").split("/")))
EOF
)"
      # C1: check the HTTP status — an expired PAT answers 401 with a JSON
      # body that must not be mistaken for an empty hook list (silent skip).
      hooks_reply="$(curl -s "${CURL_TLS[@]}" -X GET -H @"$uia_hdr" \
        -w '\n%{http_code}' "${UIA_API_BASE%/}/api/v1/repos/${PROJECT_API}/hooks?limit=50" 2>/dev/null || true)"
      hooks_code="${hooks_reply##*$'\n'}"
      hooks_json="${hooks_reply%$'\n'*}"
      if [ "$hooks_code" = "401" ] || [ "$hooks_code" = "403" ]; then
        echo "uninstall: 错误：Gitea 拒绝了 PAT（HTTP ${hooks_code}），无法注销 webhook——请到 Gitea 仓库 Settings → Webhooks 手动删除指向 ${UIA_CALLBACK} 的 webhook" >&2
        exit 1
      fi
      if [ "$hooks_code" != "200" ]; then
        note "webhook 列表查询返回 HTTP ${hooks_code:-无响应}，跳过自动注销——请手动核对"
      else
        # C2: single ?limit=50 page — warn when the cap is hit so a missed
        # hook is at least visible; and sweep stale same-instance hooks
        # (same :PORT, different URL) left behind by LAN-IP churn.
        hooks_n="$(printf '%s' "$hooks_json" | python3 -c '
import json, sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(0)' 2>/dev/null || echo 0)"
        if [ "${hooks_n:-0}" -ge 50 ]; then
          note "警告：仓库 webhook 已达 50 条（分页上限），列表可能不完整——请到 Gitea 仓库 Settings → Webhooks 人工核对"
        fi
        cb_port="${UIA_CALLBACK##*:}"
        hook_ids="$(printf '%s' "$hooks_json" | cb="$UIA_CALLBACK" cb_port="$cb_port" python3 -c '
import json, os, sys
try:
    hooks = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for h in hooks if isinstance(hooks, list) else []:
    url = (h.get("config") or {}).get("url") or ""
    stale = (url != os.environ["cb"] and os.environ.get("cb_port")
             and url.rstrip("/").endswith(":" + os.environ["cb_port"]))
    if url == os.environ["cb"] or stale:
        print(h["id"])
' 2>/dev/null || true)"
        if [ -n "$hook_ids" ]; then
          for hid in $hook_ids; do
            curl -s "${CURL_TLS[@]}" -X DELETE -H @"$uia_hdr" -o /dev/null \
              "${UIA_API_BASE%/}/api/v1/repos/${PROJECT_API}/hooks/${hid}" || true
            note "webhook 已删除（id=${hid}）"
          done
        else
          note "Gitea 上未找到回调 ${UIA_CALLBACK} 对应的 webhook，跳过"
        fi
      fi
    else
      note "仓库路径 ${UIA_REPO} 不可用，无法定位项目——请手动在 Gitea 删除指向 ${UIA_CALLBACK} 的 webhook"
    fi
  else
    note "配置字段不完整（api_base/pat/callback_url 缺失），无法自动注销 webhook——请手动在 Gitea 删除指向 ${UIA_CALLBACK:-回调 URL} 的 webhook"
  fi
  # Worktrees are the AGENT's own business now (it creates and removes
  # them per turn); just clear stale registrations from the main repo.
  if [ -d "${UIA_REPO:-}" ]; then
    git -C "$UIA_REPO" worktree prune 2>/dev/null || true
  fi
else
  note "配置缺失（${CONFIG_FILE}），跳过 webhook 注销"
fi

# --- 3. remove snapshot; keep config + state ------------------------------------

rm -rf "$SNAPSHOT_DIR"
note "快照已删除：${SNAPSHOT_DIR}"

# --- 4. release the auto-allocated port (registry-allocated only) -------------
if [ -f "$CONFIG_FILE" ]; then
  RELEASED_PORT="$(config_file="$CONFIG_FILE" python3 - 2>/dev/null <<'EOF' || true
import json, os
try:
    print(json.load(open(os.environ["config_file"]))["port"])
except Exception:
    pass
EOF
)"
  if [ -n "${RELEASED_PORT:-}" ]; then
    release_port "$PORTS_JSON" "$RELEASED_PORT" "$SERVICE_NAME"       && note "已释放自动分配端口 ${RELEASED_PORT}（ports.json）"       || note "端口 ${RELEASED_PORT} 未在注册表（显式提供，无需释放）"
  fi
fi
note "卸载完成。以下内容已保留（确认不再需要时可手动删除）："
note "  配置：${CONFIG_FILE}"
note "  状态：${STATE_DIR}"
