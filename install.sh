#!/usr/bin/env bash
# issue-agent installer (shared infrastructure) — idempotent; re-run
# updates in place. Identity is per-project: read from
# <project>/.issue-agent.json (canonical_path / default_branch / identity /
# out_of_scope / port / service_name / verify_command).
# Deploys: snapshot ~/.local/lib/<service>/, config (0600), systemd user
# unit, Gitea repository webhook (upsert, issues+issue_comment).
# Non-interactive overrides (env wins over .issue-agent.json):
#   ISSUE_AGENT_API_BASE / ISSUE_AGENT_PAT / ISSUE_AGENT_CALLBACK_URL /
#   ISSUE_AGENT_REPO / ISSUE_AGENT_SSL_VERIFY /
#   ISSUE_AGENT_CA_BUNDLE (curl --cacert; full TLS verification) /
#   ISSUE_AGENT_SERVICE_NAME (multi-project instance name; default derives
#   from the canonical path's basename, lowercased + "-issue-agent") /
#   ISSUE_AGENT_PORT (listener port) / ISSUE_AGENT_DEFAULT_BRANCH /
#   ISSUE_AGENT_CANONICAL_REPO (TEST-ONLY pin override).
#   Legacy ISSUE_AGENT_WORKTREE / ISSUE_AGENT_WORKTREE_PATH are accepted but
#   IGNORED (worktrees are the agent's own business now).
# Exit codes: 0 ok; 1 preflight/registration failure — including the
#   offline pinning rejections (missing canonical pin, non-canonical repo
#   path, existing config of another instance), all of which die before
#   any network probe or PAT step; 2 listener port occupied
#   (only when the service is NOT already active — an active daemon holds
#   the port by design, so a re-run update skips the bind check and restarts
#   the unit at the end).
# The PAT is only ever read silently and written to the 0600 config — never
# echoed, never logged, never on a curl argv (sent via a 0600 -H @file as
# "Authorization: token <PAT>"). The git push URL is built from config as
# =============================================================================
# issue-agent installer — shared infrastructure (standalone repo ~/issue-agent; extracted from guangbin79/dotfiles)
# =============================================================================
# 用法 / usage:
#   install.sh [project-path]
#
# 一条命令把 issue-agent 接入一个项目。项目身份来自 <project>/.issue-agent.json，
# 没有给出的项回退到环境变量，再回退到内置默认值（仅可选项有默认值）。
#
# ---- 输入优先序（高 → 低）---------------------------------------------------
#   1. ISSUE_AGENT_* 环境变量（自动化/e2e 面，见下）
#   2. <project>/.issue-agent.json 项目配置
#   3. 内置默认值——只对「可选项」存在；必选项缺失即退出码 1
#  env 与 json 同时给出同一字段时 env 胜出，且 stderr 记录覆盖事实（不静默）。
#
# ---- .issue-agent.json 字段 --------------------------------------------------
#   必填：canonical_path  本实例钉定的仓库绝对路径（离线校验 realpath）
#         identity        项目一句话定位（渲染进 PROTOCOL 第 0 条范围审查）
#         out_of_scope    范围外事项列表（渲染进 PROTOCOL；如兄弟仓/运维）
#   可选：default_branch  基线分支（默认 master）
#         port            监听端口（默认自动分配，见下）
#         service_name    实例名（默认 canonical_path 末段目录名小写化
#                         + "-issue-agent"；小写化是不变量）
#         verify_command  项目门禁命令（默认通用措辞）
#         agora           可选节点 {"execution_mode": "agora", "router_model":
#                         "<providerID/modelID>", "agora_home": "<绝对路径>",
#                         "agora_poll_interval": 15}——execution_mode 缺省
#                         legacy 且一个 agora 键都不写进 config（legacy 部署
#                         config 面零变化）；agora 模式要求 router_model 与
#                         agora_home 非空（离线先拒），其提交经 agora worker
#                         外部交付，启用/回滚见 README「agora 模式」
#
# ---- 环境变量面（沿用既有命名，新增三枚必填项的 env 直通）---------------------
#   ISSUE_AGENT_API_BASE / ISSUE_AGENT_PAT / ISSUE_AGENT_CALLBACK_URL /
#   ISSUE_AGENT_SSL_VERIFY / ISSUE_AGENT_CA_BUNDLE（curl --cacert 全量校验）/
#   ISSUE_AGENT_REPO（等价位置参数）/ ISSUE_AGENT_SERVICE_NAME /
#   ISSUE_AGENT_PORT / ISSUE_AGENT_DEFAULT_BRANCH /
#   ISSUE_AGENT_CANONICAL_PATH / ISSUE_AGENT_IDENTITY / ISSUE_AGENT_OUT_OF_SCOPE
#   （OUT_OF_SCOPE 用逗号分隔多个项）
#   ISSUE_AGENT_EXECUTION_MODE / ISSUE_AGENT_ROUTER_MODEL /
#   ISSUE_AGENT_AGORA_HOME / ISSUE_AGENT_AGORA_POLL_INTERVAL（agora 节同名字段
#   的 env 直通，env 胜出；重装未提供时保留既有 config 的 agora 键）
#   ISSUE_AGENT_CANONICAL_REPO 为旧测试专用钉定覆盖，保留兼容。
#   Legacy ISSUE_AGENT_WORKTREE / ISSUE_AGENT_WORKTREE_PATH 被接受但忽略
#   （worktree 现在完全是 agent 自己的事）。
#
# ---- 端口自动分配 ------------------------------------------------------------
#   env/json 都未给 port 时：先复用既有 config 的 port（同步注册表），否则
#   读 ~/.config/issue-agent-infra/ports.json 分配从 8644 起的最小未注册
#   端口；同一 service 重复安装复用其已注册端口（幂等）。注册表＝installer
#   管理的端口：自动分配与复用端口入册；env/json 显式端口归调用者自管，
#   重装时清掉本 service 旧册行。写入经 tmp 文件 + rename 原子完成。
#   端口冲突（bind 预检失败）一律退出码 2——不自动换号重试。
#   uninstall 时从 ports.json 释放本实例注册的端口。
#
# ---- 重装合并（既有 config 字段保留）-----------------------------------------
#   检测到同钉定的 ~/.config/<service>/config.json 时：env 未提供则复用旧
#   config 的 pat/api_base/callback_url/port（stderr 记录复用事实）——重装
#   免重输密钥、不改回调与监听端口；max_parallel/ssl_verify/default_branch/
#   verify_command 仅当本次 env/json 显式提供才覆盖，否则保留既有值；
#   install.sh 不写的字段（daemon 侧 reconcile_interval 等自定义项）原样
#   存活。本次 env/json 显式提供的字段照常覆盖（覆盖事实经 env_over_json 记录）。
#
# ---- 部署产物 ----------------------------------------------------------------
#   快照 ~/.local/lib/<service>/（*.py + PROTOCOL.template.md + ROUTER.template.md
#   ——daemon 启动期读取的全部模板资产）；
#   config ~/.config/<service>/config.json（0600，含 PAT、gitea_user、
#   project=owner/repo、service_name/canonical_path/identity/out_of_scope/
#   verify_command）；systemd user unit；Gitea 仓库 webhook
#   （type=gitea，仅 issues + issue_comment 事件，按 URL 幂等 upsert）。
#
# ---- 退出码 ------------------------------------------------------------------
#   0 成功（幂等：重跑=就地更新）
#   1 预检/注册失败——含离线钉定拒绝（缺 canonical 钉定、路径不符、
#     必填字段缺失、另一实例的 config 反覆盖），全部先于任何网络探测/PAT
#   2 监听端口被占用（仅当服务未 active——active 守护进程持有端口属设计内，
#     重跑更新会跳过 bind 检查并在末尾 restart 单元）
#
# ---- 密钥纪律 ----------------------------------------------------------------
#   PAT 只经静默读取写入 0600 config——绝不回显、绝不落日志、绝不上 curl
#   argv（经 0600 -H @file 以 "Authorization: token <PAT>" 发送）。
#   git push URL 由 config 拼为 https://{gitea_user}:{PAT}@host/{owner}/{repo}.git
#   （从不用 oauth2: 形式）。
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
USER="${USER:-$(id -un)}"

die()  {
  echo "install: 错误：$*" >&2
  if [ "${PORT_ALLOCATED:-0}" = "1" ] && [ -n "${PORTS_JSON:-}" ]; then
    release_port "$PORTS_JSON" "$PORT" "$SERVICE_NAME" || true
  fi
  exit 1
}
note() { echo "install: $*"; }

# ports.json 注册表：{"<port>": "<service_name>"}；写入经 tmp+rename 原子完成。
# 同一 service 复用其已注册端口（幂等重装）；否则分配 <base> 起最小未注册端口。
alloc_port() { # alloc_port <ports_json> <service_name> <base>
  # The whole read-modify-write runs under an exclusive sidecar lock so two
  # concurrent installs can never allocate the same "next free" port (the
  # tmp+rename below only makes each individual write atomic).
  ports_json="$1" svc="$2" base="$3" python3 - <<'EOF'
import fcntl, json, os
from pathlib import Path
p = Path(os.environ["ports_json"])
p.parent.mkdir(parents=True, exist_ok=True)
with open(str(p) + ".lock", "w") as lockfh:
    fcntl.flock(lockfh, fcntl.LOCK_EX)
    try:
        registry = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        registry = {}
    if not isinstance(registry, dict):
        registry = {}
    svc = os.environ["svc"]
    for port, owner in sorted(registry.items(), key=lambda kv: int(kv[0])):
        if owner == svc:
            print(port)
            raise SystemExit(0)
    port = int(os.environ["base"])
    while str(port) in registry:
        port += 1
    registry[str(port)] = svc
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, p)  # atomic
    print(port)
EOF
}

release_port() { # release_port <ports_json> <port> [service_name]
  # Same exclusive sidecar lock as alloc_port: an uninstall racing an
  # install must not resurrect a just-released row (lost update).
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

# sync_port_registry：把 <service_name> 的注册行设为 <port>（空＝移除该行）。
# 册语义＝「installer 管理的端口」：复用既有 config 端口时同步册值（旧的
# 自动分配行被替换回收，uninstall 照常释放）；env/json 显式端口归调用者
# 自管——清掉本 service 旧册行，避免幽灵占号。
sync_port_registry() { # sync_port_registry <ports_json> <service_name> <port>
  ports_json="$1" svc="$2" port="$3" python3 - <<'EOF'
import fcntl, json, os, sys
from pathlib import Path
p = Path(os.environ["ports_json"])
p.parent.mkdir(parents=True, exist_ok=True)
with open(str(p) + ".lock", "w") as lockfh:
    fcntl.flock(lockfh, fcntl.LOCK_EX)
    try:
        registry = json.loads(p.read_text(encoding="utf-8"))
    except FileNotFoundError:
        registry = {}
    except ValueError:
        # 损坏注册表按空处理（本函数是 best-effort 清理/同步面，不阻断安装；
        # 与下方非 dict 处理对齐），stderr 记录事实。
        print("install: 注意：ports.json 无法解析，按空注册表处理", file=sys.stderr)
        registry = {}
    if not isinstance(registry, dict):
        registry = {}
    svc, port = os.environ["svc"], os.environ.get("port", "")
    if port and registry.get(port) not in (None, svc):
        # 他服务的册行不可抢占（否则失败路径的 release 会摧毁受害者注册）。
        print(f"install: 错误：端口 {port} 已注册给其它服务 {registry[port]}，拒绝抢占注册表行", file=sys.stderr)
        raise SystemExit(1)
    registry = {k: v for k, v in registry.items() if v != svc}
    if port:
        registry[port] = svc
    tmp = p.with_name(p.name + ".tmp")
    tmp.write_text(json.dumps(registry, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, p)
EOF
}

# env 与 json 同字段并存时 env 胜出——记录覆盖事实，禁止静默忽略 json。
env_over_json() { # env_over_json <env_var> <json_value> <field>
  if [ -n "${!1:-}" ] && [ -n "$2" ]; then
    echo "install: 注意：环境变量 $1 覆盖 .issue-agent.json 的 $3 字段" >&2
  fi
}

for cmd in curl python3 git systemctl loginctl; do
  command -v "$cmd" >/dev/null 2>&1 || die "缺少命令：$cmd"
done

# 快照清单资产前置校验：GNU cp 单操作数失败后仍继续拷其余——源树缺模板时
# cp 会先把新 *.py 覆盖进既有快照再失败，留下「新 daemon + 无模板」半更新
# 快照（第 3 节无回滚）＝下次重启 FileNotFoundError 崩溃循环。离线先死。
for f in PROTOCOL.template.md ROUTER.template.md; do
  [ -f "$SCRIPT_DIR/$f" ] || die "源树缺快照清单资产：${SCRIPT_DIR}/${f}（拒绝半更新部署）"
done

# --- 0. per-project identity: env > <project>/.issue-agent.json > defaults --
# Every guard below runs BEFORE any prompt, network probe, or PAT step, so
# a wrong-target install dies fully offline, with no secrets involved.

# owner/repo ("project") from an origin URL: ssh://git@host:port/owner/
# repo.git, https://host/owner/repo.git and scp-like git@host:owner/repo.git
# forms (urlparse path tail, leading slashes and .git suffix stripped).
origin_project() {
  python3 - "$1" <<'EOF'
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
}

# Scrub embedded userinfo (user:pass@) from a URL before it is echoed —
# mirrors gitop._scrub: an origin like https://user:pass@host/... must never
# leak credentials into stdout/journal. The scp-like git@host: form carries
# no password and is left alone.
scrub_url() {
  printf '%s' "$1" | sed -e 's#//[^/@]*@#//***@#g'
}


DEFAULT_REPO="$(cd "${SCRIPT_DIR}/../.." && pwd)"
if [ -n "${1:-}" ]; then
  REPO_PATH="${1}"
elif [ -n "${ISSUE_AGENT_REPO:-}" ]; then
  REPO_PATH="${ISSUE_AGENT_REPO}"
else
  read -rp "项目仓库路径 [${DEFAULT_REPO}]: " REPO_PATH
  REPO_PATH="${REPO_PATH:-$DEFAULT_REPO}"
fi

# project json (may be absent — env-only installs keep working). Values are
# emitted as shell-quoted assignments (empty fields must survive verbatim;
# a whitespace-split read would collapse them).
PROJ_JSON="${REPO_PATH%/}/.issue-agent.json"
eval "$(
  python3 - "$PROJ_JSON" <<'EOF'
import json, shlex, sys
try:
    raw = json.load(open(sys.argv[1], encoding="utf-8"))
except Exception:
    raw = {}
def grab(key):
    v = raw.get(key)
    if isinstance(v, (list, tuple)):
        v = ",".join(str(x) for x in v if x)
    return shlex.quote(str(v)) if v else "''"
keys = ("canonical_path", "default_branch", "port", "service_name",
        "identity", "out_of_scope", "verify_command")
vars_ = {"canonical_path": "CANONICAL_JSON", "default_branch": "BRANCH_JSON",
         "port": "PORT_JSON", "service_name": "SVC_JSON",
         "identity": "IDENT_JSON", "out_of_scope": "OOS_JSON",
         "verify_command": "VERIFY_JSON"}
for key in keys:
    print(f"{vars_[key]}={grab(key)}")
# agora 节（计划 Task 6）：可选 {"execution_mode": "agora", "router_model":
# ..., "agora_home": ..., "agora_poll_interval": N}——env 同名直通优先。
# 误型（节非对象、字段非标量、execution_mode 真值非字符串）经专用旗标
# 变量传递——heredoc 内 SystemExit 会被外层 eval 吞掉，且哨值内嵌在值里
# 有假阳性/拼接误诊面。
agora_node_bad = ""
node = raw.get("agora")
if node is None:
    agora = {}
elif isinstance(node, dict):
    agora = node
else:
    # 节本身误型（字符串/布尔/数组）——不得静默按「无节点」落 legacy。
    agora = {}
    agora_node_bad = f"agora 节须为对象（收到 {type(node).__name__}）"
names = {"execution_mode": "MODE_JSON", "router_model": "ROUTER_MODEL_JSON",
         "agora_home": "AGORA_HOME_JSON",
         "agora_poll_interval": "AGORA_POLL_JSON"}
for key, var in names.items():
    v = agora.get(key)
    if isinstance(v, (bool, list, tuple, dict)):
        # 标量字段收到对象/数组/布尔：布尔对 execution_mode 是合法 legacy
        # 语义（config.py `or` 归一），对其余字段一律误型。
        if key == "execution_mode" and v is False:
            v = None  # false ＝显式 legacy（config.py：False or "legacy"）
        else:
            agora_node_bad = (agora_node_bad or
                              f"agora.{key} 须为标量（收到 {type(v).__name__}）")
            v = None
    if var == "MODE_JSON" and isinstance(v, (int, float)) and v:
        # 真值非字符串 execution_mode（如手编 5）：缺席映射会静默落 legacy、
        # 坏键经 unknown-key 存活规则透传，daemon 侧 str(5) 过不了白名单＝
        # 崩溃循环（falsy 0/None/"" 才合法归 legacy）。
        agora_node_bad = (agora_node_bad or
                          f"agora.execution_mode 须为字符串（收到 {v!r}）")
        v = None
    q = shlex.quote(str(v)) if v not in (None, "") else "''"
    print(f"{var}={q}")
print(f"AGORA_NODE_BAD={shlex.quote(agora_node_bad)}")
EOF
)"

# agora 节误型旗标（heredoc 内 SystemExit 会被外层 eval 吞掉，故经变量传递）
if [ -n "${AGORA_NODE_BAD:-}" ]; then
  die ".issue-agent.json 检查失败：${AGORA_NODE_BAD}（离线先死，不得静默按缺省安装）"
fi

# guard a1: canonical pin — env (new name > legacy test-only name) > json.
# Without a pin this installer refuses to run, fully offline: an unpinned
# install could silently target the wrong checkout.
CANONICAL="${ISSUE_AGENT_CANONICAL_PATH:-${ISSUE_AGENT_CANONICAL_REPO:-${CANONICAL_JSON}}}"
env_over_json ISSUE_AGENT_CANONICAL_PATH "$CANONICAL_JSON" canonical_path
[ -n "$CANONICAL" ] || die "缺少必填项 canonical_path：在 ${PROJ_JSON} 写 canonical_path，或设 ISSUE_AGENT_CANONICAL_PATH"
PIN_ERR="$(canonical="$CANONICAL" repo_path="$REPO_PATH" python3 - <<'EOF'
import os
# B6: resolve BOTH sides — the pin may itself sit behind a symlink
actual = os.path.realpath(os.environ["repo_path"])
canonical = os.path.realpath(os.environ["canonical"])
if actual != canonical:
    print(f"仓库路径不在钉定目录：期望 {os.environ['canonical']}，"
          f"实际 {os.environ['repo_path']}（realpath {actual}）")
EOF
)" || die "钉定路径校验执行失败（python3）"
[ -z "$PIN_ERR" ] || die "$PIN_ERR"

# service name: env > json > derived from the canonical path's basename,
# lowercased + "-issue-agent" (the lowercase derivation is the core
# invariant: /path/MyProject -> myproject-issue-agent).
if [ -n "${ISSUE_AGENT_SERVICE_NAME:-}" ]; then
  SERVICE_NAME="${ISSUE_AGENT_SERVICE_NAME}"
elif [ -n "$SVC_JSON" ]; then
  SERVICE_NAME="${SVC_JSON}"
else
  _BASE="$(basename "${CANONICAL%/}")"
  SERVICE_NAME="$(printf '%s' "${_BASE,,}" | tr -c 'a-z0-9._-' '-')-issue-agent"
fi
env_over_json ISSUE_AGENT_SERVICE_NAME "$SVC_JSON" service_name
SNAPSHOT_DIR="${HOME}/.local/lib/${SERVICE_NAME}"
CONFIG_DIR="${HOME}/.config/${SERVICE_NAME}"
CONFIG_FILE="${CONFIG_DIR}/config.json"
UNIT_DIR="${HOME}/.config/systemd/user"
UNIT_FILE="${UNIT_DIR}/${SERVICE_NAME}.service"
PORT="${ISSUE_AGENT_PORT:-${PORT_JSON:-}}"
DEFAULT_BRANCH="${ISSUE_AGENT_DEFAULT_BRANCH:-${BRANCH_JSON:-master}}"
env_over_json ISSUE_AGENT_DEFAULT_BRANCH "$BRANCH_JSON" default_branch
# (IDENTITY/OUT_OF_SCOPE keep their env > json resolution above; the json
# fallbacks VERIFY_COMMAND has no env passthrough.)
VERIFY_COMMAND="${VERIFY_JSON}"

# agora 模式四配置项（计划 Task 6）：env > .issue-agent.json 的 agora 节 >
# 既有 config（重装合并，见下方 OLD_* 读取）。execution_mode 缺省 legacy；
# 不显式启用则一个 agora 键都不写——legacy 部署 config 面零变化。
EXECUTION_MODE="${ISSUE_AGENT_EXECUTION_MODE:-${MODE_JSON:-}}"
ROUTER_MODEL="${ISSUE_AGENT_ROUTER_MODEL:-${ROUTER_MODEL_JSON:-}}"
AGORA_HOME_CFG="${ISSUE_AGENT_AGORA_HOME:-${AGORA_HOME_JSON:-}}"
AGORA_POLL="${ISSUE_AGENT_AGORA_POLL_INTERVAL:-${AGORA_POLL_JSON:-}}"
env_over_json ISSUE_AGENT_EXECUTION_MODE "$MODE_JSON" execution_mode
env_over_json ISSUE_AGENT_ROUTER_MODEL "$ROUTER_MODEL_JSON" router_model
env_over_json ISSUE_AGENT_AGORA_HOME "$AGORA_HOME_JSON" agora_home
env_over_json ISSUE_AGENT_AGORA_POLL_INTERVAL "$AGORA_POLL_JSON" agora_poll_interval

# B5: reject garbage SERVICE_NAME up front, fully offline — stray
# characters in SERVICE_NAME corrupt every derived path and the systemd
# unit name.
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "服务名含非法字符（仅允许字母数字开头，后随 . _ -）：${SERVICE_NAME}"

# guard b: anti-clobber — an existing config for THIS service name whose
# repo_path points elsewhere means SERVICE_NAME was overridden onto another
# live instance; refuse instead of clobbering its snapshot/config. An
# unparseable existing config is likewise rejected.
if [ -f "$CONFIG_FILE" ]; then
  OLD_REPO="$(config_file="$CONFIG_FILE" python3 - <<'EOF'
import json, os
cfg = json.load(open(os.environ["config_file"]))
print(cfg.get("repo_path") or cfg.get("canonical_path") or "")
EOF
  )" || die "已存在配置无法解析，拒绝覆盖：${CONFIG_FILE}"
  if [ -z "$OLD_REPO" ]; then
    # legacy schema (only "project"): ownership unreadable offline — trust
    # only the canonical's default-derived service name.
    _BASE="$(basename "${CANONICAL%/}")"
    DERIVED="$(printf '%s' "${_BASE,,}" | tr -c 'a-z0-9._-' '-')-issue-agent"
    [ "$SERVICE_NAME" = "$DERIVED" ] || die "legacy config 缺 repo_path/canonical_path 且 SERVICE_NAME（${SERVICE_NAME}）非该 canonical 默认派生（${DERIVED}）——无法离线确认归属，拒绝覆盖：${CONFIG_FILE}"
  else
    OLD_REAL="$(repo_path="$OLD_REPO" python3 -c 'import os; print(os.path.realpath(os.environ["repo_path"]))')"
    CANONICAL_REAL="$(canonical="$CANONICAL" python3 -c 'import os; print(os.path.realpath(os.environ["canonical"]))')"
    [ "$OLD_REAL" = "$CANONICAL_REAL" ] || die "已存在配置 ${CONFIG_FILE} 指向其它仓库（${OLD_REPO}）而非钉定目录 ${CANONICAL}——拒绝覆盖另一实例"
  fi
fi

# identity/out_of_scope validation sits below guard b: a legacy-schema
# config must get the anti-clobber verdict before required-field errors.
IDENTITY="${ISSUE_AGENT_IDENTITY:-${IDENT_JSON}}"
env_over_json ISSUE_AGENT_IDENTITY "$IDENT_JSON" identity
[ -n "$IDENTITY" ] || die "缺少必填项 identity：在 ${PROJ_JSON} 写 identity（项目一句话定位），或设 ISSUE_AGENT_IDENTITY"
OUT_OF_SCOPE="${ISSUE_AGENT_OUT_OF_SCOPE:-${OOS_JSON}}"
env_over_json ISSUE_AGENT_OUT_OF_SCOPE "$OOS_JSON" out_of_scope
[ -n "$OUT_OF_SCOPE" ] || die "缺少必填项 out_of_scope：在 ${PROJ_JSON} 写 out_of_scope（范围外事项列表），或设 ISSUE_AGENT_OUT_OF_SCOPE（逗号分隔）"

# repo validation sits below guard b: the legacy-schema anti-clobber branch
# must get the chance to refuse before the target repo is validated at all.
git -C "$REPO_PATH" rev-parse --git-dir >/dev/null 2>&1 || die "不是 git 仓库：${REPO_PATH}"
origin="$(git -C "$REPO_PATH" remote get-url origin 2>/dev/null || true)"
[ -n "$origin" ] || die "仓库没有 origin remote：${REPO_PATH}"

# TLS opt-out (F1): default -k (harmless on the plain-HTTP LAN Gitea, and
# keeps self-signed https Giteas working); a CA bundle switches every curl
# call to full verification via --cacert.
CURL_TLS=(-k)
if [ -n "${ISSUE_AGENT_CA_BUNDLE:-}" ]; then
  CURL_TLS=(--cacert "${ISSUE_AGENT_CA_BUNDLE}")
fi

# --- 1. inputs: env overrides first, prompts otherwise --------------------

# 复用既有 config 的 pat/api_base（仅 env 未提供时；guard b 已确认同钉定）：
# 重装/迁移免重输密钥，stderr 记录复用事实。
OLD_API_BASE=""; OLD_PAT=""; OLD_CB=""; OLD_PORT=""
OLD_MODE=""; OLD_ROUTER=""; OLD_AHOME=""; OLD_APOLL=""; OLD_VERIFY=""
if [ -f "$CONFIG_FILE" ]; then
  eval "$(config_file="$CONFIG_FILE" python3 - 2>/dev/null <<'EOF' || true
import json, os, shlex
try:
    c = json.load(open(os.environ["config_file"]))
except Exception:
    c = {}
for key, field, mode in (
        ("UIA_API_BASE", "api_base", "legacy"), ("UIA_PAT", "pat", "legacy"),
        ("UIA_CB", "callback_url", "legacy"), ("UIA_PORT", "port", "legacy"),
        ("UIA_MODE", "execution_mode", "str"),
        ("UIA_ROUTER", "router_model", "str"),
        ("UIA_AHOME", "agora_home", "str"),
        ("UIA_APOLL", "agora_poll_interval", "raw"),
        ("UIA_VERIFY", "verify_command", "str")):
    # 三口径：legacy＝历史 falsy→缺席（字节不变）；str＝与 config 写面的
    # 类型门同口径（非字符串按缺席——否则 str(5) 的假真值会骗过离线门，
    # 而写面随后把它类型丢弃成空值＝崩溃循环）；raw＝保真（poll 的 0/False
    # 必须进 shell 面校验离线拒）。
    v = c.get(field)
    if mode == "legacy":
        v = v or ""
    elif mode == "str":
        v = v if isinstance(v, str) else ""
    print(f"{key}={shlex.quote(str(v) if v is not None else '')}")
# execution_mode 真值非字符串（手编 5/["agora"]）：缺席映射会静默落
# legacy、坏键经 unknown-key 存活规则透传，daemon 侧 str() 过不了白名单＝
# 崩溃循环（falsy 0/False/"" 才合法归 legacy，与 config.py `or` 归一同口径）。
old_mode_bad = ""
_m = c.get("execution_mode")
if _m is not None and not isinstance(_m, str) and _m:
    old_mode_bad = f"execution_mode 须为字符串（收到 {type(_m).__name__}）"
print(f"UIA_MODE_BAD={shlex.quote(old_mode_bad)}")
EOF
)"
  OLD_API_BASE="${UIA_API_BASE:-}"
  OLD_PAT="${UIA_PAT:-}"
  OLD_CB="${UIA_CB:-}"
  OLD_PORT="${UIA_PORT:-}"
  OLD_MODE="${UIA_MODE:-}"
  OLD_ROUTER="${UIA_ROUTER:-}"
  OLD_AHOME="${UIA_AHOME:-}"
  OLD_APOLL="${UIA_APOLL:-}"
  OLD_VERIFY="${UIA_VERIFY:-}"
fi
if [ -n "${UIA_MODE_BAD:-}" ]; then
  die "既有 config 检查失败：${UIA_MODE_BAD}（daemon 载入会 ConfigError——离线先死）"
fi

# agora 有效值＝env > json > 既有 config；强校验镜像 config.py（离线先死，
# 先于任何网络探测/PAT 步骤——否则安装成功而 daemon 启动即 ConfigError
# 崩溃循环）：execution_mode 只认 legacy/agora、agora 模式必须 router_model＋
# agora_home＋verify_command 齐且 agora_home 为绝对路径、poll interval 为
# ≥1 的整数（含既有 config 里的手编值——config.py 无条件校验该字段）。
EFFECTIVE_MODE="${EXECUTION_MODE:-${OLD_MODE:-legacy}}"
case "$EFFECTIVE_MODE" in
  legacy|agora) ;;
  *) die "execution_mode 须为 legacy 或 agora（ISSUE_AGENT_EXECUTION_MODE / .issue-agent.json agora 节），当前：${EFFECTIVE_MODE}" ;;
esac
if [ "$EFFECTIVE_MODE" = "agora" ]; then
  [ -n "${ROUTER_MODEL:-$OLD_ROUTER}" ] || die "execution_mode=agora 缺 router_model（ISSUE_AGENT_ROUTER_MODEL 或 .issue-agent.json 的 agora 节）"
  [ -n "${AGORA_HOME_CFG:-$OLD_AHOME}" ] || die "execution_mode=agora 缺 agora_home（ISSUE_AGENT_AGORA_HOME 或 .issue-agent.json 的 agora 节）"
  case "${AGORA_HOME_CFG:-$OLD_AHOME}" in
    /*) ;;
    *) die "agora_home 须为绝对路径（相对路径随 daemon cwd 解析、~ 不展开），当前：${AGORA_HOME_CFG:-$OLD_AHOME}" ;;
  esac
  [ -n "${VERIFY_COMMAND:-$OLD_VERIFY}" ] || die "execution_mode=agora 缺 verify_command（config.py 强校验非空：.issue-agent.json 的 verify_command 或既有 config）"
fi
AGORA_POLL_EFF="${AGORA_POLL:-${OLD_APOLL:-}}"
if [ -n "$AGORA_POLL_EFF" ]; then
  # 空串由本守护分支排除；5 位长度上限封死 bash 算术回绕
  case "$AGORA_POLL_EFF" in
    *[!0-9]*|??????*) die "agora_poll_interval 须为 ≥1 的整数秒，长度 ≤5 位（封死 bash 算术回绕），当前：${AGORA_POLL_EFF}" ;;
  esac
  [ "$AGORA_POLL_EFF" -ge 1 ] || die "agora_poll_interval 须为 ≥1 的整数秒，当前：${AGORA_POLL_EFF}"
fi

# validate_port：垃圾值即 die；stdout 输出规范化端口（去前导零，保证注册
# 表键与 config 一致）。regex 内联长度上限（{1,5}）封死 bash 10# 对超
# int64 字面量的 mod 2^64 静默回绕。
validate_port() { # validate_port <port>
  local p="$1"
  [[ "$p" =~ ^[0-9]{1,5}$ ]] || die "监听端口必须是 1-65535 的整数（ISSUE_AGENT_PORT / .issue-agent.json port），当前：${p}"
  (( 10#$p >= 1 && 10#$p <= 65535 )) || die "监听端口必须是 1-65535 的整数（ISSUE_AGENT_PORT / .issue-agent.json port），当前：${p}"
  echo $((10#$p))
}

# 端口：env > json > 既有 config port > 自动分配（ports.json 注册表，8644 起；
# 同 service 幂等复用）。重装时 env/json 都未给端口先复用既有 config 的
# port（重装不改监听端口）。注册表＝installer 管理的端口：自动分配与复用
# 的端口入册；env/json 显式端口归调用者自管，重装时清掉本 service 旧册行。
# 两处 sync 都先 validate_port 再动注册表（失败路径零共享状态变更）；复用
# 路 PORT_ALLOCATED 保持 0——后续 die/rollback 不得释放既有行（保留的
# config 与可能仍在跑的 daemon 仍引用该端口）。
PORTS_JSON="${HOME}/.config/issue-agent-infra/ports.json"
PORT_ALLOCATED=0
env_over_json ISSUE_AGENT_PORT "$PORT_JSON" port
if [ -z "$PORT" ] && [ -n "$OLD_PORT" ]; then
  PORT="$(validate_port "$OLD_PORT")"
  sync_port_registry "$PORTS_JSON" "$SERVICE_NAME" "$PORT" || die "端口注册表同步失败（ports.json：${PORTS_JSON}）"
  echo "install: 复用既有 config 的监听端口 ${PORT}（env/json 未提供；注册表已同步）" >&2
elif [ -n "$PORT" ]; then
  PORT="$(validate_port "$PORT")"
  sync_port_registry "$PORTS_JSON" "$SERVICE_NAME" "" || die "端口注册表同步失败（ports.json：${PORTS_JSON}）"
else
  PORT="$(alloc_port "$PORTS_JSON" "$SERVICE_NAME" 8644)" || die "端口自动分配失败（ports.json：${PORTS_JSON}）"
  PORT="$(validate_port "$PORT")"
  PORT_ALLOCATED=1
  note "已自动分配监听端口 ${PORT}（注册表：${PORTS_JSON}）"
fi
if [ -n "${ISSUE_AGENT_API_BASE:-}" ]; then
  API_BASE="${ISSUE_AGENT_API_BASE%/}"
elif [ -n "$OLD_API_BASE" ]; then
  API_BASE="${OLD_API_BASE%/}"
  echo "install: 复用既有 config 的 pat/api_base（env 未提供）" >&2
else
  read -rp "Gitea API base（即 Gitea web 地址，如 http://<gitea-host>:3000）: " API_BASE
  API_BASE="${API_BASE%/}"
fi
[ -n "$API_BASE" ] || die "API base 为空"

# Gitea's version endpoint is unauthenticated — 200 is required (401 no
# longer counts as "found" the way it did on GitLab).
code="$(curl -s "${CURL_TLS[@]}" -o /dev/null -w '%{http_code}' "${API_BASE}/api/v1/version" || true)"
[ "$code" = "200" ] || die "API base 探测失败：${API_BASE}/api/v1/version 返回 ${code:-无响应}（期望 HTTP 200，Gitea 版本接口无需认证）。请确认 Gitea web URL 后重试"

if [ -n "${ISSUE_AGENT_PAT:-}" ]; then
  PAT="${ISSUE_AGENT_PAT}"
elif [ -n "$OLD_PAT" ]; then
  PAT="${OLD_PAT}"
  [ -n "${ISSUE_AGENT_API_BASE:-}" ] && echo "install: 复用既有 config 的 pat/api_base（env 未提供 PAT）" >&2
else
  read -rs -p "Gitea Personal Access Token（api 权限；输入不回显）: " PAT
  echo
fi
[ -n "$PAT" ] || die "PAT 为空"

# PAT travels in a 0600 header file read by curl -H @file, never on argv
# (F2: /proc/<pid>/cmdline is world-readable). Gitea auth header form.
HDR_FILE="$(mktemp)"
chmod 600 "$HDR_FILE"
printf 'Authorization: token %s\n' "$PAT" > "$HDR_FILE"
trap 'rm -f "$HDR_FILE"' EXIT

# PAT validation doubles as gitea_user capture: the login is needed to build
# the git push URL (https://{login}:{PAT}@host/{owner}/{repo}.git).
uout="$(curl -s "${CURL_TLS[@]}" -H @"$HDR_FILE" -w '\n%{http_code}' "${API_BASE}/api/v1/user" || true)"
user_code="${uout##*$'\n'}"
user_body="${uout%$'\n'*}"
[ "$user_code" = "200" ] || die "PAT 校验失败（GET /api/v1/user 返回 ${user_code:-无响应}）"
GITEA_USER="$(printf '%s' "$user_body" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin).get("login") or "")
except Exception:
    pass' 2>/dev/null || true)"
[ -n "$GITEA_USER" ] || die "无法从 /api/v1/user 响应解析 login 用户名"

# Default branch: env override > project json > master. The historical
# origin/HEAD auto-detection is deliberately GONE — origin/HEAD may point
# at a different branch than the project's real baseline, so any
# probe-based derivation silently re-pins on env-less reinstall. Written to
# config.json and threaded through every gitop call.


# Dual-NIC: resolve the source IP toward the GITEA host (the server that
# will call this callback), not the default route — a 1.1.1.1 probe picks
# e.g. wifi 192.168.0.x while Gitea sits on wired 192.168.1.x.
detect_lan_ip() {
  local ip="" host
  host="$(printf '%s' "$API_BASE" | sed -E 's#^[a-zA-Z][a-zA-Z0-9+.-]*://##; s#[/:].*##')"
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
DEFAULT_CALLBACK="http://$(detect_lan_ip):${PORT}"
if [ -n "${ISSUE_AGENT_CALLBACK_URL:-}" ]; then
  CALLBACK_URL="${ISSUE_AGENT_CALLBACK_URL}"
elif [ -n "$OLD_CB" ]; then
  CALLBACK_URL="$OLD_CB"
  echo "install: 复用既有 config 的 callback_url（env 未提供）" >&2
else
  read -rp "webhook 回调 URL [${DEFAULT_CALLBACK}]（回车确认默认值）: " CALLBACK_URL
  CALLBACK_URL="${CALLBACK_URL:-$DEFAULT_CALLBACK}"
fi
[ -n "$CALLBACK_URL" ] || die "回调 URL 为空"
case "$CALLBACK_URL" in
  http://*)
    note "注意：回调为明文 HTTP——webhook 密钥在局域网内可被嗅探伪造；生产环境建议配置 HTTPS 反向代理后使用 https:// 回调（当前为按设计接受的局域网方案）" ;;
esac

# --- 2. preflight: port bind + linger (before any write) -------------------

svc_active="$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
if [ "$svc_active" != "active" ] && ! python3 - "$PORT" <<'EOF'
import socket, sys
s = socket.socket()
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    s.bind(("0.0.0.0", int(sys.argv[1])))
finally:
    s.close()
EOF
then
  echo "install: 错误：端口 ${PORT} 已被占用，守护进程无法监听。" >&2
  if [ "$PORT_ALLOCATED" = "1" ]; then
    release_port "$PORTS_JSON" "$PORT" "$SERVICE_NAME" || true
  fi
  exit 2
fi

linger="$(loginctl show-user "${USER}" -p Linger 2>/dev/null || true)"
if [ "$linger" != "Linger=yes" ]; then
  echo "install: 错误：systemd linger 未启用——用户不登录时服务会被一并杀掉。" >&2
  echo "请先执行:  sudo loginctl enable-linger ${USER}" >&2
  echo "然后重新运行 install.sh" >&2
  exit 1
fi

PROJECT="$(origin_project "$origin")"
[ -n "$PROJECT" ] || die "无法从 origin URL 解析项目路径：$(scrub_url "$origin")"
# Gitea addresses a repository as two path segments (owner/repo); encode each
# segment separately so the slash stays a separator.
PROJECT_API="$(python3 - "$PROJECT" <<'EOF'
import sys, urllib.parse
print("/".join(urllib.parse.quote(p, safe="") for p in sys.argv[1].strip("/").split("/")))
EOF
)"

# --- 3. deploy snapshot ------------------------------------------------------

mkdir -p "$SNAPSHOT_DIR"
cp -f "$SCRIPT_DIR"/*.py "$SCRIPT_DIR"/PROTOCOL.template.md "$SCRIPT_DIR"/ROUTER.template.md "$SNAPSHOT_DIR"/ \
  || die "快照部署失败（cp 退出非零；源树资产不可读或目标不可写）"
note "快照已部署：${SNAPSHOT_DIR}"

# --- 4. config (0600; secret_token reused across re-runs) -------------------

mkdir -p "$CONFIG_DIR"
chmod 700 "$CONFIG_DIR"
SECRET="$(service_name="$SERVICE_NAME" python3 - 2>/dev/null <<'EOF' || true
import json, os
from pathlib import Path
p = Path.home() / ".config" / os.environ["service_name"] / "config.json"
try:
    v = json.loads(p.read_text()).get("secret_token", "")
    print(v if isinstance(v, str) else "")
except Exception:
    pass
EOF
)"
if [ -z "$SECRET" ]; then
  SECRET="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
fi
[ -n "$SECRET" ] || die "无法生成 secret_token"

# 重装合并旗标：1＝本次输入显式提供（env/json 高优先路）照常覆盖；0＝仅回退
# 内置默认值，保留既有 config 的值（F-merge：重装不得静默改写已调字段）。
CB_EXPLICIT=$([ -n "${ISSUE_AGENT_CALLBACK_URL:-}" ] && echo 1 || echo 0)
SSL_EXPLICIT=$([ -n "${ISSUE_AGENT_SSL_VERIFY:-}" ] && echo 1 || echo 0)
BRANCH_EXPLICIT=$([ -n "${ISSUE_AGENT_DEFAULT_BRANCH:-}" ] || [ -n "$BRANCH_JSON" ] && echo 1 || echo 0)
VERIFY_EXPLICIT=$([ -n "$VERIFY_JSON" ] && echo 1 || echo 0)
MP_EXPLICIT=$([ -n "${ISSUE_AGENT_MAX_PARALLEL:-}" ] && echo 1 || echo 0)
# agora 四键只在本次显式提供时写入（legacy 部署 config 面零变化）；未提供
# 时既有 config 的 agora 键经下方 unknown-key 存活规则原样保留（重装合并）。
MODE_EXPLICIT=$([ -n "${ISSUE_AGENT_EXECUTION_MODE:-}" ] || [ -n "$MODE_JSON" ] && echo 1 || echo 0)
ROUTER_EXPLICIT=$([ -n "${ISSUE_AGENT_ROUTER_MODEL:-}" ] || [ -n "$ROUTER_MODEL_JSON" ] && echo 1 || echo 0)
AHOME_EXPLICIT=$([ -n "${ISSUE_AGENT_AGORA_HOME:-}" ] || [ -n "$AGORA_HOME_JSON" ] && echo 1 || echo 0)
APOLL_EXPLICIT=$([ -n "${ISSUE_AGENT_AGORA_POLL_INTERVAL:-}" ] || [ -n "$AGORA_POLL_JSON" ] && echo 1 || echo 0)

api_base="$API_BASE" pat="$PAT" repo="$CANONICAL" cb="$CALLBACK_URL" \
  secret="$SECRET" port="$PORT" ssl_verify="${ISSUE_AGENT_SSL_VERIFY:-false}" \
  gitea_user="$GITEA_USER" project="$PROJECT" default_branch="$DEFAULT_BRANCH" \
  max_parallel="${ISSUE_AGENT_MAX_PARALLEL:-1}" \
  service_name="$SERVICE_NAME" canonical="$CANONICAL" identity="$IDENTITY" \
  out_of_scope="$OUT_OF_SCOPE" verify_command="$VERIFY_COMMAND" \
  execution_mode="$EXECUTION_MODE" router_model="$ROUTER_MODEL" \
  agora_home="$AGORA_HOME_CFG" agora_poll_interval="$AGORA_POLL" \
  old_config="$CONFIG_FILE" cb_explicit="$CB_EXPLICIT" ssl_explicit="$SSL_EXPLICIT" \
  branch_explicit="$BRANCH_EXPLICIT" verify_explicit="$VERIFY_EXPLICIT" \
  mp_explicit="$MP_EXPLICIT" mode_explicit="$MODE_EXPLICIT" \
  router_explicit="$ROUTER_EXPLICIT" ahome_explicit="$AHOME_EXPLICIT" \
  apoll_explicit="$APOLL_EXPLICIT" \
  python3 - <<'EOF'
import json, os, sys
from pathlib import Path
cfg = {
    "api_base": os.environ["api_base"],
    "pat": os.environ["pat"],
    "repo_path": os.environ["repo"],
    "canonical_path": os.environ["canonical"],
    "callback_url": os.environ["cb"],
    "secret_token": os.environ["secret"],
    "port": int(os.environ["port"]),
    "ssl_verify": os.environ["ssl_verify"].strip().lower() in ("1", "true", "yes", "on"),
    "gitea_user": os.environ["gitea_user"],
    "project": os.environ["project"],
    "service_name": os.environ["service_name"],
    "identity": os.environ["identity"],
    "out_of_scope": [s for s in os.environ["out_of_scope"].split(",") if s],
    "verify_command": os.environ["verify_command"],
    "default_branch": os.environ["default_branch"],
    "max_parallel": int(os.environ["max_parallel"]),
    "worktree_path": "",  # deprecated: kept for schema compat, always empty
}
# agora 四键（计划 Task 6）：仅显式提供才写；poll interval 走 int 强转
# （上方 shell 面已挡非正整数，坏类型在此按缺省丢弃而非崩溃）。
if os.environ["mode_explicit"] == "1" and os.environ["execution_mode"]:
    cfg["execution_mode"] = os.environ["execution_mode"]
if os.environ["router_explicit"] == "1" and os.environ["router_model"]:
    cfg["router_model"] = os.environ["router_model"]
if os.environ["ahome_explicit"] == "1" and os.environ["agora_home"]:
    cfg["agora_home"] = os.environ["agora_home"]
if os.environ["apoll_explicit"] == "1" and os.environ["agora_poll_interval"]:
    try:
        cfg["agora_poll_interval"] = int(os.environ["agora_poll_interval"])
    except ValueError:
        pass
# F-merge: reinstall must not silently rewrite tuned/custom fields. cfg
# already holds this run's explicit input (env/json) or built-in defaults;
# fields whose only source is a default keep the existing config's value,
# and unknown keys (daemon-managed, e.g. reconcile_interval) survive
# verbatim — config.py reads them with its own defaults.
old = {}
try:
    loaded = json.loads(Path(os.environ["old_config"]).read_text(encoding="utf-8"))
    if isinstance(loaded, dict):
        old = loaded
except Exception:
    old = {}
merged = []
dropped = []
_TYPES = {"callback_url": str, "ssl_verify": bool, "default_branch": str,
          "verify_command": str, "max_parallel": int}

def _typed(val, typ):
    # bool 是 int 子类：max_parallel 只收真 int（True/False 不算），
    # 防 daemon 侧 int()/bool() 强转把错误类型静默翻转为意外值。
    if typ is int:
        return isinstance(val, int) and not isinstance(val, bool)
    return isinstance(val, typ)

for key, flag in (("callback_url", "cb_explicit"),
                  ("ssl_verify", "ssl_explicit"),
                  ("default_branch", "branch_explicit"),
                  ("verify_command", "verify_explicit"),
                  ("max_parallel", "mp_explicit")):
    val = old.get(key)
    if os.environ[flag] != "1" and val not in ("", None):
        if _typed(val, _TYPES[key]):
            cfg[key] = val
            merged.append(key)
        else:
            dropped.append(key)
for key, value in old.items():
    if key not in cfg:
        if key == "agora_poll_interval" and not _typed(value, int):
            # 手编坏型 poll（空串/浮点/字符串）不得经存活规则透传——daemon
            # 侧 _bounded_int 会拒载成崩溃循环（0/负数等真 int 由 shell 面
            # 离线拒，这里只兜类型面）。
            dropped.append(key)
            continue
        cfg[key] = value
if merged:
    print("install: 保留既有 config 字段（本次输入未显式提供）："
          + ", ".join(sorted(merged)), file=sys.stderr)
if dropped:
    print("install: 注意：既有 config 字段类型不符，已回退内置默认值："
          + ", ".join(sorted(dropped)), file=sys.stderr)
p = Path.home() / ".config" / os.environ["service_name"] / "config.json"
p.parent.mkdir(parents=True, exist_ok=True)
# 0600 from the first byte (F4): os.open mode applies at creation, so no
# world-readable window exists between write and chmod.
fd = os.open(p, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as f:
    f.write(json.dumps(cfg, indent=2) + "\n")
EOF
note "配置已写入：${CONFIG_FILE}（0600）"

# --- 5. systemd user unit ------------------------------------------------------

mkdir -p "$UNIT_DIR"
cat > "$UNIT_FILE" <<EOF
[Unit]
Description=${SERVICE_NAME} Gitea issue agent
Wants=network-online.target
After=network-online.target

[Service]
ExecStart=/usr/bin/python3 %h/.local/lib/${SERVICE_NAME}/daemon.py
Restart=on-failure
RestartSec=5
Environment=PYTHONDONTWRITEBYTECODE=1
Environment="PATH=%h/.opencode/bin:/usr/local/bin:/usr/bin:/bin"
Environment=ISSUE_AGENT_SERVICE_NAME=${SERVICE_NAME}
Environment=ISSUE_AGENT_PORT=${PORT}

[Install]
WantedBy=default.target
EOF
note "unit 已写入：${UNIT_FILE}"

# --- 6. webhook upsert (ONLY issues + issue_comment events) -------------------

API_ROOT="${API_BASE%/}"
HOOKS_URL="${API_ROOT}/api/v1/repos/${PROJECT_API}/hooks"
hook_body="$(cb="$CALLBACK_URL" secret="$SECRET" python3 - <<'EOF'
import json, os
print(json.dumps({
    "type": "gitea",
    "active": True,
    "branch_filter": "*",
    "events": ["issues", "issue_comment"],
    "config": {"url": os.environ["cb"], "content_type": "json",
               "secret": os.environ["secret"]},
}))
EOF
)"

# B2: the hook body embeds the webhook secret — it must never sit on the
# curl argv (/proc/<pid>/cmdline is world-readable; the file's own F2
# rule for the PAT). Park it in a 0600 temp file and send -d @file.
BODY_FILE="$(mktemp)"
chmod 600 "$BODY_FILE"
printf '%s' "$hook_body" > "$BODY_FILE"
trap 'rm -f "$HDR_FILE" "$BODY_FILE"' EXIT

sweep_stale_hooks() { # B3: same-instance hooks are identified by :PORT —
  # delete any whose URL no longer matches the current callback, so
  # LAN-IP churn never stacks registrations across re-installs.
  local sid ids
  ids="$(printf '%s' "$hooks_json" | port="$PORT" cb="$CALLBACK_URL" python3 -c '
import json, os, sys
try:
    hooks = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for h in hooks if isinstance(hooks, list) else []:
    url = (h.get("config") or {}).get("url") or ""
    if url != os.environ["cb"] and url.rstrip("/").endswith(":" + os.environ["port"]):
        print(h["id"])
' 2>/dev/null || true)"
  for sid in $ids; do
    curl -s "${CURL_TLS[@]}" -X DELETE -H @"$HDR_FILE" -o /dev/null \
      "${HOOKS_URL}/${sid}" 2>/dev/null || true
    note "已清理陈旧 webhook（id=${sid}：同端口 ${PORT} 的旧回调地址）"
  done
}

hooks_json="$(curl -s "${CURL_TLS[@]}" -X GET -H @"$HDR_FILE" "${HOOKS_URL}?limit=50" 2>/dev/null || true)"
sweep_stale_hooks
existing_id="$(printf '%s' "$hooks_json" | cb="$CALLBACK_URL" python3 -c '
import json, os, sys
try:
    hooks = json.load(sys.stdin)
except Exception:
    sys.exit(0)
for h in hooks if isinstance(hooks, list) else []:
    if (h.get("config") or {}).get("url") == os.environ["cb"]:
        print(h["id"]); break
' 2>/dev/null || true)"
# F6: single page only — warn when the 50-hook page cap is hit so a
# missed match (and thus a duplicate registration) is at least visible.
hooks_n="$(printf '%s' "$hooks_json" | python3 -c '
import json, sys
try:
    print(len(json.load(sys.stdin)))
except Exception:
    print(0)
' 2>/dev/null || echo 0)"
if [ "${hooks_n:-0}" -ge 50 ]; then
  note "警告：仓库 webhook 已达 50 条（分页上限），列表可能不完整——若重复注册，请到 Gitea 仓库 Settings → Webhooks 人工核对"
fi

if [ -n "$existing_id" ]; then
  out="$(curl -s "${CURL_TLS[@]}" -X PATCH -H @"$HDR_FILE" -H "Content-Type: application/json" \
    -d @"$BODY_FILE" -w '\n%{http_code}' "${HOOKS_URL}/${existing_id}" || true)"
  hook_code="${out##*$'\n'}"
  [ "$hook_code" = "200" ] || die "webhook 更新失败（HTTP ${hook_code:-无响应}）"
  hook_id="$existing_id"
  note "webhook 已更新（id=${hook_id}）"
else
  out="$(curl -s "${CURL_TLS[@]}" -X POST -H @"$HDR_FILE" -H "Content-Type: application/json" \
    -d @"$BODY_FILE" -w '\n%{http_code}' "${HOOKS_URL}" || true)"
  hook_code="${out##*$'\n'}"
  body="${out%$'\n'*}"
  case "$hook_code" in
    200|201) ;;
    *) die "webhook 注册失败（HTTP ${hook_code:-无响应}）" ;;
  esac
  hook_id="$(printf '%s' "$body" | python3 -c '
import json, sys
try:
    print(json.load(sys.stdin)["id"])
except Exception:
    pass' 2>/dev/null || true)"
  note "webhook 已注册（id=${hook_id:-?}）"
fi

# B3 post-pass: re-list and sweep again so nothing that slipped in
# around the upsert outlives this run
hooks_json="$(curl -s "${CURL_TLS[@]}" -X GET -H @"$HDR_FILE" "${HOOKS_URL}?limit=50" 2>/dev/null || true)"
sweep_stale_hooks

# --- 7. post-registration reachability (warn only) ---------------------------

if [ -n "${hook_id:-}" ]; then
  # Gitea test delivery is a push-event probe — proves reachability, so a
  # failure is a warning, never a rollback trigger.
  tcode="$(curl -s "${CURL_TLS[@]}" -X POST -H @"$HDR_FILE" -H "Content-Type: application/json" \
    -d "{\"ref\":\"${DEFAULT_BRANCH}\"}" -o /dev/null -w '%{http_code}' \
    "${HOOKS_URL}/${hook_id}/tests" || true)"
  case "$tcode" in
    200|204) ;;
    *) note "警告：Gitea test-hook 触发返回 HTTP ${tcode:-无响应}——回调 URL 可能不可达，请检查本机防火墙对端口 ${PORT} 的放行" ;;
  esac
else
  note "警告：未能取得 hook id，跳过 test-hook 可达性检查"
fi

# --- 8. protected-default-branch check (warn only) -----------------------------

# Gitea exposes branch protections as a list; an empty list, 403 or 404 all
# mean "not protected" for our purposes (warn, never fail).
def_br="$(curl -s "${CURL_TLS[@]}" -H @"$HDR_FILE" \
  "${API_ROOT}/api/v1/repos/${PROJECT_API}" 2>/dev/null \
  | fallback="$DEFAULT_BRANCH" python3 -c 'import json, os, sys
try:
    print(json.load(sys.stdin).get("default_branch") or os.environ["fallback"])
except Exception:
    print(os.environ["fallback"])' 2>/dev/null || echo "$DEFAULT_BRANCH")"
pout="$(curl -s "${CURL_TLS[@]}" -X GET -H @"$HDR_FILE" \
  -w '\n%{http_code}' "${API_ROOT}/api/v1/repos/${PROJECT_API}/branch_protections" || true)"
pcode="${pout##*$'\n'}"
pbody="${pout%$'\n'*}"
case "$pcode" in
  200)
    prot="$(printf '%s' "$pbody" | def_br="$def_br" python3 -c '
import json, os, sys
try:
    items = json.load(sys.stdin)
except Exception:
    print("unknown"); raise SystemExit
if not isinstance(items, list):
    print("unknown"); raise SystemExit
print("yes" if any(i.get("branch_name") == os.environ["def_br"] for i in items) else "no")' 2>/dev/null || true)" ;;
  403|404) prot="no" ;;
  *) prot="unknown" ;;
esac
if [ "$prot" = "no" ]; then
  echo "⚠️  警告：${def_br} 分支未受保护——推送到 ${def_br} 未被 Gitea 拦截！" >&2
  echo "    建议在 Gitea 仓库 Settings → Branches 中保护 ${def_br}。" >&2
elif [ "$prot" = "unknown" ]; then
  note "警告：无法确认 ${def_br} 分支的保护状态（API 查询失败），请人工核对"
fi

# --- 9. start + verify (rollback on failure, config kept) ---------------------

rollback() {
  echo "install: 服务未激活，回滚（保留 ${CONFIG_FILE}）……" >&2
  # F5: the webhook would keep pointing at a dead callback — delete it
  # best-effort and always leave the manual-removal hint.
  if [ -n "${hook_id:-}" ]; then
    curl -s "${CURL_TLS[@]}" -X DELETE -H @"$HDR_FILE" -o /dev/null \
      "${HOOKS_URL}/${hook_id}" 2>/dev/null || true
    echo "install: 已尝试删除 webhook（id=${hook_id}）；若仍存在请到 Gitea 仓库 Settings → Webhooks 手动删除" >&2
  fi
  systemctl --user stop "${SERVICE_NAME}.service" 2>/dev/null || true
  systemctl --user disable "${SERVICE_NAME}.service" 2>/dev/null || true
  rm -f "$UNIT_FILE"
  rm -rf "$SNAPSHOT_DIR"
  systemctl --user daemon-reload 2>/dev/null || true
  if [ "${PORT_ALLOCATED:-0}" = "1" ]; then
    release_port "$PORTS_JSON" "$PORT" "$SERVICE_NAME" || true
  fi
}

systemctl --user daemon-reload
if ! systemctl --user enable --now "${SERVICE_NAME}.service"; then
  rollback
  die "enable --now 失败"
fi
state="$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
if [ "$state" != "active" ]; then
  rollback
  die "服务未进入 active（当前 ${state:-unknown}）"
fi
# F0: enable --now does NOT restart an already-active unit, so an
# in-place update would still run the old snapshot — restart explicitly.
if [ "$svc_active" = "active" ]; then
  systemctl --user restart "${SERVICE_NAME}.service"
  state="$(systemctl --user is-active "${SERVICE_NAME}.service" 2>/dev/null || true)"
  if [ "$state" != "active" ]; then
    # B1/F0: a failed update-restart must roll back like a failed fresh
    # install — otherwise a stopped unit, the new snapshot, and the
    # patched webhook are all left behind.
    rollback
    die "更新后重启服务失败（当前 ${state:-unknown}，已回滚）——请 journalctl --user -u ${SERVICE_NAME} 排查"
  fi
  note "服务已重启以加载更新后的快照"
fi
MODE_NOTE=""
if [ "$EFFECTIVE_MODE" = "agora" ]; then MODE_NOTE="；执行模式 agora"; fi
note "安装完成：${SERVICE_NAME}.service 已 active（端口 ${PORT}）；项目 ${PROJECT}；默认分支 ${DEFAULT_BRANCH}；回调 ${CALLBACK_URL}${MODE_NOTE}"
