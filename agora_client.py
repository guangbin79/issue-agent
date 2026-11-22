"""agora HTTP 服务客户端（计划 Task 1）——python3 stdlib only。

服务发现（对齐 agora-plugin 同款规则）：<home>/.agora/service.json 的
port（base=http://127.0.0.1:<port>）与 <home>/.agora/root.token（两形：
v2 明文 / v3 authsvc JSON {"token": ...}，F160）。构造
惰性：不触网、不读盘，首个 API 调用才做发现。

凭据两段：root 牌仅用于 POST /cred_for_sid 为 "issue-agent:<service_name>"
签发/取回 dispatcher 牌（一个客户端实例恰签发一次，进程内缓存）；此后
任务面（create/dispatch/board/post/message_pull(/check_mail)/doctor/
deliver/wiki_write）全部携
dispatcher 牌，root 牌不再出现。

错误三分类（调用方按类处置）：
  - AgoraUnavailable：拒连/超时/5xx/service.json 缺失或坏
  - AgoraRejected：HTTP 4xx（消息含服务端 error 文本）
  - AgoraProtocolError：200 但响应非 JSON 对象
任何异常文本不得含 root/dispatcher 牌值（服务端回显也照样抹掉）。
"""

import http.client
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path


class AgoraUnavailable(RuntimeError):
    """agora 服务不可达或不可用（拒连/超时/5xx/服务发现失败）。"""


class AgoraRejected(RuntimeError):
    """agora 拒绝请求（HTTP 4xx），消息含服务端 error 文本。"""


class AgoraProtocolError(RuntimeError):
    """agora 返回 200 但响应不是 JSON 对象。"""


def parse_root_token(raw):
    """F160 两形：root.token 原文 → 牌值。v3 authsvc 写 JSON 对象
    {"token": ...}——dict 形按 v3 取键（缺键/非 str/深嵌套溢出均抛
    ValueError，由调用方归入各自发现失败口径）；其余（含 JSON 解析失败）
    按 v2 明文，返回 strip 后文本（合法性交 _checked_token / 调用方入口
    校验）。"""
    text = raw.strip()
    try:
        cred = json.loads(text)
    except ValueError:
        cred = None
    except RecursionError as exc:  # 深嵌套坏形不是 ValueError 子类——就地归入 fail-closed 通道
        raise ValueError("root.token JSON too deeply nested") from exc
    if isinstance(cred, dict):
        if not isinstance(cred.get("token"), str):
            raise ValueError("root.token JSON lacks string token")
        return cred["token"]
    return text


def _checked_token(token, error_cls, where):
    """牌值入客户端前的信任边界校验：可打印 ASCII 且无空白。

    非法 header 值会让 http.client 在 putheader 抛 ValueError（消息含牌值
    repr）且不在 _call 捕获面——在入口拒绝，牌值永远到不了 header。"""
    if not token or not all(0x21 <= ord(c) <= 0x7E for c in token):
        raise error_cls(f"{where}: token is missing or has illegal characters")
    return token


class AgoraClient:
    """绑定一个 service 身份的 agora 任务面客户端（构造惰性）。"""

    def __init__(self, service_name, home, poll_interval=15, timeout_s=10):
        self.service_name = service_name
        self.home = home
        self._poll_s = max(1, int(poll_interval))
        self.timeout_s = timeout_s
        self._base = None       # http://127.0.0.1:<port>，首次调用发现
        self._root_token = None
        self._dispatcher_token = None
        self._cred_lock = threading.Lock()  # Task 3 起 monitor/turn 并发：恰签发一次
        # ag-ac5771 R1 #2：_base/_root_token 成对写锁（勿用 _cred_lock——
        # _ensure_dispatcher_token 持锁调 _discover 再取本锁会自锁）
        self._discover_lock = threading.Lock()

    def poll_interval(self) -> float:
        """monitor 轮询间隔（秒）。"""
        return float(self._poll_s)

    # ---- 服务发现与凭据 ----

    def _read_service(self):
        """读 service.json + root.token（不落缓存）。失败一律 AgoraUnavailable。"""
        agora_dir = Path(self.home) / ".agora"
        try:
            service = json.loads(
                (agora_dir / "service.json").read_text(encoding="utf-8"))
            port = int(service["port"])
            if not 1 <= port <= 65535:  # 越界值会在 urlopen 抛 OverflowError（非 OSError）逸出分类
                raise ValueError(f"port out of range: {port}")
            root_token = parse_root_token(
                (agora_dir / "root.token").read_text(encoding="utf-8"))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise AgoraUnavailable(
                f"agora service discovery failed under {self.home}: "
                f"{type(exc).__name__}") from exc
        root_token = _checked_token(
            root_token, AgoraUnavailable,
            f"agora root token under {self.home}")
        return f"http://127.0.0.1:{port}", root_token

    def _discover(self):
        """读 service.json + root.token（首次一次，进程内缓存）。"""
        if self._base is None:
            with self._discover_lock:
                if self._base is None:  # 双检：并发首发现只落一次
                    self._base, self._root_token = self._read_service()
        return self._root_token

    def _ensure_dispatcher_token(self):
        with self._cred_lock:
            if self._dispatcher_token is None:
                root_token = self._discover()
                payload = self._call("/cred_for_sid",
                                     {"sid": f"issue-agent:{self.service_name}"},
                                     root_token)
                token = payload.get("token")
                if not isinstance(token, str):
                    raise AgoraProtocolError(
                        "/cred_for_sid -> 200 response missing token")
                token = _checked_token(
                    token, AgoraProtocolError,
                    "/cred_for_sid -> response token")
                self._dispatcher_token = token
            return self._dispatcher_token

    # ---- 传输 ----

    def _scrub(self, text):
        """抹掉已知牌值：异常文本零牌泄漏（含服务端回显场景）。"""
        for token in (self._root_token, self._dispatcher_token):
            if token:
                text = text.replace(token, "***")
        return text

    def _call(self, path, body, token, _rediscover=True):
        base_used = self._base  # 终闸B S1：重发判定的比较基准＝本次实发 base
        request = urllib.request.Request(
            base_used + path, data=json.dumps(body).encode("utf-8"),
            method="POST",
            headers={"Content-Type": "application/json",
                     "X-Agora-Token": token})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as exc:  # 先于 OSError（其子类）
            try:
                server_error = json.loads(
                    exc.read().decode("utf-8", "replace")).get("error", "")
            except (ValueError, AttributeError, OSError,
                    http.client.HTTPException):  # 取错体失败不遮蔽原状态码
                server_error = ""
            message = self._scrub(
                f"{path} -> HTTP {exc.code}: {server_error}")
            if 400 <= exc.code < 500:
                raise AgoraRejected(message) from exc
            raise AgoraUnavailable(message) from exc
        except (OSError, http.client.HTTPException) as exc:
            # URLError/拒连/读超时/截断（IncompleteRead 非 OSError 子类，并入同分类）
            # ag-ac5771 缺陷2：拒连可能是 agorad 重启换端口（service.json
            # 变更）而端口/base 发现缓存终身制——重读 service.json，对本次
            # 实发 base 端口确变才失效缓存重发一次（R1 #1：同址瞬断/读超时
            # 已可能送达服务端，重发＝双发非幂等 POST；5xx 同理不在此支）。
            # dispatcher 牌缓存不动。
            # shortcut: 读超时等已送达形态与端口变更共窗时仍重发一次（双发面
            # 有界：dispatch 被服务端拒、post 重复留言）；要更强保证需按异常
            # 形态区分预发/后发失败，urllib 异常形状跨版本不稳定故不做。
            if _rediscover and base_used is not None:
                try:
                    base, root_token = self._read_service()
                except AgoraUnavailable as disc_exc:
                    # R1 #4：保留原连接语境（journal 只打 str(exc)）
                    raise AgoraUnavailable(self._scrub(
                        f"{path} unreachable: {exc}; "
                        f"rediscovery failed: {disc_exc}")) from exc
                with self._discover_lock:  # 成对写（R1 #2）：与 _discover 同锁
                    port_changed = base != base_used  # 基准＝本次实发 base（终闸B S1）
                    if port_changed:
                        self._base, self._root_token = base, root_token
                if port_changed:
                    if token != self._dispatcher_token:
                        # root 牌调用（cred_for_sid）改携新牌重发（终闸C：
                        # 旧 root 牌已随换牌出 _scrub 词表，服务端回显即泄漏；
                        # root.token 同轮换时携旧牌也必被拒）
                        token = self._root_token
                    return self._call(path, body, token, _rediscover=False)
            raise AgoraUnavailable(self._scrub(
                f"{path} unreachable: {exc}")) from exc
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            raise AgoraProtocolError(self._scrub(
                f"{path} -> 200 response is not JSON: "
                f"{raw[:120].decode('utf-8', 'replace')!r}")) from exc
        if not isinstance(payload, dict):
            raise AgoraProtocolError(
                f"{path} -> 200 response is not a JSON object: "
                f"{type(payload).__name__}")
        return payload

    def _task_call(self, path, body):
        return self._call(path, body, self._ensure_dispatcher_token())

    # ---- 任务面（全部携 dispatcher 牌）----

    def create_task(self, *, goal, project_dir, target_branch, verify_cmd,
                    model, title, client_key, deliverable="external_branch",
                    after=None, verify_base_exempt=None,
                    review_cmd=None) -> str:
        body = {"goal": goal, "project_dir": project_dir,
                "target_branch": target_branch, "verify_cmd": verify_cmd,
                "model": model, "title": title, "client_key": client_key,
                "deliverable": deliverable}
        if after is not None:  # 服务端字符串校验拒 null：None 不含键
            body["after"] = after
        if verify_base_exempt is not None:  # F107：仅显式设置才上送
            body["verify_base_exempt"] = verify_base_exempt
        if review_cmd:  # F114 门审：非空才上送（None/空＝协议锚路径）
            body["review_cmd"] = review_cmd
        payload = self._task_call("/task_create", body)
        tid = payload.get("task")
        if not isinstance(tid, str) or not tid:
            raise AgoraProtocolError(
                "/task_create -> 200 response missing task id")
        return tid

    def dispatch(self, tid) -> None:
        self._task_call("/task_dispatch", {"task": tid})

    def task_update(self, tid, verify_cmd) -> None:
        """ag-fb9e5a：verify 命令登记/改写（dispatcher 牌＝建单身份，服务端
        F2 语义：登记即锁定；worker 角色全局永拒——daemon 代执行正是
        verify 提案执行回路的授权面）。"""
        self._task_call("/task_update",
                        {"task": tid, "verify_cmd": verify_cmd})

    def root_task_update(self, tid, verify_cmd) -> None:
        """ag-72646a：root 牌 verify 改写——评论批准执行器的唯一执行面。
        verify 建单即锁后 dispatcher/worker 的 task_update 均 403
        （ag-0f7d7f 判例：最终人工以 root HTTP 落账）；root 牌直接来自
        服务发现（service.json＋root.token），不经 cred_for_sid。"""
        self._call("/task_update", {"task": tid, "verify_cmd": verify_cmd},
                   self._discover())

    def task_nudge(self, tid, body) -> dict:
        """ag-da7410 C：既有 /task_nudge HTTP 面 dispenser 提示注入（服务
        返回 {"nudged": bool, "reason": ...}；body ≤2000 字符服务端校验）。"""
        return self._task_call("/task_nudge", {"task": tid, "body": body})

    def task_cancel(self, tid, force=False) -> dict:
        """task_cancel（dispatcher 身份可取消自己名下任务；叶子任务级联 0）。
        F28 忙碌闸应答直透（200+cancelled=0+reason=worker_busy——由调用方
        决定是否携 force 重试）；force 仅 True 时上送。"""
        body = {"task": tid}
        if force:
            body["force"] = True
        return self._task_call("/task_cancel", body)

    def board(self, tid=None) -> dict:
        return self._task_call("/board", {"task": tid} if tid is not None else {})

    def deliver(self, tid, outcome, commit, base=None, reason="") -> dict:
        body = {"task": tid, "outcome": outcome, "commit": commit,
                "reason": reason}
        if base is not None:  # 同 after：None 不含键
            body["base"] = base
        return self._task_call("/task_deliver", body)

    def post(self, tid, kind, body) -> None:
        self._task_call("/post", {"task": tid, "kind": kind, "body": body})

    def message_pull(self) -> list:
        """F160 二段⑦：收件箱拉取——v3 工具名 message_pull 经 v2 兼容根
        路由 /check_mail（dispatcher 牌）。响应 200 非 messages list ＝
        AgoraProtocolError（调用方按毒形态处置）。"""
        payload = self._task_call("/check_mail", {})
        messages = payload.get("messages")
        if not isinstance(messages, list):
            raise AgoraProtocolError(
                "/check_mail -> 200 response missing messages list")
        return messages

    def wiki_write(self, path, text) -> None:
        """F160 二段⑧：wiki 页写入（/wiki_write，dispatcher 牌）。wiki/
        tasks/ 前缀服务独占拒写（AgoraRejected）——规格页只写 issue-specs/
        面。4xx（含旧版服务无此路由的 404）同样 AgoraRejected，由调用方
        决定回退。"""
        self._task_call("/wiki_write", {"path": path, "text": text})

    def doctor(self) -> dict:
        return self._task_call("/doctor", {})
