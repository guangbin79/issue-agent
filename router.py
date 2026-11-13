"""持久语义 Router（计划 Task 2）——只读语义面，产出受 schema 约束的决策。

设计 §9.1 分工：daemon 确定性调用，Router 只做需求分析与模型选择。一个
跨回合持久的 opencode 会话承担所有语义决策；全部 decide 调用在独立
router_lock 内串行（同一持久会话不并发收 prompt）；session id 持久化在
账本根（state.ROUTER_SESSION_KEY），重启后续用而非重建。

安全边界（不可协商）：
- issue 文本是不可信输入；Router 只返回 schema 决策，daemon 校验后才建单。
- 决策键集恒为六个 schema 键——verify_cmd 等多余键在重建 raw 时被丢弃，
  「验收命令只能来自实例可信配置」不靠提示词自觉，靠本模块的硬过滤。
- Router 绝不携带 write-gate credential_prefix：语义面无写权限需求。

坏决策（坏 JSON/未知 action/空 goal/非法模型）抛 RouterDecisionError，
由调用方（daemon，计划 Task 3）走 question/defer，不执行。
"""

import json
import re
import threading
from dataclasses import dataclass
from pathlib import Path

import runner
import state as state_mod
from runner import SessionInvalid

DECISION_KEYS = ("schema_version", "action", "title", "goal", "model", "reason")
ALLOWED_ACTIONS = ("dispatch", "ignore", "question")
TITLE_MAX_CHARS = 40
ROUTER_SESSION_TITLE = "issue-agent router"
_PLACEHOLDER_RE = re.compile(
    r"\{\{(IID|EVENT|ISSUE_CONTEXT|AVAILABLE_MODELS)\}\}")


class RouterDecisionError(RuntimeError):
    """Router 输出未通过决策 schema 校验（坏 JSON/未知 action/空 goal/非法模型）。"""


@dataclass(frozen=True)
class RouterDecision:
    action: str
    title: str
    goal: str
    model: str
    reason: str
    raw: dict


def available_models_from_doctor(payload) -> list:
    """AgoraClient.doctor() 响应 -> 可用 worker 模型清单（Task 2 唯一来源）。

    agorad 契约（report.doctor_view）：models 键＝"providerID/modelID" 字符
    串列表；oc 不可达时键缺席。降级为空清单——dispatch 决策会被校验拒绝，
    失败方向是安全方向（不误派工）。脏条目（非字符串）滤掉。"""
    models = payload.get("models") if isinstance(payload, dict) else None
    if not isinstance(models, list):
        return []
    return [m for m in models if isinstance(m, str) and m]


def build_router_prompt(template_text, *, iid, event, issue_context,
                        available_models):
    """ROUTER.template.md + 本次输入 -> 单条 prompt 消息。

    输入以 JSON 嵌入（含不可信 issue 文本——模板已声明它只是分析素材）；
    占位符单趟正则替换：先替换出的文本不会再被扫描——issue 文本里注入
    的 "{{AVAILABLE_MODELS}}" 等字面量只能原样留在文本区，不被二次展开。"""
    values = {
        "IID": str(iid),
        "EVENT": json.dumps(event, ensure_ascii=False, indent=2),
        "ISSUE_CONTEXT": json.dumps(issue_context, ensure_ascii=False,
                                    indent=2),
        "AVAILABLE_MODELS": "\n".join(available_models),
    }
    return _PLACEHOLDER_RE.sub(lambda match: values[match.group(1)],
                               template_text)


def _extract_json_object(reply_text):
    """回复文本 -> dict；剥一层 ``` 围栏（模型习惯，归一不拒）。

    坏 JSON / 非 dict JSON 一律 RouterDecisionError。"""
    text = (reply_text or "").strip()
    if text.startswith("```"):
        newline = text.find("\n")
        text = text[newline + 1:] if newline != -1 else ""
        if text.endswith("```"):
            text = text[:-3]
    try:
        parsed = json.loads(text)
    except (json.JSONDecodeError, ValueError) as exc:
        raise RouterDecisionError(
            f"router reply is not JSON: {text[:80]!r}") from exc
    if not isinstance(parsed, dict):
        raise RouterDecisionError(
            f"router reply is not a JSON object: {type(parsed).__name__}")
    return parsed


def _validate_decision(reply_text, available_models) -> RouterDecision:
    """LLM 回复 -> 归一化+校验后的 RouterDecision。

    归一：多余键丢弃（raw 重建为恒等六键）、title 截 40 字、空白剥离。
    拒绝：未知 action；dispatch 的空 goal；dispatch 的模型不在可用清单
    （ignore/question 不建单，goal/model 不消费、允许为空）。"""
    parsed = _extract_json_object(reply_text)
    action = parsed.get("action")
    if action not in ALLOWED_ACTIONS:
        raise RouterDecisionError(f"unknown action: {action!r}")
    title = str(parsed.get("title") or "").strip()[:TITLE_MAX_CHARS]
    goal = str(parsed.get("goal") or "").strip()
    model = str(parsed.get("model") or "").strip()
    reason = str(parsed.get("reason") or "").strip()
    if action == "dispatch":
        if not goal:
            raise RouterDecisionError("dispatch decision with empty goal")
        if model not in available_models:
            raise RouterDecisionError(
                f"dispatch model not in available models: {model!r}")
    raw = dict(zip(DECISION_KEYS, (parsed.get("schema_version"), action,
                                   title, goal, model, reason)))
    return RouterDecision(action=action, title=title, goal=goal, model=model,
                          reason=reason, raw=raw)


class SemanticRouter:
    """一个跨回合持久 opencode 会话的语义 Router。

    router_lock 串行所有 decide（并发时 runner 调用 max_active==1）；
    SessionInvalid 至多重建一次（新会话不可能再失效——runner 只对
    续用路径报 SessionInvalid）。Router 自身模型＝实例 router_model，
    与它为 worker 选的任务模型分离。"""

    def __init__(self, *, template_path, cwd, router_model, state_path,
                 timeout_s=runner.DEFAULT_TIMEOUT_S,
                 new_session=runner.new_session,
                 continue_session=runner.continue_session, warn=None,
                 ledger_lock=None):
        self._template = Path(template_path).read_text(encoding="utf-8")
        self._cwd = cwd
        self._model = router_model or None
        self._state_path = state_path
        self._timeout_s = timeout_s
        self._new_session = new_session
        self._continue_session = continue_session
        self._warn = warn if warn is not None else (lambda message: None)
        # 与 orchestrator 共用的账本锁（OCR medium：persist 的
        # load→mutate→save 若与 orchestrator 写交错会丢更新）。None＝不
        # 加锁（测试/单进程场景）；生产接线传 orchestrator 同一把可重入锁
        # ——Task 3 持锁调 decide 时 persist 嵌套获取不自锁。
        self._ledger_lock = ledger_lock
        self._lock = threading.Lock()
        # 重启恢复：账本根已有 sid 则续用（会话真死了由 SessionInvalid 重建）
        self._session_id = state_mod.router_session_id(
            state_mod.load_state(state_path, warn=self._warn))

    def decide(self, *, iid, event, issue_context,
               available_models) -> RouterDecision:
        """一个 issue 事件 -> 一个受 schema 约束的 RouterDecision。"""
        message = build_router_prompt(
            self._template, iid=iid, event=event,
            issue_context=issue_context,
            available_models=available_models)
        with self._lock:
            if self._session_id is None:
                return self._decide_on_new_session(message, available_models)
            try:
                result = self._continue_session(
                    cwd=self._cwd, session_id=self._session_id,
                    message=message, timeout_s=self._timeout_s,
                    model=self._model)
            except SessionInvalid as exc:
                self._warn(f"router session invalid, rebuilding once: {exc}")
                self._session_id = None
                return self._decide_on_new_session(message, available_models)
            return _validate_decision(result.reply_text, available_models)

    def _decide_on_new_session(self, message, available_models):
        result = self._new_session(
            cwd=self._cwd, title=ROUTER_SESSION_TITLE, message=message,
            timeout_s=self._timeout_s, model=self._model)
        if not result.session_id:
            raise RouterDecisionError("router session id missing from reply")
        self._session_id = result.session_id
        self._persist_session_id(result.session_id)
        return _validate_decision(result.reply_text, available_models)

    def _persist_session_id(self, sid):
        if self._ledger_lock is not None:
            with self._ledger_lock:
                self._persist_session_id_locked(sid)
        else:
            self._persist_session_id_locked(sid)

    def _persist_session_id_locked(self, sid):
        ledger = state_mod.load_state(self._state_path, warn=self._warn)
        ledger[state_mod.ROUTER_SESSION_KEY] = sid
        state_mod.save_state(self._state_path, ledger)
