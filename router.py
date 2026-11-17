"""语义 Router（计划 Task 2；P1-5：每次新会话）——只读语义面，产出受
schema 约束的决策。

设计 §9.1 分工：daemon 确定性调用，Router 只做需求分析与模型选择。每次
decide/answer 新建独立 opencode 会话——输入本已自包含（模板＋issue 上下文
＋可用模型清单），持久会话的跨 issue 上下文累积只会腐化决策（issue A
的坑位串味 issue B、历史挤爆模板），无任何正向依赖；会话级故障隔离到
单次调用，重启无会话账本可丢。decide 仍在 router_lock 内串行（限流；会话
本身互不共享状态，answer 可与 decide 并发）。

安全边界（不可协商）：
- issue 文本是不可信输入；Router 只返回 schema 决策，daemon 校验后才建单。
- 决策键集恒为七个 schema 键——verify_cmd 等多余键在重建 raw 时被丢弃，
  「验收命令只能来自实例可信配置」不靠提示词自觉，靠本模块的硬过滤。
- Router 绝不携带 write-gate credential_prefix：语义面无写权限需求。

坏决策（坏 JSON/未知 action/空 goal/非法模型）抛 RouterDecisionError，
由调用方（daemon，计划 Task 3）走 question/defer，不执行。
断路器（P1-5）：连续失败（会话故障/超时/坏决策）达 max_consecutive_failures
（缺省 3）即开路——后续调用不再打模型，直接抛 RouterCircuitOpen，由
orchestrator 映射 blocked 人工处理；任一次完整成功复位计数。
"""

import json
import re
import threading
from dataclasses import dataclass
from pathlib import Path

import runner

DECISION_KEYS = ("schema_version", "action", "title", "goal", "model",
                 "deliverable_hint", "reason")
ALLOWED_ACTIONS = ("dispatch", "ignore", "question", "answer")
# deliverable_hint 白名单（ag-da7410 A）：唯一显式值 out_of_band（需勘察的
# 验证/解答类，零提交 review-only）；external_branch 是缺省而非 hint 值。
ALLOWED_DELIVERABLE_HINTS = ("out_of_band",)
TITLE_MAX_CHARS = 40
ROUTER_SESSION_TITLE = "issue-agent router"
_PLACEHOLDER_RE = re.compile(
    r"\{\{(IID|EVENT|ISSUE_CONTEXT|AVAILABLE_MODELS)\}\}")


class RouterDecisionError(RuntimeError):
    """Router 输出未通过决策 schema 校验（坏 JSON/未知 action/空 goal/非法模型）。"""


class RouterCircuitOpen(RuntimeError):
    """断路器开路（P1-5）：连续失败达上限，调用方应 blocked 求助而非重试。"""


@dataclass(frozen=True)
class RouterDecision:
    action: str
    title: str
    goal: str
    model: str
    reason: str
    raw: dict
    # ag-da7410 A：dispatch 的交付面提示——"out_of_band"＝需勘察的验证/
    # 解答类（零提交、done body=结论文本）；None＝代码修复类缺省
    # external_branch。answer/ignore/question 不建单，恒 None。
    deliverable_hint: str = None


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

    归一：多余键丢弃（raw 重建为恒等七键）、title 截 40 字、空白剥离、
    deliverable_hint 缺省归一 None。
    拒绝：未知 action；非法 deliverable_hint；dispatch 的空 goal；
    dispatch 的模型不在可用清单；answer 的空 goal（goal＝答案文本本体）。
    （ignore/question 不建单，goal/model 不消费、允许为空；answer 的
    model 不消费、允许任意。）"""
    parsed = _extract_json_object(reply_text)
    action = parsed.get("action")
    if action not in ALLOWED_ACTIONS:
        raise RouterDecisionError(f"unknown action: {action!r}")
    title = str(parsed.get("title") or "").strip()[:TITLE_MAX_CHARS]
    goal = str(parsed.get("goal") or "").strip()
    model = str(parsed.get("model") or "").strip()
    reason = str(parsed.get("reason") or "").strip()
    hint = str(parsed.get("deliverable_hint") or "").strip() or None
    if hint is not None and hint not in ALLOWED_DELIVERABLE_HINTS:
        raise RouterDecisionError(f"unknown deliverable_hint: {hint!r}")
    if action == "dispatch":
        if not goal:
            raise RouterDecisionError("dispatch decision with empty goal")
        if model not in available_models:
            raise RouterDecisionError(
                f"dispatch model not in available models: {model!r}")
    elif action == "answer":
        if not goal:
            raise RouterDecisionError("answer decision with empty goal")
    raw = dict(zip(DECISION_KEYS, (parsed.get("schema_version"), action,
                                   title, goal, model, hint, reason)))
    return RouterDecision(action=action, title=title, goal=goal, model=model,
                          reason=reason, raw=raw, deliverable_hint=hint)


class SemanticRouter:
    """每次新会话的语义 Router（P1-5）。

    无会话账本、无续用/重建路径——decide/answer 各自 new_session 一次性往返；
    decide 串行（router_lock），answer 独立锁（可与 decide 并发）。连续失败
    计数达 max_consecutive_failures 开路：任一次完整成功（会话往返＋决策
    校验）复位，失败形态不区分（模型故障与坏决策同计——都打不通链路）。
    Router 自身模型＝实例 router_model，与它为 worker 选的任务模型分离。"""

    def __init__(self, *, template_path, cwd, router_model,
                 timeout_s=runner.DEFAULT_TIMEOUT_S,
                 new_session=runner.new_session, warn=None,
                 max_consecutive_failures=3):
        self._template = Path(template_path).read_text(encoding="utf-8")
        self._cwd = cwd
        self._model = router_model or None
        self._timeout_s = timeout_s
        self._new_session = new_session
        self._warn = warn if warn is not None else (lambda message: None)
        self._max_failures = max(1, int(max_consecutive_failures))
        self._lock = threading.Lock()  # decide 串行（限流）
        self._answer_lock = threading.Lock()  # answer 与 decide 互不阻塞
        self._failures = 0

    def _check_open(self):
        if self._failures >= self._max_failures:
            raise RouterCircuitOpen(
                f"router circuit open: {self._failures} consecutive failures "
                f">= {self._max_failures}")

    def _roundtrip(self, message, *, title, on_success):
        """断路计数内的一次新会话往返＋结果验收；异常计连败后原样上抛。"""
        self._check_open()
        try:
            result = self._new_session(cwd=self._cwd, title=title,
                                       message=message,
                                       timeout_s=self._timeout_s,
                                       model=self._model)
            outcome = on_success(result.reply_text)
        except Exception:
            self._failures += 1
            raise
        self._failures = 0
        return outcome

    def decide(self, *, iid, event, issue_context,
               available_models) -> RouterDecision:
        """一个 issue 事件 -> 一个受 schema 约束的 RouterDecision（新会话）。"""
        message = build_router_prompt(
            self._template, iid=iid, event=event,
            issue_context=issue_context,
            available_models=available_models)
        with self._lock:
            return self._roundtrip(
                message, title=f"issue-agent router issue#{iid}",
                on_success=lambda reply: _validate_decision(
                    reply, available_models))

    def answer(self, *, iid, task, question) -> str:
        """worker 提问答复（计划 Task 5）：新会话直答，不做 schema 校验；
        worker 提问按不可信输入处理——与进入会话的 issue 文本同口径。"""
        message = (
            f"[worker-question] issue#{iid} agora 任务 {task} 的 worker "
            "提问，请直接给出一段简明中文答复（不要 JSON、不要决策 "
            "schema）：\n\n" + (question or ""))
        with self._answer_lock:
            return self._roundtrip(
                message, title=f"issue-agent router answer issue#{iid}",
                on_success=lambda reply: reply)
