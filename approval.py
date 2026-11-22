"""评论授权执行器（ag-72646a）——issue 面板批准制的消费端，纯代码零 LLM。

背景（真实事故 ag-0f7d7f）：verify 建单即锁后，daemon（dispatcher 牌）与
主会话的 task_update 均 403，最终人工以 root HTTP 落账。用户裁定改 issue
面板批准制，三条硬约束落在本模块：批准链纯代码零 LLM（永不触 Router）；
批准＝引用已公示裁定 id（「批准 <8 位小写 hex>」严格全行匹配，禁自由文本
命令）；单用途（consumed 落盘跨重启，重放拒绝）。

数据面：裁定 store＝agora_monitor 名下 <state_path>.agora-rulings.json
（T3 ag-791c36 形态：rulings[rid] = {task, iid, action, params, time}，
全部写者共用 monitor._store_lock）。v1 仅消费 action=="task_update" 的
裁定，params 形如 {"verify_cmd_old": <公示时 board verify_cmd 的逐字
快照——服务端未登记时为空串 ""，恒为 str 禁 None>, "verify_cmd": 新值}
——执行参数逐字节来自公示裁定（评论只携带 rid）；执行前经 board 复核
verify_cmd 仍为旧值或已同值（幂等保护：上次已落账的收口为成功且零重发）。
执行面＝AgoraClient.root_task_update（root 牌直调 plaza /task_update，
本模块唯一 root 面）。拒绝面（不执行、journal＋回评原因）：非白名单作者／
正文回读不符／未知裁定 id／已 consumed 重放／动作非 v1 可执行面／board
基线偏离。执行异常一次即转人工，不自动重试——消费即记 note id，人工重发
批准评论即重试通道（若上次已落账，幂等复核收口为成功）。
"""

import re

from agora_client import (AgoraProtocolError, AgoraRejected, AgoraUnavailable)

# 批准指令严格形态：全行「批准 ＋ 8 位小写 hex 裁定 id」（与 T3 处置卡的
# uuid4().hex[:8] 同源；大写/尾缀/前缀一律不认——禁自由文本命令）
APPROVAL_RE = re.compile(r"批准 ([0-9a-f]{8})")

_AGORA_ERRORS = (AgoraUnavailable, AgoraRejected, AgoraProtocolError)


class CommentApproval:
    """「批准 <裁定id>」评论 → 回读核实 → root task_update → 回评＋消费。"""

    def __init__(self, *, api, project, agora_client, monitor,
                 approvers=(), journal=None, post_note=None):
        self._api = api
        self._project = project
        self._agora = agora_client
        self._monitor = monitor
        self._approvers = frozenset(approvers)
        self._journal = journal if journal is not None else (
            lambda message: None)
        self._post_note = post_note if post_note is not None else (
            lambda iid, text: self._api.post_note(
                self._project, iid, self._api.build_note_body(text)))

    def handle(self, iid, comment_id) -> None:
        """处理一条已被 fsm 分类为 approve 的评论。全链路零 LLM、零重试。"""
        if not isinstance(comment_id, int):
            self._journal(f"approval: issue#{iid} comment id missing, ignored")
            return
        try:
            comment = self._api.get_issue_comment(self._project, comment_id)
        except Exception as exc:  # noqa: BLE001 评审环 B 册：UnicodeDecodeError/
            # IncompleteRead 等传输形态不在 ApiAuthError/ApiTransientError
            # 映射面（api._request_json 不映射）——读回面全闭：零执行、
            # journal 转人工（评论已消费，人工重发批准评论即重试通道）
            self._journal(f"approval: issue#{iid} comment {comment_id} "
                          f"read-back failed, not executed: {exc!r}")
            return
        body = comment.get("body") or ""
        login = ((comment.get("user") or {}).get("login")) or ""
        if login not in self._approvers:
            self._reject(iid, f"账号 {login or '未知'} 不在批准白名单，未执行")
            return
        m = APPROVAL_RE.fullmatch(body.strip())
        if m is None:
            self._reject(iid, "回读正文与「批准 <裁定id>」严格格式不符"
                          "（疑似篡改），未执行")
            return
        rid = m.group(1)
        ruling = self._monitor.ruling_get(rid)
        if ruling is None:
            self._reject(iid, f"未知裁定 id {rid}（store 无此记录），未执行")
            return
        if ruling.get("iid") != iid:
            # 公示绑定：裁定只可在其处置卡公示的 issue 面板上批准
            self._reject(iid, f"裁定 {rid} 公示于 issue#{ruling.get('iid')}"
                          "，非本 issue 面板，未执行")
            return
        if ruling.get("consumed"):
            self._reject(iid, f"裁定 {rid} 已消费，重放拒绝（单用途）")
            return
        params = ruling.get("params") if isinstance(
            ruling.get("params"), dict) else {}
        tid = ruling.get("task")
        new_cmd = params.get("verify_cmd")
        if ruling.get("action") != "task_update" \
                or not isinstance(tid, str) or not tid \
                or not isinstance(new_cmd, str) or not new_cmd.strip():
            self._reject(iid, f"裁定 {rid} 非 v1 可执行面"
                          "（仅 task_update verify_cmd），未执行")
            return
        try:
            task = self._agora.board(tid).get("task")
        except _AGORA_ERRORS as exc:
            self._fail(iid, rid, f"board 复核不可达：{exc}")
            return
        # 契约外形态按不可读处理（原 monitor verify 提案回路同款纪律，
        # ag-fb9e5a 裁定延续）：非 dict 不得让异常逃逸，非 str cmd 绝不带
        # 病执行
        verify = task.get("verify") if isinstance(task, dict) else None
        current = verify.get("cmd") if isinstance(verify, dict) else None
        if current == new_cmd:
            # 幂等保护：上次执行已落账（回包丢失形态）——零重发收口＋消费
            self._done(iid, rid, tid,
                       f"verify_cmd 已与公示裁定同值，幂等收口（零重发）")
            return
        if not isinstance(current, str) \
                or current != params.get("verify_cmd_old"):
            self._reject(iid, f"任务 {tid} 的 verify_cmd 已偏离公示基线"
                          "（board 现值既非旧值也非新值、或形态不可读），"
                          "未执行、未消费，需重新裁定")
            return
        try:
            # 参数逐字节来自公示裁定——本调用是执行器唯一 root 面
            self._agora.root_task_update(tid, new_cmd)
        except _AGORA_ERRORS as exc:
            self._fail(iid, rid, f"task_update 执行失败：{exc}")
            return
        self._done(iid, rid, tid, f"verify_cmd ← {new_cmd}"
                   "（与公示裁定逐字节一致）")

    # ---- 出口三型：拒绝（不执行）／失败（转人工）／完成（收口＋消费） ----

    def _reject(self, iid, why):
        self._journal(f"approval: issue#{iid} rejected: {why}")
        self._reply(iid, f"【批准拒绝】{why}。")

    def _fail(self, iid, rid, why):
        self._journal(f"approval: issue#{iid} ruling {rid} failed: {why}")
        self._reply(iid, f"【批准执行失败｜裁定 {rid}】{why}；已转人工处理"
                      "（不自动重试、未标记消费——重新评论"
                      f"「批准 {rid}」即人工重试通道）。")

    def _done(self, iid, rid, tid, what):
        consumed = self._monitor.ruling_mark_consumed(rid)
        suffix = "裁定标记已消费。" if consumed else \
            "警告：消费标记落盘失败（重启后该裁定可被重放）。"
        self._journal(f"approval: issue#{iid} ruling {rid} done on {tid}"
                      f" (consumed={consumed})")
        self._reply(iid, f"【批准执行｜裁定 {rid}】任务 {tid}：{what}；{suffix}")

    def _reply(self, iid, text):
        """回评尽力而为：通道异常只 journal，不反噬执行结果。"""
        try:
            self._post_note(iid, text)
        except Exception as exc:  # noqa: BLE001 (回评失败不得中断执行收口)
            self._journal(f"approval: issue#{iid} reply failed: {exc!r}")
