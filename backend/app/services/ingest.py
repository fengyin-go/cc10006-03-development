"""多源接入业务规则：状态流转、准入校验与跨模块同步都收在这里。

准入门槛：接入通道只有参数校验通过且关联原始表处于可接收状态时，才允许
从「待校验」进入「运行中」。校验结果带参数指纹与规则版本，连接参数或规则
发生变化后旧结果一律失效；失败与成功都会回写原始层待同步状态、下游待同步
队列与操作审计。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services import ingest_rules
from app.store import store

MODULE = "ingest"
BRONZE_MODULE = "bronze"
DOWNSTREAM_MODULE = "ingest_downstream"
AUDIT_MODULE = "audit"
REQUIRED_FIELDS = ["通道编号", "源端类型", "连接参数"]
STATUS_ORDER = ["草稿", "待校验", "运行中", "已停止"]
ACTION_RULES = {"提交校验": "待校验", "启动接入": "运行中", "停止接入": "已停止"}
NEGATIVE_ACTIONS = ["停止接入"]
SUBMITTABLE_STATUSES = ["草稿", "待校验", "已停止"]
EDITABLE_STATUSES = ["草稿", "待校验", "已停止"]
DOWNSTREAM_STATUSES = ["待同步", "可放行", "已同步"]
AUDIT_OPERATOR = "值班管理员"

FIELD_ALIASES = {
    "源端": "源端类型",
    "类型": "源端类型",
    "source_type": "源端类型",
    "参数": "连接参数",
    "connection": "连接参数",
    "connection_params": "连接参数",
    "目标层级": "目标范围",
    "目标schema": "目标范围",
    "目标路径": "目标路径",
    "原始表": "关联原始表",
    "bronze": "关联原始表",
    "bronze_table": "关联原始表",
}


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _next_id(module: str) -> int:
    rows = store.rows(module)
    return max((int(row.get("id", 0)) for row in rows), default=0) + 1


def _norm_ref(value: Any) -> str:
    """关联键统一大小写与空白，避免大小写不一致导致关联落空。"""
    return str(value or "").strip().upper()


def _normalize_fields(values: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in values.items():
        canonical = FIELD_ALIASES.get(str(key).strip(), key)
        normalized[canonical] = value
    return normalized


def _latest_validation(entry: dict[str, Any]) -> dict[str, Any] | None:
    rows = [
        row for row in store.rows("ingest_validations")
        if _norm_ref(row.get("通道编号")) == _norm_ref(entry.get("通道编号"))
        and not row.get("已作废")
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: int(row.get("id", 0)))


def _find_bronze(entry: dict[str, Any]) -> dict[str, Any] | None:
    code = str(entry.get("关联原始表") or "").strip()
    if not code:
        return None
    for row in store.rows(BRONZE_MODULE):
        if _norm_ref(row.get("表编号")) == _norm_ref(code):
            return row
    return None


def _failed_field_summary(result: dict[str, Any]) -> str:
    reasons = result.get("失败原因") or {}
    return "；".join(f"{field}：{reason}" for field, reason in reasons.items())


def _append_audit(
    entry: dict[str, Any],
    *,
    action: str,
    outcome: str,
    detail: str,
    result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    rows = store.rows(AUDIT_MODULE)
    record = {
        "id": _next_id(AUDIT_MODULE),
        "status": "已归档" if outcome == "通过" else "待审核",
        "pending": outcome != "通过",
        "abnormal": outcome != "通过",
        "日志编号": f"AUDI-{_next_id(AUDIT_MODULE):04d}",
        "操作类型": f"多源接入-{action}",
        "操作对象": str(entry.get("通道编号") or f"接入通道{entry.get('id')}"),
        "操作详情": detail,
        "操作来源": "多源接入页面",
        "执行时间": _now(),
        "影响行数": str(len(result.get("失败字段", []))) if result else "0",
        "回滚能力": "可回滚：重新提交校验",
        "规则版本": ingest_rules.current_rule_version() if result else "",
        "校验结果": result.get("校验结果", "") if result else "",
        "失败字段": "、".join(result.get("失败字段") or []) if result else "",
    }
    rows.append(record)
    return record


def _sync_downstream(entry: dict[str, Any], *, status: str, reason: str) -> dict[str, Any]:
    """维护下游待同步队列：成功即出队，失败或待校验则留在队列里。"""
    rows = store.rows(DOWNSTREAM_MODULE)
    code = _norm_ref(entry.get("通道编号"))
    item = next((row for row in rows if _norm_ref(row.get("通道编号")) == code), None)
    if status == "已同步":
        if item is not None:
            rows.remove(item)
        return {
            "通道编号": entry.get("通道编号"),
            "关联原始表": entry.get("关联原始表"),
            "状态": "已同步",
            "说明": reason,
        }
    if item is None:
        item = {
            "id": _next_id(DOWNSTREAM_MODULE),
            "通道编号": entry.get("通道编号"),
            "关联原始表": entry.get("关联原始表"),
            "状态": status,
            "说明": reason,
            "更新时间": _now(),
        }
        rows.append(item)
        return item
    item["状态"] = status
    item["说明"] = reason
    item["关联原始表"] = entry.get("关联原始表")
    item["更新时间"] = _now()
    return item


def _sync_bronze(
    entry: dict[str, Any],
    result: dict[str, Any] | None,
    *,
    state: str,
) -> None:
    """把准入结论同步到原始层的待同步状态。

    state 取「已同步」「可放行」「待同步」：前两者不改原始表自身的待校验
    计数，只有准入失败或通道停止时才把原始表重新挂回待同步。
    """
    bronze = _find_bronze(entry)
    if bronze is None:
        return
    bronze["待同步状态"] = state
    bronze["同步更新时间"] = _now()
    if state == "已同步":
        bronze["待同步原因"] = f"接入通道 {entry.get('通道编号')} 准入通过并已进入运行中"
        bronze["pending"] = False
        bronze["abnormal"] = False
        return
    if state == "可放行":
        bronze["待同步原因"] = f"接入通道 {entry.get('通道编号')} 准入校验通过，等待启动接入"
        return
    reasons = (result or {}).get("失败原因") or {}
    bronze["待同步原因"] = "；".join(
        f"{field}：{reason}" for field, reason in reasons.items()
    ) or f"接入通道 {entry.get('通道编号')} 未进入运行中"
    bronze["pending"] = True
    bronze["abnormal"] = True


class IngestService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        source_type: str | None = None,
        acceptance_lane: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("通道编号", ""))]
        if source_type:
            wanted = ingest_rules.normalize_source_type(source_type) or source_type
            rows = [
                row for row in rows
                if (ingest_rules.normalize_source_type(row.get("源端类型"))
                    or str(row.get("源端类型") or "").strip()) == wanted
            ]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if acceptance_lane == "待校验":
            rows = [
                row for row in rows
                if row.get("status") in ("草稿", "待校验")
                and self.admission_of(row)["校验结果"] != "通过"
            ]
        rows = [self._decorate(row) for row in rows]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        return self._detail(entry)

    def admission_of(self, entry: dict[str, Any]) -> dict[str, Any]:
        bronze = _find_bronze(entry)
        return ingest_rules.evaluate_admission(entry, bronze)

    def _decorate(self, entry: dict[str, Any]) -> dict[str, Any]:
        result = self.admission_of(entry)
        latest = _latest_validation(entry)
        row = dict(entry)
        row["准入结论"] = result["校验结果"]
        row["失败字段"] = "、".join(result["失败字段"]) or "—"
        row["有效规则编号"] = result["有效规则编号"]
        row["关联原始表"] = result["关联原始表"] or "—"
        row["关联原始表状态"] = result["原始表状态"]
        row["待同步状态"] = str(entry.get("待同步状态") or "—")
        row["最近校验编号"] = latest.get("校验编号") if latest else "—"
        if entry.get("status") == "运行中" and latest and latest.get("校验结果") != "通过":
            row["准入结论"] = "运行中（校验记录已失效，需重新校验）"
        row["校验记录已失效"] = bool(
            latest and latest.get("参数指纹") != result.get("参数指纹")
        )
        return row

    def _detail(self, entry: dict[str, Any]) -> dict[str, Any]:
        row = self._decorate(entry)
        row["准入校验"] = self.admission_of(entry)
        row["校验记录"] = self.validations_of(entry)
        return row

    def validations_of(self, entry: dict[str, Any]) -> list[dict[str, Any]]:
        code = _norm_ref(entry.get("通道编号"))
        records = [
            row for row in store.rows("ingest_validations")
            if _norm_ref(row.get("通道编号")) == code
        ]
        return sorted(records, key=lambda row: int(row.get("id", 0)), reverse=True)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        normalized = _normalize_fields(values)
        missing = [
            field for field in REQUIRED_FIELDS
            if not str(normalized.get(field) or "").strip()
        ]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": _next_id(MODULE)}
        entry.update({field: normalized.get(field) for field in REQUIRED_FIELDS})
        for field in (
            "接入模式", "批次大小", "校验规则", "异常处理", "通道负责人",
            "目标范围", "目标路径", "关联原始表",
        ):
            entry[field] = normalized.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        entry["待同步状态"] = "待同步"
        entry["准入校验"] = None
        rows.append(entry)
        _sync_downstream(entry, status="待校验", reason="通道已登记，等待提交准入校验")
        _append_audit(
            entry,
            action="登记",
            outcome="待校验",
            detail=f"登记接入通道 {entry.get('通道编号')}，等待提交准入校验",
        )
        entry["准入校验"] = self.admission_of(entry)
        return entry, []

    def update_entry(
        self, entry_id: int, values: dict[str, Any]
    ) -> tuple[dict[str, Any] | None, str, list[str], dict[str, Any] | None]:
        """修改连接参数或目标范围：旧校验结果作废并回落到待校验。"""
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"接入通道 {entry_id} 不存在或已归档", [], None
        if entry.get("status") not in EDITABLE_STATUSES:
            return (
                None,
                f"接入通道当前状态为「{entry.get('status')}」，运行中或已停止的通道需先停止接入再修改参数",
                [],
                None,
            )
        normalized = _normalize_fields(values)
        missing = [
            field for field in REQUIRED_FIELDS
            if field in normalized and not str(normalized.get(field) or "").strip()
        ]
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}", missing, None
        before = ingest_rules.evaluate_admission(entry, _find_bronze(entry))
        changed = []
        for field, value in normalized.items():
            if field in ("action", "校验记录编号"):
                continue
            if str(entry.get(field) or "") != str(value or ""):
                changed.append(field)
            entry[field] = value
        entry["status"] = "待校验"
        entry["pending"] = True
        entry["待同步状态"] = "待同步"
        for record in self.validations_of(entry):
            record["已作废"] = True
            record["作废原因"] = "连接参数或规则变化后重新提交，旧校验结果不再放行"
        after = ingest_rules.evaluate_admission(entry, _find_bronze(entry))
        _sync_bronze(entry, after, state="待同步")
        _sync_downstream(
            entry,
            status="待校验",
            reason=f"连接参数变更（{'、'.join(changed) or '无字段变化'}），旧校验结果已失效",
        )
        _append_audit(
            entry,
            action="修改参数",
            outcome="待校验",
            detail=(
                f"接入通道 {entry.get('通道编号')} 变更字段：{'、'.join(changed) or '无'}；"
                f"参数指纹 {before.get('参数指纹')} → {after.get('参数指纹')}，旧校验结果作废"
            ),
        )
        entry["准入校验"] = after
        return entry, "连接参数已更新，旧校验结果已失效，请重新提交校验", [], after

    def run_action(
        self,
        entry_id: int,
        action: str,
        *,
        validation_id: Any = None,
    ) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"接入通道 {entry_id} 不存在或已归档", None
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于多源接入可执行范围", None
        if action == "提交校验":
            return self._submit(entry)
        if action == "启动接入":
            return self._start(entry, validation_id)
        return self._stop(entry)

    def _record_validation(
        self, entry: dict[str, Any], result: dict[str, Any]
    ) -> dict[str, Any]:
        """把一次准入结论落成可查询的校验记录。"""
        record = {
            "id": _next_id("ingest_validations"),
            "校验编号": f"IVAL-{_next_id('ingest_validations'):04d}",
            "通道编号": entry.get("通道编号"),
            "校验结果": result["校验结果"],
            "失败字段": list(result["失败字段"]),
            "失败原因": dict(result["失败原因"]),
            "失败原因摘要": _failed_field_summary(result),
            "检查项": result["检查项"],
            "冲突说明": result["冲突说明"],
            "参数指纹": result["参数指纹"],
            "规则版本": result["规则版本"],
            "有效规则编号": result["有效规则编号"],
            "有效规则名称": result["有效规则名称"],
            "关联原始表": result["关联原始表"],
            "原始表状态": result["原始表状态"],
            "提交人": AUDIT_OPERATOR,
            "提交时间": _now(),
            "已作废": False,
        }
        store.rows("ingest_validations").append(record)
        return record

    def _submit(
        self, entry: dict[str, Any]
    ) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if entry.get("status") not in SUBMITTABLE_STATUSES:
            reason = f"接入通道当前状态为「{entry.get('status')}」，不允许重复提交校验"
            _append_audit(entry, action="提交校验", outcome="失败", detail=reason)
            return entry, reason, {"校验结果": "未通过", "失败字段": [], "失败原因": {}}

        result = self.admission_of(entry)
        record = self._record_validation(entry, result)

        entry["status"] = "待校验"
        entry["pending"] = True
        entry["abnormal"] = not result["ok"]
        entry["准入校验"] = result
        entry["最近校验编号"] = record["校验编号"]
        entry["最近校验结果"] = result["校验结果"]

        if result["ok"]:
            entry["待同步状态"] = "可放行"
            _sync_bronze(entry, result, state="可放行")
            _sync_downstream(entry, status="可放行", reason="准入校验通过，等待启动接入")
            detail = (
                f"接入通道 {entry.get('通道编号')} 准入校验通过"
                f"（规则 {result['有效规则编号']}，参数指纹 {result['参数指纹']}）"
            )
            _append_audit(entry, action="提交校验", outcome="通过", detail=detail, result=result)
            return entry, "准入校验通过，可启动接入", result

        entry["待同步状态"] = "待同步"
        _sync_bronze(entry, result, state="待同步")
        _sync_downstream(
            entry,
            status="待同步",
            reason=f"准入校验未通过：{_failed_field_summary(result)}",
        )
        detail = (
            f"接入通道 {entry.get('通道编号')} 准入校验未通过；"
            f"失败字段：{'、'.join(result['失败字段'])}；{_failed_field_summary(result)}"
        )
        _append_audit(entry, action="提交校验", outcome="失败", detail=detail, result=result)
        return entry, f"准入校验未通过：{_failed_field_summary(result)}", result

    def _start(
        self, entry: dict[str, Any], validation_id: Any
    ) -> tuple[dict[str, Any], str, dict[str, Any]]:
        bronze = _find_bronze(entry)
        result = self.admission_of(entry)

        if entry.get("status") != "待校验":
            reason = (
                f"接入通道 {entry.get('通道编号')} 当前状态为「{entry.get('status')}」，"
                f"不允许直接启动接入；须先通过准入校验"
            )
            _append_audit(
                entry, action="启动接入", outcome="失败", detail=reason, result=result
            )
            return entry, reason, {
                "校验结果": "未通过",
                "失败字段": result.get("失败字段") or [],
                "失败原因": result.get("失败原因") or {},
            }

        latest = _latest_validation(entry)
        if latest is None or latest.get("已作废"):
            reason = "缺少有效准入校验记录，不允许启动接入；请先提交准入校验"
            _sync_bronze(entry, result, state="待同步")
            _sync_downstream(entry, status="待同步", reason=reason)
            _append_audit(entry, action="启动接入", outcome="失败", detail=reason, result=result)
            return entry, reason, {**result, "失败字段": result["失败字段"]}
        if validation_id is not None and str(validation_id) != str(latest.get("校验编号")):
            reason = (
                f"指定校验记录 {validation_id} 不是当前有效校验"
                f"（当前为 {latest.get('校验编号')}），请按最新结果启动"
            )
            _append_audit(entry, action="启动接入", outcome="失败", detail=reason, result=result)
            return entry, reason, result
        if latest.get("校验结果") != "通过":
            reason = (
                f"准入校验未通过，不允许启动接入；失败字段："
                f"{'、'.join(latest.get('失败字段') or []) or '—'}"
                f"（{latest.get('失败原因摘要') or '无具体原因'}）"
            )
            _sync_bronze(entry, latest, state="待同步")
            _sync_downstream(entry, status="待同步", reason=reason)
            _append_audit(
                entry, action="启动接入", outcome="失败", detail=reason, result=latest
            )
            return entry, reason, {
                "校验结果": "未通过",
                "失败字段": latest.get("失败字段") or [],
                "失败原因": latest.get("失败原因") or {},
                "校验编号": latest.get("校验编号"),
            }
        if latest.get("参数指纹") != result.get("参数指纹"):
            reason = (
                f"连接参数或准入规则已变更（校验指纹 {latest.get('参数指纹')}，"
                f"当前指纹 {result.get('参数指纹')}），旧校验结果不能继续放行；"
                f"请重新提交准入校验"
            )
            _sync_bronze(entry, result, state="待同步")
            _sync_downstream(entry, status="待同步", reason=reason)
            _append_audit(entry, action="启动接入", outcome="失败", detail=reason, result=result)
            return entry, reason, {**result, "校验结果": "未通过"}

        # 校验通过但原始表已不可接收：现场复核为准，拦下启动。
        if not result["ok"]:
            reason = f"准入条件不再满足：{_failed_field_summary(result)}"
            _sync_bronze(entry, result, state="待同步")
            _sync_downstream(entry, status="待同步", reason=reason)
            _append_audit(entry, action="启动接入", outcome="失败", detail=reason, result=result)
            return entry, reason, result

        entry["status"] = "运行中"
        entry["pending"] = False
        entry["abnormal"] = False
        entry["待同步状态"] = "已同步"
        entry["启动校验编号"] = latest.get("校验编号")
        entry["启动时间"] = _now()
        latest["已放行"] = True
        latest["放行时间"] = _now()
        _sync_bronze(entry, result, state="已同步")
        _sync_downstream(
            entry,
            status="已同步",
            reason=f"接入通道于 {entry['启动时间']} 进入运行中，待同步项已消除",
        )
        detail = (
            f"接入通道 {entry.get('通道编号')} 经校验 {latest.get('校验编号')} 放行进入运行中；"
            f"关联原始表 {bronze.get('表编号') if bronze else '—'} 待同步状态置为已同步"
        )
        _append_audit(entry, action="启动接入", outcome="通过", detail=detail, result=result)
        return entry, "准入校验通过且关联原始表可接收，接入通道已进入运行中", result

    def _stop(self, entry: dict[str, Any]) -> tuple[dict[str, Any], str, dict[str, Any]]:
        if entry.get("status") != "运行中":
            reason = f"接入通道当前状态为「{entry.get('status')}」，只有运行中的通道可以停止"
            _append_audit(entry, action="停止接入", outcome="失败", detail=reason)
            return entry, reason, {
                "校验结果": "未执行",
                "失败字段": [],
                "失败原因": {},
            }
        entry["status"] = "已停止"
        entry["pending"] = True
        entry["abnormal"] = True
        entry["待同步状态"] = "待同步"
        entry["停止时间"] = _now()
        result = self.admission_of(entry)
        _sync_bronze(entry, result, state="待同步")
        _sync_downstream(
            entry,
            status="待同步",
            reason=f"接入通道 {entry.get('通道编号')} 已停止，下游需重新同步",
        )
        _append_audit(
            entry,
            action="停止接入",
            outcome="通过",
            detail=f"接入通道 {entry.get('通道编号')} 停止接入，关联原始表待同步状态置为待同步",
            result={"校验结果": "已停止", "失败字段": []},
        )
        return entry, "接入通道已停止，关联原始表与下游队列回到待同步", result

    def revalidate_after_rule_change(self, *, rule_version: str) -> dict[str, Any]:
        """准入规则变化后统一复核，并把结果同步到原始层与下游待同步清单。

        - 规则版本推进后，旧版本校验记录一律标记失效，不能继续放行；
        - 待校验/草稿/已停止的通道按新规则重算，回写待同步状态；
        - 运行中的通道若仍满足新规则则保留运行，否则回落到待校验并计入 blocked。
        """
        rule_version = ingest_rules.set_rule_version(rule_version)
        affected: list[dict[str, Any]] = []
        blocked: list[str] = []
        released: list[str] = []
        for entry in store.rows(MODULE):
            for record in self.validations_of(entry):
                if not record.get("已作废") and record.get("规则版本") != rule_version:
                    record["已作废"] = True
                    record["作废原因"] = (
                        f"准入规则版本变更为 {rule_version}，旧版本校验结果不再放行"
                    )
        for entry in store.rows(MODULE):
            current = ingest_rules.evaluate_admission(entry, _find_bronze(entry))
            live_records = [
                row for row in self.validations_of(entry) if not row.get("已作废")
            ]
            stale_rule = (
                not live_records
                or any(row.get("规则版本") != rule_version for row in live_records)
            )
            if entry.get("status") == "运行中":
                if current["ok"] and not stale_rule:
                    continue
                if current["ok"]:
                    # 参数仍满足新规则：保留运行中，补记一条当前规则版本的
                    # 复核记录接续，旧版本记录已作废但仍可查询。
                    entry["准入校验"] = current
                    record = self._record_validation(entry, current)
                    record["复核来源"] = "规则变更复核"
                    record["已放行"] = True
                    record["放行时间"] = _now()
                    entry["最近校验编号"] = record["校验编号"]
                    entry["最近校验结果"] = "通过"
                    released.append(str(entry.get("通道编号")))
                    affected.append({
                        "通道编号": entry.get("通道编号"),
                        "通道状态": entry.get("status"),
                        "准入结论": current["校验结果"],
                        "失败字段": [],
                        "待同步状态": entry.get("待同步状态"),
                        "处理": "规则版本更新，运行状态保留",
                    })
                    continue
                entry["status"] = "待校验"
                entry["pending"] = True
                entry["abnormal"] = True
                blocked.append(str(entry.get("通道编号")))

            entry["准入校验"] = current
            entry["待同步状态"] = "可放行" if current["ok"] else "待同步"
            _sync_bronze(
                entry,
                current,
                state="可放行" if current["ok"] else "待同步",
            )
            _sync_downstream(
                entry,
                status="待同步",
                reason=(
                    f"准入规则版本变更为 {rule_version}，原校验结果不再放行；"
                    f"{_failed_field_summary(current) or '需重新提交校验'}"
                ),
            )
            affected.append({
                "通道编号": entry.get("通道编号"),
                "通道状态": entry.get("status"),
                "准入结论": current["校验结果"],
                "失败字段": current["失败字段"],
                "待同步状态": entry.get("待同步状态"),
                "处理": "需重新提交准入校验",
            })
        if affected or blocked:
            _append_audit(
                {"通道编号": "准入规则"},
                action="规则变更复核",
                outcome="失败" if blocked else "通过",
                detail=(
                    f"准入规则版本变更为 {rule_version}，复核 {len(affected)} 条通道；"
                    f"{'被拦下：' + '、'.join(blocked) if blocked else '无通道被拦下'}；"
                    f"{'运行保留：' + '、'.join(released) if released else '无运行中通道保留'}；"
                    f"待同步清单已同步"
                ),
            )
        return {
            "rule_version": rule_version,
            "affected": affected,
            "blocked": blocked,
            "released": released,
            "待同步清单": [
                {
                    "通道编号": row.get("通道编号"),
                    "关联原始表": row.get("关联原始表"),
                    "状态": row.get("状态"),
                    "说明": row.get("说明"),
                }
                for row in store.rows(DOWNSTREAM_MODULE)
            ],
        }
