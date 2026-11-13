"""多源接入接口：维护接入通道，覆盖准入校验、启动接入、停止接入等动作。

准入条件由 `app.services.ingest_rules` 定义，路由层只负责转发与展示，不做业务判断。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services import ingest_rules
from app.services.ingest import IngestService
from app.store import store

router = APIRouter(prefix="/api/ingest", tags=["多源接入"])

service = IngestService()

LIST_FIELDS = [
    "通道编号", "源端类型", "连接参数", "目标范围", "目标路径", "关联原始表",
    "准入结论", "失败字段", "有效规则编号", "待同步状态", "接入模式", "批次大小",
    "校验规则", "异常处理", "通道负责人",
]
STATUSES = ["草稿", "待校验", "运行中", "已停止"]
ACCEPTANCE_LANES = ["待校验"]


@router.get("/downstream")
def list_downstream() -> dict[str, Any]:
    """下游待同步队列：准入失败的通道留在这里，成功放行的会移出。"""
    rows = store.rows("ingest_downstream")
    return {
        "total": len(rows),
        "items": rows,
        "说明": "通道准入失败或停止接入时进入队列；成功进入运行中后自动出队",
    }


@router.get("/rules")
def list_rules() -> dict[str, Any]:
    """返回可复核的准入规则清单：源端类型、连接参数与目标范围都在规则内。"""
    return {
        "rule_version": ingest_rules.current_rule_version(),
        "items": ingest_rules.rule_catalog(),
        "acceptable_bronze_status": ingest_rules.ACCEPTABLE_BRONZE_STATUS,
        "conflict_policy": "源端类型与连接参数冲突时按严格度更高的规则生效",
        "source_types": sorted(ingest_rules.SOURCE_TYPE_RULES),
    }


@router.post("/rules/revalidate")
def revalidate_rules(payload: EntryPayload | None = None) -> dict[str, Any]:
    """规则变化后跨模块同步：按当前规则复核全部通道并回写待同步清单。"""
    rule_version = str((payload.values if payload else {}).get("rule_version") or "").strip()
    rule_version = rule_version or ingest_rules.current_rule_version()
    result = service.revalidate_after_rule_change(rule_version=rule_version)
    return {"ok": True, "message": "准入规则复核完成，待同步清单已同步", **result}


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按通道编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、运行中、已停止"),
    source_type: str | None = Query(default=None, description="按源端类型筛出待校验通道"),
    acceptance_lane: str | None = Query(
        default=None, description="准入批次：传「待校验」只返回尚未通过准入的通道"
    ),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按通道编号、状态、源端类型与准入批次过滤多源接入列表。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    if acceptance_lane and acceptance_lane not in ACCEPTANCE_LANES:
        raise HTTPException(
            status_code=400,
            detail=f"准入批次「{acceptance_lane}」不支持，可选：{'、'.join(ACCEPTANCE_LANES)}",
        )
    items, total = service.list_entries(
        keyword=keyword,
        status=status,
        source_type=source_type,
        acceptance_lane=acceptance_lane,
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出多源接入清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "ingest", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条接入通道明细，附带准入校验结论与历史校验记录。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"接入通道 {entry_id} 不存在或已归档")
    return entry


@router.get("/{entry_id}/validations")
def list_validations(entry_id: int) -> dict[str, Any]:
    """查询接入通道的历史校验记录；失败记录保留，只标记是否已作废。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"接入通道 {entry_id} 不存在或已归档")
    return {
        "entry_id": entry_id,
        "通道编号": entry.get("通道编号"),
        "items": entry.get("校验记录", []),
    }


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条接入通道，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="接入通道已登记，待提交准入校验", entry=entry)


@router.put("/{entry_id}", response_model=ActionResult)
def update_entry(entry_id: int, payload: EntryPayload) -> ActionResult:
    """修改连接参数或目标范围；旧校验结果作废，通道回落到待校验。"""
    entry, message, missing, _ = service.update_entry(entry_id, payload.values)
    return ActionResult(ok=entry is not None, message=message, entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条接入通道执行提交校验、启动接入、停止接入。

    准入失败时返回失败字段与原因；校验不通过的通道不能被启动。
    """
    action = str(payload.values.get("action") or "").strip()
    validation_id = payload.values.get("校验记录编号")
    entry, message, result = service.run_action(
        entry_id, action, validation_id=validation_id
    )
    if entry is None:
        return ActionResult(ok=False, message=message, entry=None)
    if action in ("提交校验", "启动接入") and (result or {}).get("校验结果") != "通过":
        return ActionResult(
            ok=False,
            message=message,
            entry=entry,
            failed_fields=list((result or {}).get("失败字段") or []),
            validation=(result or {}).get("校验编号"),
        )
    return ActionResult(ok=True, message=message, entry=entry)
