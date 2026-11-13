"""原始层管理业务规则：状态流转、字段校验与筛选口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "bronze"
REQUIRED_FIELDS = ["表编号", "来源通道", "目标路径"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用同步": "已启用", "停用同步": "已停用"}
NEGATIVE_ACTIONS = ["停用同步"]


class BronzeService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("表编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        rows = [self._decorate(row) for row in rows]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def _decorate(self, row: dict[str, Any]) -> dict[str, Any]:
        """把原始表自身的待同步状态与准入阻塞原因带到列表上。"""
        decorated = dict(row)
        decorated["待同步状态"] = str(row.get("待同步状态") or "—")
        decorated["待同步原因"] = str(row.get("待同步原因") or "—")
        decorated["可接收"] = row.get("status") == "已启用"
        return decorated

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(MODULE, entry_id)
        if row is None:
            return None
        decorated = self._decorate(row)
        decorated["关联接入通道"] = [
            {
                "通道编号": channel.get("通道编号"),
                "通道状态": channel.get("status"),
                "待同步状态": channel.get("待同步状态"),
            }
            for channel in store.rows("ingest")
            if str(channel.get("关联原始表") or "").strip().upper()
            == str(row.get("表编号") or "").strip().upper()
        ]
        return decorated

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"原始表 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于原始层管理可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"原始表已{action}"
