"""多源接入准入规则：把接入条件沉淀为可复核、可版本化的规则集。

规则覆盖源端类型、连接参数与目标范围；当源端类型与连接参数冲突时，
取严格度更高的规则作为有效规则。规则集带版本号，规则变化后通道上旧的
校验结果会因为参数指纹变化而失效，必须重新提交校验。
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any

RULE_VERSION = "2026.11.1"

# 规则集生效版本：规则变化时由复核接口推进，旧版本的校验结果不再放行。
_RUNTIME_RULE_VERSION: dict[str, str] = {"version": RULE_VERSION}


def current_rule_version() -> str:
    """当前生效的规则版本。"""
    return _RUNTIME_RULE_VERSION["version"]


def set_rule_version(version: str) -> str:
    """推进规则版本；返回实际生效的版本号。"""
    text = str(version or "").strip()
    if text:
        _RUNTIME_RULE_VERSION["version"] = text
    return _RUNTIME_RULE_VERSION["version"]

PARSE_RULE_ID = "RULE-INGE-PARSE-001"
TYPE_RULE_ID = "RULE-INGE-TYPE-001"
CONFLICT_RULE_ID = "RULE-INGE-CONFLICT-001"
SCOPE_RULE_ID = "RULE-INGE-SCOPE-001"
BRONZE_RULE_ID = "RULE-INGE-BRONZE-001"

TARGET_PATH_FIELD = "目标路径"
TARGET_SCOPE_FIELD = "目标范围"
LINKED_BRONZE_FIELD = "关联原始表"
SOURCE_TYPE_FIELD = "源端类型"
CONNECTION_FIELD = "连接参数"

# 连接参数别名：页面上填中文名或英文名都能落到同一个标准字段。
CONNECTION_ALIASES: dict[str, str] = {
    "host": "host",
    "主机": "host",
    "地址": "host",
    "主机地址": "host",
    "port": "port",
    "端口": "port",
    "database": "database",
    "db": "database",
    "库名": "database",
    "数据库": "database",
    "username": "username",
    "user": "username",
    "用户名": "username",
    "password": "password",
    "密码": "password",
    "brokers": "brokers",
    "bootstrap.servers": "brokers",
    "集群地址": "brokers",
    "topic": "topic",
    "主题": "topic",
    "group_id": "group_id",
    "消费组": "group_id",
    "path": "path",
    "路径": "path",
    "format": "format",
    "文件格式": "format",
    "endpoint": "endpoint",
    "接口地址": "endpoint",
    "url": "endpoint",
    "method": "method",
    "请求方法": "method",
    "token": "token",
    "令牌": "token",
}

PARAM_LABELS: dict[str, str] = {
    "host": "主机",
    "port": "端口",
    "database": "库名",
    "username": "用户名",
    "password": "密码",
    "brokers": "集群地址",
    "topic": "主题",
    "group_id": "消费组",
    "path": "路径",
    "format": "文件格式",
    "endpoint": "接口地址",
    "method": "请求方法",
    "token": "令牌",
}

# 每种源端类型一条可复核规则；严格度用于冲突裁决，数字越大越严格。
SOURCE_TYPE_RULES: dict[str, dict[str, Any]] = {
    "JDBC": {
        "规则编号": "RULE-INGE-JDBC-001",
        "规则名称": "JDBC 源端准入参数校验",
        "源端类型": "JDBC",
        "严格度": 4,
        "必填连接参数": ["host", "port", "database", "username"],
        "数值参数": {"port": (1, 65535)},
        "允许目标范围": ["原始层-ODS"],
    },
    "Kafka": {
        "规则编号": "RULE-INGE-KAFKA-001",
        "规则名称": "Kafka 源端准入参数校验",
        "源端类型": "Kafka",
        "严格度": 3,
        "必填连接参数": ["brokers", "topic", "group_id"],
        "数值参数": {},
        "允许目标范围": ["原始层-实时"],
    },
    "API": {
        "规则编号": "RULE-INGE-API-001",
        "规则名称": "API 源端准入参数校验",
        "源端类型": "API",
        "严格度": 3,
        "必填连接参数": ["endpoint", "method", "token"],
        "数值参数": {},
        "允许目标范围": ["原始层-API", "原始层-实时"],
    },
    "FILE": {
        "规则编号": "RULE-INGE-FILE-001",
        "规则名称": "文件源端准入参数校验",
        "源端类型": "FILE",
        "严格度": 2,
        "必填连接参数": ["path", "format"],
        "数值参数": {},
        "允许目标范围": ["原始层-ODS", "原始层-归档"],
    },
}

SOURCE_TYPE_ALIASES: dict[str, str] = {
    "JDBC": "JDBC",
    "MYSQL": "JDBC",
    "ORACLE": "JDBC",
    "KAFKA": "Kafka",
    "API": "API",
    "HTTP": "API",
    "接口": "API",
    "FILE": "FILE",
    "FTP": "FILE",
    "SFTP": "FILE",
    "文件": "FILE",
}

TARGET_SCOPE_RULE: dict[str, Any] = {
    "规则编号": SCOPE_RULE_ID,
    "规则名称": "目标范围与落湖路径校验",
    "源端类型": "全部",
    "严格度": 2,
    "必填字段": [TARGET_SCOPE_FIELD, TARGET_PATH_FIELD],
    "允许目标范围": ["原始层-ODS", "原始层-实时", "原始层-归档", "原始层-API"],
}

ACCEPTABLE_BRONZE_STATUS = "已启用"


def rule_catalog() -> list[dict[str, Any]]:
    """返回可复核的规则清单，供页面展示和值班核对。"""
    rows: list[dict[str, Any]] = []
    for rule in SOURCE_TYPE_RULES.values():
        rows.append({
            "规则编号": rule["规则编号"],
            "规则名称": rule["规则名称"],
            "源端类型": rule["源端类型"],
            "严格度": rule["严格度"],
            "必填连接参数": [
                f"{PARAM_LABELS.get(field, field)}（{field}）" for field in rule["必填连接参数"]
            ],
            "数值参数": {
                f"{PARAM_LABELS.get(key, key)}（{key}）": f"{low}-{high}"
                for key, (low, high) in rule["数值参数"].items()
            },
            "允许目标范围": list(rule["允许目标范围"]),
            "规则版本": current_rule_version(),
        })
    rows.append({
        "规则编号": TARGET_SCOPE_RULE["规则编号"],
        "规则名称": TARGET_SCOPE_RULE["规则名称"],
        "源端类型": "全部",
        "严格度": TARGET_SCOPE_RULE["严格度"],
        "必填连接参数": [],
        "数值参数": {},
        "必填字段": list(TARGET_SCOPE_RULE["必填字段"]),
        "允许目标范围": list(TARGET_SCOPE_RULE["允许目标范围"]),
        "规则版本": current_rule_version(),
    })
    rows.append({
        "规则编号": BRONZE_RULE_ID,
        "规则名称": "关联原始表可接收状态校验",
        "源端类型": "全部",
        "严格度": 4,
        "必填连接参数": [],
        "数值参数": {},
        "必填字段": [LINKED_BRONZE_FIELD],
        "允许目标范围": [],
        "规则版本": current_rule_version(),
    })
    return rows


def normalize_source_type(raw: Any) -> str | None:
    text = str(raw or "").strip()
    if not text:
        return None
    upper = text.upper()
    if upper in SOURCE_TYPE_ALIASES:
        return SOURCE_TYPE_ALIASES[upper]
    if upper in SOURCE_TYPE_RULES:
        return upper
    return None


def normalize_param_key(raw: str) -> str | None:
    text = raw.strip()
    lowered = text.lower()
    return CONNECTION_ALIASES.get(text) or CONNECTION_ALIASES.get(lowered)


def parse_connection_params(raw: Any) -> tuple[dict[str, Any], list[str]]:
    """把连接参数解析成标准字段字典。

    支持 JSON 对象和 ``key=value;key=value`` 文本两种写法；无法识别的片段
    会作为第二个返回值交给规则层判失败，避免静默忽略。
    """
    text = str(raw or "").strip()
    if not text:
        return {}, []
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        loaded = None
    if isinstance(loaded, dict):
        normalized: dict[str, Any] = {}
        unknown: list[str] = []
        for key, value in loaded.items():
            canonical = normalize_param_key(str(key))
            if canonical is None:
                unknown.append(f"{key}={value}")
                continue
            normalized[canonical] = value
        return normalized, unknown

    params: dict[str, Any] = {}
    unknown = []
    for token in re.split(r"[;,\n]", text):
        token = token.strip()
        if not token:
            continue
        if "=" in token:
            key, value = token.split("=", 1)
        elif ":" in token:
            key, value = token.split(":", 1)
        else:
            unknown.append(token)
            continue
        canonical = normalize_param_key(key)
        if canonical is None:
            unknown.append(token)
            continue
        params[canonical] = value.strip()
    return params, unknown


def _param_field(canonical: str) -> str:
    label = PARAM_LABELS.get(canonical, canonical)
    return f"{label}（{canonical}）"


def _markers_for_types(params: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for source_type, rule in SOURCE_TYPE_RULES.items():
        counts[source_type] = sum(1 for field in rule["必填连接参数"] if field in params)
    return counts


def _stricter(left_type: str, right_type: str) -> str:
    """返回更严格的源端类型：先比严格度，再比必填参数数量。"""
    left = SOURCE_TYPE_RULES[left_type]
    right = SOURCE_TYPE_RULES[right_type]
    if right["严格度"] > left["严格度"]:
        return right_type
    if right["严格度"] < left["严格度"]:
        return left_type
    if len(right["必填连接参数"]) > len(left["必填连接参数"]):
        return right_type
    return left_type


def select_effective_rule(
    declared_type: str | None,
    params: dict[str, Any],
) -> tuple[str | None, dict[str, Any] | None, list[dict[str, str]]]:
    """裁决源端类型与连接参数冲突：冲突时以更严格的规则为准。"""
    conflicts: list[dict[str, str]] = []
    if declared_type is None:
        return None, None, conflicts
    effective_type = declared_type
    declared_rule = SOURCE_TYPE_RULES[declared_type]
    candidates = _markers_for_types(params)
    for source_type, marker_count in candidates.items():
        if source_type == declared_type or marker_count == 0:
            continue
        candidate_rule = SOURCE_TYPE_RULES[source_type]
        winner = _stricter(effective_type, source_type)
        if winner == source_type:
            conflicts.append({
                "规则编号": CONFLICT_RULE_ID,
                "说明": (
                    f"连接参数含 {candidate_rule['规则名称']} 的特征字段，"
                    f"与声明的源端类型「{declared_type}」冲突；"
                    f"按更严格规则改由「{source_type}」校验（严格度 "
                    f"{candidate_rule['严格度']} > {declared_rule['严格度']}）"
                ),
            })
            effective_type = source_type
        else:
            conflicts.append({
                "规则编号": CONFLICT_RULE_ID,
                "说明": (
                    f"连接参数含 {candidate_rule['规则名称']} 的特征字段，"
                    f"与声明的源端类型「{declared_type}」冲突；"
                    f"声明规则严格度更高，仍按「{declared_type}」校验"
                ),
            })
    return effective_type, SOURCE_TYPE_RULES[effective_type], conflicts


def connection_signature(
    *,
    source_type: str | None,
    params: dict[str, Any],
    target_scope: Any,
    target_path: Any,
    rule_id: str,
    rule_version: str,
) -> str:
    """连接参数与规则版本的联合指纹；任一变化都会让旧校验结果失效。"""
    payload = {
        "源端类型": source_type,
        "连接参数": params,
        "目标范围": str(target_scope or "").strip(),
        "目标路径": str(target_path or "").strip(),
        "规则编号": rule_id,
        "规则版本": rule_version,
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


def evaluate_admission(entry: dict[str, Any], bronze: dict[str, Any] | None) -> dict[str, Any]:
    """按规则集校验一条接入通道，返回字段级的准入结论。"""
    checks: list[dict[str, Any]] = []
    failed_fields: list[str] = []
    reasons: dict[str, str] = {}

    def fail(rule_id: str, target: str, field: str, reason: str) -> None:
        checks.append({
            "规则编号": rule_id,
            "校验对象": target,
            "结论": "未通过",
            "字段": field,
            "说明": reason,
        })
        if field not in failed_fields:
            failed_fields.append(field)
        reasons.setdefault(field, reason)

    def pass_check(rule_id: str, target: str, detail: str = "校验通过") -> None:
        checks.append({
            "规则编号": rule_id,
            "校验对象": target,
            "结论": "通过",
            "字段": "",
            "说明": detail,
        })

    raw_source_type = str(entry.get(SOURCE_TYPE_FIELD) or "").strip()
    source_type = normalize_source_type(raw_source_type)
    params, unparsed = parse_connection_params(entry.get(CONNECTION_FIELD))

    # 规则一：源端类型必须纳入规则集。
    if source_type is None:
        fail(
            TYPE_RULE_ID,
            "源端类型",
            SOURCE_TYPE_FIELD,
            f"源端类型「{raw_source_type or '空'}」未纳入准入规则集，"
            f"可选类型：{'、'.join(SOURCE_TYPE_RULES)}",
        )
    else:
        pass_check(TYPE_RULE_ID, "源端类型", f"源端类型「{source_type}」已纳入规则集")

    # 规则二：连接参数必须可解析。
    if raw_source_type and not str(entry.get(CONNECTION_FIELD) or "").strip():
        fail(PARSE_RULE_ID, "连接参数", CONNECTION_FIELD, "连接参数为空，无法解析")
    elif unparsed:
        fail(
            PARSE_RULE_ID,
            "连接参数",
            CONNECTION_FIELD,
            f"连接参数存在无法识别的片段：{'、'.join(unparsed)}；请使用 key=value 或 JSON 格式",
        )
    else:
        pass_check(PARSE_RULE_ID, "连接参数", f"已解析 {len(params)} 个连接参数字段")

    # 规则三：源端类型与连接参数冲突时，按更严格的规则校验。
    effective_type, effective_rule, conflicts = select_effective_rule(source_type, params)
    for conflict in conflicts:
        checks.append({
            "规则编号": CONFLICT_RULE_ID,
            "校验对象": "源端类型与连接参数",
            "结论": "已裁决",
            "字段": SOURCE_TYPE_FIELD,
            "说明": conflict["说明"],
        })
    if effective_type is None:
        effective_rule_id = TYPE_RULE_ID
        effective_rule_name = "源端类型准入校验（未匹配规则）"
    else:
        effective_rule_id = effective_rule["规则编号"]
        effective_rule_name = effective_rule["规则名称"]
        if conflicts:
            pass_check(
                CONFLICT_RULE_ID,
                "规则裁决",
                f"有效规则：{effective_rule_id}（{effective_rule_name}）",
            )

    # 规则四：按有效规则校验必填连接参数与数值范围。
    if effective_type is not None and effective_rule is not None:
        for canonical in effective_rule["必填连接参数"]:
            value = params.get(canonical)
            if value is None or not str(value).strip():
                fail(
                    effective_rule_id,
                    "连接参数",
                    _param_field(canonical),
                    f"缺少必填连接参数 {_param_field(canonical)}",
                )
                continue
            low_high = effective_rule.get("数值参数", {}).get(canonical)
            if low_high:
                low, high = low_high
                try:
                    number = int(str(value).strip())
                except ValueError:
                    fail(
                        effective_rule_id,
                        "连接参数",
                        _param_field(canonical),
                        f"{_param_field(canonical)} 必须是 {low}-{high} 的整数",
                    )
                    continue
                if not low <= number <= high:
                    fail(
                        effective_rule_id,
                        "连接参数",
                        _param_field(canonical),
                        f"{_param_field(canonical)} 超出允许范围 {low}-{high}",
                    )
                    continue
            pass_check(
                effective_rule_id,
                "连接参数",
                f"{_param_field(canonical)} 已提供",
            )

    # 规则五：目标范围必须在允许清单内，目标路径必须落位。
    target_scope = str(entry.get(TARGET_SCOPE_FIELD) or "").strip()
    target_path = str(entry.get(TARGET_PATH_FIELD) or "").strip()
    allowed_scopes = list(TARGET_SCOPE_RULE["允许目标范围"])
    if effective_rule is not None:
        allowed_scopes = list(effective_rule["允许目标范围"])
    if not target_scope:
        fail(SCOPE_RULE_ID, "目标范围", TARGET_SCOPE_FIELD, "缺少目标范围，无法确认落湖层级")
    elif target_scope not in allowed_scopes:
        fail(
            SCOPE_RULE_ID,
            "目标范围",
            TARGET_SCOPE_FIELD,
            f"目标范围「{target_scope}」不在当前规则允许清单：{'、'.join(allowed_scopes)}",
        )
    else:
        pass_check(SCOPE_RULE_ID, "目标范围", f"目标范围「{target_scope}」在允许清单内")
    if not target_path:
        fail(SCOPE_RULE_ID, "目标路径", TARGET_PATH_FIELD, "缺少目标路径，接入后无处落湖")
    else:
        pass_check(SCOPE_RULE_ID, "目标路径", "目标路径已配置")

    # 规则六：关联原始表必须存在且处于可接收状态。
    bronze_code = str(entry.get(LINKED_BRONZE_FIELD) or "").strip()
    bronze_status = str(bronze.get("status") if bronze else "不存在").strip()
    bronze_block_reason = ""
    if not bronze_code:
        fail(
            BRONZE_RULE_ID,
            "关联原始表",
            LINKED_BRONZE_FIELD,
            "缺少关联原始表，接入通道没有可接收的落湖目标",
        )
    elif bronze is None:
        fail(
            BRONZE_RULE_ID,
            "关联原始表",
            LINKED_BRONZE_FIELD,
            f"关联原始表 {bronze_code} 不存在，无法确认可接收状态",
        )
    elif bronze_status != ACCEPTABLE_BRONZE_STATUS:
        bronze_block_reason = (
            f"关联原始表 {bronze_code} 当前状态为「{bronze_status}」，"
            f"未处于可接收状态「{ACCEPTABLE_BRONZE_STATUS}」；"
            f"待校验的原始表需先完成提交校验并启用同步"
        )
        fail(BRONZE_RULE_ID, "关联原始表", LINKED_BRONZE_FIELD, bronze_block_reason)
    else:
        pass_check(BRONZE_RULE_ID, "关联原始表", f"关联原始表 {bronze_code} 处于可接收状态")

    signature = connection_signature(
        source_type=source_type,
        params=params,
        target_scope=target_scope,
        target_path=target_path,
        rule_id=effective_rule_id,
        rule_version=current_rule_version(),
    )
    ok = not failed_fields

    return {
        "ok": ok,
        "校验结果": "通过" if ok else "未通过",
        "通道编号": str(entry.get("通道编号") or ""),
        "源端类型": source_type or raw_source_type,
        "有效规则编号": effective_rule_id,
        "有效规则名称": effective_rule_name,
        "规则版本": current_rule_version(),
        "冲突说明": [conflict["说明"] for conflict in conflicts],
        "检查项": checks,
        "失败字段": failed_fields,
        "失败原因": reasons,
        "关联原始表": bronze_code,
        "原始表状态": bronze_status,
        "原始表可接收": bronze_status == ACCEPTABLE_BRONZE_STATUS,
        "原始表阻塞原因": bronze_block_reason,
        "参数指纹": signature,
        "目标范围": target_scope,
        "目标路径": target_path,
        "连接参数": params,
    }
