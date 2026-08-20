"""DataIngest HTTP 接口 — 对外的数据写入 URL。

别人只要向一个网址发 POST 请求，就能把「实体 + 关系」JSON 数据写入 gStore。

接口一览
---------
POST /insert    写入一批实体和关系（核心接口）
POST /query     读库：执行一条 SPARQL 查询
POST /records   读取 DataIngest 写入的业务记录
GET  /health    健康检查（含 gStore 连通性）

启动方式
--------
双击「启动.bat」（推荐）: 自动装依赖 → 连 gStore → 建库 → 启动服务
开发:
    python api.py                     # 只启动服务，不做前置检查
生产:
    gunicorn -w 2 -b 0.0.0.0:8006 api:app

配置
----
所有配置在 config.ini（gStore 地址/账号、默认库名、服务端口），用记事本改。
本模块只依赖同文件夹内的文件，不依赖 gbuilder2.0 等任何外部文件夹，
可整包拷贝到别的电脑独立运行。

本层只做「收发请求 + 参数校验」，真正的写入和查询逻辑在 service.py（低耦合）。
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

from flask import Flask, g, jsonify, request
from pydantic import ValidationError

# import 路径兼容两种摆放方式：
# - 项目内: gbuilder2.0/Algorithm/DataIngest/（package 方式）
# - 独立版: 几个 .py 平级放在同一文件夹（同级 import）
try:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from Algorithm.DataIngest.models import InsertRequest
    from Algorithm.DataIngest.service import GStoreWriter, escape_sparql_string
    from Algorithm.DataIngest.config import load_config
except ImportError:
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from models import InsertRequest
    from service import GStoreWriter, escape_sparql_string
    from config import load_config

# ================================================================
# 日志
# ================================================================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper()),
    format="%(asctime)s [%(levelname)s] %(name)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("data_ingest")

flask_log = logging.getLogger("werkzeug")
flask_log.setLevel(logging.WARNING)

app = Flask(__name__)

# ================================================================
# gStore 写入客户端（单例）
# ================================================================
_writer = GStoreWriter()

# 弹窗脚本路径（与 api.py 同目录的 dialog.py）
_DIALOG_SCRIPT = Path(__file__).resolve().parent / "dialog.py"


def _ask_database_name(existing: list[str]) -> str:
    """弹窗让操作者输入数据库名，返回输入的名字；取消则返回空串。

    弹窗用独立进程运行（dialog.py），不占用 Flask 的工作线程。
    """
    lines = ["请输入要写入的数据库名称：", ""]
    if existing:
        lines.append("gStore 中已有的库：")
        lines.append("  " + "、".join(existing))
        lines.append("")
        lines.append("输入已有库名 → 直接写入该库；输入新名字 → 自动创建新库。")
    else:
        lines.append("gStore 里还没有任何库，输入名字后将自动创建。")
    prompt = "\n".join(lines)
    try:
        proc = subprocess.run(
            [sys.executable, str(_DIALOG_SCRIPT), prompt],
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
    except Exception as exc:  # 弹窗打不开（如无图形界面的服务器）
        logger.warning("弹窗调用失败: %s", exc)
        return ""
    if proc.returncode != 0:
        return ""
    return proc.stdout.strip()


# ================================================================
# 请求中间件（与 KnowledgeExtract/api.py 风格一致）
# ================================================================
@app.before_request
def before_request():
    """给每个请求挂上请求 ID 和开始时间。"""
    g.request_id = request.headers.get("X-Request-ID", uuid.uuid4().hex[:12])
    g.start_time = time.monotonic()


@app.after_request
def after_request(response):
    """记录每个请求的状态与耗时。"""
    elapsed_ms = (time.monotonic() - g.get("start_time", time.monotonic())) * 1000
    response.headers["X-Request-ID"] = g.get("request_id", "-")
    response.headers["X-Response-Time-ms"] = f"{elapsed_ms:.1f}"
    logger.info(
        "[%s] %s %s → %s  %.0fms",
        g.get("request_id", "-"),
        request.method,
        request.path,
        response.status_code,
        elapsed_ms,
    )
    return response


# ================================================================
# 接口实现
# ================================================================

@app.route("/insert", methods=["POST"])
def insert():
    """把实体 + 关系写入 gStore。

    请求体示例::

        {
          "db_name": "my_graph",           # 可选，留空用默认库
          "entities": [
            {"name": "主发电机", "type": "equipment",
             "description": "船舶主电源设备",
             "properties": {"额定功率": "500kW"}}
          ],
          "relations": [
            {"source": "主发电机", "type": "depends_on",
             "target": "燃油系统", "description": "需要燃油供给"}
          ]
        }

    返回: 200 写入成功；400 数据格式不对/弹窗被取消；502 gStore 不可用或写入失败。

    库名的确定规则：
    - JSON 里带了 ``db_name`` → 直接用，不弹窗（程序化调用走这条）；
    - JSON 里没带 ``db_name`` → 电脑上弹出窗口，由操作者输入库名：
      库已存在 → 直接写入；库不存在 → 自动创建后再写入。
    """
    body = request.get_json(silent=True) or {}

    # 1) 参数校验（pydantic，出错时把原因讲清楚）
    try:
        payload = InsertRequest.model_validate(body)
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(part) for part in first.get("loc", ()))
        return jsonify({
            "success": False,
            "error": f"数据格式不对（{where}）: {first.get('msg', '')}",
        }), 400

    # 2) 确定目标库：JSON 里给了 db_name 直接用；没给就弹窗问
    db_name = payload.db_name or None
    if not db_name:
        db_name = _ask_database_name(_writer.list_databases())
        if not db_name:
            return jsonify({
                "success": False,
                "error": "弹窗被取消，未输入数据库名称，本次未写入任何数据",
            }), 400
        logger.info("弹窗确认目标库: %s", db_name)

    # 3) 调用核心写入层（本层不碰 gStore 细节）
    result = _writer.insert(
        [e.model_dump() for e in payload.entities],
        [r.model_dump() for r in payload.relations],
        db_name,
    )

    # 4) 统一返回
    if result.get("success"):
        return jsonify(result), 200
    return jsonify(result), 502


@app.route("/query", methods=["POST"])
def query():
    """读库：执行一条 SPARQL 查询。

    请求体示例::

        {
          "db_name": "my_graph",      # 可选，留空用默认库
          "sparql": "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
        }

    返回 gStore 的原始查询结果（数据在 results.bindings 里）。
    """
    body = request.get_json(silent=True) or {}
    sparql = (body.get("sparql") or "").strip()
    if not sparql:
        return jsonify({
            "success": False,
            "error": "缺少 sparql 查询语句（请求体里要有 sparql 字段）",
        }), 400

    result = _writer.query(sparql, body.get("db_name") or None)
    if isinstance(result, dict) and result.get("StatusCode") not in (None, 0):
        return jsonify({
            "success": False,
            "error": f"gStore 查询失败: {result.get('StatusMsg', result)}",
        }), 502
    return jsonify(result), 200


def _binding_value(binding: dict, name: str):
    value = binding.get(name)
    if isinstance(value, dict):
        return value.get("value")
    return value


def _record_timestamp(record: object):
    if not isinstance(record, dict):
        return None
    for key in ("updateTime", "updatedAt", "update_time", "updated_at", "createTime", "createdAt"):
        value = record.get(key)
        if not value:
            continue
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
        except ValueError:
            continue
    return None


def _is_newer_record(candidate: dict, current: dict) -> bool:
    candidate_time = _record_timestamp(candidate.get("record"))
    current_time = _record_timestamp(current.get("record"))
    if candidate_time is None:
        return current_time is None
    if current_time is None:
        return True
    return candidate_time >= current_time


@app.route("/records", methods=["POST"])
def records():
    """Read service records written by ``/insert``.

    The endpoint hides the gStore-specific SPARQL binding format and returns
    the JSON stored in the ``field_recordJson`` property.  ``projectId`` is
    intentionally not reconstructed here because the database name is the
    project boundary and the Java service owns that mapping.
    """
    body = request.get_json(silent=True) or {}
    db_name = (body.get("db_name") or "").strip()
    table_name = (body.get("table_name") or "").strip()
    if not db_name or not table_name:
        return jsonify({
            "success": False,
            "error": "缺少 db_name 或 table_name",
        }), 400

    namespace = _writer.namespace.rstrip("/") + "/"
    table_literal = escape_sparql_string(table_name)
    sparql = f'''SELECT DISTINCT ?businessKey ?recordJson WHERE {{
  ?entity <{namespace}prop/field_tableName> "{table_literal}" .
  ?entity <{namespace}prop/field_businessKey> ?businessKey .
  ?entity <{namespace}prop/field_recordJson> ?recordJson .
}}'''
    result = _writer.query(sparql, db_name)
    if isinstance(result, dict) and result.get("StatusCode") not in (None, 0):
        return jsonify({
            "success": False,
            "error": f"gStore 查询失败: {result.get('StatusMsg', result)}",
        }), 502

    result_section = result.get("results", {}) if isinstance(result, dict) else {}
    bindings = result_section.get("bindings", []) if isinstance(result_section, dict) else []
    records_by_key = {}
    for binding in bindings:
        raw_record = _binding_value(binding, "recordJson")
        if not raw_record:
            continue
        try:
            record = json.loads(raw_record)
        except (TypeError, json.JSONDecodeError) as exc:
            return jsonify({
                "success": False,
                "error": f"recordJson 不是合法 JSON: {exc}",
            }), 502
        candidate = {
            "business_key": _binding_value(binding, "businessKey"),
            "record": record,
        }
        business_key = candidate["business_key"]
        if business_key is None:
            records_by_key[f"__row_{len(records_by_key)}"] = candidate
        elif business_key not in records_by_key or _is_newer_record(
                candidate, records_by_key[business_key]):
            records_by_key[business_key] = candidate

    return jsonify({
        "success": True,
        "db_name": db_name,
        "table_name": table_name,
        "records": list(records_by_key.values()),
    }), 200


@app.route("/health", methods=["GET"])
def health():
    """健康检查：服务本身 + gStore 连通性。"""
    gstore_ok = _writer.check()
    return jsonify({
        "status": "ok",
        "service": "data-ingest",
        "gstore_endpoint": _writer.endpoint,
        "gstore_connected": gstore_ok,
        "default_db": _writer.db,
    }), 200


# ================================================================
# 启动入口
# ================================================================

def start_service() -> None:
    """启动 HTTP 服务（startup.py 完成前置检查后调用；也可直接 python api.py）。"""
    port = load_config()["service_port"]
    logger.info("DataIngest 服务启动: http://0.0.0.0:%s", port)
    logger.info("   写入接口: POST http://<本机IP>:%s/insert", port)
    logger.info("   读库接口: POST http://<本机IP>:%s/query", port)
    logger.info("   业务记录读取: POST http://<本机IP>:%s/records", port)
    logger.info("   健康检查: GET  http://<本机IP>:%s/health", port)
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    start_service()
