"""DataIngest 核心服务层 — 把「实体 + 关系」JSON 写入 gStore 图数据库。

设计目标：低耦合。本模块不依赖 Flask 等 Web 框架，只认普通 dict/list；
HTTP 层（api.py）直接调用它，命令行、其他语言的服务也可以单独复用。

写入约定（与 gbuilder2.0 KnowledgeExtract 管线一致，读取方可直接消费）：

- 命名空间默认 ``http://gbuilder.org/knowledge/``；
- 实体 URI：``<ns>entity/<名字>``；
- 类型 / 描述：``<ns>has_type>`` / ``<ns>has_description>``（字面量）；
- 自定义属性：``<ns>prop/<属性名>``；
- 关系：``<ns>relation/<关系类型>``，并建重化节点 ``<ns>relation/<uuid>``
  记录 source / target / 描述（读取方会自动剔除这些辅助节点）。

写入方式：SPARQL INSERT DATA 分批提交（gStore JSON operation 协议）。

配置集中在同文件夹的 ``config.ini``（用记事本可改）：
- gStore 地址 / 账号 / 密码（[gstore] 段）
- 默认库名（[gstore] 段 database，留空表示写入时弹窗询问）
- 命名空间、服务端口
本模块不依赖任何外部文件夹，可整包拷贝到别的电脑独立运行。
"""

from __future__ import annotations

import os
import re
import tempfile
import uuid
from datetime import datetime
from typing import Any


# ================================================================
# 文本与 URI 工具（与 KnowledgeExtract/knowledge_graph.py 约定一致）
# ================================================================

def sanitize_uri(text: str) -> str:
    """把一段文本清洗成可安全放进 URI 的形式。"""
    safe = re.sub(r'[<>"{}|^`\\\s]', "_", str(text))
    safe = re.sub(r"_+", "_", safe)
    return safe.strip("_")


def escape_sparql_string(s: str) -> str:
    """转义 SPARQL 字符串字面量。"""
    return (
        str(s)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def entity_uri(name: str, namespace: str) -> str:
    """实体名 → gStore URI。"""
    return f"<{namespace}entity/{sanitize_uri(name)}>"


# ================================================================
# JSON → 三元组
# ================================================================

def entities_to_triples(entities: list[dict[str, Any]], namespace: str) -> list[str]:
    """把实体列表转成 NT 格式三元组。

    每个实体生成：has_type、has_description（如有）、自定义属性（如有）。
    """
    triples: list[str] = []
    for entity in entities:
        uri = entity_uri(entity["name"], namespace)
        if entity.get("type"):
            triples.append(
                f'{uri} <{namespace}has_type> '
                f'"{escape_sparql_string(entity["type"])}" .'
            )
        if entity.get("description"):
            triples.append(
                f'{uri} <{namespace}has_description> '
                f'"{escape_sparql_string(entity["description"])}" .'
            )
        for key, value in (entity.get("properties") or {}).items():
            triples.append(
                f'{uri} <{namespace}prop/{sanitize_uri(key)}> '
                f'"{escape_sparql_string(value)}" .'
            )
    return triples


def relations_to_triples(relations: list[dict[str, Any]], namespace: str) -> list[str]:
    """把关系列表转成 NT 格式三元组。

    主三元组：<起点> <ns>relation/<类型> <终点>；
    另建重化节点记录 source / target / 描述（供追溯使用）。
    """
    triples: list[str] = []
    for relation in relations:
        source_uri = entity_uri(relation["source"], namespace)
        target_uri = entity_uri(relation["target"], namespace)
        predicate = f"<{namespace}relation/{sanitize_uri(relation['type'])}>"
        triples.append(f"{source_uri} {predicate} {target_uri} .")

        rel_uri = f"<{namespace}relation/{uuid.uuid4().hex[:12]}>"
        triples.append(f"{rel_uri} <{namespace}source> {source_uri} .")
        triples.append(f"{rel_uri} <{namespace}target> {target_uri} .")
        if relation.get("description"):
            triples.append(
                f'{rel_uri} <{namespace}has_description> '
                f'"{escape_sparql_string(relation["description"])}" .'
            )
    return triples


# ================================================================
# gStore 写入客户端
# ================================================================

class GStoreWriter:
    """gStore 写入客户端（只负责写，不依赖任何 Web 框架）。

    Parameters
    ----------
    endpoint : str | None
        gStore 地址，形如 http://host:port；None 时读 config.ini。
    user / password : str | None
        认证信息；None 时读 config.ini。
    db : str | None
        默认库名；None 时读 config.ini。
    namespace : str | None
        命名空间；None 时读 config.ini。
    """

    def __init__(
        self,
        endpoint: str | None = None,
        user: str | None = None,
        password: str | None = None,
        db: str | None = None,
        namespace: str | None = None,
    ) -> None:
        from config import load_config

        cfg = load_config()
        self.endpoint = endpoint or cfg["gstore_endpoint"]
        self.user = user or cfg["gstore_user"]
        self.password = password or cfg["gstore_password"]
        self.db = db or cfg["default_database"]
        self.namespace = namespace or cfg["namespace"]
        self._last_error: str = ""

    # ----------------------------------------------------------------
    # 底层 HTTP
    # ----------------------------------------------------------------

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        """向 gStore 发一个 JSON operation 请求（自动带上账号密码）。"""
        payload.setdefault("username", self.user)
        payload.setdefault("password", self.password)
        try:
            import requests

            resp = requests.post(
                self.endpoint,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=60,
            )
            return resp.json() if resp.text else {}
        except Exception as exc:  # gStore 不可达等一切网络错误
            self._last_error = str(exc)
            return {"StatusCode": -1, "error": str(exc)}

    def check(self) -> bool:
        """gStore 服务是否可用。"""
        result = self._post({"operation": "check"})
        return result.get("StatusCode") == 0

    # ----------------------------------------------------------------
    # 建库（库不存在时自动创建，和 KnowledgeExtract 建库方式一致）
    # ----------------------------------------------------------------

    def ensure_database(self, db_name: str) -> bool:
        """确保库存在并已加载：先 load，失败则空库 build + load。"""
        result = self._post({"operation": "load", "db_name": db_name})
        if result.get("StatusCode") == 0:
            return True

        # 库不存在：用一个最小 NT 文件空库 build，再 load。
        # 注意：若 gStore 部署在容器里看不到宿主机路径，build 可能只建了
        # 空库（不影响后续 INSERT DATA 写入，数据照常入库）。
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".nt", delete=False, encoding="utf-8"
        ) as f:
            f.write(
                f"<{self.namespace}root> <{self.namespace}created> "
                f'"{datetime.now().isoformat()}" .\n'
            )
            tmp_path = f.name
        try:
            build = self._post({
                "operation": "build",
                "db_name": db_name,
                "file_path": tmp_path,
            })
            if build.get("StatusCode") != 0:
                self._last_error = (
                    f"库 {db_name} 不存在，自动创建失败: "
                    f"{build.get('StatusMsg', build)}"
                )
                return False
            self._post({"operation": "load", "db_name": db_name})
            return True
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    def list_databases(self) -> list[str]:
        """列出 gStore 里已有的数据库名（弹窗提示用）。"""
        result = self._post({"operation": "show"})
        if isinstance(result, dict):
            for key in ("databases", "list", "result"):
                if key in result and isinstance(result[key], list):
                    return [str(d) for d in result[key]]
        if isinstance(result, list):
            return [str(d) for d in result]
        return []

    # ----------------------------------------------------------------
    # 读取
    # ----------------------------------------------------------------

    def query(self, sparql: str, db_name: str | None = None) -> dict[str, Any]:
        """执行一条 SPARQL 查询（读库），返回 gStore 的原始 JSON 结果。

        Parameters
        ----------
        sparql : str
            SPARQL 查询语句，如 ``SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10``
        db_name : str | None
            库名；None 用默认库。

        Returns
        -------
        dict
            gStore 返回的原始结果（``results.bindings`` 里是查到的数据）。
        """
        db = db_name or self.db
        return self._post({
            "operation": "query",
            "db_name": db,
            "sparql": sparql,
        })

    # ----------------------------------------------------------------
    # 写入
    # ----------------------------------------------------------------

    def _insert_via_sparql(
        self, db_name: str, triples: list[str], batch_size: int = 100
    ) -> int:
        """SPARQL INSERT DATA 分批写入，返回成功写入的三元组数。"""
        total = 0
        for i in range(0, len(triples), batch_size):
            batch = triples[i:i + batch_size]
            sparql = "INSERT DATA {\n" + "\n".join(batch) + "\n}"
            result = self._post({
                "operation": "query",
                "db_name": db_name,
                "sparql": sparql,
            })
            if result.get("StatusCode") == 0:
                total += len(batch)
            else:
                self._last_error = result.get("StatusMsg") or str(result)
        return total

    def insert(
        self,
        entities: list[dict[str, Any]],
        relations: list[dict[str, Any]],
        db_name: str | None = None,
    ) -> dict[str, Any]:
        """把实体 + 关系写入 gStore。

        Parameters
        ----------
        entities : list[dict]
            实体列表，每项至少含 ``name``，可带 type / description / properties。
        relations : list[dict]
            关系列表，每项至少含 source / type / target。
        db_name : str | None
            目标库名；None 用默认库。

        Returns
        -------
        dict
            ``{"success": bool, "db_name", "entities", "relations",
            "triples", "message"|"error"}``。
        """
        db = db_name or self.db

        if not self.check():
            return {
                "success": False,
                "error": f"连不上 gStore（{self.endpoint}），请确认 gStore 已启动",
            }
        if not self.ensure_database(db):
            return {"success": False, "error": self._last_error}

        triples = entities_to_triples(entities, self.namespace) + \
            relations_to_triples(relations, self.namespace)
        if not triples:
            return {
                "success": True,
                "db_name": db,
                "entities": 0,
                "relations": 0,
                "triples": 0,
                "message": "没有可写入的数据（entities 和 relations 均为空）",
            }

        written = self._insert_via_sparql(db, triples)
        if written < len(triples):
            return {
                "success": False,
                "db_name": db,
                "triples": written,
                "error": (
                    f"部分三元组写入失败（成功 {written}/{len(triples)}）："
                    f"{self._last_error}"
                ),
            }
        return {
            "success": True,
            "db_name": db,
            "entities": len(entities),
            "relations": len(relations),
            "triples": written,
            "message": f"写入成功：{len(entities)} 个实体、{len(relations)} 条关系、"
                       f"共 {written} 条三元组已进入 gStore 库 {db}",
        }

    def __repr__(self) -> str:
        return f"GStoreWriter(endpoint={self.endpoint}, db={self.db})"


__all__ = [
    "GStoreWriter",
    "sanitize_uri",
    "escape_sparql_string",
    "entity_uri",
    "entities_to_triples",
    "relations_to_triples",
]
