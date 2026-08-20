"""DataIngest — 对外数据写入模块。

给别人一个 URL，把「实体 + 关系」JSON POST 过来，数据自动写入 gStore 图数据库。

分层（低耦合）：
- models.py — 数据长什么样（pydantic 请求模型）
- service.py — 怎么写入 gStore（核心逻辑，不依赖 Web 框架，可单独复用）
- api.py — 对外 URL（Flask 薄壳，只负责收发 HTTP 请求）
"""

from .models import EntityInput, InsertRequest, RelationInput
from .service import (
    GStoreWriter,
    entities_to_triples,
    entity_uri,
    escape_sparql_string,
    relations_to_triples,
    sanitize_uri,
)

__all__ = [
    "EntityInput",
    "InsertRequest",
    "RelationInput",
    "GStoreWriter",
    "entities_to_triples",
    "entity_uri",
    "escape_sparql_string",
    "relations_to_triples",
    "sanitize_uri",
]
