"""DataIngest 数据模型 — 外部合作方提交数据时使用的请求结构。

与 service.py / api.py 解耦：这里只定义「数据长什么样」，
service.py 负责「怎么写入」，api.py 负责「怎么收发 HTTP 请求」。
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntityInput(BaseModel):
    """一个实体（知识图谱里的一个节点）。

    name 必填；type / description / properties 可选。
    """

    name: str = Field(..., description="实体名称，如：主发电机")
    type: str = Field(
        "concept",
        description="实体类型，如 equipment / system / parameter / concept",
    )
    description: str = Field("", description="实体描述（一句话说明它是什么）")
    properties: dict[str, str | int | float] = Field(
        default_factory=dict,
        description="自定义属性键值对，如 {'额定功率': '500kW', '电压等级': 400}",
    )


class RelationInput(BaseModel):
    """一条关系（知识图谱里两个实体之间的一根连线）。

    source / type / target 必填，description 可选。
    """

    source: str = Field(..., description="起点实体名，如：主发电机")
    type: str = Field(..., description="关系类型，如 has_part / depends_on / connects_to")
    target: str = Field(..., description="终点实体名，如：燃油系统")
    description: str = Field("", description="关系描述（可选）")


class InsertRequest(BaseModel):
    """POST /insert 的请求体：一批实体 + 一批关系，写入指定库。"""

    db_name: str = Field("", description="要写入的 gStore 库名；留空则用服务默认库")
    entities: list[EntityInput] = Field(default_factory=list)
    relations: list[RelationInput] = Field(default_factory=list)


__all__ = ["EntityInput", "RelationInput", "InsertRequest"]
