"""warning_rule 解析：模型分数 → 异常等级的映射规则。

proto 里 AnomalyTaskConfig.warning_rule 是 str，但格式一直没定
（全貌文档 §12 U7：由 S、总负责人决定）。本模块是 P 侧立的 v1 提案格式 +
解析器，作为和 S 联调时的格式输入（可执行代码 = 最清晰的格式规格）。

v1 提案格式（等 U7 确认）：
    "2.0:HIGH; 1.0:MEDIUM; 0.5:LOW"
    - 每条 = "阈值:等级"，分号分隔；阈值必须**严格降序**
      （匹配取第一个 score>=阈值 的等级，降序保证语义唯一）；
    - 空 / None / 全空白 = 无规则（classify 返回 None，不设等级）；
    - 任何格式错误（阈值非有穷数字、缺冒号、等级为空、非降序）
      → 抛 ValueError，配置错误要显式暴露，不静默吞掉。

消费端：当前 engine 不消费 warning_rule（等级规则由 S 还是 P 配、格式是什么
= U7 未决）。解析器先就绪，等格式拍板后接"写 S 事件时的 severity/level"。
"""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class WarningRule:
    """解析后的规则：按阈值降序的 (threshold, level) 列表。"""

    pairs: tuple[tuple[float, str], ...]

    def classify(self, score: float) -> str | None:
        """分数 → 等级：第一个 score >= threshold 的等级；都不满足返回 None。"""
        for threshold, level in self.pairs:
            if score >= threshold:
                return level
        return None


def parse(rule_str: str | None) -> WarningRule | None:
    """解析 warning_rule 字符串。空/None/全空白/全空段 → None（无规则）。"""
    if not rule_str or not rule_str.strip():
        return None
    pairs: list[tuple[float, str]] = []
    prev: float | None = None
    saw_segment = False
    for part in rule_str.split(";"):
        item = part.strip()
        if not item:
            continue
        saw_segment = True
        if ":" not in item:
            raise ValueError(f"warning_rule 段缺少 ':'：'{part}'")
        raw_threshold, raw_level = item.split(":", 1)
        threshold = float(raw_threshold.strip())       # 非数字 → ValueError 显式暴露
        if not math.isfinite(threshold):
            raise ValueError(f"warning_rule 阈值必须是有穷数字：'{raw_threshold}'")
        level = raw_level.strip()
        if not level:
            raise ValueError(f"warning_rule 段等级为空：'{part}'")
        if prev is not None and threshold >= prev:
            raise ValueError(
                f"warning_rule 阈值必须严格降序：'{threshold}' >= '{prev}'")
        pairs.append((threshold, level))
        prev = threshold
    if not saw_segment:
        return None                                 # 全是分隔符/空白（如 ";;;"）→ 无规则
    return WarningRule(pairs=tuple(pairs))
