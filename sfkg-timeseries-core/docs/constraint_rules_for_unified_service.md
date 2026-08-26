# 约束规则对统一服务的说明

## 1. 一条规则表示什么

Core 接收的是具体序列上的规则，通常位于
`SyncConstraintsRequest.items[].rule`。

- `variable_mapping` 将规则变量映射到具体的 `sequence_id`。
- `terms` 是一个线性组合。
- `lower_bound` 和 `upper_bound` 是这个线性组合的下界和上界。
- 规则在当前窗口上得出一个整体的“满足/不满足”结果。

## 2. 界限

至少提供一个界：

- 只有 `lower_bound`：表示 `value >= lower_bound`。
- 只有 `upper_bound`：表示 `value <= upper_bound`。
- 两者都有：表示 `lower_bound <= value <= upper_bound`。

不要用 `Double.MAX_VALUE` 或其他魔法值代替缺失的界。
Proto3 中应通过 `has_lower_bound()` / `has_upper_bound()` 区分是否设置。

## 3. term 的类型

`ConstraintTerm.aggregation` 可以是：

- `CONSTRAINT_AGGREGATION_SAMPLE`：逐点规则，使用 `sample_offset`。
- `CONSTRAINT_AGGREGATION_AVERAGE`：当前窗口均值。
- `CONSTRAINT_AGGREGATION_MAXIMUM`：当前窗口最大值。
- `CONSTRAINT_AGGREGATION_MINIMUM`：当前窗口最小值。

同一条规则可以在线性组合中混用 `AVERAGE` 、`MAXIMUM` 和
`MINIMUM`。例如：

```text
0.5 * avg(x) + 2 * max(y) - min(z) <= 100
```

但同一条规则不能同时出现 `SAMPLE` term 和窗口统计 term。
统计 term 的 `sample_offset` 应为 `0`。

## 4. OR 和 AND

`or_group_id` 非空的规则属于同一个 OR 项；不同 OR 项之间是 AND。
`or_group_id` 为空的规则自成一个独立项。

```text
(rule_a OR rule_b) AND rule_c
```

这里的 `rule_a` 和 `rule_b` 各自是“整个当前窗口是否满足”，
不是在不同时间点之间做 OR。一个 OR 项可以同时包含逐点规则和
窗口统计规则，但每条规则自身仍不能混合两种 term 类型。

Core 不根据设备、类别或其他分组信息自动扩展规则；规则应已经对应具体序列。

## 5. 对统一服务的对接要点

1. 保存和恢复 `lower_bound` / `upper_bound` 的可选状态。
2. 原样传递 `terms[].aggregation` 和 `or_group_id`。
3. 不要用无限大默认值补齐缺失的界。
4. 在发送配置前校验：至少一个界、界有效且有序、term 类型不混合。
5. 统一服务不需要实现窗口的 `avg/max/min` 增量维护，也不需要实现持续违反报告；这些由 Core 负责。

## 6. 简单示例

```text
rule_a:
  mapping: x -> seq-temp-001
  term: 1 * avg(x)
  upper_bound: 80
  or_group_id: temperature-safe

rule_b:
  mapping: x -> seq-temp-001
  term: 1 * max(x)
  lower_bound: 10
  or_group_id: temperature-safe
```

`rule_a` 或 `rule_b` 任意一条在当前窗口满足，该 OR 项就满足。
