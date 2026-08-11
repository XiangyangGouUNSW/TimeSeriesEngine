"""warning_rule v1 解析器专项测试（纯函数，不连服务）。

覆盖：
  1. 正常解析 + classify 分级（降序阈值取第一个命中）；
  2. 空白容忍（空格/空段跳过）；
  3. 空/None → 无规则（classify 返回 None）；
  4. 非法输入显式抛 ValueError：缺冒号 / 非数字阈值 / NaN-inf / 空等级 / 非降序 / 全空段。

用法（sfkg 环境）：
  python tools/test_warning_rule.py
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
for _p in (str(ROOT / "src"), str(ROOT / "generated")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from warning_rule import WarningRule, parse

_PASS = 0


def _ok(name: str) -> None:
    global _PASS
    _PASS += 1
    print(f"  ✓ {name}")


def test_parse_and_classify() -> None:
    print("\n[解析 + 分级]")
    rule = parse("2.0:HIGH; 1.0:MEDIUM; 0.5:LOW")
    assert rule is not None
    assert rule.pairs == ((2.0, "HIGH"), (1.0, "MEDIUM"), (0.5, "LOW"))
    assert rule.classify(2.5) == "HIGH"
    assert rule.classify(2.0) == "HIGH"        # 边界：score == 阈值 命中
    assert rule.classify(1.5) == "MEDIUM"
    assert rule.classify(0.5) == "LOW"
    assert rule.classify(0.4) is None          # 低于全部阈值 → 无等级
    _ok("正常解析 + 降序阈值取第一个命中 + 边界")

    rule2 = parse("1:HIGH")                    # 单条、整数阈值
    assert rule2 is not None and rule2.classify(1.0) == "HIGH"
    assert rule2.classify(0.99) is None
    _ok("单条规则 + 整数阈值")

    rule3 = parse(" 2 : HIGH ; 1 : MEDIUM ")   # 空白容忍
    assert rule3 is not None and rule3.pairs == ((2.0, "HIGH"), (1.0, "MEDIUM"))
    _ok("空白容忍（空格 + 空段）")


def test_empty_rule() -> None:
    print("\n[空规则 → 无等级]")
    for r in (None, "", "   ", "; ; ;", ";;;"):
        assert parse(r) is None, f"输入 {r!r} 应为无规则"
    assert WarningRule(pairs=()).classify(99.0) is None
    _ok("None/空/全空白/全空段 → 无规则，classify 返回 None")


def test_invalid_input() -> None:
    print("\n[非法输入显式抛 ValueError]")
    bad = [
        "HIGH",                    # 非空段缺冒号
        "abc:HIGH",                # 阈值非数字
        "nan:HIGH",                # 非有限数字
        "inf:HIGH",                # 非有限数字
        "2.0:",                    # 等级为空
        "1.0:LOW;2.0:HIGH",        # 非降序
        "1.0:HIGH;1.0:LOW",        # 相等阈值（非严格降序）
    ]
    for s in bad:
        try:
            parse(s)
        except ValueError:
            continue
        raise AssertionError(f"{s!r} 应抛 ValueError")
    _ok(f"8 种非法输入全部显式抛 ValueError（配置错误不静默）")


def main() -> None:
    test_parse_and_classify()
    test_empty_rule()
    test_invalid_input()
    print(f"\nwarning_rule 解析专项测试通过 ✓（{_PASS} 项断言）")


if __name__ == "__main__":
    main()
