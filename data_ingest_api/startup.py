"""一键启动入口：装依赖 → 连 gStore → 建库 → 启动 HTTP 服务。

双击「启动.bat」或运行 ``python startup.py`` 即可，全过程自动完成：

1. 检查并自动安装缺少的第三方库（flask / pydantic / requests）；
2. 连接 config.ini 里配置的 gStore 数据库（连不上会提示改哪个配置）；
3. 若配置了默认库名，检查/自动创建该库；
4. 启动 HTTP 服务，等待别人 POST 数据写入。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

# 输出统一用 UTF-8（启动.bat 里已 chcp 65001 配合显示中文），
# errors="replace" 保证即使控制台编码不支持某些符号也不会崩溃
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass


def _pip_install(packages: list[str]) -> bool:
    """用 pip 安装包，多路兜底，全部失败返回 False。

    依次尝试：
    1. 清华镜像源（国内网络快）
    2. 官方 PyPI 源
    3. 补装 pip 本身（ensurepip）后再试一次官方源
    """
    mirror = ["-i", "https://pypi.tuna.tsinghua.edu.cn/simple"]
    for extra in (mirror, []):
        proc = subprocess.run(
            [sys.executable, "-m", "pip", "install", *extra, *packages],
            check=False,
        )
        if proc.returncode == 0:
            return True
    # pip 模块本身缺失/损坏的兜底
    subprocess.run(
        [sys.executable, "-m", "ensurepip", "--upgrade"],
        capture_output=True,
        check=False,
    )
    proc = subprocess.run(
        [sys.executable, "-m", "pip", "install", *packages],
        check=False,
    )
    return proc.returncode == 0


def ensure_dependencies() -> bool:
    """检查第三方库，缺了就自动安装；装不上给出手动方案并返回 False。"""
    missing: list[str] = []
    for module in ("flask", "pydantic", "requests"):
        try:
            __import__(module)
        except ImportError:
            missing.append(module)
    if not missing:
        return True

    print(f"首次运行，正在自动安装依赖: {', '.join(missing)} ...")
    if _pip_install(missing):
        # 安装后复查，确保真的可以导入（防装了个寂寞）
        still_missing = [m for m in missing if _module_missing(m)]
        if not still_missing:
            print("依赖安装完成。\n")
            return True

    print()
    print("自动安装失败，请按下面方法手动安装：")
    print("  1. 按 Win+R，输入 cmd 回车，打开命令行")
    print(f"  2. 粘贴运行: pip install {' '.join(missing)} "
          "-i https://pypi.tuna.tsinghua.edu.cn/simple")
    print("  3. 装好后重新双击 启动.bat")
    print()
    print("如果提示找不到 pip：说明 Python 没装好，")
    print("请到 python.org 官网重新安装，安装时勾选 Add Python to PATH。")
    return False


def _module_missing(module: str) -> bool:
    try:
        __import__(module)
        return False
    except ImportError:
        return True


def main() -> int:
    print("=" * 56)
    print("  数据写入接口 — 一键启动")
    print("=" * 56)

    # 微软商店的 Python 占位程序不能用（会跳商店、跑不起来），先拦下来
    if "WindowsApps" in sys.executable:
        print("检测到 Python 是微软商店的占位版本（不能用）。")
        print("请到 python.org 官网下载安装正式版，")
        print("安装时勾选 Add Python to PATH，然后重新双击 启动.bat。")
        return 1

    if not ensure_dependencies():
        return 1

    from service import GStoreWriter
    from config import load_config

    writer = GStoreWriter()
    service_port = load_config()["service_port"]

    print(f"\n[1/3] 连接 gStore 数据库: {writer.endpoint}")
    if not writer.check():
        print("  ✗ 连不上 gStore！")
        print("    请用记事本打开本文件夹里的 config.ini，")
        print("    把 [gstore] 段里的 host / port 改成数据库所在电脑的 IP 和端口，")
        print("    保存后重新双击启动。")
        return 1
    print("  ✓ gStore 连接正常")

    db = writer.db
    if db:
        print(f"[2/3] 检查默认库: {db}")
        if not writer.ensure_database(db):
            print(f"  ✗ 默认库不可用: {writer._last_error}")
            return 1
        print("  ✓ 默认库可用（库不存在时已自动创建）")
    else:
        print("[2/3] 默认库: 未设置")
        print("      写入时若请求里没带 db_name，会弹窗询问库名：")
        print("      已有库直接写入，新名字自动建库。")

    print(f"[3/3] 启动 HTTP 服务: http://0.0.0.0:{service_port}")
    print("=" * 56)
    print("  写入接口: POST http://<本机IP>:%s/insert" % service_port)
    print("  读库接口: POST http://<本机IP>:%s/query" % service_port)
    print("  健康检查: GET  http://<本机IP>:%s/health" % service_port)
    print("=" * 56)
    print("服务已就绪，等待数据写入...（关闭本窗口即停止服务）\n")

    from api import start_service

    start_service()
    return 0


if __name__ == "__main__":
    sys.exit(main())
