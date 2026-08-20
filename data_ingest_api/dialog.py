"""弹窗输入数据库名称的小工具（供 api.py 在写入前调用）。

用法:
    python dialog.py "提示文字"

行为:
    - 用户输入库名并确定 → 标准输出打印库名，退出码 0
    - 用户点取消 / 关闭弹窗 → 什么都不打印，退出码 1

用独立进程运行弹窗，是为了和 Flask 服务线程互不干扰（弹窗用主线程，
服务请求用工作线程，谁也卡不住谁）。
"""

import sys
import tkinter as tk
from tkinter import simpledialog

prompt = sys.argv[1] if len(sys.argv) > 1 else "请输入要写入的数据库名称："

root = tk.Tk()
root.withdraw()
root.attributes("-topmost", True)  # 弹窗置顶，避免被其他窗口挡住
name = simpledialog.askstring("选择 / 新建数据库", prompt, parent=root)
root.destroy()

if name and name.strip():
    # 统一用 UTF-8 输出，防止中文库名在 Windows 控制台编码下乱码
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    print(name.strip())
    sys.exit(0)
sys.exit(1)
