"""读取本文件夹下 config.ini 的配置。

独立性设计：所有配置都集中在 config.ini 一个文件里（用记事本就能改），
代码只依赖标准库 configparser，不依赖任何外部文件。
"""

from __future__ import annotations

import configparser
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parent / "config.ini"

# 内置默认值（config.ini 不存在或某项没填时使用）
_DEFAULTS: dict = {
    "gstore_endpoint": "http://127.0.0.1:9999",
    "gstore_user": "root",
    "gstore_password": "123456",
    "default_database": "",
    "namespace": "http://gbuilder.org/knowledge/",
    "service_port": 8006,
}


def load_config() -> dict:
    """读取 config.ini 并合并默认值，返回配置字典。

    Returns
    -------
    dict
        键: gstore_endpoint / gstore_user / gstore_password /
        default_database / namespace / service_port
    """
    values = dict(_DEFAULTS)
    if not CONFIG_PATH.exists():
        return values

    parser = configparser.ConfigParser()
    try:
        parser.read(CONFIG_PATH, encoding="utf-8")
    except Exception:
        return values

    if parser.has_section("gstore"):
        section = parser["gstore"]
        host = section.get("host", "127.0.0.1").strip()
        port = section.get("port", "9999").strip()
        if host:
            values["gstore_endpoint"] = f"http://{host}:{port}"
        values["gstore_user"] = (
            section.get("user", values["gstore_user"]).strip()
            or values["gstore_user"]
        )
        values["gstore_password"] = section.get(
            "password", values["gstore_password"]
        )
        values["default_database"] = section.get(
            "database", values["default_database"]
        ).strip()
        values["namespace"] = (
            section.get("namespace", values["namespace"]).strip()
            or values["namespace"]
        )

    if parser.has_section("service"):
        try:
            values["service_port"] = int(
                parser["service"].get("port", values["service_port"])
            )
        except ValueError:
            pass

    return values


__all__ = ["load_config", "CONFIG_PATH"]
