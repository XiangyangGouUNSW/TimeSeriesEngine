#!/usr/bin/env bash
# 从 proto/ 重新生成 gRPC stub 到 generated/。
#
# proto 是跨模块接口合同，改动 proto（或从 C 端/S 端同步新 proto）后运行本脚本。
# generated/ 已加入 .gitignore，各端各自生成、不提交。
#
# 用法：
#   tools/gen_proto.sh                    # 用当前环境 python
#   PYTHON=/path/to/python tools/gen_proto.sh   # 指定解释器
#
# 依赖：grpcio-tools（requirements.txt 已含）

set -euo pipefail

# 脚本所在目录的上一级 = 模块根目录
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="${PYTHON:-python}"

cd "$ROOT"

if ! command -v "$PYTHON" >/dev/null 2>&1; then
    echo "error: 找不到 python：$PYTHON（可用 PYTHON=/path/to/python 指定）" >&2
    exit 1
fi

# 确认 grpcio-tools 可用，给一个清晰的错误提示
if ! "$PYTHON" -c 'import grpc_tools' >/dev/null 2>&1; then
    echo "error: 缺少 grpc_tools，请先安装：pip install grpcio-tools" >&2
    exit 1
fi

mkdir -p generated

"$PYTHON" -m grpc_tools.protoc \
    -I proto \
    --python_out=generated --grpc_python_out=generated \
    proto/timeseries_core.proto proto/timeseries_analysis.proto

echo "OK：stub 已重新生成到 generated/"
echo "  generated/timeseries_core_pb2.py / _grpc.py"
echo "  generated/timeseries_analysis_pb2.py / _grpc.py"
