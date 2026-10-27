#!/bin/bash
# 双击运行：普通模式（只做服务器，DNS 交给路由器）
cd "$(dirname "$0")" || exit 1
echo "面板启动后打开 http://127.0.0.1:8787"
echo "按 Ctrl+C 停止"
python3 m1_server.py "$@"
