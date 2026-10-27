#!/bin/bash
# 双击运行：接管 DNS（需要输入开机密码，53 端口必须 root）
# 适用：路由器「上网设置 → DNS」指向本机
cd "$(dirname "$0")" || exit 1
echo "面板： http://127.0.0.1:8787"
echo "按 Ctrl+C 停止"
echo
sudo python3 m1_server.py --dns
