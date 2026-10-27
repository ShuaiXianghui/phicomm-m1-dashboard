#!/bin/bash
# 卸载开机自启服务
PLIST="/Library/LaunchDaemons/com.alexshuai.m1server.plist"
echo "==> 停止并移除"
sudo launchctl bootout system "$PLIST" 2>/dev/null || true
sudo rm -f "$PLIST"
echo "已卸载。之后可以手动跑：python3 m1_server.py --dns --device-mac b0f893244ee6"
