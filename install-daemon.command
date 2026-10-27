#!/bin/bash
# 把 m1_server 装成开机自启的后台服务（LaunchDaemon）
#
# 注意：程序会被复制到 /usr/local/m1-monitor 再运行。
# 原因：macOS 的 TCC 隐私保护，root 的 LaunchDaemon 读不了 ~/Documents 里的文件
# （会报 Operation not permitted），所以必须放到不受保护的位置。
#
# 改完代码后，重新跑一次这个脚本就会覆盖过去。
set -e
SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="/usr/local/m1-monitor"
PLIST="/Library/LaunchDaemons/com.alexshuai.m1server.plist"
LABEL="com.alexshuai.m1server"
PY="/usr/bin/python3"
MAC="${1:-}"
if [ -z "$MAC" ]; then
  echo "用法： ./install-daemon.command <设备MAC>"
  echo "示例： ./install-daemon.command b0f893244ee6"
  echo
  echo "MAC 怎么找："
  echo "  arp -a | grep -i phicomm        # 主机名里带 Phicomm 的那台"
  echo "  或看路由器后台的设备列表"
  exit 1
fi

echo "源目录： $SRC"
echo "安装到： $DEST"
echo "Python： $PY"
echo "设备 MAC：$MAC"
echo

if [ ! -x "$PY" ]; then
  echo "找不到 $PY，请先装 Xcode Command Line Tools：xcode-select --install"
  exit 1
fi

echo "==> 停止旧实例（含卡在重启循环里的服务）"
sudo launchctl bootout system "$PLIST" 2>/dev/null || true
sudo pkill -f "m1_server.py" 2>/dev/null || true
sleep 1

echo "==> 复制程序到 $DEST"
sudo mkdir -p "$DEST"
sudo cp "$SRC/m1_server.py" "$DEST/m1_server.py"
if [ -f "$SRC/m1_data.db" ] && [ ! -f "$DEST/m1_data.db" ]; then
  echo "    迁移已有数据库"
  sudo cp "$SRC/m1_data.db" "$DEST/m1_data.db"
fi
sudo chown -R root:wheel "$DEST"
sudo chmod 755 "$DEST"

echo "==> 写入 LaunchDaemon"
TMP=$(mktemp)
cat > "$TMP" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PY</string>
        <string>-u</string>
        <string>$DEST/m1_server.py</string>
        <string>--dns</string>
        <string>--device-mac</string>
        <string>$MAC</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$DEST</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>/Library/Logs/m1server.log</string>
    <key>StandardErrorPath</key>
    <string>/Library/Logs/m1server.err.log</string>
</dict>
</plist>
EOF
plutil -lint "$TMP"
sudo cp "$TMP" "$PLIST"
rm -f "$TMP"
sudo chown root:wheel "$PLIST"
sudo chmod 644 "$PLIST"

echo "==> 注册并启动"
sudo launchctl bootstrap system "$PLIST"
sleep 3

echo
if pgrep -f "$DEST/m1_server.py" > /dev/null; then
  echo "✅ 已启动： http://127.0.0.1:8787"
else
  echo "⚠️  进程没起来，看日志："
  echo "    sudo tail -30 /Library/Logs/m1server.err.log"
fi
echo
echo "日志： tail -f /Library/Logs/m1server.log"
echo "停止： sudo launchctl bootout system $PLIST"
