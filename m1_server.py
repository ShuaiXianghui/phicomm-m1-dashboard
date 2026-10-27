#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
斐讯 M1（悟空）空气检测仪 —— 本地服务器 + 网页面板

原理：
    原厂固件的 M1 会作为 TCP 客户端，主动连接 aircat.phicomm.com:9000，
    服务器每 5 秒下发一个心跳帧，设备回一段以 #END# 结尾的数据，中间是 JSON
    （temperature / humidity / value=PM2.5 / hcho）。

    本程序做的就是"冒充"那个云端：在局域网里开 9000 端口，接收设备连接，
    定时下发心跳、解析回包、写入 SQLite，并在网页上实时展示。

用法：
    python3 m1_server.py                 # 正常模式（等真设备连进来）
    python3 m1_server.py --simulate      # 模拟模式（假设备，用于演示和自测）
    python3 m1_server.py --web-port 8787 --tcp-port 9000

只依赖标准库，不需要 pip install。
"""

import argparse
import asyncio
import struct
import csv
import io
import json
import os
import re
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

APP_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DB = os.path.join(APP_DIR, "m1_data.db")

# 原厂协议心跳帧（取自社区逆向结果）
HEARTBEAT = (
    b"\xaa\xef\x012\xeb\x119\x8f\x0b\x00\x00\x00\x00\x00\x00\x00\x00"
    b"\xb0\xf8\x93\x11\xbe#\x007\x00\x00\x02"
    b'{"type":5,"status":1}\xff#END#'
)

METRICS = [
    ("temperature", "温度", "°C", "#ffb454"),
    ("humidity", "湿度", "%", "#4fc3f7"),
    ("pm25", "PM2.5", "µg/m³", "#7ed957"),
    ("hcho", "甲醛", "mg/m³", "#ff6b9d"),
]


# --------------------------------------------------------------------------
# 存储
# --------------------------------------------------------------------------
class Store:
    def __init__(self, path, retain_days=30):
        self.path = path
        self.retain_days = retain_days
        self.lock = threading.Lock()
        self.connected = False
        self.peer = None
        self.last_seen = None
        self.last_error = None
        self._init_db()

    def _conn(self):
        c = sqlite3.connect(self.path, timeout=10)
        c.execute("PRAGMA journal_mode=WAL")
        return c

    def _init_db(self):
        with self._conn() as c:
            c.execute(
                """CREATE TABLE IF NOT EXISTS readings(
                       ts INTEGER PRIMARY KEY,
                       temperature REAL,
                       humidity REAL,
                       pm25 REAL,
                       hcho REAL,
                       raw TEXT
                   )"""
            )
            c.execute("CREATE INDEX IF NOT EXISTS idx_ts ON readings(ts)")
            c.execute(
                """CREATE TABLE IF NOT EXISTS hourly(
                       ts INTEGER PRIMARY KEY,
                       temperature REAL,
                       humidity REAL,
                       pm25 REAL,
                       hcho REAL,
                       n INTEGER
                   )"""
            )
            # 老数据迁移：甲醛若还是 µg/m³ 的量级，折算成 mg/m³
            c.execute("UPDATE readings SET hcho = ROUND(hcho/1000.0, 4) WHERE hcho > 1")

    def add(self, values, raw=None):
        now = int(time.time())
        with self.lock, self._conn() as c:
            c.execute(
                "INSERT OR REPLACE INTO readings(ts,temperature,humidity,pm25,hcho,raw)"
                " VALUES(?,?,?,?,?,?)",
                (
                    now,
                    values.get("temperature"),
                    values.get("humidity"),
                    values.get("pm25"),
                    values.get("hcho"),
                    raw,
                ),
            )
        self.last_seen = now

    def set_link(self, connected, peer=None, error=None):
        with self.lock:
            self.connected = connected
            self.peer = peer
            if error:
                self.last_error = error

    def latest(self):
        with self._conn() as c:
            cur = c.execute(
                "SELECT ts,temperature,humidity,pm25,hcho FROM readings ORDER BY ts DESC LIMIT 1"
            )
            row = cur.fetchone()
        if not row:
            return None
        return {
            "ts": row[0],
            "temperature": row[1],
            "humidity": row[2],
            "pm25": row[3],
            "hcho": row[4],
        }

    def _table_for(self, hours):
        """超过保留期的查询自动走小时聚合表。"""
        return "hourly" if hours > self.retain_days * 24 else "readings"

    def rollup(self, retain_days=None):
        """把超过保留期的原始数据聚合成小时均值，然后删掉原始行。"""
        days = self.retain_days if retain_days is None else retain_days
        cutoff = int(time.time() - days * 86400)
        with self.lock, self._conn() as c:
            old = c.execute(
                "SELECT COUNT(*) FROM readings WHERE ts < ?", (cutoff,)
            ).fetchone()[0]
            if not old:
                return 0, 0
            c.execute(
                """INSERT OR REPLACE INTO hourly(ts,temperature,humidity,pm25,hcho,n)
                   SELECT (ts/3600)*3600, AVG(temperature), AVG(humidity),
                          AVG(pm25), AVG(hcho), COUNT(*)
                   FROM readings WHERE ts < ? GROUP BY ts/3600""",
                (cutoff,),
            )
            c.execute("DELETE FROM readings WHERE ts < ?", (cutoff,))
            agg = c.execute("SELECT COUNT(*) FROM hourly").fetchone()[0]
        # 回收磁盘空间；VACUUM 不能放在事务里，单独开连接
        try:
            v = sqlite3.connect(self.path, timeout=10, isolation_level=None)
            v.execute("VACUUM")
            v.close()
        except sqlite3.Error:
            pass
        print("[db] 归档 %d 条原始记录，聚合表共 %d 条" % (old, agg))
        return old, agg

    def db_stats(self):
        out = {"raw": 0, "hourly": 0, "bytes": 0}
        try:
            out["bytes"] = os.path.getsize(self.path)
        except OSError:
            pass
        try:
            with self._conn() as c:
                out["raw"] = c.execute("SELECT COUNT(*) FROM readings").fetchone()[0]
                out["hourly"] = c.execute("SELECT COUNT(*) FROM hourly").fetchone()[0]
        except sqlite3.Error:
            pass
        return out

    def series(self, hours=24, points=360):
        """按时间分桶取平均，返回不超过 points 个点。

        原始数据 5 秒一条，一天 1.7 万条；直接全量返回既慢又没必要，
        图上也就几百个像素宽。
        """
        since = int(time.time() - hours * 3600)
        table = self._table_for(hours)
        bucket = max(5, int(hours * 3600 / max(points, 1)))
        with self._conn() as c:
            cur = c.execute(
                "SELECT (ts/?)*? AS t, AVG(temperature), AVG(humidity),"
                " AVG(pm25), AVG(hcho) FROM %s WHERE ts >= ?"
                " GROUP BY t ORDER BY t ASC" % table,
                (bucket, bucket, since),
            )
            rows = cur.fetchall()
        return [
            {"ts": r[0], "temperature": r[1], "humidity": r[2], "pm25": r[3], "hcho": r[4]}
            for r in rows
        ]

    def stats(self, hours=24):
        since = int(time.time() - hours * 3600)
        out = {}
        with self._conn() as c:
            table = self._table_for(hours)
            for key in ("temperature", "humidity", "pm25", "hcho"):
                cur = c.execute(
                    "SELECT MIN(%s),MAX(%s),AVG(%s),COUNT(%s) FROM %s"
                    " WHERE ts >= ? AND %s IS NOT NULL" % (key, key, key, key, table, key),
                    (since,),
                )
                lo, hi, avg, n = cur.fetchone()
                out[key] = {"min": lo, "max": hi, "avg": avg, "n": n}
        return out

    def export_csv(self, hours=24 * 7):
        since = int(time.time() - hours * 3600)
        with self._conn() as c:
            cur = c.execute(
                "SELECT ts,temperature,humidity,pm25,hcho FROM readings"
                " WHERE ts >= ? ORDER BY ts ASC",
                (since,),
            )
            rows = cur.fetchall()
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["时间", "temperature_C", "humidity_%", "pm25_ugm3", "hcho_mgm3"])
        for r in rows:
            w.writerow(
                [
                    datetime.fromtimestamp(r[0]).strftime("%Y-%m-%d %H:%M:%S"),
                    r[1],
                    r[2],
                    r[3],
                    r[4],
                ]
            )
        return buf.getvalue()


# --------------------------------------------------------------------------
# 协议解析
# --------------------------------------------------------------------------
def extract_objects(data):
    """从原始字节里抠出所有完整的 JSON 对象（括号配对，容忍噪声字节）。"""
    text = data.decode("utf-8", "ignore")
    objs, depth, start = [], 0, -1
    for i, ch in enumerate(text):
        if ch == "{":
            if depth == 0:
                start = i
            depth += 1
        elif ch == "}" and depth:
            depth -= 1
            if depth == 0 and start >= 0:
                frag = text[start : i + 1]
                try:
                    objs.append((json.loads(frag), frag))
                except Exception:
                    pass
                start = -1
    return objs


def normalize(obj):
    """把设备字段折算成统一名字。原厂用 value 表示 PM2.5。"""
    mapping = (("temperature", "temperature"), ("humidity", "humidity"),
               ("value", "pm25"), ("pm25", "pm25"), ("hcho", "hcho"))
    out = {}
    for src, dst in mapping:
        if src in obj:
            try:
                out[dst] = float(obj[src])
            except (TypeError, ValueError):
                pass
    # 原厂甲醛上报单位是 µg/m³（10 / 20 / 40 这种量级），
    # 统一折算成 mg/m³ 再入库，国标 0.08 的阈值才对得上。
    if out.get("hcho") is not None and out["hcho"] > 1:
        out["hcho"] = round(out["hcho"] / 1000.0, 4)
    return out or None


# --------------------------------------------------------------------------
# 设备侧 TCP 服务
# --------------------------------------------------------------------------
class DeviceServer:
    def __init__(self, store, host, port, device_mac=""):
        self.store = store
        self.host = host
        self.port = port
        self.device_mac = device_mac.replace(":", "").replace("-", "").lower()
        self.server = None

    async def start(self):
        self.server = await asyncio.start_server(
            self.handle, self.host, self.port, limit=1 << 20
        )
        print("[tcp] 监听 %s:%d，等待 M1 连入" % (self.host, self.port))

    def heartbeat_frame(self):
        """原厂帧模板。第 17~22 字节看起来是设备 MAC，
        实测那台机器的 MAC 是 b0:f8:93:24:4e:e6，所以支持按 MAC 替换。"""
        if not self.device_mac:
            return HEARTBEAT
        mac = bytes(int(self.device_mac[i : i + 2], 16) for i in range(0, 12, 2))
        return HEARTBEAT[:17] + mac + HEARTBEAT[23:]

    def consume(self, data):
        for obj, frag in extract_objects(data):
            vals = normalize(obj)
            if vals:
                self.store.add(vals, frag)
                print("[tcp] 数据：%s" % json.dumps(vals, ensure_ascii=False))

    async def handle(self, reader, writer):
        peer = writer.get_extra_info("peername")
        ip = peer[0] if peer else "?"
        print("[tcp] 设备连入：%s" % ip)
        self.store.set_link(True, ip)
        frame = self.heartbeat_frame()
        try:
            # 有些固件会先主动上报一次，先听两秒
            try:
                first = await asyncio.wait_for(reader.read(2048), timeout=2)
                if first:
                    print("[tcp] <= %s" % first.hex()[:400])
                    self.consume(first)
            except asyncio.TimeoutError:
                pass

            while True:
                writer.write(frame)
                await writer.drain()
                print("[tcp] => %s" % frame.hex()[:120])
                try:
                    data = await asyncio.wait_for(
                        reader.readuntil(b"#END#"), timeout=8
                    )
                except asyncio.TimeoutError:
                    print("[tcp] 心跳后 8 秒无回应")
                    await asyncio.sleep(2)
                    continue
                print("[tcp] <= %s" % data.hex()[:400])
                self.consume(data)
                await asyncio.sleep(5)
        except (asyncio.IncompleteReadError, ConnectionResetError, BrokenPipeError) as exc:
            print("[tcp] 连接结束：%r" % (exc,))
        except Exception as exc:  # noqa: BLE001
            self.store.set_link(False, ip, str(exc))
            print("[tcp] 异常：%r" % (exc,))
        finally:
            print("[tcp] 设备断开：%s" % ip)
            self.store.set_link(False)
            try:
                writer.close()
            except Exception:
                pass


# --------------------------------------------------------------------------
# 模拟设备（演示用）
# --------------------------------------------------------------------------
async def simulator(host, port, stop):
    """假装自己是 M1：连上服务器，收到心跳就回一包数据。"""
    import math
    import random

    while not stop.is_set():
        try:
            reader, writer = await asyncio.open_connection(host, port)
            print("[sim] 已连接到本地服务器")
            t0 = time.time()
            while not stop.is_set():
                await reader.readuntil(b"#END#")
                t = time.time() - t0
                payload = {
                    "type": 5,
                    "temperature": round(24.5 + 1.2 * math.sin(t / 900) + random.uniform(-.2, .2), 2),
                    "humidity": round(52 + 6 * math.sin(t / 1200) + random.uniform(-1, 1), 1),
                    "value": int(28 + 18 * math.sin(t / 600) + random.uniform(-3, 3)),
                    "hcho": round(0.03 + 0.01 * math.sin(t / 1500) + random.uniform(-.002, .002), 3),
                }
                body = json.dumps(payload).encode()
                writer.write(b"\xaa\xef" + body + b"\xff#END#")
                await writer.drain()
                await asyncio.sleep(5)
        except asyncio.CancelledError:
            raise
        except Exception:
            await asyncio.sleep(3)


# --------------------------------------------------------------------------
# 迷你 DNS：把 aircat.phicomm.com 指到本机，其余照常转发
# --------------------------------------------------------------------------
class DNSServer(asyncio.DatagramProtocol):
    """把 aircat.phicomm.com 指到本机，其余域名按 mode 转发。

    mode="doh"  用 HTTPS 查询上游（推荐）。本机若跑着代理，UDP/TCP 的 53 端口
                都会被劫持成 198.18.x 假 IP，只有 443 上的 DoH 是干净的。
    mode="udp"  直接把查询转给 upstream（适用于「DHCP 下发 DNS」那种不改 WAN 的场景）。
    """

    QMAP = {1: "A", 28: "AAAA", 5: "CNAME", 2: "NS", 12: "PTR", 15: "MX", 16: "TXT", 6: "SOA", 65: "HTTPS"}

    def __init__(self, target_ip, upstream="doh", mode=None,
                 hijack=("aircat.phicomm.com",)):
        self.target_ip = target_ip
        self.upstream = upstream
        self.mode = mode or ("doh" if upstream == "doh" else "udp")
        self.hijack = tuple(h.lower().rstrip(".") for h in hijack)
        self.transport = None
        self.cache = {}

    def connection_made(self, transport):
        self.transport = transport

    # ---- 解析 -------------------------------------------------------
    @staticmethod
    def parse_qname(data):
        i, labels = 12, []
        while i < len(data) and data[i]:
            ln = data[i]
            labels.append(data[i + 1 : i + 1 + ln].decode("ascii", "ignore"))
            i += ln + 1
        if not labels:
            return None, 12
        return ".".join(labels).lower(), i + 5

    @staticmethod
    def _encode_name(n):
        out = b""
        for part in n.rstrip(".").split("."):
            if part:
                out += bytes([len(part)]) + part.encode("ascii", "ignore")
        return out + b"\x00"

    def build_answer(self, data, qend, ip):
        tid = data[:2]
        question = data[12:qend]
        header = tid + b"\x81\x80" + b"\x00\x01\x00\x01\x00\x00\x00\x00"
        answer = (
            b"\xc0\x0c" + b"\x00\x01" + b"\x00\x01"
            + (30).to_bytes(4, "big") + b"\x00\x04"
            + bytes(int(x) for x in ip.split("."))
        )
        return header + question + answer

    # ---- 上游：普通 UDP ---------------------------------------------
    async def forward(self, data, addr):
        loop = asyncio.get_event_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setblocking(False)
        try:
            sock.sendto(data, (self.upstream, 53))
            reply = await asyncio.wait_for(loop.sock_recv(sock, 4096), timeout=3)
            self.transport.sendto(reply, addr)
        except Exception:
            pass
        finally:
            try:
                sock.close()
            except Exception:
                pass

    # ---- 上游：DoH ---------------------------------------------------
    @staticmethod
    def _doh_fetch(name, qtype):
        import urllib.parse
        import urllib.request

        qt = DNSServer.QMAP.get(qtype, "A")
        url = "https://223.5.5.5/resolve?name=%s&type=%s" % (
            urllib.parse.quote(name), qt)
        req = urllib.request.Request(url, headers={"accept": "application/dns-json"})
        with urllib.request.urlopen(req, timeout=4) as r:
            return json.loads(r.read().decode("utf-8"))

    def _build_from_json(self, data, qend, payload):
        tid = data[:2]
        question = data[12:qend]
        status = int(payload.get("Status", 0))
        rcode = status if status in (0, 3) else 2
        answers, n = b"", 0
        for a in payload.get("Answer") or []:
            t = a.get("type")
            try:
                if t == 1:
                    rd = socket.inet_aton(a["data"])
                elif t == 28:
                    rd = socket.inet_pton(socket.AF_INET6, a["data"])
                elif t in (5, 2, 12):
                    rd = self._encode_name(a["data"])
                else:
                    continue
            except Exception:
                continue
            ttl = max(5, min(int(a.get("TTL", 60)), 600))
            answers += b"\xc0\x0c" + struct.pack(">HHIH", t, 1, ttl, len(rd)) + rd
            n += 1
        header = tid + struct.pack(">HHHHH", 0x8180 | (rcode & 0xF), 1, n, 0, 0)
        return header + question + answers

    @staticmethod
    def _servfail(data, qend):
        return data[:2] + struct.pack(">HHHHH", 0x8182, 1, 0, 0, 0) + data[12:qend]

    async def answer_doh(self, data, addr, name, qtype, qend):
        key = (name, qtype)
        now = time.time()
        hit = self.cache.get(key)
        if hit and hit[0] > now:
            self.transport.sendto(self._build_from_json(data, qend, hit[1]), addr)
            return
        loop = asyncio.get_event_loop()
        try:
            payload = await loop.run_in_executor(None, self._doh_fetch, name, qtype)
        except Exception:
            self.transport.sendto(self._servfail(data, qend), addr)
            return
        ttls = [int(a.get("TTL", 60)) for a in (payload.get("Answer") or [])]
        self.cache[key] = (now + max(5, min(min(ttls) if ttls else 30, 300)), payload)
        self.transport.sendto(self._build_from_json(data, qend, payload), addr)

    # ---- 入口 -------------------------------------------------------
    def datagram_received(self, data, addr):
        if len(data) < 12:
            return
        name, qend = self.parse_qname(data)
        if not name:
            return
        if any(name == h or name.endswith("." + h) for h in self.hijack):
            self.transport.sendto(self.build_answer(data, qend, self.target_ip), addr)
            return
        if self.mode == "doh":
            qtype = struct.unpack(">H", data[qend - 4 : qend - 2])[0]
            asyncio.ensure_future(self.answer_doh(data, addr, name, qtype, qend))
        else:
            asyncio.ensure_future(self.forward(data, addr))


# --------------------------------------------------------------------------
# 网页面板
# --------------------------------------------------------------------------
PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>斐讯 M1 · 空气质量</title>
<style>
  :root{
    --bg:#0f1216; --panel:#171c23; --line:#232b36; --fg:#e8eef7; --dim:#8b98a8;
    --ok:#7ed957; --warn:#ffb454; --bad:#ff5d5d;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);
       font-family:-apple-system,BlinkMacSystemFont,"PingFang SC","Helvetica Neue",sans-serif;}
  .wrap{max-width:1080px;margin:0 auto;padding:24px 20px 60px;}
  header{display:flex;align-items:baseline;gap:14px;flex-wrap:wrap;margin-bottom:18px}
  h1{font-size:19px;font-weight:600;margin:0}
  .status{font-size:12.5px;color:var(--dim);display:flex;align-items:center;gap:7px}
  .dot{width:7px;height:7px;border-radius:50%;background:var(--bad);display:inline-block}
  .dot.on{background:var(--ok);box-shadow:0 0 8px var(--ok)}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;margin-bottom:16px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px}
  .card .k{font-size:12.5px;color:var(--dim);display:flex;justify-content:space-between}
  .card .v{font-size:34px;font-weight:600;letter-spacing:-.5px;margin-top:6px;font-variant-numeric:tabular-nums}
  .card .u{font-size:13px;color:var(--dim);font-weight:400;margin-left:4px}
  .card .sub{font-size:11.5px;color:var(--dim);margin-top:6px}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px 18px}
  .tabs{display:flex;gap:8px;margin-bottom:10px;flex-wrap:wrap}
  .tab{font-size:12.5px;padding:5px 11px;border-radius:999px;border:1px solid var(--line);
       background:transparent;color:var(--dim);cursor:pointer}
  .tab.act{color:var(--fg);border-color:#3a4757;background:#1e252e}
  canvas{width:100%;height:280px;display:block}
  .foot{display:flex;justify-content:space-between;align-items:center;margin-top:12px;
        font-size:12px;color:var(--dim);gap:12px;flex-wrap:wrap}
  a.btn{color:var(--fg);text-decoration:none;border:1px solid var(--line);padding:6px 12px;
        border-radius:8px;font-size:12.5px}
  a.btn:hover{background:#1e252e}
</style></head><body><div class="wrap">
<header>
  <h1>斐讯 M1 · 空气质量</h1>
  <div class="status"><span class="dot" id="dot"></span><span id="link">连接中…</span></div>
</header>

<div class="grid" id="cards"></div>

<div class="panel">
  <div class="tabs" id="tabs"></div>
  <canvas id="chart"></canvas>
  <div class="foot">
    <span id="range">—</span>
    <span>
      <a class="btn" href="/api/export.csv">导出 CSV</a>
    </span>
  </div>
</div>

<script>
const METRICS = __METRICS__;
let cur = 0, data = [], hoverIdx = -1, view = null;
const cv = document.getElementById("chart");

function fmt(v, d){ return (v===null||v===undefined) ? "—" : Number(v).toFixed(d); }

function pm25Class(v){
  if(v==null) return "";
  if(v<=35) return "var(--ok)";
  if(v<=75) return "#c9e265";
  if(v<=115) return "var(--warn)";
  if(v<=150) return "#ff8a4c";
  return "var(--bad)";
}
function hchoClass(v){
  if(v==null) return "";
  if(v<=0.08) return "var(--ok)";
  if(v<=0.1) return "var(--warn)";
  return "var(--bad)";
}

function renderCards(latest){
  const box = document.getElementById("cards");
  box.innerHTML = METRICS.map(m=>{
    const v = latest ? latest[m.key] : null;
    let color = "";
    if(m.key==="pm25") color = pm25Class(v);
    if(m.key==="hcho") color = hchoClass(v);
    const digits = m.key==="hcho" ? 3 : (m.key==="humidity" ? 1 : 1);
    return `<div class="card">
      <div class="k"><span>${m.label}</span></div>
      <div class="v" style="color:${color||"var(--fg)"}">${fmt(v,digits)}<span class="u">${m.unit}</span></div>
      <div class="sub">${m.hint||""}</div>
    </div>`;
  }).join("");
}

function draw(){
  const c = document.getElementById("chart");
  const dpr = window.devicePixelRatio || 1;
  const w = c.clientWidth, h = c.clientHeight;
  c.width = w*dpr; c.height = h*dpr;
  const g = c.getContext("2d");
  g.setTransform(dpr,0,0,dpr,0,0);
  g.clearRect(0,0,w,h);

  const m = METRICS[cur];
  const pts = data.map(d=>({t:d.ts, v:d[m.key]})).filter(p=>p.v!==null && p.v!==undefined);
  if(pts.length < 2){
    g.fillStyle="#5c6878"; g.font="13px -apple-system"; g.textAlign="center";
    g.fillText("还没有足够的数据", w/2, h/2);
    return;
  }
  const padL=44, padR=12, padT=14, padB=26;
  const xs = pts.map(p=>p.t), ys = pts.map(p=>p.v);
  const x0 = Math.min(...xs), x1 = Math.max(...xs);
  let y0 = Math.min(...ys), y1 = Math.max(...ys);
  if(y1 - y0 < (m.span || 1)) y1 = y0 + (m.span || 1);
  // 数值不超过国标 1.5 倍时，把国标线纳进来，方便看"离超标还有多远"
  const th0 = (m.lines && m.lines.length) ? m.lines[0] : null;
  if(th0 !== null && y1 < th0 && y1 > th0 * 0.5) y1 = th0 * 1.05;
  const pad = (y1-y0)*0.12; y0-=pad; y1+=pad;

  const X = t => padL + (w-padL-padR) * (x1===x0?0.5:(t-x0)/(x1-x0));
  const Y = v => padT + (h-padT-padB) * (1-(v-y0)/(y1-y0));

  g.strokeStyle="#232b36"; g.lineWidth=1;
  g.font="11px -apple-system"; g.fillStyle="#6b7787"; g.textAlign="right";
  for(let i=0;i<=4;i++){
    const v = y0 + (y1-y0)*i/4, y = Y(v);
    g.beginPath(); g.moveTo(padL,y); g.lineTo(w-padR,y); g.stroke();
    g.fillText(v.toFixed(m.digits), padL-8, y+4);
  }

  // 国标参考线
  (m.lines || []).forEach(lv => {
    if (lv < y0 || lv > y1) return;
    const ly = Y(lv);
    g.save();
    g.setLineDash([5,4]);
    g.strokeStyle = "rgba(255,107,107,.55)";
    g.lineWidth = 1;
    g.beginPath(); g.moveTo(padL, ly); g.lineTo(w-padR, ly); g.stroke();
    g.restore();
    g.fillStyle = "rgba(255,138,138,.9)";
    g.font = "10px -apple-system";
    g.textAlign = "right";
    g.fillText("国标 " + lv.toFixed(m.digits), w-padR-2, ly-4);
    g.textAlign = "center";
  });
  // 国标线在可视范围外时，右上角给个提示
  if ((m.lines||[]).length && m.lines.every(lv => lv < y0 || lv > y1)) {
    g.fillStyle = "#6b7787";
    g.font = "11px -apple-system";
    g.textAlign = "right";
    g.fillText("国标 " + m.lines[0].toFixed(m.digits), w-padR, padT + 10);
    g.textAlign = "center";
  }
  const pad2 = n => ("0"+n).slice(-2);
  const ticks = 4;
  const span = x1 - x0;
  let prevT = null;
  for(let i=0;i<=ticks;i++){
    const t = x0 + span*i/ticks;
    const d = new Date(t*1000);
    const hm = pad2(d.getHours())+":"+pad2(d.getMinutes());
    let lbl = hm;
    if(span > 6*3600){
      const changed = prevT===null || new Date(prevT*1000).getDate()!==d.getDate();
      if(changed) lbl = (d.getMonth()+1)+"/"+d.getDate()+" "+hm;
    }
    let x = X(t);
    if(i===0){ g.textAlign="left"; x = padL; }
    else if(i===ticks){ g.textAlign="right"; x = w-padR; }
    else { g.textAlign="center"; }
    g.fillText(lbl, x, h-8);
    prevT = t;
  }

  g.beginPath();
  pts.forEach((p,i)=> i? g.lineTo(X(p.t),Y(p.v)) : g.moveTo(X(p.t),Y(p.v)));
  g.strokeStyle=m.color; g.lineWidth=2; g.lineJoin="round"; g.stroke();

  const grad = g.createLinearGradient(0,padT,0,h-padB);
  grad.addColorStop(0, m.color+"55"); grad.addColorStop(1, m.color+"00");
  g.lineTo(X(pts[pts.length-1].t), h-padB); g.lineTo(X(pts[0].t), h-padB); g.closePath();
  g.fillStyle=grad; g.fill();

  view = {x0, x1, w, h, padL, padR, padT, padB, X, Y, pts, m};
  drawHover(g, w, h);
}

function roundRect(g, x, y, w, h, r){
  g.beginPath();
  g.moveTo(x+r, y);
  g.arcTo(x+w, y, x+w, y+h, r);
  g.arcTo(x+w, y+h, x, y+h, r);
  g.arcTo(x, y+h, x, y, r);
  g.arcTo(x, y, x+w, y, r);
  g.closePath();
}

function drawHover(g, w, h){
  if(hoverIdx < 0 || !view) return;
  const d = data[hoverIdx];
  if(!d) return;
  const v = d[view.m.key];
  if(v === null || v === undefined) return;
  const x = view.X(d.ts), y = view.Y(v);

  g.save();
  g.setLineDash([3,3]);
  g.strokeStyle = "#3a4757"; g.lineWidth = 1;
  g.beginPath(); g.moveTo(x, view.padT); g.lineTo(x, h - view.padB); g.stroke();
  g.restore();

  g.beginPath(); g.arc(x, y, 4, 0, Math.PI*2);
  g.fillStyle = view.m.color; g.fill();
  g.strokeStyle = "#0f1216"; g.lineWidth = 2; g.stroke();

  const dt = new Date(d.ts*1000);
  const p2 = n => ("0"+n).slice(-2);
  const fmt = (val, n) => (val===null||val===undefined) ? "—" : Number(val).toFixed(n);
  const lines = [
    `${p2(dt.getMonth()+1)}/${p2(dt.getDate())} ${p2(dt.getHours())}:${p2(dt.getMinutes())}`,
    `温度   ${fmt(d.temperature,1)} °C`,
    `湿度   ${fmt(d.humidity,1)} %`,
    `PM2.5  ${fmt(d.pm25,0)} µg/m³`,
    `甲醛   ${fmt(d.hcho,3)} mg/m³`,
  ];
  g.font = "12px -apple-system, PingFang SC, sans-serif";
  const bw = Math.max(...lines.map(t => g.measureText(t).width)) + 20;
  const bh = lines.length * 17 + 12;
  let bx = x + 14;
  if(bx + bw > w - 6) bx = x - bw - 14;
  let by = Math.max(6, Math.min(y - bh/2, h - bh - 26));  // 别盖住横轴时间
  g.fillStyle = "rgba(23,28,35,.97)";
  g.strokeStyle = "#2f3a48"; g.lineWidth = 1;
  roundRect(g, bx, by, bw, bh, 8); g.fill(); g.stroke();
  g.textAlign = "left";
  lines.forEach((t, i) => {
    g.fillStyle = i === 0 ? "#8b98a8" : "#e8eef7";
    g.fillText(t, bx + 10, by + 20 + i * 17);
  });
}

function nearestIdx(clientX){
  if(!view || !data.length) return -1;
  const rect = cv.getBoundingClientRect();
  const px = clientX - rect.left;
  let best = -1, bd = 1e9;
  for(let i = 0; i < data.length; i++){
    const dx = Math.abs(view.X(data[i].ts) - px);
    if(dx < bd){ bd = dx; best = i; }
  }
  return bd <= 30 ? best : -1;
}

function drawTabs(){
  document.getElementById("tabs").innerHTML = METRICS.map((m,i)=>
    `<button class="tab ${i===cur?"act":""}" onclick="sel(${i})">${m.label}</button>`
  ).join("");
}
function sel(i){ cur=i; drawTabs(); draw(); }

async function tick(){
  try{
    const r = await fetch("/api/state");
    const j = await r.json();
    data = j.series || [];
    if(hoverIdx >= data.length) hoverIdx = -1;
    renderCards(j.latest);
    const on = j.connected;
    document.getElementById("dot").className = "dot" + (on?" on":"");
    document.getElementById("link").textContent = on
      ? `设备在线 ${j.peer||""} · 最后更新 ${j.last_seen_ago ?? "—"}s 前`
      : "设备未连接";
    const n = data.length;
    const st = j.storage || {};
    const mb = st.bytes ? (st.bytes/1048576).toFixed(1) : "?";
    document.getElementById("range").textContent = n
      ? `近 ${j.hours} 小时 · ${n} 个点 · 库 ${mb} MB（原始 ${st.raw||0} / 聚合 ${st.hourly||0}）`
      : "暂无数据";
    draw();
  }catch(e){
    document.getElementById("link").textContent = "面板服务未响应";
  }
}

window.addEventListener("resize", draw);
cv.style.cursor = "crosshair";
cv.addEventListener("mousemove", e => {
  const i = nearestIdx(e.clientX);
  if(i !== hoverIdx){ hoverIdx = i; draw(); }
});
cv.addEventListener("mouseleave", () => {
  if(hoverIdx !== -1){ hoverIdx = -1; draw(); }
});
function touchAt(e){
  if(!e.touches || !e.touches[0]) return;
  const i = nearestIdx(e.touches[0].clientX);
  if(i !== hoverIdx){ hoverIdx = i; draw(); }
}
cv.addEventListener("touchstart", touchAt, {passive:true});
cv.addEventListener("touchmove", touchAt, {passive:true});
drawTabs(); tick(); setInterval(tick, 5000);
</script></div></body></html>
"""


def build_page():
    extra = {
        "temperature": {"digits": 1, "lines": [], "span": 1},
        "humidity": {"digits": 0, "lines": [], "span": 5},
        "pm25": {"digits": 0, "lines": [35, 75], "span": 10},
        "hcho": {"digits": 3, "lines": [0.08], "span": 0.02},
    }
    metrics = [
        {"key": k, "label": label, "unit": unit, "color": color,
         "digits": extra[k]["digits"], "lines": extra[k]["lines"],
         "span": extra[k]["span"],
         "hint": {"pm25": "≤35 优 · ≤75 良 · >115 轻度污染",
                  "hcho": "国标 0.08 mg/m³（GB/T 18883-2022）"}.get(k, "")}
        for k, label, unit, color in METRICS
    ]
    return PAGE.replace("__METRICS__", json.dumps(metrics, ensure_ascii=False))


class Handler(BaseHTTPRequestHandler):
    store = None
    page = ""
    hours = 24

    def log_message(self, fmt, *args):  # 静音默认日志
        pass

    def _send(self, body, ctype="application/json; charset=utf-8", code=200, extra=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/":
            return self._send(self.page, "text/html; charset=utf-8")
        if u.path == "/api/state":
            latest = self.store.latest()
            now = int(time.time())
            payload = {
                "connected": self.store.connected,
                "peer": self.store.peer,
                "last_seen_ago": (now - self.store.last_seen) if self.store.last_seen else None,
                "hours": self.hours,
                "latest": latest,
                "series": self.store.series(self.hours),
                "stats": self.store.stats(self.hours),
                "last_error": self.store.last_error,
                "storage": self.store.db_stats(),
            }
            return self._send(json.dumps(payload, ensure_ascii=False))
        if u.path == "/api/history":
            q = parse_qs(u.query)
            hours = float(q.get("hours", [self.hours])[0])
            return self._send(
                json.dumps({"hours": hours, "series": self.store.series(hours)},
                           ensure_ascii=False)
            )
        if u.path == "/api/export.csv":
            body = self.store.export_csv(24 * 7)
            name = "m1_%s.csv" % datetime.now().strftime("%Y%m%d_%H%M")
            return self._send(
                body,
                "text/csv; charset=utf-8",
                extra={"Content-Disposition": 'attachment; filename="%s"' % name},
            )
        return self._send(json.dumps({"error": "not found"}), code=404)


# --------------------------------------------------------------------------
def lan_ip():
    """取本机真实局域网地址。

    不能直接用 UDP connect 的办法：开着代理/虚拟网卡时（比如 Clash 的
    198.18.0.0/15）会拿到一个设备根本连不上的地址。
    """
    try:
        out = subprocess.check_output(["ifconfig"], text=True, stderr=subprocess.DEVNULL)
    except Exception:
        return "127.0.0.1"
    cands = []
    for m in re.finditer(r"inet (\d+\.\d+\.\d+\.\d+)", out):
        ip = m.group(1)
        if ip.startswith(("127.", "169.254.", "198.18.", "198.19.")):
            continue
        cands.append(ip)
    for prefix in ("192.168.", "10.", "172."):
        for ip in cands:
            if ip.startswith(prefix):
                return ip
    return cands[0] if cands else "127.0.0.1"


async def maintenance(store, retain_days, stop):
    """启动时归档一次，之后每 6 小时一次。"""
    while not stop.is_set():
        try:
            store.rollup(retain_days)
        except Exception as exc:  # noqa: BLE001
            print("[db] 归档失败：%r" % (exc,))
        try:
            await asyncio.wait_for(stop.wait(), timeout=6 * 3600)
        except asyncio.TimeoutError:
            pass


async def main_async(args, store):
    loop = asyncio.get_event_loop()
    dev = DeviceServer(store, args.bind, args.tcp_port, args.device_mac)
    await dev.start()

    stop = asyncio.Event()
    if args.retain_days > 0:
        asyncio.ensure_future(maintenance(store, args.retain_days, stop))
    sim_task = None
    if args.simulate:
        sim_task = asyncio.ensure_future(simulator("127.0.0.1", args.tcp_port, stop))

    # HTTP 服务放在线程里
    Handler.store = store
    Handler.page = build_page()
    Handler.hours = args.hours
    httpd = ThreadingHTTPServer((args.bind if args.bind != "0.0.0.0" else "", args.web_port), Handler)
    t = threading.Thread(target=httpd.serve_forever, daemon=True)
    t.start()

    ip = args.advertise_ip or lan_ip()
    if args.dns:
        loop2 = asyncio.get_event_loop()
        try:
            await loop2.create_datagram_endpoint(
                lambda: DNSServer(ip, upstream=args.dns_upstream),
                local_addr=(args.bind, args.dns_port),
            )
            print("[dns] 监听 %s:%d，aircat.phicomm.com -> %s"
                  % (args.bind, args.dns_port, ip))
        except PermissionError:
            print("[dns] 端口 %d 需要 root：请用 sudo 运行，或改用 --dns-port 5353 测试"
                  % args.dns_port)
        except OSError as exc:
            print("[dns] 启动失败：%r（端口被占用？）" % (exc,))

    print("")
    print("  面板：   http://127.0.0.1:%d" % args.web_port)
    print("  局域网： http://%s:%d" % (ip, args.web_port))
    print("  数据库： %s" % store.path)
    print("  设备端： TCP %d（把 aircat.phicomm.com 指到 %s）" % (args.tcp_port, ip))
    if args.dns:
        print("  DNS：    %s:%d  上游=%s（把路由器的 DNS 改成这个地址）"
              % (ip, args.dns_port, args.dns_upstream))
    if args.simulate:
        print("  模式：   模拟设备（数据是假的，用于演示）")
    print("")

    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        stop.set()
        if sim_task:
            sim_task.cancel()
        httpd.shutdown()


def main():
    ap = argparse.ArgumentParser(description="斐讯 M1 本地服务器 + 网页面板")
    ap.add_argument("--bind", default="0.0.0.0")
    ap.add_argument("--tcp-port", type=int, default=9000)
    ap.add_argument("--web-port", type=int, default=8787)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--hours", type=int, default=24, help="面板默认展示的小时数")
    ap.add_argument("--retain-days", type=int, default=30,
                    help="原始数据保留天数，超过后按小时聚合（0=不归档）")
    ap.add_argument("--simulate", action="store_true", help="用假设备跑通链路")
    ap.add_argument("--device-mac", default="", help="设备 MAC，用于拼装原厂帧")
    ap.add_argument("--advertise-ip", default="",
                    help="对外通告的本机 IP（自动探测不准时手动指定）")
    ap.add_argument("--dns", action="store_true",
                    help="同时启动 DNS 劫持（把 aircat.phicomm.com 指到本机）")
    ap.add_argument("--dns-port", type=int, default=53, help="DNS 端口，53 需要 root")
    ap.add_argument("--dns-upstream", default="doh",
                    help="其余域名的上游：doh（推荐）或某个 DNS 的 IP")
    args = ap.parse_args()

    store = Store(args.db, args.retain_days)
    try:
        asyncio.run(main_async(args, store))
    except KeyboardInterrupt:
        print("\n已退出")


if __name__ == "__main__":
    main()
