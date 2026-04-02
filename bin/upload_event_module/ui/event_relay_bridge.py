# -*- coding: utf-8 -*-
import queue
import re
import socket
import subprocess
import sys
import ipaddress

from PyQt6.QtCore import QObject, QTimer, pyqtSignal

from ..config import config
from ..core.parser import extract_event_info
from ..logger import log_error, log_warning
from ..services.event_relay_server import EventRelayServer


class EventRelayBridge(QObject):
    event_received = pyqtSignal(str, str, str)  # content, status, notice_type
    status_changed = pyqtSignal(str)

    def __init__(
        self,
        parent=None,
        host: str = "0.0.0.0",
        port: int = 62345,
        max_per_tick: int = 6,
        interval_ms: int = 200,
        status_callback=None,
    ):
        super().__init__(parent)
        self._host = host
        self._port = port
        self._display_host = "127.0.0.1"
        self._running = False
        self._event_queue: queue.Queue = queue.Queue()
        self._max_per_tick = max_per_tick
        self._interval_ms = interval_ms
        self._status_callback = status_callback

        bind_host = (
            host or getattr(config, "relay_bind_host", "0.0.0.0") or "0.0.0.0"
        ).strip()
        self._host = bind_host if bind_host else "0.0.0.0"
        self._display_host = self._resolve_display_host()

        self._server = EventRelayServer(
            host=self._host,
            port=self._port,
            event_queue=self._event_queue,
            allowed_origins=list(getattr(config, "relay_allowed_origins", [])),
            enable_proxy_dingtalk=bool(
                getattr(config, "relay_enable_proxy_dingtalk", False)
            ),
        )

        self._poll_timer = QTimer(self)
        self._poll_timer.timeout.connect(self._poll_events)

        # Keep display IP/hostname in sync when network changes.
        self._display_host_timer = QTimer(self)
        self._display_host_timer.setInterval(15_000)
        self._display_host_timer.timeout.connect(self._refresh_display_host_status)

    def start(self):
        try:
            ok = self._server.start()
            if ok:
                self._running = True
                self._display_host = self._resolve_display_host()
                self._set_status(self._format_running_status(self._display_host))
                if not self._display_host_timer.isActive():
                    self._display_host_timer.start()
            else:
                self._set_status("启动失败")
        except Exception as exc:
            log_error(f"EventRelay: 启动异常: {exc}")
            self._set_status("启动失败")
        self._poll_timer.start(self._interval_ms)

    def stop(self):
        self._running = False
        if self._poll_timer.isActive():
            self._poll_timer.stop()
        if self._display_host_timer.isActive():
            self._display_host_timer.stop()
        try:
            self._server.stop()
        except Exception:
            pass
        self._set_status("已停止")

    def _set_status(self, status: str):
        try:
            self.status_changed.emit(status)
        except Exception:
            pass
        if self._status_callback:
            self._status_callback(status)

    def _format_running_status(self, display_host: str) -> str:
        return f"运行中: http://{display_host}:{self._port}/"

    def _resolve_display_host(self) -> str:
        # UI display uses IPv4 directly to avoid LAN hostname resolution issues.
        return self._resolve_ipv4_for_display()

    def _refresh_display_host_status(self):
        if not self._running:
            return
        new_host = self._resolve_display_host()
        if new_host == self._display_host:
            return
        self._display_host = new_host
        self._set_status(self._format_running_status(self._display_host))

    def _resolve_ipv4_for_display(self) -> str:
        preferred = self._resolve_preferred_ipv4_from_adapters()
        if preferred:
            return preferred
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect(("8.8.8.8", 80))
                ip = sock.getsockname()[0]
                if self._is_private_lan_ipv4(ip):
                    return ip
        except Exception:
            pass
        try:
            host = socket.gethostname()
            _, _, addr_list = socket.gethostbyname_ex(host)
            for ip in addr_list:
                if self._is_private_lan_ipv4(ip):
                    return ip
        except Exception:
            pass
        return "127.0.0.1"

    @staticmethod
    def _is_private_lan_ipv4(ip: str) -> bool:
        if not ip:
            return False
        try:
            addr = ipaddress.IPv4Address(ip)
        except Exception:
            return False
        if (
            addr.is_loopback
            or addr.is_link_local
            or addr.is_multicast
            or addr.is_reserved
            or addr.is_unspecified
        ):
            return False
        private_networks = (
            ipaddress.IPv4Network("10.0.0.0/8"),
            ipaddress.IPv4Network("172.16.0.0/12"),
            ipaddress.IPv4Network("192.168.0.0/16"),
        )
        return any(addr in network for network in private_networks)

    def _resolve_preferred_ipv4_from_adapters(self) -> str:
        if sys.platform != "win32":
            return ""
        try:
            result = subprocess.run(
                ["ipconfig"],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="ignore",
                check=False,
            )
        except Exception:
            return ""
        if result.returncode != 0:
            return ""

        adapter_entries: list[tuple[str, str]] = []
        current_adapter = ""
        adapter_pattern = re.compile(r"^\S.*:$")
        ipv4_pattern = re.compile(r"\b((?:\d{1,3}\.){3}\d{1,3})\b")

        for raw_line in result.stdout.splitlines():
            line = raw_line.rstrip()
            if not line.strip():
                continue
            if adapter_pattern.match(line):
                current_adapter = line.rstrip(":").strip()
                continue
            if (
                "IPv4" not in line
                and "IPv4 地址" not in line
                and "IPv4 Address" not in line
            ):
                continue
            match = ipv4_pattern.search(line)
            if not match:
                continue
            ip = match.group(1)
            if not current_adapter:
                current_adapter = "Unknown"
            adapter_entries.append((current_adapter, ip))

        if not adapter_entries:
            return ""

        include_keywords = [
            "wi-fi",
            "wifi",
            "wlan",
            "wireless",
            "ethernet",
            "以太网",
            "无线",
            "local area",
        ]
        exclude_keywords = [
            "virtual",
            "vmware",
            "virtualbox",
            "hyper-v",
            "vethernet",
            "wsl",
            "docker",
            "vpn",
            "tap",
            "tun",
            "loopback",
            "npcap",
            "bluetooth",
            "hamachi",
            "zerotier",
            "tailscale",
            "host-only",
            "虚拟",
            "回环",
            "隧道",
            "桥接",
        ]

        def _score(adapter_name: str, ip: str) -> int:
            if not self._is_private_lan_ipv4(ip):
                return -1
            lower_name = adapter_name.lower()
            if any(key in lower_name for key in exclude_keywords):
                return -1
            score = 10
            if any(key in lower_name for key in include_keywords):
                score += 20
            return score

        best_ip = ""
        best_score = -1
        for adapter_name, ip in adapter_entries:
            score = _score(adapter_name, ip)
            if score > best_score:
                best_score = score
                best_ip = ip

        return best_ip if best_score >= 0 else ""

    def _poll_events(self):
        processed = 0
        while processed < self._max_per_tick:
            try:
                payload = self._event_queue.get_nowait()
            except queue.Empty:
                break
            except Exception as exc:
                log_warning(f"EventRelay: 读取事件队列失败: {exc}")
                break

            content = (payload or {}).get("content")
            if not content:
                continue
            info = extract_event_info(content)
            if not info:
                continue
            self.event_received.emit(
                info.get("content") or content,
                info.get("status", ""),
                info.get("notice_type", ""),
            )
            processed += 1
