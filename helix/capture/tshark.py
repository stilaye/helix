"""
TsharkCapture — context-managed tshark packet capture with JSON output.

Captures network traffic during test execution and writes structured JSON
that protocol-specific parsers can query field-by-field without pcap libraries.

Why -T json?
  tshark -T json outputs one JSON object per frame with every decoded field.
  This eliminates the need for scapy/dpkt/pyshark and works in CI containers
  that can't run GUI tools. Parsers use json.load() — zero extra dependencies.

Usage:
    with TsharkCapture(interface="eth0", filter_expr="tcp port 445") as cap:
        smb_client.write_file("/test.bin", data)
    # cap.output_path now has the JSON file
    frames = cap.load_frames()
    dialect = SMBParser(frames).dialect
"""

from __future__ import annotations

import json
import logging
import os
import signal
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)


class TsharkCapture:
    """
    Background tshark capture writing decoded frames as JSON.

    Args:
        interface: Network interface to capture on (e.g., "eth0").
        filter_expr: BPF capture filter (e.g., "tcp port 445 and host 10.0.0.1").
        ssh: If provided, runs tshark on the remote node via SSH.
             If None, runs locally (useful when test client IS on the network path).
        output_dir: Where to write the capture file. Defaults to a temp dir.
        extra_fields: Additional tshark field names to include (-e flag).
    """

    def __init__(
        self,
        interface: str = "eth0",
        filter_expr: str = "",
        ssh: "SSHClient | None" = None,
        output_dir: str | Path | None = None,
        extra_fields: list[str] | None = None,
    ) -> None:
        self._iface = interface
        self._filter = filter_expr
        self._ssh = ssh
        self._extra_fields = extra_fields or []
        self._proc: subprocess.Popen | None = None
        self._remote_pid: int | None = None
        self._output_path: Path | None = None
        self._output_dir = Path(output_dir) if output_dir else None

    @property
    def output_path(self) -> Path | None:
        return self._output_path

    def start(self) -> None:
        """Start tshark in the background."""
        out_dir = self._output_dir or Path(tempfile.mkdtemp(prefix="helix_cap_"))
        out_dir.mkdir(parents=True, exist_ok=True)
        ts = int(time.time())
        filename = f"capture_{self._iface}_{ts}.json"
        self._output_path = out_dir / filename

        if self._ssh:
            self._start_remote(self._output_path)
        else:
            self._start_local(self._output_path)

        # Give tshark a moment to bind the interface before traffic starts
        time.sleep(0.3)
        logger.info("TsharkCapture: started on %s (filter: %s)", self._iface, self._filter or "none")

    def stop(self) -> Path:
        """Stop capture and return path to the JSON output file."""
        if self._ssh and self._remote_pid:
            self._stop_remote()
        elif self._proc:
            self._stop_local()

        # Allow tshark to flush remaining frames
        time.sleep(0.5)
        logger.info("TsharkCapture: stopped, output at %s", self._output_path)
        return self._output_path

    def load_frames(self) -> list[dict[str, Any]]:
        """
        Parse the captured JSON file and return a list of frame dicts.
        Each frame has tshark's decoded field tree.
        Returns [] if capture file is missing or empty.
        """
        if not self._output_path or not self._output_path.exists():
            logger.warning("TsharkCapture: no output file at %s", self._output_path)
            return []

        content = self._output_path.read_text(encoding="utf-8").strip()
        if not content:
            return []

        # tshark -T json wraps all frames in an array
        # But if it was interrupted it may be missing the closing bracket
        if not content.endswith("]"):
            content = content.rstrip(",\n") + "]"

        try:
            return json.loads(content)
        except json.JSONDecodeError as e:
            logger.error("TsharkCapture: failed to parse JSON: %s", e)
            return []

    def attach_to_allure(self, name: str = "packet_capture.json") -> None:
        """Attach capture file to Allure report."""
        try:
            import allure
            if self._output_path and self._output_path.exists():
                allure.attach.file(
                    str(self._output_path),
                    name=name,
                    attachment_type=allure.attachment_type.JSON,
                )
        except ImportError:
            pass

    # ─── Context manager ─────────────────────────────────────────────────────

    def __enter__(self) -> "TsharkCapture":
        self.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _build_cmd(self, output_path: Path) -> list[str]:
        """Build the tshark command list."""
        cmd = [
            "tshark",
            "-i", self._iface,
            "-T", "json",          # Structured JSON per frame
            "-l",                  # Flush after each packet
            "-q",                  # Suppress per-packet summary to stdout
            "-w", str(output_path),
        ]

        if self._filter:
            cmd += ["-f", self._filter]

        # Standard fields for all captures
        standard_fields = [
            "frame.number", "frame.time_epoch", "frame.len",
            "ip.src", "ip.dst",
            "tcp.srcport", "tcp.dstport",
        ]
        for field in standard_fields + self._extra_fields:
            cmd += ["-e", field]

        return cmd

    def _start_local(self, output_path: Path) -> None:
        cmd = self._build_cmd(output_path)
        logger.debug("TsharkCapture local cmd: %s", " ".join(cmd))
        self._proc = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    def _stop_local(self) -> None:
        if self._proc and self._proc.poll() is None:
            self._proc.send_signal(signal.SIGINT)
            try:
                self._proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self._proc.kill()
        self._proc = None

    def _start_remote(self, output_path: Path) -> None:
        # Run tshark on remote node, write to a temp path there
        self._remote_output = f"/tmp/helix_cap_{int(time.time())}.json"
        cmd_str = " ".join(self._build_cmd(Path(self._remote_output)))
        # Start in background, capture PID
        result = self._ssh.run(f"nohup {cmd_str} >/dev/null 2>&1 & echo $!", check=False)
        try:
            self._remote_pid = int(result.stdout.strip())
        except (ValueError, AttributeError):
            logger.warning("TsharkCapture: could not get remote PID")

    def _stop_remote(self) -> None:
        if self._remote_pid:
            self._ssh.run(f"kill -INT {self._remote_pid}", check=False)
            time.sleep(1)
        # Copy back to local
        if self._output_path:
            self._ssh.get_file(self._remote_output, str(self._output_path))
            self._ssh.run(f"rm -f {self._remote_output}", check=False)
        self._remote_pid = None
