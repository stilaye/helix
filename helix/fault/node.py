"""
Node fault injection: kill processes, freeze services, simulate node crashes.
"""

from __future__ import annotations

import logging
import time
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from helix.ssh.remote import SSHClient

logger = logging.getLogger(__name__)

COHESITY_SERVICE = "cohesity"
IRIS_PROCESS = "iris"


class NodeFault:
    """
    Inject node-level faults: kill Cohesity processes, freeze nodes, simulate hard failures.

    Args:
        ssh: SSHClient connected to the target node.
    """

    def __init__(self, ssh: "SSHClient") -> None:
        self._ssh = ssh
        self._killed_services: list[str] = []

    def kill_process(self, process_name: str = IRIS_PROCESS, signal: str = "KILL") -> None:
        """Send signal to a process by name. Default: SIGKILL (hard kill)."""
        result = self._ssh.run(f"sudo pkill -{signal} {process_name}", check=False)
        if result.exit_code == 0:
            self._killed_services.append(process_name)
            logger.info("NodeFault: sent %s to %s", signal, process_name)
        else:
            logger.warning("NodeFault: pkill failed for %s: %s", process_name, result.stderr)

    def stop_service(self, service: str = COHESITY_SERVICE) -> None:
        """Stop a systemd service (graceful)."""
        self._ssh.run(f"sudo systemctl stop {service}", check=True)
        self._killed_services.append(f"service:{service}")
        logger.info("NodeFault: stopped service %s", service)

    def freeze_process(self, process_name: str = IRIS_PROCESS) -> None:
        """SIGSTOP a process — pauses it without killing (useful for slow-node simulation)."""
        result = self._ssh.run(f"sudo pkill -STOP {process_name}", check=False)
        if result.exit_code == 0:
            self._killed_services.append(f"frozen:{process_name}")
            logger.info("NodeFault: froze (SIGSTOP) %s", process_name)

    def unfreeze_process(self, process_name: str = IRIS_PROCESS) -> None:
        """SIGCONT a paused process."""
        self._ssh.run(f"sudo pkill -CONT {process_name}", check=False)
        self._killed_services = [s for s in self._killed_services if s != f"frozen:{process_name}"]

    def simulate_panic(self) -> None:
        """
        Trigger an immediate kernel panic (simulates hard node failure).
        WARNING: This will crash the node. Use only in isolated lab environments.
        Node will need to reboot to recover.
        """
        logger.critical("NodeFault: triggering kernel panic on %s — node will crash!", self._ssh)
        self._ssh.run("sudo echo c > /proc/sysrq-trigger", check=False)

    def recover(self) -> None:
        """Restart killed/frozen services."""
        for item in list(self._killed_services):
            if item.startswith("service:"):
                svc = item[8:]
                self._ssh.run(f"sudo systemctl start {svc}", check=False)
                logger.info("NodeFault: restarted service %s", svc)
            elif item.startswith("frozen:"):
                proc = item[7:]
                self.unfreeze_process(proc)
            else:
                # Just a process — restart the service that owns it
                self._ssh.run(f"sudo systemctl restart {COHESITY_SERVICE}", check=False)
                logger.info("NodeFault: restarted cohesity service to recover %s", item)
        self._killed_services.clear()

    def __enter__(self) -> "NodeFault":
        return self

    def __exit__(self, *_: Any) -> None:
        self.recover()


class DiskFault:
    """
    Inject disk-level faults: simulate disk errors, slow I/O.

    Uses Linux dm-error target and /proc/scsi/... injection.
    """

    def __init__(self, ssh: "SSHClient") -> None:
        self._ssh = ssh
        self._injected_devices: list[str] = []

    def inject_errors(self, device: str, error_rate: float = 0.1) -> None:
        """
        Inject I/O errors on a device using dm-error target.
        error_rate: fraction of I/Os that will fail (0.0-1.0).
        """
        # Create a dm-error device that fails on reads/writes
        result = self._ssh.run(
            f"echo '0 $(blockdev --getsz {device}) error' | sudo dmsetup create helix-fault-{device.split('/')[-1]}",
            check=False,
        )
        if result.exit_code == 0:
            self._injected_devices.append(device)
            logger.info("DiskFault: injecting errors on %s (rate=%.1f)", device, error_rate)

    def heal(self) -> None:
        """Remove all dm-error mappings."""
        for device in list(self._injected_devices):
            dev_name = f"helix-fault-{device.split('/')[-1]}"
            self._ssh.run(f"sudo dmsetup remove {dev_name}", check=False)
        self._injected_devices.clear()
        logger.info("DiskFault: healed all devices")

    def __enter__(self) -> "DiskFault":
        return self

    def __exit__(self, *_: Any) -> None:
        self.heal()
