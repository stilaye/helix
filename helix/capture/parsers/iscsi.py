"""
iSCSI packet parser — extracts iSCSI session metadata from tshark JSON frames.

Fields validated:
  - Login/logout sequence completeness
  - CHAP authentication exchange
  - SCSI command opcodes (READ, WRITE, INQUIRY)
  - Login status (0x0000 = success, non-zero = failure)

tshark fields used:
    iscsi.opcode              (0x03=LoginReq, 0x23=LoginResp, 0x06=LogoutReq, 0x26=LogoutResp, 0x01=SCSI Cmd, 0x21=SCSI Resp)
    iscsi.login.status        (0x0000=success, 0x0101=auth fail)
    iscsi.scsi.lun            (LUN ID)
    scsi.cdb.opcode           (0x00=TestUnitReady, 0x12=INQUIRY, 0x28=READ, 0x2A=WRITE, 0x25=ReadCapacity)
    chap.type                 (0x00=challenge, 0x01=response, 0x02=success)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# iSCSI PDU opcodes
ISCSI_LOGIN_REQUEST = "0x03"
ISCSI_LOGIN_RESPONSE = "0x23"
ISCSI_LOGOUT_REQUEST = "0x06"
ISCSI_LOGOUT_RESPONSE = "0x26"
ISCSI_SCSI_COMMAND = "0x01"
ISCSI_SCSI_RESPONSE = "0x21"
ISCSI_NOP_OUT = "0x00"
ISCSI_NOP_IN = "0x20"
ISCSI_DATA_OUT = "0x05"
ISCSI_DATA_IN = "0x25"

# SCSI CDB opcodes
SCSI_TEST_UNIT_READY = "0x00"
SCSI_INQUIRY = "0x12"
SCSI_READ_CAPACITY = "0x25"
SCSI_READ_10 = "0x28"
SCSI_WRITE_10 = "0x2a"

SCSI_OP_NAMES = {
    SCSI_TEST_UNIT_READY: "TEST_UNIT_READY",
    SCSI_INQUIRY: "INQUIRY",
    SCSI_READ_CAPACITY: "READ_CAPACITY",
    SCSI_READ_10: "READ(10)",
    SCSI_WRITE_10: "WRITE(10)",
}


@dataclass
class ISCSIParseResult:
    login_success: bool = False
    login_status: str | None = None            # "0x0000" = success
    logout_clean: bool = False
    chap_used: bool = False
    scsi_commands: dict[str, int] = field(default_factory=dict)   # op_name -> count
    luns_accessed: set[str] = field(default_factory=set)
    login_count: int = 0
    logout_count: int = 0
    frame_count: int = 0
    error_count: int = 0

    @property
    def session_complete(self) -> bool:
        """True if login was followed by a clean logout."""
        return self.login_success and self.logout_clean

    @property
    def has_io(self) -> bool:
        return bool(self.scsi_commands.get("READ(10)") or self.scsi_commands.get("WRITE(10)"))


class ISCSIParser:
    """
    Parse tshark JSON frames for iSCSI protocol validation.

    Usage:
        frames = TsharkCapture(...).load_frames()
        result = ISCSIParser(frames).parse()
        assert result.login_success, f"iSCSI login failed: {result.login_status}"
        assert result.has_io, "No READ/WRITE commands in capture"
        assert result.logout_clean, "iSCSI session not cleanly terminated"
    """

    def __init__(self, frames: list[dict[str, Any]]) -> None:
        self._frames = frames

    def parse(self) -> ISCSIParseResult:
        result = ISCSIParseResult()
        result.frame_count = len(self._frames)

        for frame in self._frames:
            layers = frame.get("_source", {}).get("layers", {})
            self._parse_frame(layers, result)

        logger.debug(
            "ISCSIParser: %d frames, login=%s, io=%s, cmds=%s",
            result.frame_count, result.login_success, result.has_io,
            result.scsi_commands
        )
        return result

    def _parse_frame(self, layers: dict[str, Any], result: ISCSIParseResult) -> None:
        iscsi = layers.get("iscsi", {})
        scsi = layers.get("scsi", {})
        chap = layers.get("chap", {})

        if not iscsi and not scsi and not chap:
            return

        # CHAP authentication
        if chap:
            result.chap_used = True

        opcode = iscsi.get("iscsi.opcode")
        if not opcode:
            return

        opcode = opcode.lower()

        if opcode == ISCSI_LOGIN_REQUEST:
            result.login_count += 1

        elif opcode == ISCSI_LOGIN_RESPONSE:
            status = iscsi.get("iscsi.login.status")
            if status is not None:
                result.login_status = status
                if status == "0x0000":
                    result.login_success = True
                    logger.info("ISCSIParser: login successful")
                else:
                    result.error_count += 1
                    logger.error("ISCSIParser: login failed with status %s", status)

        elif opcode == ISCSI_LOGOUT_REQUEST:
            result.logout_count += 1

        elif opcode == ISCSI_LOGOUT_RESPONSE:
            result.logout_clean = True

        elif opcode == ISCSI_SCSI_COMMAND:
            # Track LUN
            lun = iscsi.get("iscsi.scsi.lun")
            if lun:
                result.luns_accessed.add(lun)

            # Track SCSI opcode
            cdb_opcode = scsi.get("scsi.cdb.opcode", "").lower()
            if cdb_opcode:
                op_name = SCSI_OP_NAMES.get(cdb_opcode, f"UNKNOWN(0x{cdb_opcode})")
                result.scsi_commands[op_name] = result.scsi_commands.get(op_name, 0) + 1

        elif opcode == ISCSI_SCSI_RESPONSE:
            # Check SCSI status
            scsi_status = scsi.get("scsi.status")
            if scsi_status and scsi_status != "0x00":  # 0x00 = GOOD
                result.error_count += 1
                logger.warning("ISCSIParser: SCSI error status %s", scsi_status)
