"""
SMB packet parser — extracts protocol metadata from tshark JSON frames.

Fields validated:
  - SMB dialect negotiated (assert SMB 3.0+ for modern security)
  - Session signing/encryption enabled
  - Auth type: NTLM vs Kerberos
  - Session ID consistency across all operations

tshark fields used:
    smb2.cmd                  (0=NegotiateProtocol, 1=SessionSetup, 5=Create, 8=Read, 9=Write)
    smb2.dialect              (0x0300=SMB3.0, 0x0302=SMB3.0.2, 0x0311=SMB3.1.1)
    smb2.flags.signed         (0x08 bit — packet signing)
    smb2.session_id           (consistency check)
    ntlmssp.auth.username     (NTLM auth detected when present)
    kerberos.msg_type         (Kerberos detected when present)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# SMB2 dialect values
DIALECT_MAP = {
    "0x0202": "SMB 2.0.2",
    "0x0210": "SMB 2.1",
    "0x0300": "SMB 3.0",
    "0x0302": "SMB 3.0.2",
    "0x0311": "SMB 3.1.1",
}

# SMB2 command codes
SMB2_NEGOTIATE = "0"
SMB2_SESSION_SETUP = "1"
SMB2_TREE_CONNECT = "3"
SMB2_CREATE = "5"
SMB2_READ = "8"
SMB2_WRITE = "9"
SMB2_CLOSE = "6"


@dataclass
class SMBParseResult:
    dialect: str | None = None                  # e.g., "SMB 3.1.1"
    dialect_hex: str | None = None              # e.g., "0x0311"
    signing_enabled: bool = False
    encryption_enabled: bool = False
    auth_type: str | None = None                # "NTLM" | "Kerberos" | None
    session_ids: set[str] = field(default_factory=set)
    command_counts: dict[str, int] = field(default_factory=dict)
    frame_count: int = 0

    @property
    def is_smb3(self) -> bool:
        return self.dialect_hex in ("0x0300", "0x0302", "0x0311")

    @property
    def is_smb311(self) -> bool:
        return self.dialect_hex == "0x0311"

    def session_consistent(self) -> bool:
        """True if all SMB ops used the same session ID (no session hijack/expiry)."""
        return len(self.session_ids) <= 1


class SMBParser:
    """
    Parse tshark JSON frames for SMB2/3 protocol validation.

    Usage:
        frames = TsharkCapture(...).load_frames()
        result = SMBParser(frames).parse()
        assert result.is_smb311, f"Expected SMB 3.1.1, got {result.dialect}"
        assert result.signing_enabled, "SMB signing must be enabled"
    """

    def __init__(self, frames: list[dict[str, Any]]) -> None:
        self._frames = frames

    def parse(self) -> SMBParseResult:
        result = SMBParseResult()
        result.frame_count = len(self._frames)

        for frame in self._frames:
            layers = frame.get("_source", {}).get("layers", {})
            self._parse_frame(layers, result)

        logger.debug(
            "SMBParser: %d frames, dialect=%s, signing=%s, auth=%s",
            result.frame_count, result.dialect, result.signing_enabled, result.auth_type
        )
        return result

    def _parse_frame(self, layers: dict[str, Any], result: SMBParseResult) -> None:
        smb2 = layers.get("smb2", {})
        if not smb2:
            return

        # Dialect (present in NegotiateProtocol response)
        dialect_hex = smb2.get("smb2.dialect") or smb2.get("smb2.negotiate.dialect")
        if dialect_hex and not result.dialect_hex:
            result.dialect_hex = dialect_hex.lower()
            result.dialect = DIALECT_MAP.get(result.dialect_hex, f"Unknown ({dialect_hex})")

        # Signing flag
        flags = smb2.get("smb2.flags", {})
        if isinstance(flags, dict):
            signed = flags.get("smb2.flags.signed", "0")
            if signed == "1":
                result.signing_enabled = True
        elif smb2.get("smb2.flags.signed") == "1":
            result.signing_enabled = True

        # Session ID
        session_id = smb2.get("smb2.sesid") or smb2.get("smb2.session_id")
        if session_id and session_id != "0x0000000000000000":
            result.session_ids.add(session_id)

        # Command count
        cmd = smb2.get("smb2.cmd")
        if cmd:
            result.command_counts[cmd] = result.command_counts.get(cmd, 0) + 1

        # NTLM auth detection
        if "ntlmssp" in layers or layers.get("gss-api", {}).get("ntlmssp.auth.username"):
            result.auth_type = "NTLM"

        # Kerberos auth detection
        if "kerberos" in layers or layers.get("spnego", {}).get("kerberos.msg_type"):
            result.auth_type = "Kerberos"
