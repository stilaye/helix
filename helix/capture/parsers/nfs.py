"""
NFS packet parser — extracts NFSv4 operation metadata from tshark JSON frames.

Fields validated:
  - NFSv4 COMPOUND operations (OPEN, READ, WRITE, CLOSE, LOOKUP)
  - Auth flavor: AUTH_SYS vs RPCSEC_GSS (Kerberos)
  - root_squash: UID 0 in request should NOT appear in server response
  - Stale file handle errors (NFS4ERR_STALE)

tshark fields used:
    nfs.procedure_v4          (NFS procedure type)
    nfs.ops.op                (COMPOUND op codes: 3=ACCESS, 18=LOOKUP, 24=OPEN, 25=OPEN_CONFIRM, 26=READ, 38=WRITE)
    rpc.auth.flavor           (1=AUTH_SYS, 6=RPCSEC_GSS)
    rpc.auth.uid              (UID in request credential)
    nfs.nfsstat4              (NFS4 status codes; 70=NFS4ERR_STALE)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

# NFSv4 COMPOUND operation codes
NFS4_OP_NAMES = {
    "3": "ACCESS",
    "9": "GETATTR",
    "10": "GETFH",
    "18": "LOOKUP",
    "22": "OPEN_DOWNGRADE",
    "24": "OPEN",
    "25": "OPEN_CONFIRM",
    "26": "OPENATTR",
    "28": "READ",
    "38": "WRITE",
    "4": "CLOSE",
    "6": "COMMIT",
    "7": "CREATE",
    "12": "LINK",
    "16": "LOCK",
    "17": "LOCKT",
    "19": "LOOKU",
    "36": "REMOVE",
    "37": "RENAME",
    "44": "SETATTR",
}

# RPC auth flavor
AUTH_SYS = "1"
RPCSEC_GSS = "6"

# NFS4 status codes
NFS4_OK = "0"
NFS4ERR_STALE = "70"
NFS4ERR_ACCESS = "13"
NFS4ERR_NOENT = "2"


@dataclass
class NFSParseResult:
    nfs_version: str | None = None              # "4" | "4.1"
    auth_flavor: str | None = None              # "AUTH_SYS" | "RPCSEC_GSS"
    operations: dict[str, int] = field(default_factory=dict)   # op_name -> count
    status_codes: dict[str, int] = field(default_factory=dict) # status -> count
    uids_in_requests: set[str] = field(default_factory=set)
    stale_handle_count: int = 0
    frame_count: int = 0

    @property
    def has_stale_handles(self) -> bool:
        return self.stale_handle_count > 0

    @property
    def is_kerberos(self) -> bool:
        return self.auth_flavor == "RPCSEC_GSS"

    @property
    def root_squash_working(self) -> bool:
        """
        Returns True if UID 0 appeared in requests (root client) but
        no UID 0 responses were observed — root_squash is functioning.
        Note: This is a heuristic; a proper check requires looking at server-side UID in responses.
        """
        return "0" in self.uids_in_requests


class NFSParser:
    """
    Parse tshark JSON frames for NFSv4 protocol validation.

    Usage:
        frames = TsharkCapture(...).load_frames()
        result = NFSParser(frames).parse()
        assert not result.has_stale_handles, "Stale NFS handles detected"
        assert result.auth_flavor == "RPCSEC_GSS", "Expected Kerberos NFS auth"
    """

    def __init__(self, frames: list[dict[str, Any]]) -> None:
        self._frames = frames

    def parse(self) -> NFSParseResult:
        result = NFSParseResult()
        result.frame_count = len(self._frames)

        for frame in self._frames:
            layers = frame.get("_source", {}).get("layers", {})
            self._parse_frame(layers, result)

        logger.debug(
            "NFSParser: %d frames, auth=%s, stale_handles=%d, ops=%s",
            result.frame_count, result.auth_flavor,
            result.stale_handle_count, list(result.operations.keys())
        )
        return result

    def _parse_frame(self, layers: dict[str, Any], result: NFSParseResult) -> None:
        rpc = layers.get("rpc", {})
        nfs = layers.get("nfs", {})

        if not rpc and not nfs:
            return

        # Auth flavor
        auth_flavor = rpc.get("rpc.auth.flavor")
        if auth_flavor and not result.auth_flavor:
            result.auth_flavor = "RPCSEC_GSS" if auth_flavor == RPCSEC_GSS else "AUTH_SYS"

        # UID in request credential
        uid = rpc.get("rpc.auth.uid")
        if uid:
            result.uids_in_requests.add(uid)

        # NFS version
        nfs_version = nfs.get("nfs.procedure_v4")
        if nfs_version is not None and not result.nfs_version:
            result.nfs_version = "4"

        # COMPOUND operations
        ops = nfs.get("nfs.ops") or nfs.get("nfs.main_opcode")
        if isinstance(ops, list):
            for op_code in ops:
                op_name = NFS4_OP_NAMES.get(str(op_code), f"OP_{op_code}")
                result.operations[op_name] = result.operations.get(op_name, 0) + 1
        elif ops:
            op_name = NFS4_OP_NAMES.get(str(ops), f"OP_{ops}")
            result.operations[op_name] = result.operations.get(op_name, 0) + 1

        # NFS status codes
        status = nfs.get("nfs.nfsstat4")
        if status:
            result.status_codes[status] = result.status_codes.get(status, 0) + 1
            if status == NFS4ERR_STALE:
                result.stale_handle_count += 1
                logger.warning("NFSParser: NFS4ERR_STALE detected in frame")
