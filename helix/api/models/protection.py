"""Pydantic models for protection groups, backup jobs, and snapshots."""

from __future__ import annotations
from datetime import datetime
from typing import Literal
from pydantic import BaseModel


class ProtectionGroup(BaseModel):
    """A Cohesity protection group (backup job configuration)."""

    id: int | None = None
    name: str = ""
    environment: str | None = None  # e.g. "kPhysical", "kVMware"
    policy_id: str | None = None
    is_active: bool = True
    is_paused: bool = False
    source_ids: list[int] = []
    view_name: str | None = None

    @property
    def is_running(self) -> bool:
        return self.is_active and not self.is_paused

    model_config = {"populate_by_name": True, "extra": "allow"}


class BackupJob(BaseModel):
    """A single backup run / protection job execution."""

    id: int | None = None
    job_id: int | None = None
    run_id: str | None = None   # string run identifier for get_backup_run() calls
    status: str = "kUnknown"
    run_type: Literal["kRegular", "kFull", "kLog", "kSystem"] = "kRegular"
    start_time_usecs: int | None = None
    end_time_usecs: int | None = None
    total_bytes_transferred: int | None = None
    error_msg: str | None = None

    @property
    def is_running(self) -> bool:
        return self.status in ("kRunning", "kAccepted")

    @property
    def is_complete(self) -> bool:
        return self.status in ("kSuccess", "kFailure", "kCanceled")

    @property
    def is_success(self) -> bool:
        return self.status == "kSuccess"

    @property
    def start_time(self) -> datetime | None:
        if self.start_time_usecs:
            return datetime.fromtimestamp(self.start_time_usecs / 1e6)
        return None

    model_config = {"populate_by_name": True, "extra": "allow"}


class SnapshotInfo(BaseModel):
    """A point-in-time snapshot created by a protection job."""

    id: str | None = None
    job_id: int | None = None
    job_run_id: int | None = None
    started_time_usecs: int | None = None
    expiry_time_usecs: int | None = None
    source_id: int | None = None
    total_bytes_on_tier: int | None = None

    @property
    def created_at(self) -> datetime | None:
        if self.started_time_usecs:
            return datetime.fromtimestamp(self.started_time_usecs / 1e6)
        return None

    model_config = {"populate_by_name": True, "extra": "allow"}
