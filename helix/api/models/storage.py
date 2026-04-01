"""Pydantic models for storage views, volumes, and share configurations."""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel


class ShareConfig(BaseModel):
    """Configuration for a NAS share (SMB, NFS, or S3-compatible view)."""

    name: str = ""
    path: str | None = None
    protocol_access: list[Literal["kSMB", "kNFS", "kS3"]] = []
    smb_acl_enabled: bool = False
    nfs_squash: Literal["kNone", "kRootSquash", "kAllSquash"] = "kNone"
    s3_bucket_enabled: bool = False
    storage_domain_id: int | None = None

    model_config = {"populate_by_name": True, "extra": "allow"}


class VolumeInfo(BaseModel):
    """Physical or virtual volume information."""

    id: str | None = None
    name: str = ""
    mount_path: str | None = None
    total_bytes: int | None = None
    used_bytes: int | None = None
    type: str | None = None

    @property
    def usage_pct(self) -> float | None:
        if self.total_bytes and self.total_bytes > 0 and self.used_bytes is not None:
            return (self.used_bytes / self.total_bytes) * 100.0
        return None

    model_config = {"populate_by_name": True, "extra": "allow"}


class BucketPolicy(BaseModel):
    """S3 bucket lifecycle/access policy."""

    bucket_name: str = ""
    versioning_enabled: bool = False
    lifecycle_rules: list[dict] = []
    access_policy: dict | None = None

    model_config = {"extra": "allow"}
