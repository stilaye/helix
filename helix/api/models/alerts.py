"""Pydantic models for Helios alerts and events."""

from __future__ import annotations
from datetime import datetime
from pydantic import BaseModel


class Alert(BaseModel):
    """A Cohesity cluster alert."""

    id: str | None = None
    alert_type: str | None = None
    severity: str = "kInfo"    # kInfo | kWarning | kCritical
    message: str = ""
    timestamp_usecs: int | None = None
    cluster_id: str | None = None
    resolved: bool = False

    @property
    def is_critical(self) -> bool:
        return self.severity == "kCritical"

    @property
    def created_at(self) -> datetime | None:
        if self.timestamp_usecs:
            return datetime.fromtimestamp(self.timestamp_usecs / 1e6)
        return None

    model_config = {"populate_by_name": True, "extra": "allow"}
