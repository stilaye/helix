"""Pydantic models for cluster and node state from Helios API."""

from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, field_validator, model_validator


class NodeState(BaseModel):
    """State of a single Cohesity cluster node."""

    id: int | str | None = None
    ip: str | None = None
    status: str = "kUnknown"
    role: Literal["kLeader", "kFollower", "kUnknown"] = "kUnknown"
    disk_count: int = 0
    uptime_secs: int = 0
    software_version: str | None = None

    @field_validator("status")
    @classmethod
    def normalize_status(cls, v: str) -> str:
        return v if v.startswith("k") else f"k{v.capitalize()}"

    @property
    def is_healthy(self) -> bool:
        return self.status == "kHealthy"

    @property
    def is_leader(self) -> bool:
        return self.role == "kLeader"

    model_config = {"populate_by_name": True, "extra": "allow"}


class ClusterInfo(BaseModel):
    """Aggregated cluster health info computed from node states."""

    cluster_id: str
    name: str | None = None
    nodes: list[NodeState] = []
    quorum_ok: bool = False
    helios_version: str | None = None

    @model_validator(mode="after")
    def validate_quorum(self) -> "ClusterInfo":
        if self.nodes:
            healthy = sum(1 for n in self.nodes if n.is_healthy)
            self.quorum_ok = healthy >= (len(self.nodes) // 2 + 1)
        return self

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    @property
    def healthy_node_count(self) -> int:
        return sum(1 for n in self.nodes if n.is_healthy)

    @property
    def leader(self) -> NodeState | None:
        return next((n for n in self.nodes if n.is_leader), None)

    def __str__(self) -> str:
        return (
            f"ClusterInfo(id={self.cluster_id}, "
            f"nodes={self.healthy_node_count}/{self.node_count} healthy, "
            f"quorum_ok={self.quorum_ok})"
        )

    model_config = {"populate_by_name": True, "extra": "allow"}
