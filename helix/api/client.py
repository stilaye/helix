"""
Helios REST API Client.

Base URL: https://helios.cohesity.com/irisservices/api/v1/public/
Auth:     apiKey header (+ accessClusterId for cluster-specific endpoints)

Two call modes:
  - mcm_request()     — MCM/SaaS-level operations (no cluster context)
  - cluster_request() — Operations scoped to a specific registered cluster
"""

from __future__ import annotations

import logging
from typing import Any

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception

from helix.api.auth import APIKeyAuth
from helix.api.models.cluster import ClusterInfo, NodeState
from helix.api.models.protection import ProtectionGroup, BackupJob, SnapshotInfo
from helix.api.models.alerts import Alert
from helix.constants import HeliosEndpoint

logger = logging.getLogger(__name__)


def _is_retryable(exc: BaseException) -> bool:
    """Retry on 5xx or connection errors, never on 4xx (including 401 invalid key)."""
    if isinstance(exc, requests.HTTPError):
        return exc.response is not None and exc.response.status_code >= 500
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class HeliosClient:
    """
    Authenticated Helios REST API client.

    Usage:
        client = HeliosClient(api_key="xxx", cluster_id=1234)
        info = client.get_cluster_info()
        jobs = client.list_protection_jobs()
    """

    def __init__(
        self,
        api_key: str,
        cluster_id: str | int | None = None,
        base_url: str = HeliosEndpoint.BASE_URL,
        timeout: int = 30,
    ) -> None:
        self._auth = APIKeyAuth(api_key)
        self._cluster_id = cluster_id
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers["user-agent"] = "helix-framework/1.0"

    # ─── Internal request helpers ─────────────────────────────────────────────

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        stop=stop_after_attempt(3),
        reraise=True,
    )
    def _request(
        self,
        method: str,
        path: str,
        cluster_id: str | int | None = None,
        **kwargs: Any,
    ) -> requests.Response:
        """Core request method with retry on 5xx. 401 = invalid key, not retried."""
        headers: dict[str, str] = {}
        self._auth.inject(headers, cluster_id=cluster_id)
        url = f"{self._base_url}{path}"
        logger.debug("%s %s (cluster_id=%s)", method.upper(), url, cluster_id)
        resp = self._session.request(
            method, url, headers=headers, timeout=self._timeout, **kwargs
        )
        resp.raise_for_status()
        return resp

    def mcm_request(self, method: str, path: str, **kwargs: Any) -> Any:
        """MCM-level request — no cluster context."""
        return self._request(method, path, cluster_id=None, **kwargs).json()

    def cluster_request(
        self, method: str, path: str, cluster_id: str | int | None = None, **kwargs: Any
    ) -> Any:
        """Cluster-specific request — injects accessClusterId header."""
        cid = cluster_id or self._cluster_id
        if cid is None:
            raise ValueError("cluster_id required for cluster-specific endpoints. Set at construction or pass explicitly.")
        return self._request(method, path, cluster_id=cid, **kwargs).json()

    # ─── Health & Cluster ─────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Verify Helios API is reachable and API key is valid."""
        try:
            self.mcm_request("GET", HeliosEndpoint.MCM_CLUSTERS)
            return True
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 401:
                raise PermissionError("Invalid Helios API key — check HELIOS_API_KEY") from e
            raise

    def list_clusters(self) -> list[dict[str, Any]]:
        """List all clusters registered in Helios."""
        return self.mcm_request("GET", HeliosEndpoint.MCM_CLUSTERS)

    def get_cluster_info(self, cluster_id: str | int | None = None) -> ClusterInfo:
        """Get cluster health, node states, and quorum status."""
        nodes_raw = self.cluster_request("GET", HeliosEndpoint.NODES, cluster_id=cluster_id)
        nodes = [NodeState.model_validate(n) for n in nodes_raw]
        healthy_nodes = [n for n in nodes if n.status == "kHealthy"]
        quorum_ok = len(healthy_nodes) >= (len(nodes) // 2 + 1)
        return ClusterInfo(
            cluster_id=str(cluster_id or self._cluster_id or "unknown"),
            nodes=nodes,
            quorum_ok=quorum_ok,
        )

    # ─── Protection / Backup ──────────────────────────────────────────────────

    def list_protection_jobs(self, cluster_id: str | int | None = None) -> list[ProtectionGroup]:
        """List all protection jobs (backup configurations) on the cluster."""
        raw = self.cluster_request("GET", HeliosEndpoint.PROTECTION_JOBS, cluster_id=cluster_id)
        if isinstance(raw, list):
            return [ProtectionGroup.model_validate(j) for j in raw]
        return []

    def trigger_backup(
        self,
        job_id: int,
        cluster_id: str | int | None = None,
        run_type: str = "kRegular",
    ) -> BackupJob:
        """Trigger an immediate backup run for a protection job."""
        payload = {"id": job_id, "runType": run_type}
        raw = self.cluster_request(
            "POST", HeliosEndpoint.PROTECTION_JOBS_RUN, cluster_id=cluster_id, json=payload
        )
        return BackupJob.model_validate(raw if isinstance(raw, dict) else {"id": job_id})

    def get_backup_job(self, job_id: int, cluster_id: str | int | None = None) -> BackupJob:
        """Get current status of a backup run."""
        raw = self.cluster_request(
            "GET", f"{HeliosEndpoint.PROTECTION_JOBS}/{job_id}", cluster_id=cluster_id
        )
        return BackupJob.model_validate(raw)

    # ─── Snapshots ────────────────────────────────────────────────────────────

    def list_snapshots(
        self, job_id: int | None = None, cluster_id: str | int | None = None
    ) -> list[SnapshotInfo]:
        """List snapshots, optionally filtered by protection job."""
        params = {"jobId": job_id} if job_id else {}
        raw = self.cluster_request(
            "GET", HeliosEndpoint.SNAPSHOTS, cluster_id=cluster_id, params=params
        )
        if isinstance(raw, list):
            return [SnapshotInfo.model_validate(s) for s in raw]
        return []

    # ─── Recovery ─────────────────────────────────────────────────────────────

    def restore(
        self,
        snapshot_id: str,
        dest_path: str,
        cluster_id: str | int | None = None,
    ) -> dict[str, Any]:
        """Trigger a restore from snapshot to destination path."""
        payload = {"snapshotId": snapshot_id, "targetPath": dest_path}
        return self.cluster_request(
            "POST", HeliosEndpoint.RESTORE_RECOVER, cluster_id=cluster_id, json=payload
        )

    # ─── Alerts ───────────────────────────────────────────────────────────────

    def list_alerts(
        self, max_alerts: int = 50, cluster_id: str | int | None = None
    ) -> list[Alert]:
        """Fetch recent cluster alerts."""
        params = {"maxAlerts": max_alerts}
        raw = self.mcm_request("GET", HeliosEndpoint.MCM_ALERTS, params=params)
        if isinstance(raw, list):
            return [Alert.model_validate(a) for a in raw]
        return []

    # ─── Stats ────────────────────────────────────────────────────────────────

    def get_protection_summary(self) -> dict[str, Any]:
        """Get MCM-level protection job statistics summary."""
        return self.mcm_request("GET", HeliosEndpoint.MCM_STATS_PROTECTION)

    def close(self) -> None:
        """Close underlying HTTP session."""
        self._session.close()

    def __enter__(self) -> "HeliosClient":
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()
