"""
Helios API Key Authentication.

Helios uses static API key auth — no OAuth, no token refresh.
Key is obtained via: Helios UI → Settings → Access Management → API Keys → Add API Key.

Two header modes:
  - MCM calls:             apiKey header only
  - Cluster-specific calls: apiKey + accessClusterId headers
"""

from __future__ import annotations


class APIKeyAuth:
    """
    Injects Helios authentication headers into every request.

    Args:
        api_key: The Helios API key (permanent until revoked).
    """

    def __init__(self, api_key: str) -> None:
        if not api_key or api_key.strip() == "":
            raise ValueError("HELIOS_API_KEY must be set — obtain from Helios UI > Settings > API Keys")
        self._key = api_key.strip()

    def inject(
        self,
        headers: dict[str, str],
        cluster_id: str | int | None = None,
    ) -> dict[str, str]:
        """
        Add authentication headers to an existing headers dict (mutates and returns).

        Args:
            headers: The request headers dict to augment.
            cluster_id: If provided, adds accessClusterId header for cluster-specific endpoints.

        Returns:
            The updated headers dict.
        """
        headers["apiKey"] = self._key
        headers["accept"] = "application/json"
        headers["content-type"] = "application/json"
        if cluster_id is not None:
            headers["accessClusterId"] = str(cluster_id)
        return headers

    def __repr__(self) -> str:
        masked = f"{self._key[:4]}...{self._key[-4:]}" if len(self._key) > 8 else "****"
        return f"APIKeyAuth(key={masked})"
