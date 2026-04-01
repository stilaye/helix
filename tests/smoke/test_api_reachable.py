"""
Smoke: Helios API reachability — auth, ping, basic endpoint validation.

Verifies:
  - API key authentication works
  - MCM endpoints respond with valid data
  - Cluster-specific endpoints work with accessClusterId header
"""

import pytest
import allure

pytestmark = [pytest.mark.smoke]


@allure.suite("Smoke")
@allure.feature("Helios API")
class TestHeliosAPIReachable:

    @allure.title("Helios API authentication succeeds with provided API key")
    def test_auth_succeeds(self, helios_client):
        """
        A 401 here means the HELIOS_API_KEY env var is wrong or the key was revoked.
        Keys are created in Helios UI: Settings → Access Management → API Keys.
        """
        with allure.step("Ping Helios API"):
            result = helios_client.ping()
        assert result, "Helios API ping returned empty response"

    @allure.title("MCM clusters endpoint returns registered clusters")
    def test_list_clusters(self, helios_client):
        """Verify the MCM /clusters endpoint returns at least one cluster."""
        with allure.step("Fetch registered clusters"):
            clusters = helios_client.list_clusters()
        assert len(clusters) >= 1, "No clusters registered in Helios MCM"

    @allure.title("Cluster-specific API call works with accessClusterId header")
    def test_cluster_specific_endpoint(self, helios_client, helix_config):
        """
        Verify that cluster-specific calls (with accessClusterId header) work.
        This tests the APIKeyAuth.inject() method with cluster_id parameter.
        """
        if not helix_config.cluster_id:
            pytest.skip("--cluster-id not provided")

        with allure.step("Fetch protection jobs with accessClusterId"):
            jobs = helios_client.list_protection_jobs()
        # Empty list is fine — cluster may have no jobs configured yet
        assert isinstance(jobs, list), "Expected a list of protection jobs"

    @allure.title("Helios alerts endpoint returns valid response")
    def test_alerts_endpoint(self, helios_client):
        """Verify the alerts endpoint responds (even if no active alerts)."""
        with allure.step("Fetch cluster alerts"):
            alerts = helios_client.list_alerts()
        assert isinstance(alerts, list), "Expected a list of alerts"
        critical = [a for a in alerts if a.is_critical]
        if critical:
            allure.attach(
                "\n".join(f"  [{a.severity}] {a.description}" for a in critical),
                name="critical_alerts.txt",
                attachment_type=allure.attachment_type.TEXT,
            )
