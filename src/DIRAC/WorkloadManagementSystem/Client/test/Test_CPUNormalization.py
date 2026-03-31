"""Unit tests for CPUNormalization.getCPUTime()"""
from unittest.mock import patch

from DIRAC import S_OK, S_ERROR


@patch("DIRAC.WorkloadManagementSystem.Client.CPUNormalization.TimeLeft")
@patch("DIRAC.WorkloadManagementSystem.Client.CPUNormalization.gConfig")
class TestGetCPUTime:
    """Tests for getCPUTime() fallback chain."""

    def _import_getCPUTime(self):
        from DIRAC.WorkloadManagementSystem.Client.CPUNormalization import getCPUTime

        return getCPUTime

    def test_from_batch_system(self, mock_gConfig, mock_TimeLeft):
        """Primary path: batch system returns CPU work left."""
        mock_gConfig.getValue.return_value = 0
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_OK(30000)  # HS06*s

        result = self._import_getCPUTime()(cpuNormalizationFactor=10.0)

        # 30000 / 10.0 = 3000 seconds
        assert result == 3000
        mock_TimeLeft.return_value.getTimeLeft.assert_called_once()

    def test_batch_system_returns_zero(self, mock_gConfig, mock_TimeLeft):
        """When batch system reports 0 time left, trust it and return 0."""
        mock_gConfig.getValue.return_value = 0
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_OK(0)

        result = self._import_getCPUTime()(cpuNormalizationFactor=10.0)

        assert result == 0
        # Should NOT fall through to CS fallbacks
        mock_gConfig.getValue.assert_not_called()

    def test_from_queue_cs(self, mock_gConfig, mock_TimeLeft):
        """Fallback: batch system fails, uses queue maxCPUTime from CS."""
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_ERROR("No batch info")

        config_values = {
            "/LocalSite/GridCE": "ce.example.com",
            "/LocalSite/CEQueue": "default",
            "/LocalSite/Site": "LCG.Example.com",
        }

        def mock_getValue(key, default=0):
            if key in config_values:
                return config_values[key]
            # maxCPUTime in minutes
            if "maxCPUTime" in key:
                return 120.0  # 120 minutes
            return default

        mock_gConfig.getValue.side_effect = mock_getValue

        with patch(
            "DIRAC.WorkloadManagementSystem.Client.CPUNormalization.getQueueInfo",
            return_value=S_OK(
                {"QueueCSSection": "/Resources/Sites/LCG/LCG.Example.com/CEs/ce.example.com/Queues/default"}
            ),
        ):
            result = self._import_getCPUTime()(cpuNormalizationFactor=10.0)

        # 120 minutes * 60 = 7200 seconds
        assert result == 7200

    def test_fallback_max_cpu_time(self, mock_gConfig, mock_TimeLeft):
        """Last resort: everything fails, uses /Resources/Computing/CEDefaults/MaxCPUTime."""
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_ERROR("No batch info")

        config_values = {
            "/LocalSite/GridCE": "ce.example.com",
            "/LocalSite/CEQueue": "default",
            "/LocalSite/Site": "LCG.Example.com",
            "/Resources/Computing/CEDefaults/MaxCPUTime": 86400,
        }

        def mock_getValue(key, default=0):
            if key in config_values:
                return config_values[key]
            return default

        mock_gConfig.getValue.side_effect = mock_getValue

        with patch(
            "DIRAC.WorkloadManagementSystem.Client.CPUNormalization.getQueueInfo",
            return_value=S_OK(
                {"QueueCSSection": "/Resources/Sites/LCG/LCG.Example.com/CEs/ce.example.com/Queues/default"}
            ),
        ):
            result = self._import_getCPUTime()(cpuNormalizationFactor=10.0)

        # maxCPUTime from queue returned 0, so falls through to CEDefaults/MaxCPUTime
        assert result == 86400

    def test_nothing_available_returns_zero(self, mock_gConfig, mock_TimeLeft):
        """Fail safe: no batch info, no CS config, returns 0."""
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_ERROR("No batch info")

        config_values = {
            "/LocalSite/GridCE": "ce.example.com",
            "/LocalSite/CEQueue": "default",
            "/LocalSite/Site": "LCG.Example.com",
        }

        def mock_getValue(key, default=0):
            if key in config_values:
                return config_values[key]
            return default

        mock_gConfig.getValue.side_effect = mock_getValue

        with patch(
            "DIRAC.WorkloadManagementSystem.Client.CPUNormalization.getQueueInfo",
            return_value=S_OK(
                {"QueueCSSection": "/Resources/Sites/LCG/LCG.Example.com/CEs/ce.example.com/Queues/default"}
            ),
        ):
            result = self._import_getCPUTime()(cpuNormalizationFactor=10.0)

        assert result == 0

    def test_cpu_normalization_factor_from_config(self, mock_gConfig, mock_TimeLeft):
        """When cpuNormalizationFactor=0, it should be read from local config."""
        mock_TimeLeft.return_value.getTimeLeft.return_value = S_OK(50000)  # HS06*s

        mock_gConfig.getValue.side_effect = lambda key, default=0: {
            "/LocalSite/CPUNormalizationFactor": 5.0,
        }.get(key, default)

        result = self._import_getCPUTime()(cpuNormalizationFactor=0)

        # 50000 / 5.0 = 10000 seconds
        assert result == 10000
