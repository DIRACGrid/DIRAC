"""Unit tests for SystemAdministratorHandler helper functions and
the SystemAdministratorClientCLI do_update input validation.
"""

import pytest
from unittest.mock import MagicMock, patch

from DIRAC.FrameworkSystem.Service.SystemAdministratorHandler import (
    _directory_label,
    _normalise_version,
)


# ---------------------------------------------------------------------------
# Tests: _normalise_version
# ---------------------------------------------------------------------------


class TestNormaliseVersion:
    @pytest.mark.parametrize("value", ["", "   "])
    def test_empty_or_whitespace_raises(self, value):
        with pytest.raises(ValueError, match="No version specified"):
            _normalise_version(value)

    @pytest.mark.parametrize(
        "input_val, expected_version, expected_pre",
        [
            ("9.0.18", "v9.0.18", False),
            ("9.0.18a1", "v9.0.18a1", True),
        ],
    )
    def test_released_versions(self, input_val, expected_version, expected_pre):
        version, primary, released, pre = _normalise_version(input_val)
        assert released is True
        assert pre is expected_pre
        assert version == expected_version

    def test_extension_syntax_splits_primary(self):
        version, primary, released, pre = _normalise_version("MyExtension==9.0.18")
        assert primary == "MyExtension"
        assert version == "v9.0.18"
        assert released is True

    @pytest.mark.parametrize("keyword", ["integration", "devel", "master", "main"])
    def test_special_keywords(self, keyword):
        version, primary, released, pre = _normalise_version(keyword)
        assert released is False
        assert pre is False
        assert "DIRACGrid/DIRAC" in version
        assert "@integration" in version

    @pytest.mark.parametrize(
        "raw",
        [
            "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch",
            "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch",
        ],
    )
    def test_git_urls(self, raw):
        version, primary, released, pre = _normalise_version(f"  {raw}  ")
        assert released is False
        assert version == raw

    def test_invalid_version_raises(self):
        with pytest.raises(ValueError, match="Invalid version passed"):
            _normalise_version("not-a-valid-version")


# ---------------------------------------------------------------------------
# Tests: _directory_label
# ---------------------------------------------------------------------------


class TestDirectoryLabel:
    def test_released_version_uses_version_directly(self):
        assert _directory_label("v9.0.18", released_version=True) == "v9.0.18"

    @pytest.mark.parametrize(
        "version, expected",
        [
            (
                "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch",
                "test_branch",
            ),
            (
                "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch",
                "test_branch",
            ),
            (
                "DIRAC[server]@git+https://github.com/DIRACGrid/DIRAC.git@integration#egg=DIRAC",
                "integration",
            ),
        ],
    )
    def test_git_url_variants(self, version, expected):
        assert _directory_label(version, released_version=False) == expected


# ---------------------------------------------------------------------------
# Tests: CLI do_update input validation
# ---------------------------------------------------------------------------


class TestDoUpdate:
    """Test SystemAdministratorClientCLI.do_update input validation."""

    def _make_cli(self):
        from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import SystemAdministratorClientCLI

        cli = SystemAdministratorClientCLI.__new__(SystemAdministratorClientCLI)
        cli.host = "localhost"
        cli.port = 9162
        return cli

    @pytest.mark.parametrize("args", ["", "   "])
    def test_empty_or_whitespace_args_does_not_contact_server(self, args):
        cli = self._make_cli()
        with (
            patch(
                "DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.SystemAdministratorClient"
            ) as mock_client_cls,
            patch("DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.gLogger") as mock_logger,
        ):
            cli.do_update(args)
            mock_client_cls.assert_not_called()
            if not args.strip():
                assert mock_logger.notice.called

    @pytest.mark.parametrize(
        "version",
        [
            "9.0.18",
            "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch",
        ],
    )
    def test_valid_version_calls_client(self, version):
        cli = self._make_cli()
        with (
            patch(
                "DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.SystemAdministratorClient"
            ) as mock_client_cls,
            patch("DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.gLogger"),
        ):
            mock_instance = MagicMock()
            mock_instance.updateSoftware.return_value = {"OK": True, "Value": None}
            mock_client_cls.return_value = mock_instance

            cli.do_update(version)

            mock_client_cls.assert_called_once_with(cli.host, cli.port)
            mock_instance.updateSoftware.assert_called_once_with(version, timeout=600)
