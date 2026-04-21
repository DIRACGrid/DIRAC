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
    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="No version specified"):
            _normalise_version("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="No version specified"):
            _normalise_version("   ")

    def test_released_version(self):
        version, primary, released, pre = _normalise_version("9.0.18")
        assert released is True
        assert pre is False
        assert version == "v9.0.18"
        assert primary is None

    def test_released_prerelease_version(self):
        version, primary, released, pre = _normalise_version("9.0.18a1")
        assert released is True
        assert pre is True
        assert version == "v9.0.18a1"

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

    def test_git_url_without_spaces(self):
        raw = "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch"
        version, primary, released, pre = _normalise_version(raw)
        assert released is False
        assert version == raw

    def test_git_url_with_spaces_around_at(self):
        """Leading/trailing whitespace is stripped; internal pip spaces are kept."""
        raw = "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch"
        version, primary, released, pre = _normalise_version(f"  {raw}  ")
        assert released is False
        assert version == raw

    def test_invalid_version_no_url_raises(self):
        with pytest.raises(ValueError, match="Invalid version passed"):
            _normalise_version("not-a-valid-version")


# ---------------------------------------------------------------------------
# Tests: _directory_label
# ---------------------------------------------------------------------------


class TestDirectoryLabel:
    def test_released_version_uses_version_directly(self):
        assert _directory_label("v9.0.18", released_version=True) == "v9.0.18"

    def test_git_url_without_spaces(self):
        """Branch name after the second '@' must be preserved."""
        version = "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch"
        assert (
            _directory_label(version, released_version=False) == "git+https://github.com/fstagni/DIRAC.git@test_branch"
        )

    def test_git_url_with_spaces_around_pip_separator(self):
        """Spaces around the pip '@' separator are stripped; branch part kept."""
        version = "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch"
        assert (
            _directory_label(version, released_version=False) == "git+https://github.com/fstagni/DIRAC.git@test_branch"
        )

    def test_git_url_with_hash_fragment(self):
        """#egg= fragment must be stripped from the directory label."""
        version = "DIRAC[server]@git+https://github.com/DIRACGrid/DIRAC.git@integration#egg=DIRAC"
        assert (
            _directory_label(version, released_version=False)
            == "git+https://github.com/DIRACGrid/DIRAC.git@integration"
        )


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

    def test_empty_args_prints_usage_and_returns(self):
        cli = self._make_cli()
        with (
            patch(
                "DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.SystemAdministratorClient"
            ) as mock_client_cls,
            patch("DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.gLogger") as mock_logger,
        ):
            cli.do_update("")
            mock_client_cls.assert_not_called()
            assert mock_logger.notice.called

    def test_whitespace_only_args_does_not_contact_server(self):
        cli = self._make_cli()
        with (
            patch(
                "DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.SystemAdministratorClient"
            ) as mock_client_cls,
            patch("DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.gLogger"),
        ):
            cli.do_update("   ")
            mock_client_cls.assert_not_called()

    def test_valid_version_calls_client(self):
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

            cli.do_update("9.0.18")

            mock_client_cls.assert_called_once_with(cli.host, cli.port)
            mock_instance.updateSoftware.assert_called_once_with("9.0.18", timeout=600)

    def test_git_url_with_spaces_passes_full_version_to_server(self):
        """The version body (including internal spaces) is forwarded as-is."""
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

            raw = "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch"
            cli.do_update(raw)

            mock_instance.updateSoftware.assert_called_once_with(raw, timeout=600)
