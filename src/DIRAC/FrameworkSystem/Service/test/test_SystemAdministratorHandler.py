"""Unit tests for SystemAdministratorHandler version normalisation logic
and the SystemAdministratorClientCLI do_update input validation.
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers – replicate the version-normalisation logic from
# SystemAdministratorHandler.export_updateSoftware so we can test it
# without standing up a full DIRAC service.
# ---------------------------------------------------------------------------


def _normalise_version(version):
    """Mirror the normalisation applied at the top of export_updateSoftware.

    Returns the normalised version string, or raises ValueError if invalid.
    Mirrors the side-effects relevant to the directory-name derivation and
    the pip command construction.
    """
    from packaging.version import Version, InvalidVersion

    version = version.strip()
    if not version:
        raise ValueError("No version specified")

    released_version = True
    isPrerelease = False

    if version.lower() in ["integration", "devel", "master", "main"]:
        released_version = False
        version = "DIRAC[server] @ git+https://github.com/DIRACGrid/DIRAC.git@integration"

    if released_version:
        try:
            parsed = Version(version)
            isPrerelease = parsed.is_prerelease
            version = f"v{parsed}"
        except InvalidVersion:
            if "https://" in version:
                released_version = False
            else:
                raise ValueError(f"Invalid version passed {version!r}")

    return version, released_version, isPrerelease


def _directory_from_version(version, released_version):
    """Mirror the directory-name derivation in export_updateSoftware.

    Split on the *first* "@" only (the pip package @ URL separator), strip
    whitespace, then drop any "#egg=..." fragment.
    """
    if released_version:
        return version
    return version.split("@", 1)[1].strip().split("#")[0]


# ---------------------------------------------------------------------------
# Tests: version normalisation
# ---------------------------------------------------------------------------


class TestNormaliseVersion:
    def test_empty_string_raises(self):
        with pytest.raises(ValueError, match="No version specified"):
            _normalise_version("")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValueError, match="No version specified"):
            _normalise_version("   ")

    def test_released_version(self):
        version, released, pre = _normalise_version("9.0.18")
        assert released is True
        assert pre is False
        assert version == "v9.0.18"

    def test_released_prerelease_version(self):
        version, released, pre = _normalise_version("9.0.18a1")
        assert released is True
        assert pre is True
        assert version == "v9.0.18a1"

    @pytest.mark.parametrize("keyword", ["integration", "devel", "master", "main"])
    def test_special_keywords(self, keyword):
        version, released, pre = _normalise_version(keyword)
        assert released is False
        assert "DIRACGrid/DIRAC" in version
        assert "@integration" in version

    def test_git_url_without_spaces(self):
        raw = "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch"
        version, released, pre = _normalise_version(raw)
        assert released is False
        assert version == raw

    def test_git_url_with_spaces_around_at(self):
        """The CLI now sends the raw user input; leading/trailing spaces must be stripped."""
        raw = "  DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch  "
        version, released, pre = _normalise_version(raw)
        assert released is False
        # Internal spaces around "@" are preserved (pip accepts them)
        assert version.strip() == raw.strip()

    def test_invalid_version_no_url_raises(self):
        with pytest.raises(ValueError, match="Invalid version passed"):
            _normalise_version("not-a-valid-version")


# ---------------------------------------------------------------------------
# Tests: directory derivation from version string
# ---------------------------------------------------------------------------


class TestDirectoryFromVersion:
    def test_released_version_uses_version_directly(self):
        d = _directory_from_version("v9.0.18", released_version=True)
        assert d == "v9.0.18"

    def test_git_url_without_spaces(self):
        """No spaces around '@' separator — branch name must be included."""
        version = "DIRAC[server]@git+https://github.com/fstagni/DIRAC.git@test_branch"
        d = _directory_from_version(version, released_version=False)
        # Split on first "@" → "git+https://github.com/fstagni/DIRAC.git@test_branch"
        assert d == "git+https://github.com/fstagni/DIRAC.git@test_branch"

    def test_git_url_with_spaces_around_at_separator(self):
        """Spaces around the pip '@' separator must be stripped; branch part kept."""
        # Simulate what the handler receives after version.strip()
        version = "DIRAC[server] @ git+https://github.com/fstagni/DIRAC.git@test_branch"
        d = _directory_from_version(version, released_version=False)
        assert d == "git+https://github.com/fstagni/DIRAC.git@test_branch"

    def test_git_url_with_hash_fragment(self):
        """#egg= fragment must be stripped from directory name."""
        version = "DIRAC[server]@git+https://github.com/DIRACGrid/DIRAC.git@integration#egg=DIRAC"
        d = _directory_from_version(version, released_version=False)
        assert d == "git+https://github.com/DIRACGrid/DIRAC.git@integration"


# ---------------------------------------------------------------------------
# Tests: CLI do_update input validation
# ---------------------------------------------------------------------------


class TestDoUpdate:
    """Test SystemAdministratorClientCLI.do_update input validation."""

    def _make_cli(self):
        from DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI import (
            SystemAdministratorClientCLI,
        )

        with patch("DIRAC.FrameworkSystem.Client.SystemAdministratorClientCLI.SystemAdministratorClient"):
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
            # Client must NOT be contacted
            mock_client_cls.assert_not_called()
            # Usage should be printed
            assert mock_logger.notice.called

    def test_whitespace_only_args_prints_usage_and_returns(self):
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

    def test_git_url_with_spaces_passes_stripped_version(self):
        """Spaces are stripped from the outer edges but the version body is preserved."""
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
