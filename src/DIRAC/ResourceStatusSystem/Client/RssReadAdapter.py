"""RSS API calling and translation for the read adapter module.

This module provides functionality to:
- Call diracx RSS API endpoints and translate the outputs
- Translate responses from new diracx format to legacy format
"""

from __future__ import annotations

from diracx.core.models.rss import (
    AllowedStatus,
    BannedStatus,
    ComputeElementStatus,
    FTSStatus,
    SiteStatus,
    StorageElementStatus,
)

from DIRAC.Core.Security.DiracX import DiracXClient


def translate_storage_element_status(
    response: dict[str, StorageElementStatus],
) -> list[tuple]:
    """Translate storage element status from diracx format to legacy format.

    Args:
        response: Dictionary of storage element names to StorageElementStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "StorageElement",
                "ReadAccess",
                _translate_resource_status(status.read),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "WriteAccess",
                _translate_resource_status(status.write),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "CheckAccess",
                _translate_resource_status(status.check),
                None,
            )
        )
        legacy_format.append(
            (
                name,
                "StorageElement",
                "RemoveAccess",
                _translate_resource_status(status.remove),
                None,
            )
        )

    return legacy_format


def translate_computing_element_status(
    response: dict[str, ComputeElementStatus],
) -> list[tuple]:
    """Translate computing element status from diracx format to legacy format.

    Args:
        response: Dictionary of computing element names to ComputeElementStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "ComputeElement",
                "all",
                _translate_resource_status(status.all),
                None,
            )
        )
    return legacy_format


def translate_fts_status(response: dict[str, FTSStatus]) -> list[tuple]:
    """Translate FTS server status from diracx format to legacy format.

    Args:
        response: Dictionary of FTS server names to FTSStatus

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append(
            (
                name,
                "FTS",
                "all",
                _translate_resource_status(status.all),
                None,
            )
        )
    return legacy_format


def translate_site_status(response: dict[str, SiteStatus]) -> list[tuple]:
    """Translate site status from diracx format to legacy format.

    Args:
        response: Dictionary of site names to SiteStatus

    Returns:
        List of tuples in legacy format: (site, status)

    """
    legacy_format = []
    for name, status in response.items():
        legacy_format.append((name, _translate_resource_status(status.all)))
    return legacy_format


def _translate_resource_status(status: AllowedStatus | BannedStatus) -> str:
    """Translate a single resource status from diracx format to legacy format.

    Args:
        status: ResourceStatus (AllowedStatus or BannedStatus)

    Returns:
        Legacy status string - only "Active" or "Banned" as per DIRAC streamlining

    """
    if isinstance(status, AllowedStatus):
        # DIRAC is being streamlined to only use Active/Banned states
        # All allowed statuses (including Degraded) are mapped to Active
        return "Active"
    else:  # BannedStatus
        # All banned statuses are mapped to Banned
        return "Banned"


def get_resource_rss_status(client: DiracXClient) -> list[tuple]:
    """Get all resource statuses from the RSS API and translate to legacy format.

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    status: list[tuple] = []
    # Storage elements
    response = client.rss.get_storage_status()
    status.extend(translate_storage_element_status(response))
    # Computing Elements
    response = client.rss.get_compute_status()
    status.extend(translate_computing_element_status(response))
    # FTS
    response = client.rss.get_fts_status()
    status.extend(translate_fts_status(response))

    return status


def get_site_status(client: DiracXClient) -> list[tuple]:
    """Get site status from the RSS API and translate to legacy format.

    Returns:
        List of tuples in legacy format: (site, status)

    """
    response = client.rss.get_site_status()
    return translate_site_status(response)
