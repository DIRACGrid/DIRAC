"""RSS API calling and apply translation for the read adapter module."""

from __future__ import annotations

from DIRAC.Core.Security.DiracX import DiracXClient

from .ResponseTranslation import (
    translate_computing_element_status,
    translate_fts_status,
    translate_site_status,
    translate_storage_element_status,
)


def get_storage_element_status() -> list[tuple]:
    """Get storage element status from the RSS API and translate to legacy format.

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    with DiracXClient() as client:
        response = client.rss.get_storage_status()
        return translate_storage_element_status(response)


def get_computing_element_status() -> list[tuple]:
    """Get computing element status from the RSS API and translate to legacy format.

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    with DiracXClient() as client:
        response = client.rss.get_compute_status()
        return translate_computing_element_status(response)


def get_fts_status() -> list[tuple]:
    """Get merged FTS server status from all VOs.

    Returns:
        List of tuples in legacy format: (name, element_type, status_type, status, vo)

    """
    with DiracXClient() as client:
        response = client.rss.get_fts_status()
        return translate_fts_status(response)


def get_site_status() -> list[tuple]:
    """Get site status from the RSS API and translate to legacy format.

    Returns:
        List of tuples in legacy format: (site, status)

    """
    with DiracXClient() as client:
        response = client.rss.get_site_status()
        return translate_site_status(response)
