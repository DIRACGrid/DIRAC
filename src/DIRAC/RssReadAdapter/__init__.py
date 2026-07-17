"""Read adapter module for translating diracx RSS API responses to legacy format.

This module provides functionality to:
- Call diracx RSS API endpoints and translate the outputs
- Translate responses from new diracx format to legacy format
"""

from __future__ import annotations

from .ResponseTranslation import (
    translate_computing_element_status,
    translate_fts_status,
    translate_site_status,
    translate_storage_element_status,
)
from .Statuses import (
    get_computing_element_status,
    get_fts_status,
    get_site_status,
    get_storage_element_status,
)

__all__ = [
    "get_computing_element_status",
    "get_fts_status",
    "get_site_status",
    "get_storage_element_status",
    "translate_computing_element_status",
    "translate_fts_status",
    "translate_site_status",
    "translate_storage_element_status",
]
