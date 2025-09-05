"""Platform utilities for DIRAC platform compatibility and management.

This module provides functions for working with DIRAC platforms and OS compatibility.
"""
import re

from DIRACCommon.Core.Utilities.ReturnValues import S_ERROR, S_OK, DReturnType


def getDIRACPlatform(osList: list[str], osCompatibilityDict: dict[str, set[str]]) -> DReturnType[list[str]]:
    """Get standard DIRAC platform(s) compatible with the argument.

    NB: The returned value is a list, ordered by numeric components in the platform.
    In practice the "highest" version (which should be the most "desirable" one is returned first)

    :param list osList: list of platforms defined by resource providers
    :param dict osCompatibilityDict: dictionary with OS compatibility information
    :return: a list of DIRAC platforms that can be specified in job descriptions
    """

    if not osCompatibilityDict:
        return S_ERROR("OS compatibility info not found")

    # making an OS -> platforms dict
    os2PlatformDict = dict()
    for platform, osItems in osCompatibilityDict.items():
        for osItem in osItems:
            os2PlatformDict.setdefault(osItem, set()).add(platform)

    platforms = set()
    for os in osList:
        platforms |= os2PlatformDict.get(os, set())

    if not platforms:
        return S_ERROR(f"No compatible DIRAC platform found for {','.join(osList)}")

    return S_OK(sorted(platforms, key=_platformSortKey, reverse=True))


def _platformSortKey(version: str) -> list[str]:
    # Loosely based on distutils.version.LooseVersion
    parts = []
    for part in re.split(r"(\d+|[a-z]+|\.| -)", version.lower()):
        if not part or part == ".":
            continue
        if part[:1] in "0123456789":
            part = part.zfill(8)
        else:
            while parts and parts[-1] == "00000000":
                parts.pop()
        parts.append(part)
    return parts
