"""Stateless JobDB utilities extracted from DIRAC for DIRACCommon"""

from __future__ import annotations

import base64
import zlib


def compressJDL(jdl):
    """Return compressed JDL string."""
    return base64.b64encode(zlib.compress(jdl.encode(), -1)).decode()


def extractJDL(compressedJDL):
    """Return decompressed JDL string."""
    # the starting bracket is guaranteeed by JobManager.submitJob
    # we need the check to be backward compatible
    if isinstance(compressedJDL, bytes):
        if compressedJDL.startswith(b"["):
            return compressedJDL.decode()
    else:
        if compressedJDL.startswith("["):
            return compressedJDL
    return zlib.decompress(base64.b64decode(compressedJDL)).decode()


def fixJDL(jdl: str) -> str:
    """Fix possible lack of brackets in JDL"""
    # 1.- insert original JDL on DB and get new JobID
    # Fix the possible lack of the brackets in the JDL
    if jdl.strip()[0].find("[") != 0:
        jdl = "[" + jdl + "]"
    return jdl