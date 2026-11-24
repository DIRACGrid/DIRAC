#!/usr/bin/env python3
"""Check a release version and print the release branch it belongs to.

Usage: check_release_version.py VERSION [BRANCH]

With BRANCH, also check that VERSION may be released from it: any version from
integration, only X.Y.* from rel-vXrY.
"""
import sys

from packaging.version import Version


def main() -> None:
    version = Version(sys.argv[1])
    if version.is_devrelease:
        raise SystemExit(f"{version} is a development version")
    expected = f"rel-v{version.major}r{version.minor}"
    if len(sys.argv) > 2 and sys.argv[2] not in ("integration", expected):
        raise SystemExit(f"{version} cannot be released from {sys.argv[2]}, only from integration or {expected}")
    print(expected)


if __name__ == "__main__":
    main()
