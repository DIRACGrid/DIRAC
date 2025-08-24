#!/usr/bin/env python3
"""
Pin DIRACCommon version in setup.cfg during deployment.

This script is used during the deployment process to ensure DIRAC
depends on the exact version of DIRACCommon being released.
"""

import re
import sys
from pathlib import Path
import subprocess


def get_diraccommon_version():
    """Get the current version of DIRACCommon from setuptools_scm."""
    result = subprocess.run(
        ["python", "-m", "setuptools_scm"], cwd="dirac-common", capture_output=True, text=True, check=True
    )
    # Extract version from output like "Guessed Version 9.0.0a65.dev7+g995f95504"
    version_match = re.search(r"Guessed Version (\S+)", result.stdout)
    if not version_match:
        # Try direct output format
        version = result.stdout.strip()
    else:
        version = version_match.group(1)

    # Clean up the version for release (remove dev and git hash parts)
    version = re.sub(r"(\.dev|\+g).+", "", version)
    return version


def pin_diraccommon_version(version):
    """Pin DIRACCommon to exact version in setup.cfg."""
    setup_cfg = Path("setup.cfg")
    content = setup_cfg.read_text()

    # Replace the DIRACCommon line with exact version pin
    updated_content = re.sub(r"^(\s*)DIRACCommon\s*$", f"\\1DIRACCommon=={version}", content, flags=re.MULTILINE)

    if content == updated_content:
        print(f"Warning: DIRACCommon line not found or already pinned in setup.cfg")
        return False

    setup_cfg.write_text(updated_content)
    print(f"Pinned DIRACCommon to version {version} in setup.cfg")
    return True


def main():
    if len(sys.argv) > 1:
        version = sys.argv[1]
    else:
        version = get_diraccommon_version()

    if pin_diraccommon_version(version):
        print(f"Successfully pinned DIRACCommon to {version}")
        sys.exit(0)
    else:
        print("Failed to pin DIRACCommon version")
        sys.exit(1)


if __name__ == "__main__":
    main()
