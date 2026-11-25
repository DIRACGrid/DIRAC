#!/usr/bin/env python
import argparse
from pathlib import Path

from packaging.version import Version
import requests


def make_release(version: str, commit_hash: str, release_notes: str = "") -> None:
    """Create a draft GitHub release, for a release manager to publish.

    Publishing the draft creates the tag. A pre-release version, as determined by PEP-440,
    is marked as a pre-release so that it never becomes the "latest" release.

    :param version: The version of the new release
    :param commit_hash: Git revision used for the release
    :param release_notes: Release notes
    """
    r = requests.post(
        f"{api_root}/releases",
        json={
            "tag_name": version,
            "target_commitish": commit_hash,
            "body": release_notes,
            "draft": True,
            "prerelease": Version(version).is_prerelease,
        },
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    print(f"Created draft release at: {r.json()['html_url']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--repo", default="DIRACGrid/DIRAC")
    parser.add_argument("--version", required=True)
    parser.add_argument("--rev", required=True)
    parser.add_argument("--release-notes-fn", required=True)
    args = parser.parse_args()

    token = args.token
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}",
    }
    api_root = f"https://api.github.com/repos/{args.repo}"
    release_notes = Path(args.release_notes_fn).read_text()

    if not args.version.startswith("v"):
        raise ValueError('For consistency versions must start with "v"')

    make_release(args.version, args.rev, release_notes=release_notes)
