#!/usr/bin/env python
import argparse

from packaging.version import Version
import requests
from uritemplate import expand as uri_expand


def make_release(version, commit_hash, release_notes=""):
    """Create a new GitHub release using the given data

    This function always makes a pre-release first to ensure the "latest" release never corresponds
    to one without artifacts uploaded. If the new version number is not a pre-release, as
    determined by PEP-440, it is promoted to at full release after the uploads have completed
    successfully.

    :param str version: The version of the new release
    :param str commit_hash: Git revision used for the release
    :param str release_notes: Release notes
    """
    # Create a draft release
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
    )
    r.raise_for_status()
    release_data = r.json()
    print(f"Created draft release at: {release_data['html_url']}")

    # Publish the release
    r = requests.patch(
        release_data["url"],
        json={
            "draft": False,
        },
        headers=headers,
    )
    r.raise_for_status()
    release_data = r.json()
    print(f"Published release at: {release_data['html_url']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", required=True)
    parser.add_argument("--owner", default="DIRACGrid")
    parser.add_argument("--repo", default="DIRAC")
    parser.add_argument("--version", required=True)
    parser.add_argument("--rev", required=True)
    args = parser.parse_args()

    token = args.token
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"token {token}",
    }
    api_root = f"https://api.github.com/repos/{args.owner}/{args.repo}"

    if not args.version.startswith("v"):
        raise ValueError('For consistency versions must start with "v"')

    make_release(args.version, args.rev, release_notes="")
