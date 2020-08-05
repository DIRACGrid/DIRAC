#!/usr/bin/env bash
# http://redsymbol.net/articles/unofficial-bash-strict-mode/
set -euo pipefail
IFS=$'\n\t'

if [[ "$(jq --raw-output '.pull_request' "${GITHUB_EVENT_PATH}")" == "null" ]]; then
    git fetch "origin" 2>/dev/null >/dev/null
    LATEST_SHA256=$(git rev-parse "$(jq --raw-output '.ref' "${GITHUB_EVENT_PATH}" | sed 's@^refs/heads/@refs/remotes/origin/@')")
    CURRENT_SHA256=$(git rev-parse HEAD)
else
    git remote add "fail-fast-remote" "$(jq --raw-output '.pull_request.head.repo.clone_url' "${GITHUB_EVENT_PATH}")" 2>/dev/null >/dev/null
    git fetch "fail-fast-remote" 2>/dev/null >/dev/null
    LATEST_SHA256=$(git rev-parse "refs/remotes/fail-fast-remote/$(jq --raw-output '.pull_request.head.ref' "${GITHUB_EVENT_PATH}")")
    CURRENT_SHA256=$(jq --raw-output '.pull_request.head.sha' "${GITHUB_EVENT_PATH}")
fi

if [[ "${LATEST_SHA256}" != "${CURRENT_SHA256}" ]]; then
    echo "Latest commit is ${LATEST_SHA256}, exiting to avoid wasting CI resources"
    exit 1
fi
