#!/usr/bin/env bash
# This script will export to the `Production.cfg` file to the
# yaml format for diracx every 5 seconds
set -x
exec >>/tmp/cs-loop.log 2>&1

while [[ ! -f /home/dirac/ServerInstallDIR/bashrc ]]; do
    sleep 1;
done
sleep 1;
source /home/dirac/ServerInstallDIR/bashrc

git config --global user.name "DIRAC Server CI"
git config --global user.email "dirac-server-ci@invalid"

while true; do
    DIRAC_COMPAT_ENABLE_CS_CONVERSION=x dirac internal legacy cs-sync \
        "$DIRACOS/etc/Production.cfg" \
        /home/dirac/TestCode/diracx/tests/cli/legacy/cs_sync/convert_integration_test.yaml \
        /cs_store/initialRepo/default.yml
    git --git-dir=.git -C /cs_store/initialRepo/ commit -am "export $(date)"
    if [[ "${1}" == "--once" ]]; then
        break
    fi
    sleep 5;
done
