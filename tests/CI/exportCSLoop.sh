#!/usr/bin/env bash
# This script will export to the `Production.cfg` file to the
# yaml format for diracx every 5 seconds

source /home/dirac/ServerInstallDIR/bashrc
git config --global user.name "DIRAC Server CI"
git config --global user.email "dirac-server-ci@invalid"

while true;
do
    curl -L https://gitlab.cern.ch/chaen/chris-hackaton-cs/-/raw/master/convert-from-legacy.py |DIRAC_COMPAT_ENABLE_CS_CONVERSION=True  ~/ServerInstallDIR/diracos/bin/python - ~/ServerInstallDIR/etc/Production.cfg /cs_store/initialRepo/
    git -C /cs_store/initialRepo/ commit -am "export $(date)"
    sleep 5;
done
