#!/bin/bash

# write a config file for dteam user
export DCOMMANDS_CONFIG_DIR=${TMPDIR}

CONFFILE="${DCOMMANDS_CONFIG_DIR}/dcommands.conf"
cat > "${CONFFILE}" <<EOF
[global]
default_profile = dteam_user

[dteam_user]
group_name = dteam_user
home_dir = /dteam/diracCertification
default_se = UKI-LT2-IC-HEP-disk

EOF

echo "(1) Testing the dpwd command: Show current dir (in file catalog)"
dpwd
echo $'\n'"(2) Testing dls command: List home dir (in file catalog)"
dls
echo $'\n'"(3) Show shorthand commands settings"
dgetenv

echo $'\n'"This script has finished."
