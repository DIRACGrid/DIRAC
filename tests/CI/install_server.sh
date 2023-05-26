#!/bin/bash
#
#   Executable script to install full DIRAC server
#
#   Requires no additional external arguments
#
#.........................................................
# set -euo pipefail
set -eo pipefail
# IFS=$'\n\t'
set -x

source CONFIG

if [ -n "$DEBUG" ]
then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

echo -e "*** $(date -u) **** Getting the tests ****\n"

mkdir -p "$PWD/TestCode"
cd "$PWD/TestCode"

for repo_path in "${TESTREPO[@]}"; do
    if [[ -d "${repo_path}" ]]; then
        cp -r "${repo_path}" "$(basename "${repo_path}")"
        cd "$(basename "${repo_path}")"
        echo "Using local test repository in branch $(git branch | grep "\*" | sed -e "s/* //")"
        cd -
    else
        git clone "https://github.com/$repo_path/DIRAC.git"
        cd "$(basename "${repo_path}")"
        git checkout "$TESTBRANCH"
        echo "Using remote test repository ${repo_path} in branch ${TESTBRANCH}"
        cd -
    fi
done

cd ..

echo -e "*** $(date -u) **** Got the DIRAC tests ****\n"


echo -e "*** $(date -u) **** Server INSTALLATION START ****\n"

source "${DIRAC_CI_SETUP_SCRIPT}"
sed -i "0,/\(Host = \).*/s//\1$SERVER_HOST/" "${INSTALL_CFG_FILE}"

X509_CERT_DIR=$SERVERINSTALLDIR/etc/grid-security/certificates/ fullInstallDIRAC

echo -e "*** $(date -u) **** Server INSTALLATION DONE ****\n"

echo -e "*** $(date -u) **** Adding S3-INDIRECT SERVER CONFIGURATION"


# This specific configuration has to be local to the server only
cat >> "$SERVERINSTALLDIR"/etc/dirac.cfg <<EOL
Resources
{
  StorageElements
  {
    S3-INDIRECT
    {
      S3
      {
        Aws_access_key_id = fakeId
        Aws_secret_access_key = fakeKey
      }
    }
  }
}
EOL

if [[ "${TEST_HTTPS:-Yes}" = "No" ]]; then
  dirac-restart-component DataManagement S3Gateway "$DEBUG"
else
  dirac-restart-component Tornado Tornado "$DEBUG"
fi

echo -e "*** $(date -u) **** DONE Adding S3-INDIRECT SERVER CONFIGURATION"
