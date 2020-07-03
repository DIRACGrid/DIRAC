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

dirac-restart-component DataManagement S3Gateway "$DEBUG"

echo -e "*** $(date -u) **** DONE Adding S3-INDIRECT SERVER CONFIGURATION"

# Here, if we are testing HTTPS services, we install the equivalent services and replace the URL in the CS

if [[ "${TESTBRANCH:-No}" = "Yes" ]];
then
  echo -e "*** $(date -u) **** Installing Tornado based services"

  # Find all Tornado Handler
  # We ignore the Configuration for now because it is a bit special (the master needs to run in a separate process)
  # system_component is a space separated System (without the word System), and Component, without the 'Handler.py'.
  # For example "DataManagement TornadoFileCatalog"
  while read -r system_component;
  do
    echo -e "*** $(date -u) **** Installing Tornado service ${system_component}"
    # do NOT put quotes around ${system_component} since
    # we want it to be seen as two arguments
    # shellcheck disable=SC2086
    dirac-install-tornado-service -ddd ${system_component};
    # origin_component is the original service before Tornado (FileCatalog vs TornadoFileCatalog for example)
    orig_component=$(echo "${system_component}" | sed 's/Tornado//g' | awk '{print $2}');
    # Replace the dips url with the https url in the cs, assuming port 8443
    sed -E "s|( +${orig_component} = )dips://([a-z]+)(:[0-9]+)(/.*/)(.*)|\1https://\2:8443\4Tornado\5|g" -i  "${SERVERINSTALLDIR}"/etc/Production.cfg
  done< <(find "${SERVERINSTALLDIR}"/DIRAC/ -name 'Tornado*Handler.py' | grep -v Configuration | sed -e 's/Handler.py//g' -e 's/System//g'| awk -F '/' '{print $(NF-2), $NF}')

  # Restart the CS to take all that into account
  dirac-restart-component Configuration Server "$DEBUG"

  echo -e "*** $(date -u) **** DONE Installing Tornado services"
fi