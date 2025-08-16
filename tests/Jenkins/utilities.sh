#!/usr/bin/env bash
############################################
# General utility functions
############################################

if [[ "${TESTCODE}" ]]; then
  # Path to ci config files
  CI_CONFIG=${TESTCODE}/DIRAC/tests/Jenkins/config/ci
fi

# default: this function fixes some default values

default() {

  if [[ -z "${JENKINS_SITE}" ]]; then
    JENKINS_SITE='DIRAC.Jenkins.ch'
  fi

  if [[ -z "${JENKINS_CE}" ]]; then
    JENKINS_CE='jenkins.cern.ch'
  fi

  if [[ -z "${JENKINS_QUEUE}" ]]; then
    JENKINS_QUEUE='jenkins-queue_not_important'
  fi
}


# Finders... functions devoted to find DBs, Services, versions, etc..

#.............................................................................
#
# findSystems:
#
#   gets all system names from *DIRAC code and writes them to a file
#   named systems.
#
#.............................................................................
findSystems() {
  echo '==> [findSystems]'

  if ! cd "${TESTCODE}"; then
    echo "ERROR: cannot change to ${TESTCODE}" >&2
    exit 1
  fi
  python -m DIRAC.Core.Utilities.Extensions findSystems > systems

  echo "found $(wc -l systems)"
}


#.............................................................................
#
# findDatabases:
#
#   gets all database names from *DIRAC code and writes them to a file
#   named databases.
#
#.............................................................................

findDatabases() {
  echo '==> [findDatabases]'

  if [[ -n "${1}" ]]; then
    DBstoSearch=${1}
    if [[ "${DBstoSearch}" = "exclude" ]]; then
      echo "==> excluding ${2}"
      DBstoExclude=${2}
      DBstoSearch=' '
    fi
  else
    DBstoExclude='notExcluding'
  fi

  if ! cd "${SERVERINSTALLDIR}"; then
    echo "ERROR: cannot change to ${SERVERINSTALLDIR}" >&2
    exit 1
  fi

  #
  # HACK ALERT:
  #
  # We are avoiding, FileCatalogDB FileCatalogWithFkAndPsDB that is installed in other ways
  #  and InstalledComponentsDB which is installed at the beginning
  #
  if [[ -n "${DBstoExclude}" ]]; then
    python -m DIRAC.Core.Utilities.Extensions findDatabases | grep -vE '(FileCatalogDB|FileCatalogWithFkAndPsDB|InstalledComponentsDB)' | grep -v "${DBstoExclude}" > databases
  else
    python -m DIRAC.Core.Utilities.Extensions findDatabases | grep -vE '(FileCatalogDB|FileCatalogWithFkAndPsDB|InstalledComponentsDB)' | grep "${DBstoSearch}" > databases
  fi

  echo "found $(wc -l databases)"
}


#-------------------------------------------------------------------------------
# findServices:
#
#   gets all service names from *DIRAC code and writes them to a file
#   named services. Needs an input for searching
#
#-------------------------------------------------------------------------------

findServices(){
  echo '==> [findServices]'


  if [[ -n "${1}" ]]; then
    ServicestoSearch=${1}
    if [[ "${ServicestoSearch}" = "exclude" ]]; then
      echo "==> excluding ${2}"
      ServicestoExclude=${2}
      ServicestoSearch=' '
    fi
  else
    ServicestoExclude=' '
  fi

  if ! cd "${SERVERINSTALLDIR}"; then
    echo 'ERROR: cannot change to ' "${SERVERINSTALLDIR}" >&2
    exit 1
  fi
  if [[ -n "${ServicestoExclude}" ]]; then
    python -m DIRAC.Core.Utilities.Extensions findServices | grep -v "${ServicestoExclude}" > services
  else
    python -m DIRAC.Core.Utilities.Extensions findServices | grep "${ServicestoSearch}" > services
  fi

  echo "found $(wc -l services)"
}

findAgents(){
  echo '==> [findAgents]'

  if [[ -n "${1}" ]]; then
    AgentstoSearch=$1
    if [[ "${AgentstoSearch}" = "exclude" ]]; then
      echo "==> excluding ${2}"
      AgentstoExclude=${2}
      AgentstoSearch=' '
    fi
  else
    AgentstoExclude='notExcluding'
  fi

  if ! cd "${SERVERINSTALLDIR}"; then
    echo 'ERROR: cannot change to ' "${SERVERINSTALLDIR}" >&2
    exit 1
  fi

  # Always remove the JobAgent, which is not a real agent
  if [[ -n "${AgentstoExclude}" ]]; then
    python -m DIRAC.Core.Utilities.Extensions findAgents | grep -v "WorkloadManagementSystem JobAgent" | grep -v "${AgentstoExclude}" > agents
  else
    python -m DIRAC.Core.Utilities.Extensions findAgents | grep -v "WorkloadManagementSystem JobAgent" | grep "${AgentstoSearch}" > agents
  fi

  echo "found $(wc -l agents)"
}


#-------------------------------------------------------------------------------
# findServices:
#
#   gets all *legacy adapted* service names from *DIRAC code and writes them to a file
#   named futureServices. Needs an input for searching
#   Little bit different from findAgents as we allow multiple exclusions
#
#-------------------------------------------------------------------------------


findFutureServices() {
  echo '==> [findFutureServices]'

  # If first argument is "exclude", take the rest as the exclusion string
  local disabledPattern=""
  if [[ "${1:-}" == "exclude" ]]; then
    shift
    # Join all remaining arguments into a single string
    local disabledServices="$*"
    echo "==> excluding: $disabledServices"

    # Convert services into a grep-friendly pattern (fixed strings)
    # We replace spaces with newlines to create multiple patterns
    disabledPattern=$(printf "%s" "$disabledServices" | tr ' ' '\n')
  fi

  if ! cd "${SERVERINSTALLDIR}"; then
    echo 'ERROR: cannot change to ' "${SERVERINSTALLDIR}" >&2
    exit 1
  fi

  # Build the list of services
  python -m DIRAC.Core.Utilities.Extensions findFutureServices \
    | sed -e 's/System / /g' \
          -e 's/Handler//g' \
          -e 's/Client//g' \
          -e 's/ /\//g' \
    | grep -v "JobAgent" > futureServices

  # Apply exclusion if any
  if [[ -n "$disabledPattern" ]]; then
    # Use grep -F for fixed strings (space-safe)
    grep -Fv -f <(printf "%s\n" $disabledPattern) futureServices > futureServices.tmp
    mv futureServices.tmp futureServices
  fi

  echo "found $(wc -l < futureServices)"
}


#-------------------------------------------------------------------------------
# findExecutors:
#
#   gets all executor names from *DIRAC code and writes them to a file
#   named executors.
#
#-------------------------------------------------------------------------------

findExecutors(){
  echo '==> [findExecutors]'

  python -m DIRAC.Core.Utilities.Extensions findExecutors > executors

  echo "found $(wc -l executors)"
}



#-------------------------------------------------------------------------------
# finalCleanup:
#
#   remove symlinks, remove cached info
#-------------------------------------------------------------------------------

finalCleanup(){
  echo '==> [finalCleanup]'

  rm -Rf etc/grid-security/certificates
  rm -f etc/grid-security/host*.pem
  rm -Rf /tmp/x*
  rm -rRf .installCache
  rm -Rf /tmp/tmp.*
}


getCFGFile() {
  echo '==> [getCFGFile]'

  cp "$INSTALL_CFG_FILE" "${SERVERINSTALLDIR}/"
  sed -i "s/VAR_Release/${DIRAC_RELEASE}/g" "${SERVERINSTALLDIR}/install.cfg"
}


####################################################
# This installs the DIRAC client
#
# To know what to install, it:
# - can get a $DIRAC_RELEASE env var defined
# - or list of $ALTERNATIVE_MODULES
#
# it also wants the env variable $CSURL
#
# dirac-configure also accepts a env variable $CONFIGUREOPTIONS
#  (e.g. useful for extensions or for using the certificates:
#   --UseServerCertificate -o /DIRAC/Security/CertFile=some/location.pem -o /DIRAC/Security/KeyFile=some/location.pem

installDIRAC() {
  echo '==> Installing DIRAC client'
  if ! cd "${CLIENTINSTALLDIR}"; then
    echo "ERROR: cannot change to ${CLIENTINSTALLDIR}" >&2
    exit 1
  fi

  if [[ -n "${DIRACOS_TARBALL_PATH:-}" ]]; then
    cp "${DIRACOS_TARBALL_PATH}" "installer.sh"
  else
    if [[ -n "${DIRACOSVER:-}" ]] && [[ "${DIRACOSVER}" != "master" ]]; then
      DIRACOS2_URL="https://github.com/DIRACGrid/DIRACOS2/releases/download/${DIRACOSVER}/DIRACOS-Linux-x86_64.sh"
    else
      DIRACOS2_URL="https://github.com/DIRACGrid/DIRACOS2/releases/latest/download/DIRACOS-Linux-x86_64.sh"
    fi
    curl -L "${DIRACOS2_URL}" > "installer.sh"
  fi
  bash "installer.sh"
  rm "installer.sh"
  # TODO: Remove
  echo "source \"$PWD/diracos/diracosrc\"" > "$PWD/bashrc"
  echo "export X509_CERT_DIR=\"$PWD/diracos/etc/grid-security/certificates\"" >> "$PWD/bashrc"
  source diracos/diracosrc

  if [[ -n "${DIRAC_RELEASE+x}" ]]; then
    if [[ -z "${ALTERNATIVE_MODULES}" ]]; then
      pip install DIRAC "${DIRAC_RELEASE}"
    fi
  fi

  if [[ -n "${INSTALLATION_BRANCH}" ]]; then
    # Use this for (e.g.) running backward-compatibility tests
    echo "pip-installing DIRAC from git+https://github.com/DIRACGrid/DIRAC.git@${INSTALLATION_BRANCH}#egg=DIRAC[client]"
    pip install "git+https://github.com/DIRACGrid/DIRAC.git@${INSTALLATION_BRANCH}#egg=DIRAC[client]"
  else
    for module_path in "${ALTERNATIVE_MODULES[@]}"; do
      pip install ${PIP_INSTALL_EXTRA_ARGS:-} "${module_path}"
    done
  fi

  echo "==> Done installing, now configuring"

  if ! dirac-proxy-init --nocs --no-upload; then
    echo 'ERROR: dirac-proxy-init failed' >&2
    exit 1
  fi

  configureArgs=()
  if [[ "${TEST_DIRACX:-}" = "Yes" ]]; then
    configureArgs+=("--LegacyExchangeApiKey=diracx:legacy:InsecureChangeMe")
  fi

  if [[ -n "${INSTALLATION_BRANCH}" ]]; then
    # Use this for (e.g.) running backward-compatibility tests
    cmd="dirac-configure -S ${DIRACSETUP} -C ${CSURL} --SkipCAChecks "${configureArgs[@]}" ${CONFIGUREOPTIONS} ${DEBUG}"
  else
    cmd="dirac-configure -C ${CSURL} --SkipCAChecks ${CONFIGUREOPTIONS} ${DEBUG}"
  fi
  if ! bash -c "${cmd}"; then
    echo 'ERROR: dirac-configure failed' >&2
    exit 1
  fi

  echo '==> Done installDIRAC'

  echo "==> Installing main branch of diracx"
  installDIRACX core client

  echo "$DIRAC"
  echo "$PATH"

}

##############################################################################
# Install DiracX either from wheels or from github
# Arguments: list of DiracX submodule module names to install (core, client, etc.)

function installDIRACX() {
  # If we have no dist, we get the last tag to use it
  if [[ -z "${DIRACX_CUSTOM_SOURCE_PREFIXES:-}" ]]; then
    # Install latest by default
    pip install diracx
    return 0
  fi

  for wheel_name in "$@"; do
    wheels=( $(find "${DIRACX_CUSTOM_SOURCE_PREFIXES}" -name "diracx_${wheel_name}-*.whl") )
    if [[ ! ${#wheels[@]} -eq 1 ]]; then
        echo "ERROR: Multiple or no wheels found for ${wheel_name} in ${DIRACX_CUSTOM_SOURCE_PREFIXES}"
        exit 1
    fi
    pip install "${wheels[0]}"
  done
}

##############################################################################
# This function submits a job or more (it assumes a DIRAC client is installed)
# it needs the following environment variables:
# $DIRACUSERDN for the DN of the user used to submit the job
# $DIRACUSERROLE for the role of the proxy of the user used to submit the job

submitJob() {
  #This has to be executed from the ${CLIENTINSTALLDIR}

  echo -e "==> Submitting a simple job"
  if ! cd "${CLIENTINSTALLDIR}"; then
    echo "ERROR: cannot change to ${CLIENTINSTALLDIR}" >&2
    exit 1
  fi

  export PYTHONPATH=${TESTCODE}:${PYTHONPATH}

  if [[ -f "${TESTCODE}/${VO}DIRAC/tests/Jenkins/dirac-test-job.py" ]]; then
    cp "${TESTCODE}/${VO}DIRAC/tests/Jenkins/dirac-test-job.py" "."
  else
    cp "${TESTCODE}/DIRAC/tests/Jenkins/dirac-test-job.py" "."
  fi
  python dirac-test-job.py "${DEBUG}"

  echo '==> Done submitJob'
}

#.............................................................................
#
# diracUserAndGroup:
#
#   create a user and a group (the CS has to be running)
#
#.............................................................................

# Create a user and a group (the CS has to be running)
#
# This function is used to create a user and a group in the CS.
# The user is created with the name 'ciuser' and the group is created with the name 'prod'
# The user is added to the group and the shifter is added to the group.
# The user is also added to the group 'jenkins_fcadmin' and 'jenkins_user'
diracUserAndGroup() {
  echo '==> [diracUserAndGroup]'

  if ! dirac-admin-add-user -N ciuser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=ciuser -M lhcb-dirac-ci@cern.ch -G dirac_user -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-user failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-user -N pilot -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=pilot -M lhcb-dirac-ci@cern.ch -G dirac_user -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-user failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-user -N trialUser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=trialUser -M lhcb-dirac-ci@cern.ch -G dirac_user -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-user failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G prod -U adminusername,ciuser,trialUser -P Operator,FullDelegation,ProxyManagement,ServiceAdministrator,JobAdministrator,CSAdministrator,FileCatalogManagement,SiteManager,NormalUser,ProductionManagement VO=vo -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G pilot -U pilot -P GenericPilot,LimitedDelegation VO=vo -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G jenkins_fcadmin -U adminusername,ciuser,trialUser -P FileCatalogManagement,NormalUser -o /DIRAC/Security/UseServerCertificate=True VO=vo "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G jenkins_user -U adminusername,ciuser,trialUser -P NormalUser VO=vo -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter DataManager adminusername prod -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter TestManager adminusername prod -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter ProductionManager adminusername prod -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter LHCbPR adminusername prod -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi
}


#.............................................................................
#
# diracSite:
#
#   add a site (the CS has to be running)
#
#.............................................................................

diracAddSite() {
  echo '==> [diracAddSite]'

  if ! dirac-admin-add-site DIRAC.Jenkins.ch aNameWhatSoEver jenkins.cern.ch -o /DIRAC/Security/UseServerCertificate=yes "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-site failed' >&2
    exit 1
  fi
}

#-------------------------------------------------------------------------------
# diracServices:
#
#   installs all services
#
#-------------------------------------------------------------------------------

diracServices(){
  echo '==> [diracServices]'

  local services=$(cut -d '.' -f 1 < services | grep -v StorageElementHandler | grep -v ^ConfigurationSystem | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g')

  for serv in $services; do
    echo "==> calling dirac-install-component $serv -o /DIRAC/Security/UseServerCertificate=True ${DEBUG}"
    if ! dirac-install-component "$serv" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
      echo 'ERROR: dirac-install-component failed' >&2
      exit 1
    fi
  done
}


diracSEs(){
  echo '==> [diracSEs]'

  echo "==> Installing SE-1"
  seDir=${SERVERINSTALLDIR}/Storage/SE-1
  mkdir -p "${seDir}"
  if ! dirac-install-component DataManagement SE-1 -m StorageElement -p BasePath="${seDir}" -p Port=9148 -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-install-component failed' >&2
    exit 1
  fi

  echo "==> Installing SE-2"
  seDir=${SERVERINSTALLDIR}/Storage/SE-2
  mkdir -p "${seDir}"
  if ! dirac-install-component DataManagement SE-2 -m StorageElement -p BasePath="${seDir}" -p Port=9147 -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
    echo 'ERROR: dirac-install-component failed' >&2
    exit 1
  fi

}


#-------------------------------------------------------------------------------
# diracUninstallServices:
#
#   uninstalls all services
#
#-------------------------------------------------------------------------------

diracUninstallServices(){
  echo '==> [diracUninstallServices]'

  findServices

  # Ignore tornado services
  local services=$(cut -d '.' -f 1 < services | grep -v TokenManager | grep -v ^ConfigurationSystem | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | grep -v Tornado | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g')

  # check if errexit mode is set and disabling as the component may not exist
  local save=$-
  if [[ $save =~ e ]]; then
    set +e
  fi

  for serv in $services; do
    echo '==> calling dirac-uninstall-component' "$serv" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"
    dirac-uninstall-component -f "$serv" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"
  done

  if [[ $save =~ e ]]; then
    set -e
  fi

}


#-------------------------------------------------------------------------------
# diracAgents:
#
#   installs all agents on the file agents
#
#-------------------------------------------------------------------------------

diracAgents(){
  echo '==> [diracAgents]'

  local agents=$(cut -d '.' -f 1 < agents | grep -v CE2CSAgent | grep -v GOCDB2CS | grep -v Bdii2CS | grep -v CacheFeeder | grep -v NetworkAgent | grep -v FrameworkSystem | grep -v DataProcessingProgressAgent | grep -v RAWIntegrityAgent  | grep -v GridSiteWMSMonitoringAgent | grep -v HCAgent | grep -v GridCollectorAgent | grep -v HCProxyAgent | grep -v Nagios | grep -v AncestorFiles | grep -v BKInputData | grep -v LHCbPRProxyAgent | sed 's/System / /g' | sed 's/ /\//g')

  for agent in $agents; do
    if [[ $agent == *" JobAgent"* ]]; then
      echo '==> '
    else
      echo "==> calling dirac-cfg-add-option agent $agent -o /DIRAC/Security/UseServerCertificate=True "
      python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-cfg-add-option.py" "agent" "$agent" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"
      echo "==> calling dirac-agent $agent -o MaxCycles=1 -o /DIRAC/Security/UseServerCertificate=True ${DEBUG}"
      if ! dirac-agent "$agent" -o MaxCycles=1 -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
        echo 'ERROR: dirac-agent failed' >&2
        exit 1
      fi
    fi
  done
}


#-------------------------------------------------------------------------------
# diracDBs:
#
#   installs all databases on the file databases
#
#-------------------------------------------------------------------------------

diracDBs(){
  echo '==> [diracDBs]'

  local dbs=$(cut -d ' ' -f 2 < databases | cut -d '.' -f 1 | grep -v ^RequestDB | grep -v ^FileCatalogDB | grep -v ^InstalledComponentsDB)
  for db in $dbs; do
    if ! dirac-install-db "$db" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
      echo 'ERROR: dirac-install-db failed' >&2
      exit 1
    fi
  done
}

# Drop, then Install manually the DFC
diracDFCDB(){
  echo '==> [diracDFCDB]'

  mysql -u"$DB_ROOTUSER" -p"$DB_ROOTPWD" -h"$DB_HOST" -P"$DB_PORT" -e "DROP DATABASE IF EXISTS FileCatalogDB;"
  SRC_ROOT="$(python -c 'import os; import DIRAC; print(os.path.dirname(DIRAC.__file__))')"
  mysql -u"$DB_ROOTUSER" -p"$DB_ROOTPWD" -h"$DB_HOST" -P"$DB_PORT" < "${SRC_ROOT}/DataManagementSystem/DB/FileCatalogWithFkAndPsDB.sql"
}

# Drop, then manually install the DFC with MultiVOFileCatalogDB
diracMVDFCDB(){
  echo '==> [diracMVDFCDB]'

  SRC_ROOT="$(python -c 'import os; import DIRAC; print(os.path.dirname(DIRAC.__file__))')"
  cp "${SRC_ROOT}/DataManagementSystem/DB/FileCatalogWithFkAndPsDB.sql" "${SRC_ROOT}/DataManagementSystem/DB/MultiVOFileCatalogWithFkAndPsDB.sql"
  sed -i 's/FileCatalogDB/MultiVOFileCatalogDB/g' "${SRC_ROOT}/DataManagementSystem/DB/MultiVOFileCatalogWithFkAndPsDB.sql"
  mysql -u"$DB_ROOTUSER" -p"$DB_ROOTPWD" -h"$DB_HOST" -P"$DB_PORT" -e "DROP DATABASE IF EXISTS MultiVOFileCatalogDB;"
  mysql -u"$DB_ROOTUSER" -p"$DB_ROOTPWD" -h"$DB_HOST" -P"$DB_PORT" < "${SRC_ROOT}/DataManagementSystem/DB/MultiVOFileCatalogWithFkAndPsDB.sql"
  rm "${SRC_ROOT}/DataManagementSystem/DB/MultiVOFileCatalogWithFkAndPsDB.sql"
}

dropDBs(){
  echo '==> [dropDBs]'

  # make dbs a real array to avoid future mistake with escaping
  mapfile -t dbs < <(cut -d ' ' -f 2 < databases | cut -d '.' -f 1 | grep -v ^RequestDB | grep -v ^FileCatalogDB)
  python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-drop-db.py" "${dbs[@]}" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"
}

#-------------------------------------------------------------------------------
# diracOptimizers:
#
#   launch all optimizers from the WorkloadManagementSystem
#
#-------------------------------------------------------------------------------

diracOptimizers(){
  echo '==> [diracOptimizers]'

  local executors=$(cat executors | grep WorkloadManagementSystem | cut -d ' ' -f 2 | grep -v Base)
  for executor in $executors
  do
    echo "==> calling dirac-install-component WorkloadManagement/$executor  -o /DIRAC/Security/UseServerCertificate=True"
    if ! dirac-install-component "WorkloadManagement/$executor" -o /DIRAC/Security/UseServerCertificate=True "${DEBUG}"; then
      echo 'ERROR: dirac-install-component failed' >&2
      exit 1
    fi
  done
}

#-------------------------------------------------------------------------------
# Kill, Stop and Start scripts. Used to clean environment.
#-------------------------------------------------------------------------------


#.............................................................................
#
# killRunsv:
#
#   it makes sure there are no runsv processes running. If it finds any, it
#   terminates it. This means, no more than one Job running this kind of test
#   on the same machine at the same time ( executors =< 1 ). Indeed, it cleans
#   two particular processes, 'runsvdir' and 'runsv'.
#
#.............................................................................

killRunsv() {
  echo '==> [killRunsv]'

  # Bear in mind that we may run with 'errexit' mode. This call, if finds nothing
  # will exit 1 an error, which will make the whole script exit. However, if
  # finds nothing we are good, it means there are not leftover processes from
  # other runs. So, we disable 'errexit' mode for this call.

  # check if errexit mode is set
  local save=$-
  if [[ "${save}" =~ e ]]; then
    set +e
  fi

  pkill runsvdir
  pkill runsv

  if [[ "${save}" =~ e ]]; then
    set -e
  fi

  echo '==> [Done killRunsv]'
}

#.............................................................................
#
# killES:
#
#   it makes sure there are no OpenSearch processes running. If it finds any, it
#   terminates it.
#
#.............................................................................

killES() {
  echo '==> [killES]'
  pkill -u lhcbci opensearch
  echo '==> [Done killES]'
}

#.............................................................................
#
# stopRunsv:
#
#   if runsv is running, it stops it.
#
#.............................................................................

stopRunsv() {
  echo '==> [stopRunsv]'

  # Let's try to be a bit more delicate than the function above

  source "${SERVERINSTALLDIR}/bashrc"
  runsvctrl d "${SERVERINSTALLDIR}/startup/"*
  runsvstat "${SERVERINSTALLDIR}/startup/"*

  # If does not work, let's kill it.
  killRunsv

  echo '==> [Done stopRunsv]'
}



#.............................................................................
#
# setDiracXCreds
#
#   gets creds from x509, extract token, and put it in the right place
#
#.............................................................................
setDiracXCreds() {
    # $1 = DIRAC group
    local group="$1"
    local cache_dir="$HOME/.cache/diracx"
    local creds_file="$cache_dir/credentials.json"
    local tmpfile

    if [[ -z "$group" ]]; then
        echo "Usage: setDiracXCreds <DIRAC_GROUP>" >&2
        return 1
    fi

    # 1. Init DIRAC proxy
    dirac-proxy-init -g "$group"

    # 2. Extract DiracX token from proxy PEM
    tmpfile=$(mktemp)
    python - <<'EOF' > "$tmpfile"
from DIRAC.Core.Security.DiracX import diracxTokenFromPEM
from DIRAC.Core.Security.Locations import getProxyLocation
import json
pem_location = getProxyLocation()
token = diracxTokenFromPEM(pem_location)
if token:
    print(json.dumps(token))
EOF

    # 3. Move to ~/.cache/diracx/credentials.json
    mkdir -p "$cache_dir"
    mv "$tmpfile" "$creds_file"
    echo "DiracX credentials updated at $creds_file"
}
