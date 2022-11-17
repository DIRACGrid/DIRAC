#!/usr/bin/env bash
############################################
# General utility functions
############################################

if [[ -z "${SERVERINSTALLDIR}" ]]; then
  if [[ -z "${PILOTINSTALLDIR}" ]]; then
    echo 'Environmental variable "PILOTINSTALLDIR" is not set.'
    exit 1
  else
    SERVERINSTALLDIR=${PILOTINSTALLDIR}
  fi
fi

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
    ServicestoExclude='notExcluding'
  fi

  if ! cd "${SERVERINSTALLDIR}" -ne 0; then
    echo 'ERROR: cannot change to ' "${SERVERINSTALLDIR}" >&2
    exit 1
  fi
  if [[ -n "${ServicestoExclude}" ]]; then
    python -m DIRAC.Core.Utilities.Extensions findServices | grep -v "${ServicestoExclude}" > services
  else
    python -m DIRAC.Core.Utilities.Extensions findServices | grep "${ServicestoSearch}"> services
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
# it also wants the env variables $DIRACSETUP and $CSURL
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
  for module_path in "${ALTERNATIVE_MODULES[@]}"; do
    pip install ${PIP_INSTALL_EXTRA_ARGS:-} "${module_path}"
  done

  echo "$DIRAC"
  echo "$PATH"

  # now configuring
  cmd="dirac-configure -S ${DIRACSETUP} -C ${CSURL} --SkipCAChecks ${CONFIGUREOPTIONS} ${DEBUG}"
  if ! bash -c "${cmd}"; then
    echo 'ERROR: dirac-configure failed' >&2
    exit 1
  fi

  echo '==> Done installDIRAC'
}

##############################################################################
# This function submits a job or more (it assumes a DIRAC client is installed)
# it needs the following environment variables:
# $DIRACUSERDN for the DN of the user used to submit the job
# $DIRACUSERROLE for the role of the proxy of the user used to submit the job
# $DIRACSETUP for the setup

submitJob() {
  #This has to be executed from the ${CLIENTINSTALLDIR}

  echo -e "==> Submitting a simple job"
  if ! cd "${CLIENTINSTALLDIR}"; then
    echo "ERROR: cannot change to ${CLIENTINSTALLDIR}" >&2
    exit 1
  fi

  export PYTHONPATH=${TESTCODE}:${PYTHONPATH}
  # Get a proxy and submit the job: this job will go to the certification setup, so we suppose the JobManager there is accepting jobs

  # check if errexit mode is set and disabling as the component may not exist
  save=$-
  if [[ $save =~ e ]]; then
    set +e
  fi
  getUserProxy #this won't really download the proxy, so that's why the next command is needed
  # re-enabling it
  if [[ ${save} =~ e ]]; then
    set -e
  fi

  dirac-admin-get-proxy "${DIRACUSERDN}" "${DIRACUSERROLE}" -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem -o /DIRAC/Setup="${DIRACSETUP}" --out="/tmp/x509up_u${UID}" -ddd
  if [[ -f "${TESTCODE}/${VO}DIRAC/tests/Jenkins/dirac-test-job.py" ]]; then
    cp "${TESTCODE}/${VO}DIRAC/tests/Jenkins/dirac-test-job.py" "."
  else
    cp "${TESTCODE}/DIRAC/tests/Jenkins/dirac-test-job.py" "."
  fi
  python dirac-test-job.py -o "/DIRAC/Setup=${DIRACSETUP}" "${DEBUG}"

  echo '==> Done submitJob'
}

getUserProxy() {

  echo '==> Started getUserProxy'

  cp "${TESTCODE}/DIRAC/tests/Jenkins/dirac-cfg-update.py" "."

  if [[ -e "${CLIENTINSTALLDIR}/etc/dirac.cfg" ]]; then
    cfgFile="${CLIENTINSTALLDIR}/etc/dirac.cfg"
  elif [[ -e "${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg" ]]; then
    cfgFile="${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg"
  fi

  if ! python dirac-cfg-update.py -S "${DIRACSETUP}" --cfg "${cfgFile}" -F "${cfgFile}" -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem "${DEBUG}"; then
    echo 'ERROR: dirac-cfg-update failed' >&2
    exit 1
  fi

  #Getting a user proxy, so that we can run jobs
  if ! downloadProxy; then
    echo 'ERROR: downloadProxy failed' >&2
    exit 1
  fi

  echo '==> Done getUserProxy'
}


#-------------------------------------------------------------------------------
# OPEN SSL... let's create a fake CA and certificates
#-------------------------------------------------------------------------------


# function generateCA()
#
# This generates the CA that will be used to sign the server and client certificates

generateCA() {
  echo '==> [generateCA]'

  mkdir -p "${SERVERINSTALLDIR}/etc/grid-security/certificates"
  mkdir -p "${SERVERINSTALLDIR}/etc/grid-security/ca/"
  if ! cd "${SERVERINSTALLDIR}/etc/grid-security/ca"; then
    echo "ERROR: cannot change to ${SERVERINSTALLDIR}/etc/grid-security/ca" >&2
    exit 1
  fi

  # Initialize the ca
  mkdir -p newcerts certs crl
  touch index.txt
  echo 1000 > serial
  echo 1000 > crlnumber

  # Create the CA key
  openssl genrsa -out ca.key.pem 2048            # for unencrypted key
  chmod 400 ca.key.pem


  # Prepare OpenSSL config file, it contains extensions to put into place,
  # DN configuration, etc..
  cp "${CI_CONFIG}/openssl_config_ca.cnf" "openssl_config_ca.cnf"
  sed -i "s|#GRIDSECURITY#|${SERVERINSTALLDIR}/etc/grid-security|g" openssl_config_ca.cnf


  # Generate the CA certificate
  openssl req -config openssl_config_ca.cnf \
              -key ca.key.pem \
              -new -x509 \
              -days 7300 \
              -sha256 \
              -extensions v3_ca \
              -out ca.cert.pem

  # Copy the CA to the list of trusted CA
  cp ca.cert.pem "${SERVERINSTALLDIR}/etc/grid-security/certificates/"

  # Generate the hash link file required by openSSL to index CA certificates
  caHash=$(openssl x509 -in ca.cert.pem -noout -hash)
  # We make a relative symlink on purpose (i.e. not the full path to ca.cert.pem)
  # because otherwsie the BundleDeliveryClient will send the full path, which
  # will be wrong on the client
  ln -s "ca.cert.pem" "${SERVERINSTALLDIR}/etc/grid-security/certificates/$caHash.0"
}

#.............................................................................
#
# function generateCertificates
#
#   This function generates a random host certificate ( certificate and key ),
#   which will be stored on etc/grid-security.
#   We use the self signed CA created by generateCA function
#   In real, we'd copy them from
#   CVMFS:
#     /cvmfs/grid.cern.ch/etc/grid-security/certificates
#
#   Additional info:
#     http://www.openssl.org/docs/apps/req.html
#
#.............................................................................

generateCertificates() {
  echo '==> [generateCertificates]'
  nDays=${1:-7}

  mkdir -p "${SERVERINSTALLDIR}/etc/grid-security/"
  if ! cd "${SERVERINSTALLDIR}/etc/grid-security/"; then
    echo "ERROR: cannot change to ${SERVERINSTALLDIR}/etc/grid-security/" >&2
    exit 1
  fi

  # Generate private RSA key
  openssl genrsa -out hostkey.pem 2048  &> /dev/null
  chmod 400 hostkey.pem

  # Prepare OpenSSL config file, it contains extensions to put into place,
  # DN configuration, etc..
  cp "${CI_CONFIG}/openssl_config_host.cnf" "openssl_config_host.cnf"

  # man hostname to see why we use --all-fqdns
  # Note: if there's no dns entry for the localhost, the fqdns will be empty
  # so we append to it the local hostname, and we take the first one in the list
  fqdn=$( (hostname --all-fqdn; hostname ) | paste -sd ' ' | awk '{print $1}')
  sed -i "s/#hostname#/$fqdn/g" openssl_config_host.cnf

  # Generate X509 Certificate request based on the private key and the OpenSSL configuration
  # file, valid for nDays days (default 1).
  openssl req -config openssl_config_host.cnf \
              -key hostkey.pem \
              -new \
              -sha256 \
              -out request.csr.pem

  # Sign it using the self generated CA
  openssl ca -config "${SERVERINSTALLDIR}/etc/grid-security/ca/openssl_config_ca.cnf" \
       -days "$nDays" \
       -extensions server_cert \
       -batch \
       -in request.csr.pem \
       -out hostcert.pem

  cd -
}


#.............................................................................
#
# generateUserCredentials:
#
#   Given we know the "CA" certificates, we can use them to sign a randomly
#   generated key / host certificate. This function is very similar to
#   generateCertificates. User credentials will be stored at:
#     ${SERVERINSTALLDIR}/user
#   The user will be called "ciuser". Do not confuse with the admin user,
#   which is "ci".
#   The argument that can be passed is the validity of the certificate
#
#   Additional info:
#     http://acs.lbl.gov/~boverhof/openssl_certs.html
#
#.............................................................................

generateUserCredentials() {
  echo '==> [generateUserCredentials]'

  # validity of the certificate
  nDays=${1:-7}

  USERCERTDIR=${SERVERINSTALLDIR}/user
  # Generate directory where to store credentials
  mkdir -p "${USERCERTDIR}"
  if ! cd "${USERCERTDIR}"; then
    echo "ERROR: cannot change to ${USERCERTDIR}" >&2
    exit 1
  fi

  # What is that ?
  save=$-
  if [[ $save =~ e ]]; then
    set +e
  fi

  cp "${CI_CONFIG}/openssl_config_user.cnf" "${USERCERTDIR}/openssl_config_user.cnf"
  if [[ $save =~ e ]]; then
    set -e
  fi

  openssl genrsa -out client.key 2048 &> /dev/null
  chmod 400 client.key

  openssl req -config "${USERCERTDIR}/openssl_config_user.cnf" \
              -key "${USERCERTDIR}/client.key" \
              -new \
              -out "$USERCERTDIR/client.req"

  openssl ca -config "${SERVERINSTALLDIR}/etc/grid-security/ca/openssl_config_ca.cnf" \
             -extensions usr_cert \
             -batch \
             -days "$nDays" \
             -in "$USERCERTDIR/client.req" \
             -out "$USERCERTDIR/client.pem"
}


#.............................................................................
#
# diracCredentials:
#
#   hacks CS service to create a first dirac_admin proxy that will be used
#   to install the components and run the test ( some of them ).
#
#.............................................................................

diracCredentials() {
  echo '==> [diracCredentials]'

  sed -i 's/commitNewData = CSAdministrator/commitNewData = authenticated/g' "${SERVERINSTALLDIR}/etc/Configuration_Server.cfg"
  if ! dirac-login dirac_admin --nocs -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" -T 72 "${DEBUG}"; then
    echo 'ERROR: dirac-login failed' >&2
    exit 1
  fi
  sed -i 's/commitNewData = authenticated/commitNewData = CSAdministrator/g' "${SERVERINSTALLDIR}/etc/Configuration_Server.cfg"
}



#.............................................................................
#
# diracUserAndGroup:
#
#   create a user and a group (the CS has to be running)
#
#.............................................................................

diracUserAndGroup() {
  echo '==> [diracUserAndGroup]'

  if ! dirac-admin-add-user -N ciuser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=ciuser -M lhcb-dirac-ci@cern.ch -G dirac_user "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-user failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-user -N trialUser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=trialUser -M lhcb-dirac-ci@cern.ch -G dirac_user "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-user failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G prod -U adminusername,ciuser,trialUser -P Operator,FullDelegation,ProxyManagement,ServiceAdministrator,JobAdministrator,CSAdministrator,AlarmsManagement,FileCatalogManagement,SiteManager,NormalUser,VmRpcOperation "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G jenkins_fcadmin -U adminusername,ciuser,trialUser -P FileCatalogManagement,NormalUser "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-group -G jenkins_user -U adminusername,ciuser,trialUser -P NormalUser "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-group failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter DataManager adminusername prod "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter TestManager adminusername prod "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter ProductionManager adminusername prod "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi

  if ! dirac-admin-add-shifter LHCbPR adminusername prod "${DEBUG}"; then
    echo 'ERROR: dirac-admin-add-shifter failed' >&2
    exit 1
  fi
}


#.............................................................................
#
# diracProxies:
#
#   Upload proxies in the ProxyDB (which is supposed to be installed...)
#
#.............................................................................

diracProxies() {
  echo '==> [diracProxies]'
  # User proxy, should be uploaded anyway
  if ! dirac-login -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" -T 72 "${DEBUG}"; then
    echo 'ERROR: dirac-login failed' >&2
    exit 1
  fi
  # group proxy, will be uploaded explicitly
  if ! dirac-login prod -C "${SERVERINSTALLDIR}/user/client.pem" -K "${SERVERINSTALLDIR}/user/client.key" -T 72 "${DEBUG}"; then
    echo 'ERROR: dirac-login failed' >&2
    exit 1
  fi
}

#.............................................................................
#
# diracRefreshCS:
#
#   refresh the CS
#
#.............................................................................

diracRefreshCS() {
  echo '==> [diracRefreshCS]'
  if ! python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-refresh-cs.py" "${DEBUG}"; then
    echo 'ERROR: dirac-refresh-cs failed' >&2
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

  if ! dirac-admin-add-site DIRAC.Jenkins.ch aNameWhatSoEver jenkins.cern.ch "${DEBUG}"; then
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

  # Ignore tornado services
  local services=$(cut -d '.' -f 1 < services | grep -v Tornado | grep -v TokenManager | grep -v StorageElementHandler | grep -v ^ConfigurationSystem | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g')

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C ${SERVERINSTALLDIR}/user/client.pem -K ${SERVERINSTALLDIR}/user/client.key "${DEBUG}"

  for serv in $services; do
    echo "==> calling dirac-install-component $serv ${DEBUG}"
    if ! dirac-install-component "$serv" "${DEBUG}"; then
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
  if ! dirac-install-component DataManagement SE-1 -m StorageElement -p BasePath="${seDir}" -p Port=9148 "${DEBUG}"; then
    echo 'ERROR: dirac-install-component failed' >&2
    exit 1
  fi

  echo "==> Installing SE-2"
  seDir=${SERVERINSTALLDIR}/Storage/SE-2
  mkdir -p "${seDir}"
  if ! dirac-install-component DataManagement SE-2 -m StorageElement -p BasePath="${seDir}" -p Port=9147 "${DEBUG}"; then
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

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C ${SERVERINSTALLDIR}/user/client.pem -K ${SERVERINSTALLDIR}/user/client.key "${DEBUG}"

  # check if errexit mode is set and disabling as the component may not exist
  local save=$-
  if [[ $save =~ e ]]; then
    set +e
  fi

  for serv in $services; do
    echo '==> calling dirac-uninstall-component' "$serv" "${DEBUG}"
    dirac-uninstall-component -f "$serv" "${DEBUG}"
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

  local agents=$(cut -d '.' -f 1 < agents | grep -v CAUpdate |  grep -v CE2CSAgent | grep -v GOCDB2CS | grep -v Bdii2CS | grep -v CacheFeeder | grep -v NetworkAgent | grep -v FrameworkSystem | grep -v DataProcessingProgressAgent | grep -v RAWIntegrityAgent  | grep -v GridSiteWMSMonitoringAgent | grep -v HCAgent | grep -v GridCollectorAgent | grep -v HCProxyAgent | grep -v Nagios | grep -v AncestorFiles | grep -v BKInputData | grep -v LHCbPRProxyAgent | sed 's/System / /g' | sed 's/ /\//g')

  for agent in $agents; do
    if [[ $agent == *" JobAgent"* ]]; then
      echo '==> '
    else
      echo "==> calling dirac-cfg-add-option agent $agent"
      python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-cfg-add-option.py" "agent" "$agent"
      echo "==> calling dirac-agent $agent -o MaxCycles=1 ${DEBUG}"
      if ! dirac-agent "$agent"  -o MaxCycles=1 "${DEBUG}"; then
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
    if ! dirac-install-db "$db" "${DEBUG}"; then
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

# Drop, then manually install the DFC for MultiVOFileCatalog
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
  python "${TESTCODE}/DIRAC/tests/Jenkins/dirac-drop-db.py" "${dbs[@]}" "${DEBUG}"
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
    echo "==> calling dirac-install-component WorkloadManagement/$executor"
    if ! dirac-install-component "WorkloadManagement/$executor"
    then
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
#   it makes sure there are no ElasticSearch processes running. If it finds any, it
#   terminates it.
#
#.............................................................................

killES() {
  echo '==> [killES]'
  pkill -u lhcbci elasticsearch
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
# startRunsv:
#
#   starts runsv processes
#
#.............................................................................

startRunsv(){
  echo '==> [startRunsv]'

  # Let's try to be a bit more delicate than the function above

  source "${SERVERINSTALLDIR}/bashrc"
  runsvdir -P "${SERVERINSTALLDIR}/startup" &

  # Gives some time to the components to start
  sleep 10
  # Just in case 10 secs are not enough, we disable exit on error for this call.
  local save=$-
  if [[ $save =~ e ]]; then
    set +e
  fi
  runsvctrl u "${SERVERINSTALLDIR}/startup/"*
  if [[ $save =~ e ]]; then
    set -e
  fi

  runsvstat "${SERVERINSTALLDIR}/startup/"*

  echo '==> [Done startRunsv]'
}


#.............................................................................
#
# downloadProxy:
#
#   dowloads a proxy from the ProxyManager (a real one - which is probably the certification one) into a file
#
#.............................................................................

downloadProxy() {
  echo '==> [downloadProxy]'

  if [[ "${PILOTCFG}" ]]; then
    if [[ -e "${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg" ]]; then # called from the py3 client directory
      dirac-admin-get-proxy "${DIRACUSERDN}" "${DIRACUSERROLE}" -o /DIRAC/Security/UseServerCertificate=True --cfg "${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg" "${PILOTINSTALLDIR}/$PILOTCFG" --out="/tmp/x509up_u${UID}" "${DEBUG}"
    else # assuming it's the pilot
      dirac-admin-get-proxy "${DIRACUSERDN}" "${DIRACUSERROLE}" -o /DIRAC/Security/UseServerCertificate=True --cfg "${PILOTINSTALLDIR}/$PILOTCFG" --out="/tmp/x509up_u${UID}" "${DEBUG}"
    fi
  else
    if [[ -e "${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg" ]]; then # called from the py3 client directory
      dirac-admin-get-proxy "${DIRACUSERDN}" "${DIRACUSERROLE}" -o /DIRAC/Security/UseServerCertificate=True --cfg "${CLIENTINSTALLDIR}/diracos/etc/dirac.cfg" "${PILOTINSTALLDIR}/etc/dirac.cfg" --out="/tmp/x509up_u${UID}" "${DEBUG}"
    else # assuming it's the pilot
      dirac-admin-get-proxy "${DIRACUSERDN}" "${DIRACUSERROLE}" -o /DIRAC/Security/UseServerCertificate=True --cfg "${PILOTINSTALLDIR}/etc/dirac.cfg" --out="/tmp/x509up_u${UID}" "${DEBUG}"
    fi
  fi

  if [[ "${?}" -ne 0 ]]; then
    echo 'ERROR: cannot download proxy' >&2
    exit 1
  fi
}
