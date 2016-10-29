#!/bin/sh
#-------------------------------------------------------------------------------
# dirac_ci
#
# Several functions used for Jenkins style jobs
# They may also work on other CI systems
#
#
# fstagni@cern.ch
# 09/12/2014
#-------------------------------------------------------------------------------

# A CI job needs:
#
# === environment variables (minimum set):
# DEBUG
# WORKSPACE
# DIRACBRANCH
#
# === a default directory structure is created:
# ~/TestCode
# ~/ServerInstallDIR
# ~/PilotInstallDIR




# Def of environment variables:

if [ "$DEBUG" ]
then
  echo '==> Running in DEBUG mode'
  DEBUG='-ddd'
else
  echo '==> Running in non-DEBUG mode'
fi

if [ "$WORKSPACE" ]
then
  echo '==> We are in Jenkins I guess'
else
  WORKSPACE=$PWD
fi

if [ "$DIRACBRANCH" ]
then
  echo '==> Working on DIRAC branch ' $DIRACBRANCH
else
  DIRACBRANCH='integration'
fi

# Creating default structure
mkdir -p $WORKSPACE/TestCode # Where the test code resides
TESTCODE=$_
mkdir -p $WORKSPACE/ServerInstallDIR # Where servers are installed
SERVERINSTALLDIR=$_
mkdir -p $WORKSPACE/ClientInstallDIR # Where clients are installed
CLIENTINSTALLDIR=$_
mkdir -p $WORKSPACE/PilotInstallDIR # Where pilots are installed
PILOTINSTALLDIR=$_


# Sourcing utility file
source $TESTCODE/DIRAC/tests/Jenkins/utilities.sh



#...............................................................................
#
# installSite:
#
#   This function will install DIRAC using the install_site.sh script
#     following (more or less) instructions at diracgrid.org
#
#...............................................................................

function installSite(){
  echo '==> [installSite]'

  prepareForServer

  killRunsv
  findRelease

  generateCertificates

  getCFGFile

  echo '==> Fixing install.cfg file'
  if [ "$LcgVer" ]
  then
    echo '==> Fixing LcgVer to ' $LcgVer
    sed -i s/VAR_LcgVer/$LcgVer/g $SERVERINSTALLDIR/install.cfg
  else
    sed -i s/VAR_LcgVer/$externalsVersion/g $SERVERINSTALLDIR/install.cfg
  fi
  sed -i s,VAR_TargetPath,$SERVERINSTALLDIR,g $SERVERINSTALLDIR/install.cfg
  fqdn=`hostname --fqdn`
  sed -i s,VAR_HostDN,$fqdn,g $SERVERINSTALLDIR/install.cfg

  sed -i s/VAR_DB_User/$DB_USER/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_DB_Password/$DB_PASSWORD/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_DB_RootUser/$DB_ROOTUSER/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_DB_RootPwd/$DB_ROOTPWD/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_DB_Host/$DB_HOST/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_DB_Port/$DB_PORT/g $SERVERINSTALLDIR/install.cfg

  sed -i s/VAR_NoSQLDB_User/$NoSQLDB_USER/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_NoSQLDB_Password/$NoSQLDB_PASSWORD/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_NoSQLDB_RootUser/$NoSQLDB_ROOTUSER/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_NoSQLDB_RootPwd/$NoSQLDB_ROOTPWD/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_NoSQLDB_Host/$NoSQLDB_HOST/g $SERVERINSTALLDIR/install.cfg
  sed -i s/VAR_NoSQLDB_Port/$NoSQLDB_PORT/g $SERVERINSTALLDIR/install.cfg

  echo '==> Started installing'
  $SERVERINSTALLDIR/install_site.sh $SERVERINSTALLDIR/install.cfg
  echo '==> Completed installation'

  source $SERVERINSTALLDIR/bashrc
}


#...............................................................................
#
# fullInstall:
#
#   This function install all the DIRAC stuff known...
#
#...............................................................................

function fullInstallDIRAC(){
  echo '==> [fullInstallDIRAC]'

  finalCleanup

  #basic install, with only the CS (and ComponentMonitoring) running, together with DB InstalledComponentsDB, which is needed)
  installSite

  #replace the sources with custom ones if defined
  diracReplace

  #Dealing with security stuff
  # generateCertificates
  generateUserCredentials
  diracCredentials

  #just add a site
  diracAddSite

  #Install the Framework
  findDatabases 'FrameworkSystem'
  dropDBs
  diracDBs
  findServices 'FrameworkSystem'
  diracServices

  #create groups
  diracUserAndGroup

  echo '==> Restarting Framework ProxyManager'
  dirac-restart-component Framework ProxyManager $DEBUG

  echo '==> Restarting Framework ComponentMonitoring'
  dirac-restart-component Framework ComponentMonitoring $DEBUG

  #Now all the rest

  #DBs (not looking for FrameworkSystem ones, already installed)
  #findDatabases 'exclude' 'FrameworkSystem'
  findDatabases 'exclude' 'FrameworkSystem'
  dropDBs
  diracDBs

  #upload proxies
  diracProxies

  #fix the DBs (for the FileCatalog)
  diracDFCDB
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update-dbs.py $DEBUG

  #services (not looking for FrameworkSystem already installed)
  findServices 'exclude' 'FrameworkSystem'
  diracServices

  #fix the services
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update-services.py $DEBUG

  #fix the SandboxStore and other stuff
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update-server.py JenkinsSetup $DEBUG

  echo '==> Restarting WorkloadManagement SandboxStore'
  dirac-restart-component WorkloadManagement SandboxStore $DEBUG

  echo '==> Restarting DataManagement FileCatalog'
  dirac-restart-component DataManagement FileCatalog $DEBUG

  echo '==> Restarting Configuration Server'
  dirac-restart-component Configuration Server $DEBUG

  echo '==> Restarting ResourceStatus ResourceStatus'
  dirac-restart-component ResourceStatus ResourceStatus $DEBUG

  echo '==> Restarting ResourceStatus ResourceManagement'
  dirac-restart-component ResourceStatus ResourceManagement $DEBUG

  echo '==> Restarting ResourceStatus Publisher'
  dirac-restart-component ResourceStatus Publisher $DEBUG

  #agents
  findAgents
  diracAgents


}


function clean(){

  #Uninstalling the services
  diracUninstallServices

  #stopping runsv of services and agents
  stopRunsv

  #DBs
  findDatabases
  dropDBs
  mysql -u$DB_ROOTUSER -p$DB_ROOTPWD -h$DB_HOST -P$DB_PORT -e "DROP DATABASE IF EXISTS FileCatalogDB;"
  mysql -u$DB_ROOTUSER -p$DB_ROOTPWD -h$DB_HOST -P$DB_PORT -e "DROP DATABASE IF EXISTS InstalledComponentsDB;"

  #clean all
  finalCleanup
}

############################################
# Pilot
############################################

#...............................................................................
#
# MAIN function: DIRACPilotInstall:
#
#   This function uses the pilot code to make a DIRAC pilot installation
#   The JobAgent is not run here
#
#...............................................................................

function DIRACPilotInstall(){

  prepareForPilot

  default

  findRelease

  #Don't launch the JobAgent here
  cwd=$PWD
  cd $PILOTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $PILOTINSTALLDIR
    return
  fi

  python dirac-pilot.py -S $DIRACSETUP -r $projectVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE -M 1 --cert --certLocation=/home/dirac/certs/ -X GetPilotVersion,CheckWorkerNode,InstallDIRAC,ConfigureBasics,CheckCECapabilities,CheckWNCapabilities,ConfigureSite,ConfigureArchitecture,ConfigureCPURequirements $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: pilot script failed'
    return
  fi

  cd $cwd
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $cwd
    return
  fi
}


function fullPilot(){

  #first simply install via the pilot
  DIRACPilotInstall

  #this should have been created, we source it so that we can continue
  source $PILOTINSTALLDIR/bashrc
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot source bashrc'
    return
  fi

  #Adding the LocalSE and the CPUTimeLeft, for the subsequent tests
  dirac-configure -FDMH --UseServerCertificate -L $DIRACSE $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot configure'
    return
  fi

  #Configure for CPUTimeLeft and more
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update.py -o /DIRAC/Security/UseServerCertificate=True $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot update the CFG'
    return
  fi

  #Getting a user proxy, so that we can run jobs
  downloadProxy
  #Set not to use the server certificate for running the jobs
  dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot run dirac-configure'
    return
  fi
}
