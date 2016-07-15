############################################
# General utility functions
############################################

if [ -z $SERVERINSTALLDIR ]
then
  if [ -z $DEVROOT ]
  then
    echo 'Environmental variable "DEVROOT" is not set.'
    exit 1
  else
    SERVERINSTALLDIR=$DEVROOT
  fi
fi

if [ $DEVROOT ]
then
  # Path to ci config files
  CI_CONFIG=$DEVROOT/DIRAC/tests/Jenkins/config/ci
fi

if [ $TESTCODE ]
then
  # Path to ci config files
  CI_CONFIG=$TESTCODE/DIRAC/tests/Jenkins/config/ci
fi

# default: this function fixes some default values

function default(){

  if [ -z $JENKINS_SITE ]
  then
    JENKINS_SITE='DIRAC.Jenkins.ch'
  fi

  if [ -z $JENKINS_CE ]
  then
    JENKINS_CE='jenkins.cern.ch'
  fi

  if [ -z $JENKINS_QUEUE ]
  then
    JENKINS_QUEUE='jenkins-queue_not_important'
  fi
}


# Finders... functions devoted to find DBs, Services, versions, etc..

#.............................................................................
#
# findRelease:
#
#   If the environment variable "PRERELEASE" exists, we use a prerelease
#   instead of a regular release ( production-like ).
#   If any parameter is passed, we assume we are on pre-release mode, otherwise,
#   we assume production. It reads from releases.cfg and picks the latest version
#   which is written to {dirac,externals}.version
#
#.............................................................................

function findRelease(){
  echo '==> [findRelease]'

  # store the current branch
  currentBranch=`git --git-dir=$TESTCODE/DIRAC/.git rev-parse --abbrev-ref HEAD`

  if [ $currentBranch == 'integration' ]
  then
    echo 'we were already on integration, no need to change'
    # get the releases.cfg file
    cp $TESTCODE/DIRAC/releases.cfg $TESTCODE/releases.cfg
  else
    cwd=$PWD
    cd $TESTCODE/DIRAC/
    if [ $? -ne 0 ]
    then
      echo 'ERROR: cannot change to ' $TESTCODE/DIRAC
      return
    fi
    git checkout integration
    # get the releases.cfg file
    cp $TESTCODE/DIRAC/releases.cfg $TESTCODE/releases.cfg
    # reset the branch
    git checkout $currentBranch
    cd $cwd
    if [ $? -ne 0 ]
    then
      echo 'ERROR: cannot change to ' $cwd
      return
    fi
  fi

  PRE='p[[:digit:]]*'

  if [ ! -z "$DIRACBRANCH" ]
  then
    echo '==> Looking for DIRAC branch ' $DIRACBRANCH
  else
    echo '==> Running on last one'
  fi

  # Match project ( DIRAC ) version from releases.cfg

  # If I don't specify a DIRACBRANCH, it will get the latest "production" release
  # First, try to find if we are on a production tag
  if [ ! -z "$DIRACBRANCH" ]
  then
    projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | grep $DIRACBRANCH | head -1 | sed 's/ //g'`
  else
    projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | head -1 | sed 's/ //g'`
  fi
  # projectVersion=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`

  # The special case is when there's no 'p'... (e.g. version v6r15)
  if [ ! "$projectVersion" ]
  then
    if [ ! -z "$DIRACBRANCH" ]
    then
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]' | grep $DIRACBRANCH | head -1 | sed 's/ //g'`
    else
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]' | head -1 | sed 's/ //g'`
    fi
  fi


  # In case there are no production tags for the branch, look for pre-releases in that branch
  if [ ! "$projectVersion" ]
  then
    if [ ! -z "$DIRACBRANCH" ]
    then
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | grep $DIRACBRANCH | head -1 | sed 's/ //g'`
    else
      projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*'-pre'' | head -1 | sed 's/ //g'`
    fi
  fi

  projectVersionLine=`cat $TESTCODE/releases.cfg | grep -n $projectVersion | cut -d ':' -f 1 | head -1`
  # start := line number after "{"
  start=$(($projectVersionLine+2))
  # end   := line number after "}"
  end=$(($start+2))
  # versions :=
  versions=`sed -n "$start,$end p" $TESTCODE/releases.cfg`

  # Extract Externals version
  externalsVersion=`echo $versions | sed s/' = '/'='/g | tr ' ' '\n' | grep Externals | cut -d '=' -f2`

  # PrintOuts
  echo DIRAC:$projectVersion && echo $projectVersion > $SERVERINSTALLDIR/dirac.version
  echo EXTERNALS:$externalsVersion && echo $externalsVersion > $SERVERINSTALLDIR/externals.version

}


#.............................................................................
#
# findSystems:
#
#   gets all system names from *DIRAC code and writes them to a file
#   named systems.
#
#.............................................................................
function findSystems(){
  echo '==> [findSystems]'

  cd $TESTCODE
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $TESTCODE
    return
  fi
  find *DIRAC/ -name *System  | cut -d '/' -f 2 | sort | uniq > systems

  echo found `wc -l systems`

}


#.............................................................................
#
# findDatabases:
#
#   gets all database names from *DIRAC code and writes them to a file
#   named databases.
#
#.............................................................................

function findDatabases(){
  echo '==> [findDatabases]'

  if [ ! -z "$1" ]
  then
    DBstoSearch=$1
    if [ "$DBstoSearch" = "exclude" ]
    then
      echo '==> excluding ' $2
      DBstoExclude=$2
      DBstoSearch=' '
    fi
  else
    DBstoExclude='notExcluding'
  fi

  cd $SERVERINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $SERVERINSTALLDIR
    return
  fi
  #
  # HACK ALERT:
  #
  # We are avoiding, FileCatalogDB FileCatalogWithFkAndPsDB that is installed in other ways
  #  and InstalledComponentsDB which is installed at the beginning
  #
  if [ ! -z "$DBstoExclude" ]
  then
    find *DIRAC -name *DB.sql | grep -vE '(FileCatalogDB|FileCatalogWithFkAndPsDB|InstalledComponentsDB)' | awk -F "/" '{print $2,$4}' | grep -v $DBstoExclude | grep -v 'DIRAC' | sort | uniq > databases
  else
    find *DIRAC -name *DB.sql | grep -vE '(FileCatalogDB|FileCatalogWithFkAndPsDB|InstalledComponentsDB)' | awk -F "/" '{print $2,$4}' | grep $DBstoSearch | grep -v 'DIRAC' | sort | uniq > databases
  fi

  echo found `wc -l databases`
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


  if [ ! -z "$1" ]
  then
    ServicestoSearch=$1
    if [ "$ServicestoSearch" = "exclude" ]
    then
      echo '==> excluding ' $2
      ServicestoExclude=$2
      ServicestoSearch=' '
    fi
  else
    ServicestoExclude='notExcluding'
  fi

  cd $SERVERINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $SERVERINSTALLDIR
    return
  fi
  if [ ! -z "$ServicestoExclude" ]
  then
    find *DIRAC/*/Service/ -name *Handler.py | grep -v test | awk -F "/" '{print $2,$4}' | grep -v $ServicestoExclude | sort | uniq > services
  else
    find *DIRAC/*/Service/ -name *Handler.py | grep -v test | awk -F "/" '{print $2,$4}' | grep $ServicestoSearch | sort | uniq > services
  fi

  echo found `wc -l services`
}

findAgents(){
  echo '==> [findAgents]'


  if [ ! -z "$1" ]
  then
    ServicestoSearch=$1
    if [ "$AgentstoSearch" = "exclude" ]
    then
      echo '==> excluding ' $2
      AgentstoExclude=$2
      AgentstoSearch=' '
    fi
  else
    AgentstoExclude='notExcluding'
  fi

  cd $SERVERINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $SERVERINSTALLDIR
    return
  fi
  if [ ! -z "$AgentstoExclude" ]
  then
    find *DIRAC/*/Agent/ -name *Agent.py | grep -v test | awk -F "/" '{print $2,$4}' | grep -v $AgentstoExclude | sort | uniq > agents
  else
    find *DIRAC/*/Agent/ -name *Agent.py | grep -v test | awk -F "/" '{print $2,$4}' | grep $AgentstoSearch | sort | uniq > agents
  fi

  echo found `wc -l agents`
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

  find *DIRAC/*/Executor/ -name *.py | awk -F "/" '{print $2,$4}' | sort | uniq > executors

  echo found `wc -l executors`
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

#.............................................................................
#
# diracReplace
#
#   This function gets DIRAC sources from an alternative github repository,
#   and replace the existing sources used for installation by these ones.
#
#   It is done only the environment variable $DIRAC_ALTERNATIVE_SRC_ZIP is set
#
# Define it in your environment if you want to replace the DIRAC source with custom ones
# The URL has to be a zip file provided by github
#DIRAC_ALTERNATIVE_SRC_ZIP=''
#
#.............................................................................
#-------------------------------------------https://github.com/chaen/DIRAC/archive/rel-v6r12_NEW_PsAndFkDfc.zip------------------------------------

function diracReplace(){
  echo '==> [diracReplace]'

  if [[ -z $DIRAC_ALTERNATIVE_SRC_ZIP ]]
  then
    echo '==> Variable $DIRAC_ALTERNATIVE_SRC_ZIP not defined';
    return
  fi

  wget $DIRAC_ALTERNATIVE_SRC_ZIP $SERVERINSTALLDIR/
  zipName=$(basename $DIRAC_ALTERNATIVE_SRC_ZIP)
  unzip $zipName
  dirName="DIRAC-$(echo $zipName | sed 's/\.zip//g')"
  if [ -d "DIRAC" ];
  then
    mv $SERVERINSTALLDIR/DIRAC $SERVERINSTALLDIR/DIRAC.bak;
  else
    echo "There is no previous DIRAC directory ??!!!"
    ls
  fi
  mv $dirName $SERVERINSTALLDIR/DIRAC

}

# Getting a CFG file for the installation: this may be replaced by VOs
function getCFGFile(){
  echo '==> [getCFGFile]'

  cp $TESTCODE/DIRAC/tests/Jenkins/install.cfg $SERVERINSTALLDIR/
  sed -i s/VAR_Release/$projectVersion/g $SERVERINSTALLDIR/install.cfg
}


#.............................................................................
#
# diracInstall:
#
#   This function gets the DIRAC install script defined on $DIRAC_INSTAll and
#   runs it with some hardcoded options. The only option that varies is the
#   project version, in this case DIRAC version, obtained from the file 'dirac.version'
#   (which coincides with the project version).
#
#.............................................................................

function diracInstall(){
  echo '==> [diracInstall]'

  cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $SERVERINSTALLDIR/dirac-install
  chmod +x $SERVERINSTALLDIR/dirac-install

  diracInstallCommand
}

#This is what VOs may replace
function diracInstallCommand(){
  $SERVERINSTALLDIR/dirac-install -r `cat $SERVERINSTALLDIR/dirac.version` -t server -d
}

#.............................................................................
#
# prepareForServer:
#
#   This function gets the DIRAC install script
#
#.............................................................................

function prepareForServer(){
  echo '==> [prepareForServer]'

  #get the necessary scripts: install_site.sh file
  cp $TESTCODE/DIRAC/Core/scripts/install_site.sh $SERVERINSTALLDIR/
  chmod +x $SERVERINSTALLDIR/install_site.sh
}


#-------------------------------------------------------------------------------
# OPEN SSL... let's create a fake CA and certificates
#-------------------------------------------------------------------------------


#.............................................................................
#
# function generateCertificates
#
#   This function generates a random host certificate ( certificate and key ),
#   which will be stored on etc/grid-security. As we need a CA to validate it,
#   we simply copy it to the directory where the CA certificates are supposed
#   to be stored etc/grid-security/certificates. In real, we'd copy them from
#   CVMFS:
#     /cvmfs/grid.cern.ch/etc/grid-security/certificates
#
#   Additional info:
#     http://www.openssl.org/docs/apps/req.html
#
#.............................................................................

function generateCertificates(){
  echo '==> [generateCertificates]'

  mkdir -p $SERVERINSTALLDIR/etc/grid-security/certificates
  cd $SERVERINSTALLDIR/etc/grid-security
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $SERVERINSTALLDIR/etc/grid-security
    return
  fi

  # Generate private RSA key
  openssl genrsa -out hostkey.pem 2048 2&>1 /dev/null

  # Prepare OpenSSL config file, it contains extensions to put into place,
  # DN configuration, etc..
  cp $CI_CONFIG/openssl_config openssl_config
  fqdn=`hostname --fqdn`
  sed -i "s/#hostname#/$fqdn/g" openssl_config

  # Generate X509 Certificate based on the private key and the OpenSSL configuration
  # file, valid for one day.
  openssl req -new -x509 -key hostkey.pem -out hostcert.pem -days 1 -config openssl_config

  # Copy hostcert, hostkey to certificates ( CA dir )
  cp host{cert,key}.pem certificates/

}


#.............................................................................
#
# generateUserCredentials:
#
#   Given we know the "CA" certificates, we can use them to sign a randomly
#   generated key / host certificate. This function is very similar to
#   generateCertificates. User credentials will be stored at:
#     $SERVERINSTALLDIR/user
#   The user will be called "ciuser". Do not confuse with the admin user,
#   which is "ci".
#
#   Additional info:
#     http://acs.lbl.gov/~boverhof/openssl_certs.html
#
#.............................................................................

function generateUserCredentials(){
    echo '==> [generateUserCredentials]'

    # Generate directory where to store credentials
    mkdir -p $SERVERINSTALLDIR/user
    cd $SERVERINSTALLDIR/user
    if [ $? -ne 0 ]
    then
      echo 'ERROR: cannot change to ' $SERVERINSTALLDIR/user
      return
    fi

    cp $CI_CONFIG/openssl_config openssl_config .
    sed -i 's/#hostname#/ciuser/g' openssl_config
    openssl genrsa -out client.key 1024 2&>1 /dev/null
    openssl req -key client.key -new -out client.req -config openssl_config
    # This is a little hack to make OpenSSL happy...
    echo 00 > file.srl

    CA=$SERVERINSTALLDIR/etc/grid-security/certificates

    openssl x509 -req -in client.req -CA $CA/hostcert.pem -CAkey $CA/hostkey.pem -CAserial file.srl -out $SERVERINSTALLDIR/user/client.pem
}


#.............................................................................
#
# diracCredentials:
#
#   hacks CS service to create a first dirac_admin proxy that will be used
#   to install the components and run the test ( some of them ).
#
#.............................................................................

function diracCredentials(){
  echo '==> [diracCredentials]'

  sed -i 's/commitNewData = CSAdministrator/commitNewData = authenticated/g' $SERVERINSTALLDIR/etc/Configuration_Server.cfg
  dirac-proxy-init -g dirac_admin -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG
  sed -i 's/commitNewData = authenticated/commitNewData = CSAdministrator/g' $SERVERINSTALLDIR/etc/Configuration_Server.cfg

}



#.............................................................................
#
# diracUserAndGroup:
#
#   create a user and a group (the CS has to be running)
#
#.............................................................................

function diracUserAndGroup(){
  echo '==> [diracUserAndGroup]'

  dirac-admin-add-user -N ciuser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch -M lhcb-dirac-ci@cern.ch -G user $DEBUG
  dirac-admin-add-user -N trialUser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=trialUser/emailAddress=lhcb-dirac-ci@cern.ch -M lhcb-dirac-ci@cern.ch -G user $DEBUG

  dirac-admin-add-group -G prod -U adminusername,ciuser,trialUser -P Operator,FullDelegation,ProxyManagement,ServiceAdministrator,JobAdministrator,CSAdministrator,AlarmsManagement,FileCatalogManagement,SiteManager,NormalUser $DEBUG

  dirac-admin-add-shifter DataManager adminusername prod $DEBUG
  dirac-admin-add-shifter TestManager adminusername prod $DEBUG
  dirac-admin-add-shifter ProductionManager adminusername prod $DEBUG
  dirac-admin-add-shifter LHCbPR adminusername prod $DEBUG
}


#.............................................................................
#
# diracProxies:
#
#   Upload proxies in the ProxyDB (which is supposed to be installed...)
#
#.............................................................................

function diracProxies(){
  echo '==> [diracProxies]'

  # User proxy, should be uploaded anyway
  dirac-proxy-init -U -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG
  # group proxy, will be uploaded explicitly
  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG

}

#.............................................................................
#
# diracRefreshCS:
#
#   refresh the CS
#
#.............................................................................

function diracRefreshCS(){
  echo '==> [diracRefreshCS]'


  python $TESTCODE/DIRAC/tests/Jenkins/dirac-refresh-cs.py $DEBUG
}




#.............................................................................
#
# diracSite:
#
#   add a site (the CS has the running)
#
#.............................................................................

function diracAddSite(){
  echo '==> [diracAddSite]'

  dirac-admin-add-site DIRAC.Jenkins.org aNameWhatSoEver some.CE.org

}

#-------------------------------------------------------------------------------
# diracServices:
#
#   installs all services
#
#-------------------------------------------------------------------------------

diracServices(){
  echo '==> [diracServices]'

  #TODO: revise this list
  services=`cat services | cut -d '.' -f 1 | grep -v Bookkeeping | grep -v IRODSStorageElementHandler | grep -v ^ConfigurationSystem | grep -v LcgFileCatalogProxy | grep -v Plotting | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG

  for serv in $services
  do
    echo '==> calling dirac-install-component' $serv $DEBUG
    dirac-install-component $serv $DEBUG
  done

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

  #TODO: revise this list
  services=`cat services | cut -d '.' -f 1 | grep -v Bookkeeping | grep -v IRODSStorageElementHandler | grep -v ^ConfigurationSystem | grep -v LcgFileCatalogProxy | grep -v Plotting | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG

  for serv in $services
  do
    echo '==> calling dirac-uninstall-component' $serv $DEBUG
    dirac-uninstall-component -f $serv $DEBUG
  done

}


#-------------------------------------------------------------------------------
# diracAgents:
#
#   installs all agents on the file agents
#
#-------------------------------------------------------------------------------

diracAgents(){
  echo '==> [diracAgents]'

  #TODO: revise this list
  agents=`cat agents | cut -d '.' -f 1 | grep -v LFC | grep -v MyProxy | grep -v CAUpdate | grep -v CE2CSAgent.py | grep -v FrameworkSystem | grep -v DiracSiteAgent | grep -v StatesMonitoringAgent | grep -v DataProcessingProgressAgent | grep -v RAWIntegrityAgent  | grep -v GridSiteWMSMonitoringAgent  | grep -v GridSiteMonitoringAgent | grep -v HCAgent | grep -v GridCollectorAgent | grep -v HCProxyAgent | grep -v Nagios | grep -v AncestorFiles | grep -v BKInputData | grep -v SAMAgent | grep -v LHCbPRProxyAgent | sed 's/System / /g' | sed 's/ /\//g'`

  for agent in $agents
  do
    if [[ $agent == *" JobAgent"* ]]
    then
      echo '==> '
    else
      echo '==> calling dirac-cfg-add-option agent' $agent
      python $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-add-option.py agent $agent
      echo '==> calling dirac-agent' $agent -o MaxCycles=1 $DEBUG
      dirac-agent $agent  -o MaxCycles=1 $DEBUG
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

  dbs=`cat databases | cut -d ' ' -f 2 | cut -d '.' -f 1 | grep -v ^RequestDB | grep -v ^FileCatalogDB | grep -v ^InstalledComponentsDB`
  for db in $dbs
  do
    dirac-install-db $db $DEBUG
  done

}

# Drop, then Install manually the DFC
diracDFCDB(){
  echo '==> [diracDFCDB]'

  mysql -u$DB_ROOTUSER -p$DB_ROOTPWD -h$DB_HOST -P$DB_PORT -e "DROP DATABASE IF EXISTS FileCatalogDB;"
  mysql -u$DB_ROOTUSER -p$DB_ROOTPWD -h$DB_HOST -P$DB_PORT  < $SERVERINSTALLDIR/DIRAC/DataManagementSystem/DB/FileCatalogWithFkAndPsDB.sql
}

# drop DBs

dropDBs(){
  echo '==> [dropDBs]'

  dbs=`cat databases | cut -d ' ' -f 2 | cut -d '.' -f 1 | grep -v ^RequestDB | grep -v ^FileCatalogDB`
  python $TESTCODE/DIRAC/tests/Jenkins/dirac-drop-db.py $dbs $DEBUG

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

function killRunsv(){
  echo '==> [killRunsv]'

    # Bear in mind that we run with 'errexit' mode. This call, if finds nothing
    # will return an error, which will make the whole script exit. However, if
    # finds nothing we are good, it means there are not leftover processes from
    # other runs. So, we disable 'errexit' mode for this call.

    #set +o errexit
    runsvdir=`ps aux | grep 'runsvdir ' | grep -v 'grep'`
    #set -o errexit

    if [ ! -z "$runsvdir" ]
    then
      killall runsvdir
    fi

    # Same as before
  #set +o errexit
  runsv=`ps aux | grep 'runsv ' | grep -v 'grep'`
  #set -o errexit

    if [ ! -z "$runsv" ]
    then
      killall runsv
    fi

  }


  #.............................................................................
  #
  # stopRunsv:
  #
  #   if runsv is running, it stops it.
  #
  #.............................................................................

function stopRunsv(){
  echo '==> [stopRunsv]'

  # Let's try to be a bit more delicated than the function above

  source $SERVERINSTALLDIR/bashrc
  runsvctrl d $SERVERINSTALLDIR/startup/*
  runsvstat $SERVERINSTALLDIR/startup/*

  # If does not work, let's kill it.
  killRunsv
}


  #.............................................................................
  #
  # startRunsv:
  #
  #   starts runsv processes
  #
  #.............................................................................

function startRunsv(){
    echo '==> [startRunsv]'

    # Let's try to be a bit more delicated than the function above

    source $SERVERINSTALLDIR/bashrc
    runsvdir -P $SERVERINSTALLDIR/startup &

    # Gives some time to the components to start
    sleep 10
    # Just in case 10 secs are not enough, we disable exit on error for this call.
    set +o errexit
    runsvctrl u $SERVERINSTALLDIR/startup/*
    set -o errexit

    runsvstat $SERVERINSTALLDIR/startup/*

}




############################################
# Pilot tests Utilities


function getCertificate(){
  echo '==> [getCertificate]'
  # just gets a host certificate from a known location

  mkdir -p $PILOTINSTALLDIR/etc/grid-security/
  cp /root/hostcert.pem $PILOTINSTALLDIR/etc/grid-security/
  cp /root/hostkey.pem $PILOTINSTALLDIR/etc/grid-security/
  chmod 0600 $PILOTINSTALLDIR/etc/grid-security/hostkey.pem

}

function prepareForPilot(){
  echo '==> [prepareForPilot]'

  #cert first (host certificate)
  #getCertificate (no need...)

  #get the necessary scripts
  cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $PILOTINSTALLDIR/
  cp $TESTCODE/DIRAC/WorkloadManagementSystem/PilotAgent/dirac-pilot.py $PILOTINSTALLDIR/
  cp $TESTCODE/DIRAC/WorkloadManagementSystem/PilotAgent/pilotTools.py $PILOTINSTALLDIR/
  cp $TESTCODE/DIRAC/WorkloadManagementSystem/PilotAgent/pilotCommands.py $PILOTINSTALLDIR/

}


#.............................................................................
#
# downloadProxy:
#
#   dowloads a proxy from the ProxyManager (a real one - which is probably the certification one) into a file
#
#.............................................................................

function downloadProxy(){
  echo '==> [downloadProxy]'

  if [ $PILOTCFG ]
  then
    echo $( eval echo Executing python $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/$PILOTCFG $DEBUG)
    python $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/$PILOTCFG $DEBUG
  else
    echo $( eval echo Executing python $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $DEBUG)
    python $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $DEBUG
  fi

  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot download proxy'
    return
  fi
}
