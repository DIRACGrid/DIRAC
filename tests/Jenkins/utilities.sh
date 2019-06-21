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
#   It reads from releases.cfg and picks the latest version
#   which is written to {dirac,externals}.version
#   Unless variable $projectVersion is set: in this case, we use the specified DIRAC relese.
#
#.............................................................................

function findRelease(){
  echo '==> [findRelease]'


  if [ ! -z "$DIRAC_RELEASE" ]
  then
    echo '==> Specified release'
    echo $DIRAC_RELEASE
    projectVersion=$DIRAC_RELEASE
    echo DIRAC:$projectVersion && echo $projectVersion > $SERVERINSTALLDIR/dirac.version
  else

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
    if [ ! "$projectVersion" ]
    then
      if [ ! -z "$DIRACBRANCH" ]
      then
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | grep $DIRACBRANCH | head -1 | sed 's/ //g'`
      else
        projectVersion=`cat $TESTCODE/releases.cfg | grep '[^:]v[[:digit:]]*r[[:digit:]]*p[[:digit:]]*' | head -1 | sed 's/ //g'`
      fi
      # projectVersion=`cat releases.cfg | grep [^:]v[[:digit:]]r[[:digit:]]*$PRE | head -1 | sed 's/ //g'`
    fi

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
  fi

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


# Getting a CFG file for the installation: this may be replaced by VOs
function getCFGFile(){
  echo '==> [getCFGFile]'

  cp $TESTCODE/DIRAC/tests/Jenkins/install.cfg $SERVERINSTALLDIR/
  sed -i s/VAR_Release/$projectVersion/g $SERVERINSTALLDIR/install.cfg
}


####################################################
# This installs the DIRAC client
# it needs a $DIRAC_RELEASE env var defined
# if DIRACOSVER env var is defined, it will install dirac with DIRACOS

function installDIRAC(){

  echo '==> Installing DIRAC client'

  cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $CLIENTINSTALLDIR/dirac-install
  chmod +x $CLIENTINSTALLDIR/dirac-install

  cd $CLIENTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $CLIENTINSTALLDIR
    return
  fi

  # actually installing
  # If DIRACOSVER is not defined, use exterals
  if [ -z $DIRACOSVER ];
  then
    echo "Installing with Externals";
    ./dirac-install -r $DIRAC_RELEASE -t client $DEBUG
  else
    echo "Installing with DIRACOS $DIRACOSVER";
    ./dirac-install -r $DIRAC_RELEASE -t client --dirac-os --dirac-os-version=$DIRACOSVER $DEBUG
  fi


  if [ $? -ne 0 ]
  then
    echo 'ERROR: DIRAC client installation failed'
    return
  fi

  # now configuring
  source bashrc
  dirac-configure -S $DIRACSETUP -C $CSURL --UseServerCertificate -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-configure failed'
    return
  fi

  echo 'Content of etc/dirac.cfg:'
  more $CLIENTINSTALLDIR/etc/dirac.cfg

  source bashrc
  if [ $? -ne 0 ]
  then
    echo 'ERROR: source bashrc failed'
    return
  fi
}

##############################################################################
# This function submits a job or more (it assumes a DIRAC client is installed)

function submitJob(){

  echo -e "==> Submitting a simple job"

  #This has to be executed from the $CLIENTINSTALLDIR
  cd $CLIENTINSTALLDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $CLIENTINSTALLDIR
    return
  fi

  export PYTHONPATH=$TESTCODE:$PYTHONPATH
  #Get a proxy and submit the job: this job will go to the certification setup, so we suppose the JobManager there is accepting jobs
  getUserProxy #this won't really download the proxy, so that's why the next command is needed
  cp $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py .
  python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem -o /DIRAC/Setup=DIRAC-Certification -ddd
  cp $TESTCODE/DIRAC/tests/Jenkins/dirac-test-job.py .
  python dirac-test-job.py -o /DIRAC/Setup=DIRAC-Certification $DEBUG
}

function getUserProxy(){

  echo '==> Started getUserProxy'

  cp $TESTCODE/DIRAC/tests/Jenkins/dirac-cfg-update.py .
  python dirac-cfg-update.py -S $DIRACSETUP $CLIENTINSTALLDIR/etc/dirac.cfg -F $CLIENTINSTALLDIR/etc/dirac.cfg -o /DIRAC/Security/UseServerCertificate=True -o /DIRAC/Security/CertFile=/home/dirac/certs/hostcert.pem -o /DIRAC/Security/KeyFile=/home/dirac/certs/hostkey.pem $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-cfg-update failed'
    return
  fi

  #Getting a user proxy, so that we can run jobs
  downloadProxy
  if [ $? -ne 0 ]
  then
    echo 'ERROR: downloadProxy failed'
    return
  fi

  echo '==> Done getUserProxy'
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

  #get the necessary scripts: dirac-install.py file
  cp $TESTCODE/DIRAC/Core/scripts/dirac-install.py $SERVERINSTALLDIR/
  chmod +x $SERVERINSTALLDIR/dirac-install.py
}


#-------------------------------------------------------------------------------
# OPEN SSL... let's create a fake CA and certificates
#-------------------------------------------------------------------------------


# function generateCA()
#
# This generates the CA that will be used to sign the server and client certificates

function generateCA(){
   echo '==> [generateCA]'

   mkdir -p $SERVERINSTALLDIR/etc/grid-security/certificates
   mkdir -p $SERVERINSTALLDIR/etc/grid-security/ca/
   cd $SERVERINSTALLDIR/etc/grid-security/ca
   if [ $? -ne 0 ]
   then
     echo 'ERROR: cannot change to ' $SERVERINSTALLDIR/etc/grid-security/ca
     return
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
   cp $CI_CONFIG/openssl_config_ca.cnf openssl_config_ca.cnf
   sed -i "s|#GRIDSECURITY#|$SERVERINSTALLDIR/etc/grid-security|g" openssl_config_ca.cnf


   # Generate the CA certificate
   openssl req -config openssl_config_ca.cnf \
               -key ca.key.pem \
               -new -x509 \
               -days 7300 \
               -sha256 \
               -extensions v3_ca \
               -out ca.cert.pem

   # Copy the CA to the list of trusted CA
   cp ca.cert.pem $SERVERINSTALLDIR/etc/grid-security/certificates/

   # Generate the hash link file required by openSSL to index CA certificates
   caHash=$(openssl x509 -in ca.cert.pem -noout -hash)
   ln -s $SERVERINSTALLDIR/etc/grid-security/certificates/ca.cert.pem $SERVERINSTALLDIR/etc/grid-security/certificates/"$caHash".0

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

function generateCertificates(){
  echo '==> [generateCertificates]'

  if [ -z ${1} ]
  then
    nDays=1
  else
    nDays=$1
  fi

  mkdir -p $SERVERINSTALLDIR/etc/grid-security/
  cd $SERVERINSTALLDIR/etc/grid-security/
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $SERVERINSTALLDIR/etc/grid-security/
    return
  fi

  # Generate private RSA key
  openssl genrsa -out hostkey.pem 2048  &> /dev/null
  chmod 400 hostkey.pem

  # Prepare OpenSSL config file, it contains extensions to put into place,
  # DN configuration, etc..
  cp $CI_CONFIG/openssl_config_host.cnf openssl_config_host.cnf

  # man hostname to see why we use --all-fqdns
  # Note: if there's no dns entry for the localhost, the fqdns will be empty
  # so we append to it the local hostname, and we take the first one in the list
  fqdn=$((hostname --all-fqdn; hostname ) | paste -sd ' ' | awk {'print $1'})
  sed -i "s/#hostname#/$fqdn/g" openssl_config_host.cnf

  # Generate X509 Certificate request based on the private key and the OpenSSL configuration
  # file, valid for nDays days (default 1).
  openssl req -config openssl_config_host.cnf \
              -key hostkey.pem \
              -new \
              -sha256 \
              -out request.csr.pem

  # Sign it using the self generated CA
  openssl ca -config $SERVERINSTALLDIR/etc/grid-security/ca/openssl_config_ca.cnf \
       -days $nDays \
       -extensions server_cert \
       -batch \
       -in request.csr.pem \
       -out hostcert.pem



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
#   The argument that can be passed is the validity of the certificate
#
#   Additional info:
#     http://acs.lbl.gov/~boverhof/openssl_certs.html
#
#.............................................................................

function generateUserCredentials(){
  echo '==> [generateUserCredentials]'

  # validity of the certificate
  if [ -z ${1} ]
  then
    nDays=1
  else
    nDays=$1
  fi


  USERCERTDIR=$SERVERINSTALLDIR/user
  # Generate directory where to store credentials
  mkdir -p $USERCERTDIR
  cd $USERCERTDIR
  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot change to ' $USERCERTDIR
    return
  fi

  # What is that ?
  save=$-
  if [[ $save =~ e ]]
  then
    set +e
  fi

  cp $CI_CONFIG/openssl_config_user.cnf $USERCERTDIR/openssl_config_user.cnf
  if [[ $save =~ e ]]
  then
    set -e
  fi

  openssl genrsa -out client.key 2048 2>&1 /dev/null
  chmod 400 client.key

  openssl req -config $USERCERTDIR/openssl_config_user.cnf \
              -key $USERCERTDIR/client.key \
              -new \
              -out $USERCERTDIR/client.req

  openssl ca -config $SERVERINSTALLDIR/etc/grid-security/ca/openssl_config_ca.cnf \
             -extensions usr_cert \
             -batch \
             -days $nDays \
             -in $USERCERTDIR/client.req \
             -out $USERCERTDIR/client.pem


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
  dirac-proxy-init -g dirac_admin -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key $DEBUG --rfc
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-proxy-init failed'
    return
  fi
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

  dirac-admin-add-user -N ciuser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=ciuser/emailAddress=lhcb-dirac-ci@cern.ch -M lhcb-dirac-ci@cern.ch -G dirac_user $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-user failed'
    return
  fi

  dirac-admin-add-user -N trialUser -D /C=ch/O=DIRAC/OU=DIRAC\ CI/CN=trialUser/emailAddress=lhcb-dirac-ci@cern.ch -M lhcb-dirac-ci@cern.ch -G dirac_user $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-user failed'
    return
  fi

  dirac-admin-add-group -G prod -U adminusername,ciuser,trialUser -P Operator,FullDelegation,ProxyManagement,ServiceAdministrator,JobAdministrator,CSAdministrator,AlarmsManagement,FileCatalogManagement,SiteManager,NormalUser $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-group failed'
    return
  fi

  dirac-admin-add-shifter DataManager adminusername prod $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-shifter failed'
    return
  fi

  dirac-admin-add-shifter TestManager adminusername prod $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-shifter failed'
    return
  fi

  dirac-admin-add-shifter ProductionManager adminusername prod $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-shifter failed'
    return
  fi

  dirac-admin-add-shifter LHCbPR adminusername prod $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-shifter failed'
    return
  fi
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
  dirac-proxy-init -U -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key --rfc $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-proxy-init failed'
    return
  fi

  # group proxy, will be uploaded explicitly
  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key --rfc $DEBUG
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-proxy-init failed'
    return
  fi

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
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-refresh-cs failed'
    return
  fi
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

  dirac-admin-add-site DIRAC.Jenkins.ch aNameWhatSoEver jenkins.cern.ch
  if [ $? -ne 0 ]
  then
    echo 'ERROR: dirac-admin-add-site failed'
    return
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

  services=`cat services | cut -d '.' -f 1 | grep -v PilotsLogging | grep -v FTSManagerHandler | grep -v IRODSStorageElementHandler | grep -v ^ConfigurationSystem | grep -v Plotting | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key --rfc $DEBUG

  for serv in $services
  do
    echo '==> calling dirac-install-component' $serv $DEBUG
    dirac-install-component $serv $DEBUG
    if [ $? -ne 0 ]
    then
      echo 'ERROR: dirac-install-component failed'
      return
    fi
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

  services=`cat services | cut -d '.' -f 1 | grep -v IRODSStorageElementHandler | grep -v ^ConfigurationSystem | grep -v Plotting | grep -v RAWIntegrity | grep -v RunDBInterface | grep -v ComponentMonitoring | sed 's/System / /g' | sed 's/Handler//g' | sed 's/ /\//g'`

  # group proxy, will be uploaded explicitly
  #  echo '==> getting/uploading proxy for prod'
  #  dirac-proxy-init -U -g prod -C $SERVERINSTALLDIR/user/client.pem -K $SERVERINSTALLDIR/user/client.key --rfc $DEBUG

  # check if errexit mode is set and disabling as the component may not exist
  save=$-
  if [[ $save =~ e ]]
  then
    set +e
  fi

  for serv in $services
  do
    echo '==> calling dirac-uninstall-component' $serv $DEBUG
    dirac-uninstall-component -f $serv $DEBUG
  done

  if [[ $save =~ e ]]
  then
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

  agents=`cat agents | cut -d '.' -f 1 | grep -v MyProxy | grep -v CAUpdate | grep -v FTSAgent | grep -v CleanFTSDBAgent | grep -v CE2CSAgent | grep -v GOCDB2CS | grep -v Bdii2CS | grep -v CacheFeeder | grep -v NetworkAgent | grep -v FrameworkSystem | grep -v DiracSiteAgent | grep -v StatesMonitoringAgent | grep -v DataProcessingProgressAgent | grep -v RAWIntegrityAgent  | grep -v GridSiteWMSMonitoringAgent | grep -v HCAgent | grep -v GridCollectorAgent | grep -v HCProxyAgent | grep -v Nagios | grep -v AncestorFiles | grep -v BKInputData | grep -v LHCbPRProxyAgent | sed 's/System / /g' | sed 's/ /\//g'`

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
      if [ $? -ne 0 ]
      then
        echo 'ERROR: dirac-agent failed'
        return
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

  dbs=`cat databases | cut -d ' ' -f 2 | cut -d '.' -f 1 | grep -v ^RequestDB | grep -v ^FTSDB | grep -v ^FileCatalogDB | grep -v ^InstalledComponentsDB`
  for db in $dbs
  do
    dirac-install-db $db $DEBUG
    if [ $? -ne 0 ]
    then
      echo 'ERROR: dirac-install-db failed'
      return
    fi
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

  # Bear in mind that we may run with 'errexit' mode. This call, if finds nothing
  # will return an error, which will make the whole script exit. However, if
  # finds nothing we are good, it means there are not leftover processes from
  # other runs. So, we disable 'errexit' mode for this call.

  # check if errexit mode is set
  save=$-
  if [[ $save =~ e ]]
  then
    set +e
  fi

  runsvdir=`ps aux | grep 'runsvdir ' | grep -v 'grep'`

  if [ ! -z "$runsvdir" ]
  then
    killall runsvdir
  fi

  runsv=`ps aux | grep 'runsv ' | grep -v 'grep'`

  if [ ! -z "$runsv" ]
  then
    killall runsv
  fi

  if [[ $save =~ e ]]
  then
    set -e
  fi


}

#.............................................................................
#
# killES:
#
#   it makes sure there are no ElasticSearch processes running. If it finds any, it
#   terminates it.
#
#.............................................................................

function killES(){
  echo '==> [killES]'

    res=`ps aux | grep 'elasticsearch' | grep 'lhcbci' | grep -v 'grep' | cut -f 4 -d ' '`

    if [ ! -z "$res" ]
    then
      kill -9 $res
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
  save=$-
  if [[ $save =~ e ]]
  then
    set +e
  fi
  runsvctrl u $SERVERINSTALLDIR/startup/*
  if [[ $save =~ e ]]
  then
    set -e
  fi

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

  cp $TESTCODE/DIRAC/tests/Jenkins/dirac-proxy-download.py .

  if [ $PILOTCFG ]
  then
    if [ -e $CLIENTINSTALLDIR/etc/dirac.cfg ] # called from the client directory
    then
      echo $( eval echo Executing python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $CLIENTINSTALLDIR/etc/dirac.cfg $PILOTINSTALLDIR/$PILOTCFG $DEBUG)
      python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $CLIENTINSTALLDIR/etc/dirac.cfg $PILOTINSTALLDIR/$PILOTCFG $DEBUG
    else # assuming it's the pilot
      echo $( eval echo Executing python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/$PILOTCFG $DEBUG)
      python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/$PILOTCFG $DEBUG
    fi
  else
    if [ -e $CLIENTINSTALLDIR/etc/dirac.cfg ] # called from the client directory
    then
      echo $( eval echo Executing python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $CLIENTINSTALLDIR/etc/dirac.cfg $DEBUG)
      python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $CLIENTINSTALLDIR/etc/dirac.cfg $DEBUG
    else # assuming it's the pilot
      echo $( eval echo Executing python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/etc/dirac.cfg $DEBUG)
      python dirac-proxy-download.py $DIRACUSERDN -R $DIRACUSERROLE -o /DIRAC/Security/UseServerCertificate=True $PILOTINSTALLDIR/etc/dirac.cfg $DEBUG
    fi
  fi

  if [ $? -ne 0 ]
  then
    echo 'ERROR: cannot download proxy'
    return
  fi
}


#.............................................................................
#
# installES:
#
#   install (and run) ElasticSearch in the current directory
#
#.............................................................................

function installES(){
  echo '==> [installES]'

  curl -L -O https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-6.6.0.tar.gz
  tar -xvf elasticsearch-6.6.0.tar.gz
  cd elasticsearch-6.6.0/bin
  ./elasticsearch -d -Ecluster.name=jenkins_cluster -Enode.name=jenkins_node &

  cd ../..
}
