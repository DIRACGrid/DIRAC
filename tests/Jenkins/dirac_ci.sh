#!/bin/sh 
#-------------------------------------------------------------------------------
# dirac_ci
#
#  Several functions used for Jenkins style jobs
#
#
# fstagni@cern.ch  
# 09/12/2014
#-------------------------------------------------------------------------------

# first first: sourcing utility file
source $WORKSPACE/TestDIRAC/Jenkins/utilities.sh


############################################
# List URLs where to get scripts
############################################
DIRAC_INSTALL='https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/dirac-install.py'
DIRAC_PILOT='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/dirac-pilot.py'
DIRAC_PILOT_TOOLS='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/pilotTools.py'
DIRAC_PILOT_COMMANDS='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/WorkloadManagementSystem/PilotAgent/pilotCommands.py'
DIRAC_INSTALL_SITE='https://github.com/DIRACGrid/DIRAC/raw/integration/Core/scripts/install_site.sh --no-check-certificate'

DIRAC_RELEASES='https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/releases.cfg'
############################################

INSTALL_CFG_FILE='$WORKSPACE/TestDIRAC/Jenkins/install.cfg'


#...............................................................................
#
# installSite:
#
#   This function will install DIRAC using the install_site.sh script 
#     following (more or less) instructions at diracgrid.org
#
#...............................................................................

function installSite(){
	echo '[installSite]'
	 
	killRunsv
	findRelease

	generateCertificates

	#install_site.sh file
	mkdir $WORKSPACE/DIRAC
	cd $WORKSPACE/DIRAC
	wget -np $DIRAC_INSTALL_SITE
	chmod +x install_site.sh
	
	#Fixing install.cfg file
	cp $(eval echo $INSTALL_CFG_FILE) .
	sed -i s/VAR_Release/$projectVersion/g $WORKSPACE/DIRAC/install.cfg
	if [ ! -z "$LcgVer" ]
	then
		echo 'Fixing LcgVer to ' $LcgVer
		sed -i s/VAR_LcgVer/$LcgVer/g $WORKSPACE/DIRAC/install.cfg
	else
		sed -i s/VAR_LcgVer/$externalsVersion/g $WORKSPACE/DIRAC/install.cfg
	fi
	sed -i s,VAR_TargetPath,$WORKSPACE,g $WORKSPACE/DIRAC/install.cfg
	fqdn=`hostname --fqdn`
	sed -i s,VAR_HostDN,$fqdn,g $WORKSPACE/DIRAC/install.cfg
	
	sed -i s/VAR_DB_User/$DB_USER/g $WORKSPACE/DIRAC/install.cfg
	sed -i s/VAR_DB_Password/$DB_PASSWORD/g $WORKSPACE/DIRAC/install.cfg
	sed -i s/VAR_DB_RootUser/$DB_ROOTUSER/g $WORKSPACE/DIRAC/install.cfg
	sed -i s/VAR_DB_RootPwd/$DB_ROOTPWD/g $WORKSPACE/DIRAC/install.cfg
	sed -i s/VAR_DB_Host/$DB_HOST/g $WORKSPACE/DIRAC/install.cfg
	sed -i s/VAR_DB_Port/$DB_PORT/g $WORKSPACE/DIRAC/install.cfg
	
	#Installing
	./install_site.sh install.cfg
	
	source $WORKSPACE/bashrc
}


#...............................................................................
#
# fullInstall:
#
#   This function install all the DIRAC stuff known...
#
#...............................................................................

function fullInstallDIRAC(){
	echo '[fullInstallDIRAC]'
	
	finalCleanup

	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  

	#basic install, with only the CS (and ComponentMonitoring) running, together with DB InstalledComponentsDB, which is needed) 
	installSite

	#replace the sources with custom ones if defined
	diracReplace

	#Dealing with security stuff
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

	echo 'Restarting Framework ProxyManager'
	dirac-restart-component Framework ProxyManager $DEBUG
	
	echo 'Restarting Framework ComponentMonitoring'
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
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-cfg-update-dbs.py $WORKSPACE $DEBUG
	
	#services (not looking for FrameworkSystem already installed)
	findServices 'exclude' 'FrameworkSystem'
	diracServices

	#fix the services 
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-cfg-update-services.py $WORKSPACE $DEBUG
	
	#fix the SandboxStore and other stuff
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-cfg-update-server.py $WORKSPACE $DEBUG

	echo 'Restarting WorkloadManagement SandboxStore'
	dirac-restart-component WorkloadManagement SandboxStore $DEBUG

	echo 'Restarting DataManagement FileCatalog'
	dirac-restart-component DataManagement FileCatalog $DEBUG

	echo 'Restarting Configuration Server'
	dirac-restart-component Configuration Server $DEBUG

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
	
	#run the dirac-pilot script, the JobAgent won't necessarily match a job

	findRelease
 
	#Don't launch the JobAgent here
	python dirac-pilot.py -S $DIRACSETUP -r $projectVersion -C $CSURL -N $JENKINS_CE -Q $JENKINS_QUEUE -n $JENKINS_SITE -M 1 --cert --certLocation=/home/dirac/certs/ -X GetPilotVersion,CheckWorkerNode,InstallDIRAC,ConfigureBasics,CheckCECapabilities,CheckWNCapabilities,ConfigureSite,ConfigureArchitecture,ConfigureCPURequirements $DEBUG
}


function fullPilot(){
	
	if [ ! -z "$DEBUG" ]
	then
		echo 'Running in DEBUG mode'
		export DEBUG='-ddd'
	fi  

	#first simply install via the pilot
	DIRACPilotInstall

	#this should have been created, we source it so that we can continue
	source bashrc
	
	#Adding the LocalSE and the CPUTimeLeft, for the subsequent tests
	dirac-configure -FDMH --UseServerCertificate -L $DIRACSE $DEBUG
	
	#Configure for CPUTimeLeft and more
	python $WORKSPACE/TestDIRAC/Jenkins/dirac-cfg-update.py -V $VO -S $DIRACSETUP -o /DIRAC/Security/UseServerCertificate=True $DEBUG
	
	#Getting a user proxy, so that we can run jobs
	downloadProxy
	#Set not to use the server certificate for running the jobs 
	dirac-configure -FDMH -o /DIRAC/Security/UseServerCertificate=False $DEBUG
}