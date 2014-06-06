#!/usr/bin/env python
# $HeadURL$
"""
 Perform initial sanity checks on WN, installs and configures DIRAC and runs
 Job Agent to execute pending workload on WMS.
 It requires dirac-install script to be sitting in the same directory.
"""
__RCSID__ = "$Id$"

import os
import sys
import getopt
import urllib2
import stat
import socket
import imp
import re
import time
import pickle

cmdOpts = ( ( 'b', 'build', 'Force local compilation' ),
            ( 'd', 'debug', 'Set debug flag' ),
            ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'h', 'help', 'Show this help' ),
            ( 'i:', 'python=', 'Use python<24|25> interpreter' ),
            ( 'l:', 'project=', 'Project to install' ),
            ( 'p:', 'platform=', 'Use <platform> instead of local one' ),
            ( 't', 'test', 'Make a dry run. Do not run JobAgent' ),
            ( 'u:', 'url=', 'Use <url> to download tarballs' ),
            ( 'r:', 'release=', 'DIRAC release to install' ),
            ( 'n:', 'name=', 'Set <Site> as Site Name' ),
            ( 'D:', 'disk=', 'Require at least <space> MB available' ),
            ( 'M:', 'MaxCycles=', 'Maximum Number of JobAgent cycles to run' ),
            ( 'N:', 'Name=', 'Use <CEName> to determine Site Name' ),
            ( 'P:', 'path=', 'Install under <path>' ),
            ( 'E', 'server', 'Make a full server installation' ),
            ( 'S:', 'setup=', 'DIRAC Setup to use' ),
            ( 'C:', 'configurationServer=', 'Configuration servers to use' ),
            ( 'T:', 'CPUTime', 'Requested CPU Time' ),
            ( 'G:', 'Group=', 'DIRAC Group to use' ),
            ( 'O:', 'OwnerDN', 'Pilot OwnerDN (for private pilots)' ),
            ( 'U', 'Upload', 'Upload compiled distribution (if built)' ),
            ( 'V:', 'VO=', 'Virtual Organization' ),
            ( 'W:', 'gateway=', 'Configure <gateway> as DIRAC Gateway during installation' ),
            ( 's:', 'section=', 'Set base section for relative parsed options' ),
            ( 'o:', 'option=', 'Option=value to add' ),
            ( 'c', 'cert', 'Use server certificate instead of proxy' ),
            ( 'R:', 'reference=', 'Use this pilot reference' ),
            ( 'x:', 'execute=', 'Execute instead of JobAgent' ),
          )


rootPath = os.getcwd()
workingDir = os.getcwd()  # it is the directory where the dirac.cfg is created, in traditional installation is = to the rootPath
originalRootPath = os.getcwd()
installScript = ''
installScriptName = ''
SetDebug = False



def main():
  pythonpathCheck()
  install = InstallDIRAC()
  install.setInstallOpt()
  install.execute()
  diracScript = os.path.join( rootPath, 'scripts' )
  configure = ConfigureDIRAC( diracScript = diracScript, rootPath = rootPath, EnviRon = '', noCert = True )
  configure.setConfigureOpt()
  configure.execute()
  configure.setInProcessOpts()
  configure.startJobAgent()


class InstallDIRAC( object ):

  def __init__( self ):
    self.installOpts = []
    self.gridVersion = '2013-04-22'
    self.pythonVersion = '26'
    self.platform = ""
    self.rootPath = ''

  def setInstallOpt( self ):
    """Setup installation parameters"""

    global rootPath
    optList, __args__ = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )

    logDEBUG( 'PARAMETER [%s]' % ', '.join( map( str, optList ) ) )

    for o, v in optList:
      if o in ( '-h', '--help' ):
        print "Usage %s <opts>" % sys.argv[0]
        for cmdOpt in cmdOpts:
          print "%s %s : %s" % ( cmdOpt[0].ljust( 4 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
        sys.exit( 1 )
      elif o in ( '-b', '--build' ):
        self.installOpts.append( '-b' )
      elif o == '-d' or o == '--debug':
        self.installOpts.append( '-d' )
      elif o == '-e' or o == '--extraPackages':
        self.installOpts.append( '-e "%s"' % v )
      elif o == '-g' or o == '--grid':
        self.gridVersion = v
      elif o == '-i' or o == '--python':
        self.pythonVersion = v
      elif o in ( '-l', '--project' ):
        self.installOpts.append( "-l '%s'" % v )
      elif o == '-p' or o == '--platform':
        self.platform = v 
      elif o == '-r' or o == '--release':
        v = v.split(',',1)[0] # for traditional DIRAC Installation take the first release from the list of accepted release
        self.installOpts.append( '-r "%s"' % v )
      elif o == '-u' or o == '--url':
        self.installOpts.append( '-u "%s"' % v )
      elif o in ( '-P', '--path' ):
        self.installOpts.append( '-P "%s"' % v )
        rootPath = v
      elif o in ( '-V', '--installation' ):
        self.installOpts.append( '-V "%s"' % v )
      elif o == '-E' or o == '--server':
        self.installOpts.append( '-t "server"' )

    if self.gridVersion:
      self.installOpts.append( "-g '%s'" % self.gridVersion )
    if self.pythonVersion:
      self.installOpts.append( "-i '%s'" % self.pythonVersion )
    if self.platform:
      self.installOpts.append( '-p "%s"' % self.platform )

    logDEBUG( 'INSTALL OPTIONS [%s]' % ', '.join( map( str, self.installOpts ) ) )
    
    
    
  def execute( self ):
    try:
      pilotScript = os.path.realpath( __file__ )  # in old python versions __file__ is not defined
    except:
      pilotScript = os.path.realpath( sys.argv[0] )
    pilotScriptName = os.path.basename( pilotScript )
    pilotRootPath = os.path.dirname( pilotScript )

    installScriptName = 'dirac-install.py'
    originalRootPath = os.getcwd()
    rootPath = os.getcwd()

    ############################################################################
    # Locate installation script
    for path in ( pilotRootPath, originalRootPath, rootPath ):
      installScript = os.path.join( path, installScriptName )
      if os.path.isfile( installScript ):
        break

    if not os.path.isfile( installScript ):
      logERROR( "%s requires %s to exist in one of: %s, %s, %s" % ( pilotScriptName, installScriptName,
                                                            pilotRootPath, originalRootPath, rootPath ) )
      sys.exit( 1 )

    try:
      os.chmod( installScript, stat.S_IRWXU )  # change permission of the script
    except:
      pass

    #############################################################################
    # Do the installation

    installCmd = "%s %s" % ( installScript, " ".join( self.installOpts ) )
    logDEBUG( "Installing with: %s" % installCmd )
    if os.system( installCmd ):
      logERROR( "Could not make a proper DIRAC installation" )
      sys.exit( 1 )
  
  
    #############################################################################
    # Version print
  def printVersion( self ):
    logINFO( "Running %s" % " ".join( sys.argv ) )
    try:
      fd = open( "%s.run" % sys.argv[0], "w" )
      pickle.dump( sys.argv[1:], fd )
      fd.close()
    except:
      pass
    logINFO( "Version %s" % __RCSID__ )

    
    


class ConfigureDIRAC( object ):
  
  def __init__( self, diracScript, rootPath, EnviRon, noCert ):
    """
    diracScript is the path of the script files;
    rootPath is the local path of DIRAC, it is used as path where to install DIRAC for traditional installation and as path where to create the dirac.cfg for VOs specific installation
    EnviRon is a dictionary containing the set-up environment of a specific experiment
    noCert it is True when the setup is not Certification and it is False when the setup is certification
    """

    self.configureOpts = []
    self.inProcessOpts = []
    self.jobAgentOpts = []
    self.flavour = 'DIRAC'
    self.PilotReference = ''
    self.boincUserID = ''  #  The user ID in a BOINC computing element
    self.boincHostPlatform = ''  # The os type of the host machine running the pilot, not the virtual machine
    self.boincHostID = ''  # the host id in a  BOINC computing element
    self.boincHostName = ''  # the host name of the host machine running the pilot, not the virtual machine
    self.EnviRon = EnviRon 
    self.voFlag = False
    self.noCert = noCert
    if self.EnviRon:  # if the dictionary containing the environment of LHCbDirac is not empty, the voFlag is set to True to indicate the LHCbVo
      self.voFlag = True
      logDEBUG( 'voFlag %s' % self.voFlag )
    self.ceName = ""
    self.queueName = ""
    self.releaseVersionList = []
    self.releaseVersion = '' 
    self.releaseProject = ''
    self.diracScriptsPath = diracScript  # Set the env to use the recently installed DIRAC
    sys.path.insert( 0, self.diracScriptsPath )
    self.rootPath = rootPath
    self.inProcessOpts=[]
    self.jobCPUReq = 900
    self.maxCycles = 100
    self.userGroup = ""
    self.userDN = ""
    self.CE = ""
    self.platform = ""
    self.minDiskSpace = 2560 #MB
    self.dryRun = False
    self.testVOMSOK = False
    self.site = ""
 
  def setInProcessOpts( self ):

    global workingDir
    localUid = os.getuid()
    try:
      import pwd
      localUser = pwd.getpwuid( localUid )[0]
    except:
      localUser = 'Unknown'
    logINFO( 'User Name  = %s' % localUser )
    logINFO( 'User Id    = %s' % localUid )
    self.inProcessOpts = ['-s /Resources/Computing/CEDefaults' ]
    self.inProcessOpts.append( '-o WorkingDirectory=%s' % workingDir )
    self.inProcessOpts.append( '-o GridCE=%s' % self.ceName )
    if self.flavour in ['LCG', 'gLite', 'OSG']:
      self.inProcessOpts.append( '-o GridCEQueue=%s' % self.CE )
    self.inProcessOpts.append( '-o LocalAccountString=%s' % localUser )
    self.inProcessOpts.append( '-o TotalCPUs=%s' % 1 )
    self.inProcessOpts.append( '-o MaxCPUTime=%s' % ( int( self.jobCPUReq ) ) )
    self.inProcessOpts.append( '-o CPUTime=%s' % ( int( self.jobCPUReq ) ) )
    self.inProcessOpts.append( '-o MaxRunningJobs=%s' % 1 )
    # To prevent a wayward agent picking up and failing many jobs.
    self.inProcessOpts.append( '-o MaxTotalJobs=%s' % 10 )
    self.jobAgentOpts= ['-o MaxCycles=%s' % self.maxCycles]
    
    # jobAgentOpts.append( '-o CEUniqueID=%s' % JOB_AGENT_CE )
    if SetDebug:
      self.jobAgentOpts.append( '-o LogLevel=DEBUG' )

    if self.userGroup:
      logINFO( 'Setting DIRAC Group to "%s"' % self.userGroup )
      self.inProcessOpts .append( '-o OwnerGroup="%s"' % self.userGroup )

    if self.userDN:
      logINFO( 'Setting Owner DN to "%s"' % self.userDN )
      self.inProcessOpts .append( '-o OwnerDN="%s"' % self.userDN )

    
  def setConfigureOpt( self ):
    """Setup configuration parameters"""

    optList, __args__ = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )
    for o, v in optList:
      if o in ( '-h', '--help' ):
        print "Usage %s <opts>" % sys.argv[0]
        for cmdOpt in cmdOpts:
          print "%s %s : %s" % ( cmdOpt[0].ljust( 4 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
        sys.exit( 1 )
      elif o == '-n' or o == '--name':
        self.configureOpts.append( '-n "%s"' % v )
        self.site = v
      elif o == '-N' or o == '--Name':
        self.configureOpts.append( '-N "%s"' % v )
        self.ceName = v
      elif o == '-R' or o == '--reference':
        self.pilotReference = v
      elif o == '-d' or o == '--debug':
        self.configureOpts.append( '-d' )
        SetDebug = True
      elif o in ( '-S', '--setup' ):
        self.configureOpts.append( '-S "%s"' % v )
      elif o in ( '-C', '--configurationServer' ):
        self.configureOpts.append( '-C "%s"' % v )
      elif o in ( '-G', '--Group' ):
        self.userGroup = v
      elif o in ( '-O', '--OwnerDN' ):
        self.userDN = v
      elif o == '-t' or o == '--test':
        self.dryRun = True
      elif o in ( '-V', '--installation' ):
        #self.configureOpts.append( 'defaults-%s.cfg' % v )
        self.configureOpts.append('-V "%s"' % v)
      elif o == '-p' or o == '--platform':
        self.platform = v  
      elif o == '-D' or o == '--disk':
        try:
          self.minDiskSpace = int( v )
        except:
          pass
      elif o == '-r' or o == '--release':
        self.releaseVersionList = v
        self.releaseVersion = v.split(',',1)[0]
      elif o in ( '-l', '--project' ):
        self.releaseProject = v
        self.configureOpts.append( '-o /LocalSite/ReleaseProject=%s' % v )
      elif o in ( '-W', '--gateway' ):
        self.configureOpts.append( '-W "%s"' % v )
      elif o == '-o' or o == '--option':
        self.configureOpts.append( '-o "%s"' % v )
      elif o == '-s' or o == '--section':
        self.configureOpts.append( '-s "%s"' % v )
      elif o == '-c' or o == '--cert':
        self.configureOpts.append( '--UseServerCertificate' )
    
    self.setFlavour()
    self.configureOpts.append( '-o /LocalSite/GridMiddleware=%s' % self.flavour )
  
    if self.userGroup:
      self.configureOpts.append( '-o /AgentJobRequirements/OwnerGroup="%s"' % self.userGroup )

    if self.userDN:
      self.configureOpts.append( '-o /AgentJobRequirements/OwnerDN="%s"' % v )


    if self.PilotReference != 'Unknown':
      self.configureOpts.append( '-o /LocalSite/PilotReference=%s' % self.PilotReference )
    # add options for BOINc
    if self.boincUserID:
      self.configureOpts.append( '-o /LocalSite/BoincUserID=%s' % self.boincUserID )
    if self.boincHostID:
      self.configureOpts.append( '-o /LocalSite/BoincHostID=%s' % self.boincHostID )
    if self.boincHostPlatform:
      self.configureOpts.append( '-o /LocalSite/BoincHostPlatform=%s' % self.boincHostPlatform )
    if self.boincHostName:
      self.configureOpts.append( '-o /LocalSite/BoincHostName=%s' % self.boincHostName )


    self.getCEName()
    self.configureOpts.append( '-N "%s"' % self.ceName )
    if self.queueName:
      self.configureOpts.append( '-o /LocalSite/CEQueue=%s' % self.queueName )
    if self.ceName:
      self.configureOpts.append( '-o /LocalSite/GridCE=%s' % self.ceName )
    if self.voFlag:
      logINFO( 'Setting voFlag to "%s"' % self.voFlag )
      self.configureOpts.append('-x')
    logDEBUG ( 'CONFIGURE [%s]' % ', '.join( map( str, self.configureOpts ) ) )

    
  def setFlavour(self):

    pilotRef = 'Unknown'

    # Pilot reference is specified at submission
    if self.PilotReference:
      self.flavour = 'DIRAC'
      pilotRef = self.PilotReference

    # Take the reference from the Torque batch system
    if os.environ.has_key( 'PBS_JOBID' ):
      self.flavour = 'SSHTorque'
      pilotRef = 'sshtorque://' + self.ceName + '/' + os.environ['PBS_JOBID']
      self.queueName = os.environ['PBS_QUEUE']

    # Grid Engine
    if os.environ.has_key( 'JOB_ID' ):
      self.flavour = 'SSHGE'
      pilotRef = 'sshge://' + self.ceName + '/' + os.environ['JOB_ID']

    # Condor
    if os.environ.has_key( 'CONDOR_JOBID' ):
      self.flavour = 'SSHCondor'
      pilotRef = 'sshcondor://' + self.ceName + '/' + os.environ['CONDOR_JOBID']

    # LSF
    if os.environ.has_key( 'LSB_BATCH_JID' ):
      self.flavour = 'SSHLSF'
      pilotRef = 'sshlsf://' + self.ceName + '/' + os.environ['LSB_BATCH_JID']

    # This is the CREAM direct submission case
    if os.environ.has_key( 'CREAM_JOBID' ):
      self.flavour = 'CREAM'
      pilotRef = os.environ['CREAM_JOBID']

    # If we still have the GLITE_WMS_JOBID, it means that the submission
    # was through the WMS, take this reference then
    if os.environ.has_key( 'EDG_WL_JOBID' ):
      self.flavour = 'LCG'
      pilotRef = os.environ['EDG_WL_JOBID']

    if os.environ.has_key( 'GLITE_WMS_JOBID' ):
      if os.environ['GLITE_WMS_JOBID'] != 'N/A':
        self.flavour = 'gLite'
        pilotRef = os.environ['GLITE_WMS_JOBID']

    if os.environ.has_key( 'OSG_WN_TMP' ):
      self.flavour = 'OSG'

    self.OSG()

    # Direct SSH tunnel submission
    if os.environ.has_key( 'SSHCE_JOBID' ):
      self.flavour = 'SSH'
      pilotRef = 'ssh://' + self.ceName + '/' + os.environ['SSHCE_JOBID']

    # ARC case
    if os.environ.has_key( 'GRID_GLOBAL_JOBID' ):
      self.flavour = 'ARC'
      pilotRef = os.environ['GRID_GLOBAL_JOBID']

    # This is for BOINC case
    if os.environ.has_key( 'BOINC_JOB_ID' ):
      self.flavour = 'BOINC'
      pilotRef = os.environ['BOINC_JOB_ID']

    if self.flavour == 'BOINC':
      if os.environ.has_key( 'BOINC_USER_ID' ):
        self.boincUserID = os.environ['BOINC_USER_ID']
      if os.environ.has_key( 'BOINC_HOST_ID' ):
        self.boincHostID = os.environ['BOINC_HOST_ID']
      if os.environ.has_key( 'BOINC_HOST_PLATFORM' ):
        self.boincHostPlatform = os.environ['BOINC_HOST_PLATFORM']
      if os.environ.has_key( 'BOINC_HOST_NAME' ):
        self.boincHostName = os.environ['BOINC_HOST_NAME']

    logDEBUG( "Flavour: %s; pilot reference: %s " % ( self.flavour, pilotRef ) )

    self.PilotReference = pilotRef


  def getCEName ( self ):

    if self.flavour in ['LCG', 'gLite', 'OSG']:
      retCode, self.CE = executeAndGetOutput( 'glite-brokerinfo getCE || edg-brokerinfo getCE',self.EnviRon)
      if not retCode:
        self.ceName = self.CE.split( ':' )[0]
        if len( self.CE.split( '/' ) ) > 1:
          self.queueName = self.CE.split( '/' )[1]
      # configureOpts.append( '-N "%s"' % cliParams.ceName )
      elif os.environ.has_key( 'OSG_JOB_CONTACT' ):
    # OSG_JOB_CONTACT String specifying the endpoint to use within the job submission
    #                 for reaching the site (e.g. manager.mycluster.edu/jobmanager-pbs )
        CE = os.environ['OSG_JOB_CONTACT']
        self.ceName = CE.split( '/' )[0]
        if len( CE.split( '/' ) ) > 1:
          self.queueName = CE.split( '/' )[1]
      # configureOpts.append( '-N "%s"' % cliParams.ceName )
      else:
        logERROR( "There was an error executing brokerinfo. Setting ceName to local " )
    elif self.flavour == "CREAM":
      if os.environ.has_key( 'CE_ID' ):
        self.ceName = os.environ['CE_ID'].split( ':' )[0]
        if os.environ['CE_ID'].count( "/" ):
          self.queueName = os.environ['CE_ID'].split( '/' )[1]



  def OSG ( self ):

    """ Treat the OSG case """
    osgDir = ''
    if self.flavour == "OSG":
      vo = self.releaseProject.replace( 'DIRAC', '' ).upper()
      if not vo:
        vo = 'DIRAC'
      osgDir = os.environ['OSG_WN_TMP']
      # Make a separate directory per Project if it is defined
      jobDir = os.path.basename( self.PilotReference )
      if not jobDir:  # just in case
        import random
        jobDir = str( random.randint( 1000, 10000 ) )
      osgDir = os.path.join( osgDir, vo, jobDir )
      if not os.path.isdir( osgDir ):
        os.makedirs( osgDir )
      os.chdir( osgDir )
      try:
        import shutil
        shutil.copy( installScript, os.path.join( osgDir, installScriptName ) )
      except Exception, x:
        print sys.executable
        print sys.version
        print os.uname()
        print x
        raise x
    if os.environ.has_key( 'OSG_APP' ):
    # Try to define it here although this will be only in the local shell environment
      os.environ['VO_%s_SW_DIR' % vo] = os.path.join( os.environ['OSG_APP'], vo )

    #if self.rootPath == originalRootPath:
      # No special root path was requested
      #rootPath = os.getcwd()  
    
  def execute( self ):
    """ Configure DIRAC """

    # Instead of dumping the Full configuration, include all Server in dirac.cfg

    self.configureOpts.append('-o /LocalSite/ReleaseVersion=%s' % self.releaseVersion)
    self.configureOpts.append( '-I' )

    # just for test when normal installation
    # __result__ = os.system( "cp /home/dirac/test/dirac-configure.py /home/dirac/test/DIRAC/Core/scripts/dirac-configure.py" )

    # for test when cvmfs
    #configureCmd = "python %s %s" % ( os.path.join( "/home/dirac/test/", "dirac-configure.py" ), " ".join( self.configureOpts ) )
    
    configureCmd = "python %s %s" % ( os.path.join( self.diracScriptsPath, "dirac-configure" ), " ".join( self.configureOpts ) )

    logDEBUG( "Configuring DIRAC with: %s" % configureCmd )
    retCode, __outData__ = executeAndGetOutput( configureCmd, self.EnviRon )

    if retCode:
      logERROR( "Could not configure DIRAC" )
      sys.exit( 1 )

    # Dump the CS to cache in file

    # cfgFile = os.path.join( rootPath, "etc", "dirac.cfg" )
    # cacheScript = os.path.join( diracScriptsPath, "dirac-configuration-dump-local-cache" )
    # if os.system( "%s -f %s" % ( cacheScript, cfgFile ) ):
    #   logERROR( "Could not dump the CS to %s" % cfgFile )
    configureScript = os.path.join( self.diracScriptsPath, "dirac-configure" )


    # Set the LD_LIBRARY_PATH and PATH

    if not self.platform:
      platformPath = os.path.join(self.rootPath, "DIRAC", "Core", "Utilities", "Platform.py" )
      platFD = open( platformPath, "r" )
      PlatformModule = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
      platFD.close()
      self.platform = PlatformModule.getPlatformString()

    if not self.EnviRon or not self.noCert:  # if traditional installation
      if self.testVOMSOK:
      # Check voms-proxy-info before touching the original PATH and LD_LIBRARY_PATH
        os.system( 'which voms-proxy-info && voms-proxy-info -all' )

      diracLibPath = os.path.join( self.rootPath, self.platform, 'lib' )
      diracBinPath = os.path.join( self.rootPath, self.platform, 'bin' )
      for envVarName in ( 'LD_LIBRARY_PATH', 'PYTHONPATH' ):
        if envVarName in os.environ:
          os.environ[ '%s_SAVE' % envVarName ] = os.environ[ envVarName ]
          del( os.environ[ envVarName ] )
        else:
          os.environ[ '%s_SAVE' % envVarName ] = ""
      os.environ['LD_LIBRARY_PATH'] = "%s" % ( diracLibPath )
      os.environ['PATH'] = '%s:%s:%s' % ( diracBinPath, self.diracScriptsPath, os.getenv( 'PATH' ) )

    #########################################################################################################################
    # Check proxy

    retCode, __outData__ = executeAndGetOutput( 'dirac-proxy-info', self.EnviRon )
    if self.testVOMSOK:
      retCode, __outData__ = executeAndGetOutput( 'dirac-proxy-info | grep -q fqan', self.EnviRon )
      if retCode != 0:
        retCode, __outData__ = executeAndGetOutput( 'dirac-proxy-info 2>&1 | mail -s "dirac-pilot: missing voms certs at %s" dirac.alarms@gmail.com' % self.site, self.EnviRon )
        sys.exit( -1 )

    ##########################################################################################################################
    # Set the local architecture

    if not self.EnviRon or not self.noCert:  # if traditional installation
      architectureScriptName = "dirac-architecture"
      architectureScript = ""
      candidate = os.path.join( self.rootPath, "scripts", architectureScriptName )
      if os.path.isfile( candidate ):
        architectureScript = candidate
      else:
      # If the extension does not provide a dirac-architecture, use dirac-platform as default value
        candidate = os.path.join( self.rootPath, "scripts", "dirac-platform" )
        if os.path.isfile( candidate ):
          architectureScript = candidate

      if architectureScript:
        retCode, localArchitecture = executeAndGetOutput( architectureScript,self.EnviRon)  
        if not retCode:
          localArchitecture = localArchitecture.strip()
          os.environ['CMTCONFIG'] = localArchitecture
          logINFO( 'Setting CMTCONFIG=%s' % localArchitecture )
        # os.system( "%s -f %s -o '/LocalSite/Architecture=%s'" % ( cacheScript, cfgFile, localArchitecture ) )
        # dirac-configure will not change existing cfg unless -U option is used.
          os.system( "%s -F -o '/LocalSite/Architecture=%s'" % ( configureScript, localArchitecture ) )
        else:
          logERROR( "There was an error calling %s" % architectureScript )

   ###############################################################################################################################
   # Get host and local user info

    logINFO( 'Uname      = %s' % " ".join( os.uname() ) )
    logINFO( 'Host Name  = %s' % socket.gethostname() )
    logINFO( 'Host FQDN  = %s' % socket.getfqdn() )
    logINFO( 'workingDir = %s' % workingDir )  # this could be different than rootPath
    
    fileName = '/etc/redhat-release'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      logINFO( 'RedHat Release = %s' % f.read().strip() )
      f.close()

    fileName = '/etc/lsb-release'
    if os.path.isfile( fileName ):
      f = open( fileName, 'r' )
      logINFO( 'Linux release:\n%s' % f.read().strip() )
      f.close()

    fileName = '/proc/cpuinfo'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      cpu = f.readlines()
      f.close()
      nCPU = 0
      for line in cpu:
        if line.find( 'cpu MHz' ) == 0:
          nCPU += 1
          freq = line.split()[3]
        elif line.find( 'model name' ) == 0:
          CPUmodel = line.split( ': ' )[1].strip()
      logINFO( 'CPU (model)    = %s' % CPUmodel )
      logINFO( 'CPU (MHz)      = %s x %s' % ( nCPU, freq ) )

    fileName = '/proc/meminfo'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      mem = f.readlines()
      f.close()
      freeMem = 0
      for line in mem:
        if line.find( 'MemTotal:' ) == 0:
          totalMem = int( line.split()[1] )
        if line.find( 'MemFree:' ) == 0:
          freeMem += int( line.split()[1] )
        if line.find( 'Cached:' ) == 0:
          freeMem += int( line.split()[1] )
      logINFO( 'Memory (kB)    = %s' % totalMem )
      logINFO( 'FreeMem. (kB)  = %s' % freeMem )

   ##############################################################################################################################
   # Disk space check

    #fs = os.statvfs( rootPath )
    fs = os.statvfs( workingDir )
    # bsize;    /* file system block size */
    # frsize;   /* fragment size */
    # blocks;   /* size of fs in f_frsize units */
    # bfree;    /* # free blocks */
    # bavail;   /* # free blocks for non-root */
    # files;    /* # inodes */
    # ffree;    /* # free inodes */
    # favail;   /* # free inodes for non-root */
    # flag;     /* mount flags */
    # namemax;  /* maximum filename length */
    diskSpace = fs[4] * fs[0] / 1024 / 1024
    logINFO( 'DiskSpace (MB) = %s' % diskSpace )

    if diskSpace < self.minDiskSpace:
      logERROR( '%s MB < %s MB, not enough local disk space available, exiting'
                  % ( diskSpace, self.minDiskSpace ) )
      sys.exit( 1 )
      
  
  def getCPURequirement(self):
    """ Get job CPU requirement and queue normalization """

    if self.flavour in ['LCG', 'gLite', 'OSG']:
      logINFO( 'CE = %s' % self.CE )
      logINFO( 'LCG_SITE_CE = %s' % self.ceName )

      retCode, queueNormList = executeAndGetOutput( 'dirac-wms-get-queue-normalization %s' % self.CE,self.EnviRon )
      if not retCode:
        queueNormList = queueNormList.strip().split( ' ' )
        if len( queueNormList ) == 2:
          queueNorm = float( queueNormList[1] )
          logINFO( 'Queue Normalization = %s SI00' % queueNorm )
          if queueNorm:
          # Update the local normalization factor: We are using seconds @ 250 SI00 = 1 HS06
          # This is the ratio SpecInt published by the site over 250 (the reference used for Matching)
          # os.system( "%s -f %s -o /LocalSite/CPUScalingFactor=%s" % ( cacheScript, cfgFile, queueNorm / 250. ) )
          # os.system( "%s -f %s -o /LocalSite/CPUNormalizationFactor=%s" % ( cacheScript, cfgFile, queueNorm / 250. ) )
            os.system( "%s -F -o /LocalSite/CPUScalingFactor=%s -o /LocalSite/CPUNormalizationFactor=%s" % ( self.configureScript,
                                                                                                      queueNorm / 250.,
                                                                                                      queueNorm / 250. ) )
        else:
          logERROR( 'Fail to get Normalization of the Queue' )
      else:
        logERROR( "There was an error calling dirac-wms-get-queue-normalization" )

      retCode, queueLength = executeAndGetOutput( 'dirac-wms-get-normalized-queue-length %s' % self.CE, self.EnviRon )
      if not retCode:
        queueLength = queueLength.strip().split( ' ' )
        if len( queueLength ) == 2:
          self.jobCPUReq = float( queueLength[1] )
          logINFO( 'Normalized Queue Length = %s' % self.jobCPUReq )
        else:
          logERROR( 'Failed to get Normalized length of the Queue' )
      else:
        logERROR( "There was an error calling dirac-wms-get-normalized-queue-length" )
        
     # Instead of using the Average reported by the Site, determine a Normalization
     # os.system( "dirac-wms-cpu-normalization -U" )

    
  def startJobAgent(self):
    """Starting of the JobAgent"""
    
# Find any .cfg file uploaded with the sandbox
    
    diracAgentScript = os.path.join( self.rootPath, "scripts", "dirac-agent" )
    if self.EnviRon:
      self.rootPath = os.getcwd()  # should be changed in producion when using cvmfs but not in certif
      if not self.noCert:
        diracAgentScript = os.path.join( self.rootPath, "scripts", "dirac-agent" )
    extraCFG = []
    for i in os.listdir( self.rootPath ):
      cfg = os.path.join( self.rootPath, i )
      cfg = os.path.join(cfg,'dirac.cfg')
      if os.path.isfile( cfg ): #and re.search( '.cfg&', cfg ):
        extraCFG.append( cfg )

    #if cliParams.executeCmd:
      # Execute user command
      #logINFO( "Executing user defined command: %s" % cliParams.executeCmd )
      #sys.exit( os.system( "source bashrc; %s" % cliParams.executeCmd ) / 256 )

    logINFO( 'Starting JobAgent' )
    os.environ['PYTHONUNBUFFERED'] = 'yes'

    jobAgent = '%s WorkloadManagement/JobAgent %s %s %s' % ( diracAgentScript,
                                                         " ".join( self.jobAgentOpts ),
                                                         " ".join( self.inProcessOpts ),
                                                         " ".join( extraCFG ) )

    logINFO( "JobAgent execution command:\n%s" % jobAgent )
 
   
    if not self.dryRun:
      retCode, __outData__ = executeAndGetOutput( jobAgent, self.EnviRon )
      if retCode:
        logERROR( "Could not start the JobAgent" )
        sys.exit( 1 )


    #fs = os.statvfs( self.rootPath )
    fs = os.statvfs( workingDir )
    diskSpace = fs[4] * fs[0] / 1024 / 1024
    logINFO( 'DiskSpace (MB) = %s' % diskSpace )
    #ret = os.system( 'dirac-proxy-info' )
    #if os.environ.has_key( 'OSG_WN_TMP' ) and osgDir:
      # os.chdir( originalRootPath )
      # import shutil
      # shutil.rmtree( osgDir )
    sys.exit( 0 )



def pythonpathCheck():

  try:
    os.umask( 022 )
    pythonpath = os.getenv( 'PYTHONPATH', '' ).split( ':' )
    for p in pythonpath:
      if p == '': continue
      try:
        if os.path.normpath( p ) in sys.path:
          # In case a given directory is twice in PYTHONPATH it has to removed only once
          sys.path.remove( os.path.normpath( p ) )
      except Exception, x:
        print 'Directories in PYTHONPATH:', pythonpath
        print 'Failing path:', p, os.path.normpath( p )
        print 'sys.path:', sys.path
        raise x
  except Exception, x:
    print sys.executable
    print sys.version
    print os.uname()
    print x
    raise x


def executeAndGetOutput( cmd, EnviRon ):
    """ Execute a command on the worker node and get the output"""

    logDEBUG( 'in execute and get output' )
    try:
      import subprocess  # spawn new processes, connect to their input/output/error pipes, and obtain their return codes.
      if EnviRon !='':
        logDEBUG( "Experiment environment not empty, cmd is %s" % cmd )
        _p = subprocess.Popen( "%s" % cmd, shell = True, env=EnviRon, stdout = subprocess.PIPE,
                          stderr = subprocess.PIPE, close_fds = False )
        outData = _p.stdout.read().strip()
        returnCode = _p.wait()
        return (returnCode, outData)
      else:
        logDEBUG( "Proceed with traditional installation, cmd is %s" % cmd )
        _p = subprocess.Popen( "%s" % cmd, shell = True, stdout = subprocess.PIPE,
                          stderr = subprocess.PIPE, close_fds = False )
        outData = _p.stdout.read().strip()
        returnCode = _p.wait()
        return (returnCode, outData)
    except ImportError:
      logERROR( "Error importing subprocess" )



#############################################################################################################################
# Helper functions

def logDEBUG( msg ):
  if SetDebug:
    for _line in msg.split( "\n" ):
      print "%s UTC dirac-pilot [DEBUG] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), _line )
    sys.stdout.flush()

def logERROR( msg ):
  for _line in msg.split( "\n" ):
    print "%s UTC dirac-pilot [ERROR] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), _line )
  sys.stdout.flush()

def logINFO( msg ):
  for _line in msg.split( "\n" ):
    print "%s UTC dirac-pilot [INFO]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), _line )
  sys.stdout.flush()


if __name__ == "__main__":
    main()

  
