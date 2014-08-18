########################################################################
# $Id$
########################################################################

""" Definitions of a standard set of pilot commands

    Each commands is represented by a class inheriting CommandBase class.
    The command class constructor takes PilotParams object which is a data
    structure which keeps common parameters across all the pilot commands.

    The constructor must call the superclass constructor with the PilotParams
    object and the command name as arguments, e.g. ::

        class InstallDIRAC( CommandBase ):

          def __init__( self, pilotParams ):
            CommandBase.__init__(self, pilotParams, 'Install')
            ...

    The command class must implement execute() method for the actual command
    execution.
"""

import sys
import os
import stat
import socket
import re
import signal
import urllib2
import json

from pilotTools import CommandBase, which

__RCSID__ = "$Id$"

class GetPilotVersion( CommandBase ):
  """ Used to get the pilot version that needs to be installed.
      If passed as a parameter, uses that one. If not passed, it looks for alternatives.

      This assures that a version is always got even on non-standard Grid resources.
  """

  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( GetPilotVersion, self ).__init__( pilotParams )

    # These parameters can be set by the VO
    self.pilotCFGFileLocation = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/'
    self.pilotCFGFile = '%s-pilot.json' % self.pp.releaseProject
    
  def execute(self):
    """ Standard method for pilot commands
    """
    if self.pp.releaseVersion:
      self.log.info( "Pilot version requested as pilot script option. Nothing to do." )
    else:
      self.log.info( "Pilot version not requested as pilot script option, going to find it" )
      self.__urlretrieveTimeout( self.pilotCFGFileLocation + '/' + self.pilotCFGFile, self.pilotCFGFile, timeout = 120 )
      fp = open( self.pilotCFGFile, 'r' )
      pilotCFGFileContent = json.load( fp )
      fp.close()
      pilotVersions = [str( pv ) for pv in pilotCFGFileContent[self.pp.setup]['Version']]
      self.log.debug( "Pilot versions found: %s" % ', '.join( pilotVersions ) )
      self.log.info( "Setting pilot version to %s" % pilotVersions[0] )
      self.pp.releaseVersion = pilotVersions[0]

  def __urlretrieveTimeout( self, url, fileName = '', timeout = 0 ):
    """ Retrieve remote url to local file, with timeout wrapper
    """
    self.log.debug( "Retrieving remote file '%s'" % url )

    urlData = ''
    if timeout:
      signal.signal( signal.SIGALRM, self.__alarmTimeoutHandler )
      # set timeout alarm
      signal.alarm( timeout + 5 )
    try:
      remoteFD = urllib2.urlopen( url )
      expectedBytes = 0
      # Sometimes repositories do not return Content-Length parameter
      try:
        expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
      except Exception, x:
        self.log.warn( "Content-Length parameter not returned, skipping expectedBytes check" )

      if fileName:
        localFD = open( fileName, "wb" )
      receivedBytes = 0L
      data = remoteFD.read( 16384 )
      count = 1
      progressBar = False
      while data:
        receivedBytes += len( data )
        if fileName:
          localFD.write( data )
        else:
          urlData += data
        data = remoteFD.read( 16384 )
        if count % 20 == 0:
          print '\033[1D' + ".",
          sys.stdout.flush()
          progressBar = True
        count += 1
      if progressBar:
        # return cursor to the beginning of the line
        print '\033[1K',
        print '\033[1A'
      if fileName:
        localFD.close()
      remoteFD.close()
      if receivedBytes != expectedBytes and expectedBytes > 0:
        self.log.error( "File should be %s bytes but received %s" % ( expectedBytes, receivedBytes ) )
        return False
    except urllib2.HTTPError, x:
      if x.code == 404:
        self.log.error( "%s does not exist" % url )
        if timeout:
          signal.alarm( 0 )
        return False
    except Exception, x:
      if x == 'Timeout':
        self.log.error( "Timeout after %s seconds on transfer request for '%s'" % ( str( timeout ), url ) )
      if timeout:
        signal.alarm( 0 )
      raise x

    if timeout:
      signal.alarm( 0 )

    if fileName:
      return True
    else:
      return urlData

  def __alarmTimeoutHandler( self, *args ):
    raise Exception( 'Timeout' )


class InstallDIRAC( CommandBase ):
  """ Basically, this is used to call dirac-install with the passed parameters.

      It requires dirac-install script to be sitting in the same directory.
  """

  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( InstallDIRAC, self ).__init__( pilotParams )
    self.installOpts = []
    self.pp.rootPath = self.pp.pilotRootPath
    self.installScriptName = 'dirac-install.py'

  def _setInstallOptions( self ):
    """ Setup installation parameters
    """
    for o, v in self.pp.optList:
      if o in ( '-b', '--build' ):
        self.installOpts.append( '-b' )
      elif o == '-d' or o == '--debug':
        self.installOpts.append( '-d' )
      elif o == '-e' or o == '--extraPackages':
        self.installOpts.append( '-e "%s"' % v )
      elif o == '-g' or o == '--grid':
        self.pp.gridVersion = v
      elif o == '-i' or o == '--python':
        self.pp.pythonVersion = v
      elif o in ( '-l', '--project' ):
        self.installOpts.append( "-l '%s'" % v )
      elif o == '-p' or o == '--platform':
        self.pp.platform = v
      elif o == '-u' or o == '--url':
        self.installOpts.append( '-u "%s"' % v )
      elif o in ( '-P', '--path' ):
        self.installOpts.append( '-P "%s"' % v )
        self.pp.rootPath = v
      elif o in ( '-V', '--installation' ):
        self.installOpts.append( '-V "%s"' % v )
      elif o == '-t' or o == '--server':
        self.installOpts.append( '-t "server"' )

    if self.pp.gridVersion:
      self.installOpts.append( "-g '%s'" % self.pp.gridVersion )
    if self.pp.pythonVersion:
      self.installOpts.append( "-i '%s'" % self.pp.pythonVersion )
    if self.pp.platform:
      self.installOpts.append( '-p "%s"' % self.pp.platform )

    # The release version to install is a requirement
    self.installOpts.append( '-r "%s"' % self.pp.releaseVersion )

    self.log.debug( 'INSTALL OPTIONS [%s]' % ', '.join( map( str, self.installOpts ) ) )

  def _locateInstallationScript( self ):
    """ Locate installation script
    """
    installScript = ''
    for path in ( self.pp.pilotRootPath, self.pp.originalRootPath, self.pp.rootPath ):
      installScript = os.path.join( path, self.installScriptName )
      if os.path.isfile( installScript ):
        break
    self.installScript = installScript

    if not os.path.isfile( installScript ):
      self.log.error( "%s requires %s to exist in one of: %s, %s, %s" % ( self.pp.pilotScriptName,
                                                                          self.installScriptName,
                                                                          self.pp.pilotRootPath,
                                                                          self.pp.originalRootPath,
                                                                          self.pp.rootPath ) )
      sys.exit( 1 )

    try:
      # change permission of the script
      os.chmod( self.installScript, stat.S_IRWXU )
    except:
      pass

  def _installDIRAC( self ):
    """ launch the installation script
    """
    installCmd = "%s %s" % ( self.installScript, " ".join( self.installOpts ) )
    self.log.debug( "Installing with: %s" % installCmd )

    retCode, output = self.executeAndGetOutput( installCmd )
    self.log.info( output, header = False )

    if retCode:
      self.log.error( "Could not make a proper DIRAC installation" )
      sys.exit( 1 )
    self.log.info( "%s completed successfully" % self.installScriptName )

    diracScriptsPath = os.path.join( self.pp.rootPath, 'scripts' )
    platformScript = os.path.join( diracScriptsPath, "dirac-platform" )
    if not self.pp.platform:
      retCode, output = self.executeAndGetOutput( platformScript )
      if retCode:
        self.log.error( "Failed to determine DIRAC platform" )
        sys.exit( 1 )
      self.pp.platform = output
    diracBinPath = os.path.join( self.pp.rootPath, self.pp.platform, 'bin' )
    diracLibPath = os.path.join( self.pp.rootPath, self.pp.platform, 'lib' )

    for envVarName in ( 'LD_LIBRARY_PATH', 'PYTHONPATH' ):
      if envVarName in os.environ:
        os.environ[ '%s_SAVE' % envVarName ] = os.environ[ envVarName ]
        del( os.environ[ envVarName ] )
      else:
        os.environ[ '%s_SAVE' % envVarName ] = ""

    os.environ['LD_LIBRARY_PATH'] = "%s" % ( diracLibPath )
    sys.path.insert( 0, self.pp.rootPath )
    sys.path.insert( 0, diracScriptsPath )
    if "PATH" in os.environ:
      os.environ['PATH'] = '%s:%s:%s' % ( diracBinPath, diracScriptsPath, os.getenv( 'PATH' ) )
    else:
      os.environ['PATH'] = '%s:%s' % ( diracBinPath, diracScriptsPath )
    self.pp.diracInstalled = True

  def execute( self ):
    """ What is called all the time
    """

    self._setInstallOptions()
    self._locateInstallationScript()
    self._installDIRAC()


class ConfigureDIRAC( CommandBase ):
  """ Command to configure DIRAC
  """

  def __init__( self, pilotParams ):
    """ c'tor

        Here, we have to pay attention to the paths. Specifically, we need to know where to look for
        - executables (scripts)
        - DIRAC python code
        If the pilot has installed DIRAC (and extensions) in the traditional way, so using the dirac-install.py script,
        simply the current directory is used, and:
        - scripts will be in cwd/scripts.
        - DIRAC python code will be all sitting in cwd
        - the local dirac.cfg file will be found in cwd/etc

        For a more general case of non-traditional installations, we should use the PATH and PYTHONPATH as set by the
        installation phase.

        Executables and code will be searched there.
        The dirac.cfg file has to be created in the first directory of the PATH - ?????
    """
    super( ConfigureDIRAC, self ).__init__( pilotParams )

    # this variable contains the options that are passed to dirac-configure, and that will fill the local dirac.cfg file
    self.configureOpts = []
    self.CE = ""
    self.testVOMSOK = False

    self.boincUserID = ''
    self.boincHostID= ''
    self.boincHostPlatform = ''
    self.boincHostName = ''

  def __setConfigureOptions( self ):
    """ Setup configuration parameters
    """

    if self.pp.site:
      self.configureOpts.append( '-n "%s"' % self.pp.site )
    if self.pp.ceName:
      self.configureOpts.append( '-N "%s"' % self.pp.ceName )
    if self.debugFlag:
      self.configureOpts.append( '-d' )
    if self.pp.setup:
        self.configureOpts.append( '-S "%s"' % self.pp.setup )
    if self.pp.configServer:
      self.configureOpts.append( '-C "%s"' % self.pp.configServer )
    if self.pp.releaseProject:
      self.configureOpts.append( '-o /LocalSite/ReleaseProject=%s' % self.pp.releaseProject )
    if self.pp.gateway:
      self.configureOpts.append( '-W "%s"' % self.pp.gateway )
    if self.pp.useServerCertificate:
      self.configureOpts.append( '--UseServerCertificate' )

    for o, v in self.pp.optList:
      if o == '-o' or o == '--option':
        self.configureOpts.append( '-o "%s"' % v )
      elif o == '-s' or o == '--section':
        self.configureOpts.append( '-s "%s"' % v )


    self.__setFlavour()
    self.configureOpts.append( '-o /LocalSite/GridMiddleware=%s' % self.pp.flavour )

    if self.pp.userGroup:
      self.configureOpts.append( '-o /AgentJobRequirements/OwnerGroup="%s"' % self.pp.userGroup )

    if self.pp.userDN:
      self.configureOpts.append( '-o /AgentJobRequirements/OwnerDN="%s"' % v )


    if self.pp.pilotReference != 'Unknown':
      self.configureOpts.append( '-o /LocalSite/PilotReference=%s' % self.pp.pilotReference )
    # add options for BOINc
    if self.boincUserID:
      self.configureOpts.append( '-o /LocalSite/BoincUserID=%s' % self.boincUserID )
    if self.boincHostID:
      self.configureOpts.append( '-o /LocalSite/BoincHostID=%s' % self.boincHostID )
    if self.boincHostPlatform:
      self.configureOpts.append( '-o /LocalSite/BoincHostPlatform=%s' % self.boincHostPlatform )
    if self.boincHostName:
      self.configureOpts.append( '-o /LocalSite/BoincHostName=%s' % self.boincHostName )


    self.__getCEName()
    self.configureOpts.append( '-N "%s"' % self.pp.ceName )
    if self.pp.queueName:
      self.configureOpts.append( '-o /LocalSite/CEQueue=%s' % self.pp.queueName )
    if self.pp.ceName:
      self.configureOpts.append( '-o /LocalSite/GridCE=%s' % self.pp.ceName )
    self.log.debug ( 'CONFIGURE [%s]' % ', '.join( map( str, self.configureOpts ) ) )


  def __setFlavour(self):

    pilotRef = 'Unknown'

    # Pilot reference is specified at submission
    if self.pp.pilotReference:
      self.pp.flavour = 'DIRAC'
      pilotRef = self.pp.pilotReference

    # Take the reference from the Torque batch system
    if os.environ.has_key( 'PBS_JOBID' ):
      self.pp.flavour = 'SSHTorque'
      pilotRef = 'sshtorque://' + self.pp.ceName + '/' + os.environ['PBS_JOBID']
      self.pp.queueName = os.environ['PBS_QUEUE']

    # Take the reference from the OAR batch system
    if os.environ.has_key( 'OAR_JOBID' ):
      self.pp.flavour = 'SSHOAR'
      pilotRef = 'sshoar://' + self.pp.ceName + '/'+os.environ['OAR_JOBID']

    # Grid Engine
    if os.environ.has_key( 'JOB_ID' ):
      self.pp.flavour = 'SSHGE'
      pilotRef = 'sshge://' + self.pp.ceName + '/' + os.environ['JOB_ID']

    # Condor
    if os.environ.has_key( 'CONDOR_JOBID' ):
      self.pp.flavour = 'SSHCondor'
      pilotRef = 'sshcondor://' + self.pp.ceName + '/' + os.environ['CONDOR_JOBID']

    # LSF
    if os.environ.has_key( 'LSB_BATCH_JID' ):
      self.pp.flavour = 'SSHLSF'
      pilotRef = 'sshlsf://' + self.pp.ceName + '/' + os.environ['LSB_BATCH_JID']

    # This is the CREAM direct submission case
    if os.environ.has_key( 'CREAM_JOBID' ):
      self.pp.flavour = 'CREAM'
      pilotRef = os.environ['CREAM_JOBID']

    # If we still have the GLITE_WMS_JOBID, it means that the submission
    # was through the WMS, take this reference then
    if os.environ.has_key( 'EDG_WL_JOBID' ):
      self.pp.flavour = 'LCG'
      pilotRef = os.environ['EDG_WL_JOBID']

    if os.environ.has_key( 'GLITE_WMS_JOBID' ):
      if os.environ['GLITE_WMS_JOBID'] != 'N/A':
        self.pp.flavour = 'gLite'
        pilotRef = os.environ['GLITE_WMS_JOBID']

    if os.environ.has_key( 'OSG_WN_TMP' ):
      self.pp.flavour = 'OSG'

    self.__doOSG()

    # Direct SSH tunnel submission
    if os.environ.has_key( 'SSHCE_JOBID' ):
      self.pp.flavour = 'SSH'
      pilotRef = 'ssh://' + self.pp.ceName + '/' + os.environ['SSHCE_JOBID']

    # ARC case
    if os.environ.has_key( 'GRID_GLOBAL_JOBID' ):
      self.pp.flavour = 'ARC'
      pilotRef = os.environ['GRID_GLOBAL_JOBID']

    # This is for BOINC case
    if os.environ.has_key( 'BOINC_JOB_ID' ):
      self.pp.flavour = 'BOINC'
      pilotRef = os.environ['BOINC_JOB_ID']

    if self.pp.flavour == 'BOINC':
      if os.environ.has_key( 'BOINC_USER_ID' ):
        self.boincUserID = os.environ['BOINC_USER_ID']
      if os.environ.has_key( 'BOINC_HOST_ID' ):
        self.boincHostID = os.environ['BOINC_HOST_ID']
      if os.environ.has_key( 'BOINC_HOST_PLATFORM' ):
        self.boincHostPlatform = os.environ['BOINC_HOST_PLATFORM']
      if os.environ.has_key( 'BOINC_HOST_NAME' ):
        self.boincHostName = os.environ['BOINC_HOST_NAME']

    self.log.debug( "Flavour: %s; pilot reference: %s " % ( self.pp.flavour, pilotRef ) )

    self.pp.pilotReference = pilotRef

  def __getCEName ( self ):

    # FIXME: is this necessary at all?
    if self.pp.flavour in ['LCG', 'gLite', 'OSG']:
      retCode, self.CE = self.executeAndGetOutput( 'glite-brokerinfo getCE || edg-brokerinfo getCE',self.pp.installEnv)
      if not retCode:
        self.pp.ceName = self.CE.split( ':' )[0]
        if len( self.CE.split( '/' ) ) > 1:
          self.pp.queueName = self.CE.split( '/' )[1]
      # configureOpts.append( '-N "%s"' % cliParams.ceName )
      elif os.environ.has_key( 'OSG_JOB_CONTACT' ):
    # OSG_JOB_CONTACT String specifying the endpoint to use within the job submission
    #                 for reaching the site (e.g. manager.mycluster.edu/jobmanager-pbs )
        CE = os.environ['OSG_JOB_CONTACT']
        self.pp.ceName = CE.split( '/' )[0]
        if len( CE.split( '/' ) ) > 1:
          self.queueName = CE.split( '/' )[1]
      # configureOpts.append( '-N "%s"' % cliParams.ceName )
      else:
        self.log.error( "There was an error executing brokerinfo. Setting ceName to local " )
    elif self.pp.flavour == "CREAM":
      if os.environ.has_key( 'CE_ID' ):
        self.pp.ceName = os.environ['CE_ID'].split( ':' )[0]
        if os.environ['CE_ID'].count( "/" ):
          self.queueName = os.environ['CE_ID'].split( '/' )[1]



  def __doOSG ( self ):
    """ Treat the OSG case """

    osgDir = ''
    if self.pp.flavour == "OSG":
      vo = self.pp.releaseProject.replace( 'DIRAC', '' ).upper()
      if not vo:
        vo = 'DIRAC'
      osgDir = os.environ['OSG_WN_TMP']
      # Make a separate directory per Project if it is defined
      jobDir = os.path.basename( self.pp.pilotReference )
      if not jobDir:  # just in case
        import random
        jobDir = str( random.randint( 1000, 10000 ) )
      osgDir = os.path.join( osgDir, vo, jobDir )
      if not os.path.isdir( osgDir ):
        os.makedirs( osgDir )
      os.chdir( osgDir )
      try:
        import shutil
        shutil.copy( self.pp.installScript, os.path.join( osgDir, self.pp.installScriptName ) )
      except Exception, x:
        print sys.executable
        print sys.version
        print os.uname()
        print x
        raise x
    if os.environ.has_key( 'OSG_APP' ):
    # Try to define it here although this will be only in the local shell environment
      os.environ['VO_%s_SW_DIR' % vo] = os.path.join( os.environ['OSG_APP'], vo )

  def execute( self ):
    """ What is called all the time
    """

    self.__setConfigureOptions()
    self.__getCPURequirement()

    # Instead of dumping the Full configuration, include all Server in dirac.cfg

    self.configureOpts.append('-o /LocalSite/ReleaseVersion=%s' % self.pp.releaseVersion)
    self.configureOpts.append( '-I' )
    configureScript = "dirac-configure"
    if self.pp.installEnv:
      configureScript += ' -O pilot.cfg -DM'
      self.log.debug( "Configuring DIRAC with environment set to %s" % self.pp.installEnv )

    configureCmd = "%s %s" % ( configureScript, " ".join( self.configureOpts ) )

    retCode, configureOutData = self.executeAndGetOutput( configureCmd, self.pp.installEnv )

    self.log.debug( configureOutData )

    if retCode:
      self.log.error( "Could not configure DIRAC" )
      sys.exit( 1 )

    if not self.pp.installEnv:  # if traditional installation
      if self.testVOMSOK:
      # Check voms-proxy-info before touching the original PATH and LD_LIBRARY_PATH
        os.system( 'which voms-proxy-info && voms-proxy-info -all' )

    #########################################################################################################################
    # Check proxy

    retCode, __outData__ = self.executeAndGetOutput( 'dirac-proxy-info', self.pp.installEnv )
    if self.testVOMSOK:
      retCode, __outData__ = self.executeAndGetOutput( 'dirac-proxy-info | grep -q fqan', self.pp.installEnv )
      if retCode != 0:
        self.log.debug( "dirac-pilot: missing voms certs at %s" % self.pp.site )
        sys.exit( -1 )

    ##########################################################################################################################
    # Set the local architecture

    if not self.pp.installEnv:  # if traditional installation
      architectureScript = which( "dirac-architecture" )
      if architectureScript is None:
        architectureScript = which( "dirac-platform" )

      if architectureScript:
        retCode, localArchitecture = self.executeAndGetOutput( architectureScript,self.pp.installEnv)
        if not retCode:
          localArchitecture = localArchitecture.strip()
          os.environ['CMTCONFIG'] = localArchitecture
          self.log.info( 'Setting CMTCONFIG=%s' % localArchitecture )
        # os.system( "%s -f %s -o '/LocalSite/Architecture=%s'" % ( cacheScript, cfgFile, localArchitecture ) )
        # dirac-configure will not change existing cfg unless -U option is used.
          os.system( "%s -F -o '/LocalSite/Architecture=%s'" % ( configureScript, localArchitecture ) )
        else:
          self.log.error( "There was an error calling %s" % architectureScript )

    ###############################################################################################################################
    # Get host and local user info

    self.log.info( 'Uname      = %s' % " ".join( os.uname() ) )
    self.log.info( 'Host Name  = %s' % socket.gethostname() )
    self.log.info( 'Host FQDN  = %s' % socket.getfqdn() )
    self.log.info( 'WorkingDir = %s' % self.pp.workingDir )  # this could be different than rootPath

    fileName = '/etc/redhat-release'
    if os.path.exists( fileName ):
      f = open( fileName, 'r' )
      self.log.info( 'RedHat Release = %s' % f.read().strip() )
      f.close()

    fileName = '/etc/lsb-release'
    if os.path.isfile( fileName ):
      f = open( fileName, 'r' )
      self.log.info( 'Linux release:\n%s' % f.read().strip() )
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
      self.log.info( 'CPU (model)    = %s' % CPUmodel )
      self.log.info( 'CPU (MHz)      = %s x %s' % ( nCPU, freq ) )

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
      self.log.info( 'Memory (kB)    = %s' % totalMem )
      self.log.info( 'FreeMem. (kB)  = %s' % freeMem )

    ##############################################################################################################################
    # Disk space check

    #fs = os.statvfs( rootPath )
    fs = os.statvfs( self.pp.workingDir )
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
    self.log.info( 'DiskSpace (MB) = %s' % diskSpace )

    if diskSpace < self.pp.minDiskSpace:
      self.log.error( '%s MB < %s MB, not enough local disk space available, exiting'
                  % ( diskSpace, self.pp.minDiskSpace ) )
      sys.exit( 1 )

    self.pp.diracConfigured = True

  def __getCPURequirement(self):
    """ Get job CPU requirement and queue normalization """
    
    #FIXME: this can disappear, in favor of just calling dirac-wms-cpu-normalization, maybe in a separate command
    # Also all this distinctions on the flavour should be dropped from here, and put instead in the configuration,
    # as explained in the RFC

    if self.pp.flavour in ['LCG', 'gLite', 'OSG']:
      self.log.info( 'CE = %s' % self.CE )
      self.log.info( 'LCG_SITE_CE = %s' % self.pp.ceName )

      retCode, queueNormList = self.executeAndGetOutput( 'dirac-wms-get-queue-normalization %s' % self.CE,self.pp.installEnv )
      if not retCode:
        queueNormList = queueNormList.strip().split( ' ' )
        if len( queueNormList ) == 2:
          queueNorm = float( queueNormList[1] )
          self.log.info( 'Queue Normalization = %s SI00' % queueNorm )
          if queueNorm:
            # Update the local normalization factor: We are using seconds @ 250 SI00 = 1 HS06
            # This is the ratio SpecInt published by the site over 250 (the reference used for Matching)
            # os.system( "%s -f %s -o /LocalSite/CPUScalingFactor=%s" % ( cacheScript, cfgFile, queueNorm / 250. ) )
            # os.system( "%s -f %s -o /LocalSite/CPUNormalizationFactor=%s" % ( cacheScript, cfgFile, queueNorm / 250. ) )
            os.system( "%s -F -o /LocalSite/CPUScalingFactor=%s -o /LocalSite/CPUNormalizationFactor=%s" % ( self.pp.configureScript,
                                                                                                             queueNorm / 250.,
                                                                                                             queueNorm / 250. ) )
        else:
          self.log.error( 'Fail to get Normalization of the Queue' )
      else:
        self.log.error( "There was an error calling dirac-wms-get-queue-normalization" )

      retCode, queueLength = self.executeAndGetOutput( 'dirac-wms-get-normalized-queue-length %s' % self.CE, self.pp.installEnv )
      if not retCode:
        queueLength = queueLength.strip().split( ' ' )
        if len( queueLength ) == 2:
          self.pp.jobCPUReq = float( queueLength[1] )
          self.log.info( 'Normalized Queue Length = %s' % self.pp.jobCPUReq )
        else:
          self.log.error( 'Failed to get Normalized length of the Queue' )
      else:
        self.log.error( "There was an error calling dirac-wms-get-normalized-queue-length" )

      # Instead of using the Average reported by the Site, determine a Normalization
      # os.system( "dirac-wms-cpu-normalization -U" )

class LaunchAgent( CommandBase ):
  """ Prepare and launch the job agent
  """

  def __init__( self, pilotParams ):
    """ c'tor
    """
    super( LaunchAgent, self ).__init__( pilotParams )
    self.inProcessOpts = []
    self.jobAgentOpts = []

  def __setInProcessOpts( self ):

    localUid = os.getuid()
    try:
      import pwd
      localUser = pwd.getpwuid( localUid )[0]
    except:
      localUser = 'Unknown'
    self.log.info( 'User Name  = %s' % localUser )
    self.log.info( 'User Id    = %s' % localUid )
    self.inProcessOpts = ['-s /Resources/Computing/CEDefaults' ]
    self.inProcessOpts.append( '-o WorkingDirectory=%s' % self.pp.workingDir )
    self.inProcessOpts.append( '-o GridCE=%s' % self.pp.ceName )
    if self.pp.flavour in ['LCG', 'gLite', 'OSG']:
      self.inProcessOpts.append( '-o GridCEQueue=%s' % self.pp.CE )
    self.inProcessOpts.append( '-o LocalAccountString=%s' % localUser )
    self.inProcessOpts.append( '-o TotalCPUs=%s' % 1 )
    self.inProcessOpts.append( '-o MaxCPUTime=%s' % ( int( self.pp.jobCPUReq ) ) )
    self.inProcessOpts.append( '-o CPUTime=%s' % ( int( self.pp.jobCPUReq ) ) )
    self.inProcessOpts.append( '-o MaxRunningJobs=%s' % 1 )
    # To prevent a wayward agent picking up and failing many jobs.
    self.inProcessOpts.append( '-o MaxTotalJobs=%s' % 10 )
    self.jobAgentOpts= ['-o MaxCycles=%s' % self.pp.maxCycles]

    if self.debugFlag:
      self.jobAgentOpts.append( '-o LogLevel=DEBUG' )

    if self.pp.userGroup:
      self.log.info( 'Setting DIRAC Group to "%s"' % self.pp.userGroup )
      self.inProcessOpts .append( '-o OwnerGroup="%s"' % self.pp.userGroup )

    if self.pp.userDN:
      self.log.info( 'Setting Owner DN to "%s"' % self.pp.userDN )
      self.inProcessOpts.append( '-o OwnerDN="%s"' % self.pp.userDN )

    if self.pp.installEnv:
      # The instancePath is where the agent works
      self.inProcessOpts.append( '-o /LocalSite/InstancePath=%s' % self.pp.workingDir )
      # The file pilot.cfg has to be created previously by ConfigureDIRAC
      self.inProcessOpts.append( 'pilot.cfg' )


  def __startJobAgent(self):
    """ Starting of the JobAgent
    """

    # Find any .cfg file uploaded with the sandbox or generated by previous commands

    diracAgentScript = "dirac-agent"
    extraCFG = []
    for i in os.listdir( self.pp.rootPath ):
      cfg = os.path.join( self.pp.rootPath, i )
      if os.path.isfile( cfg ) and re.search( '.cfg&', cfg ):
        extraCFG.append( cfg )

    if self.pp.executeCmd:
      # Execute user command
      self.log.info( "Executing user defined command: %s" % self.pp.executeCmd )
      sys.exit( os.system( "source bashrc; %s" % self.pp.executeCmd ) / 256 )

    self.log.info( 'Starting JobAgent' )
    os.environ['PYTHONUNBUFFERED'] = 'yes'

    jobAgent = '%s WorkloadManagement/JobAgent %s %s %s' % ( diracAgentScript,
                                                             " ".join( self.jobAgentOpts ),
                                                             " ".join( self.inProcessOpts ),
                                                             " ".join( extraCFG ) )


    if not self.pp.dryRun:
      retCode, output = self.executeAndGetOutput( jobAgent, self.pp.installEnv )
      self.log.info( output, header = False )
      if retCode:
        self.log.error( "Could not start the JobAgent" )
        sys.exit( 1 )

    fs = os.statvfs( self.pp.workingDir )
    diskSpace = fs[4] * fs[0] / 1024 / 1024
    self.log.info( 'DiskSpace (MB) = %s' % diskSpace )
    sys.exit( 0 )

  def execute( self ):
    """ What is called all the time
    """
    self.__setInProcessOpts()
    self.__startJobAgent()

