########################################################################
# $Id$
########################################################################

""" A set of common tools to be used in pilot commands
"""

import sys
import time
import os
import pickle
import getopt
import imp
import types
import urllib2
import signal

__RCSID__ = '$Id$'

def printVersion( log ):

  log.info( "Running %s" % " ".join( sys.argv ) )
  try:
    with open( "%s.run" % sys.argv[0], "w" ) as fd:
      pickle.dump( sys.argv[1:], fd )
  except OSError:
    pass
  log.info( "Version %s" % __RCSID__ )

def pythonPathCheck():

  try:
    os.umask( 18 ) # 022
    pythonpath = os.getenv( 'PYTHONPATH', '' ).split( ':' )
    print 'Directories in PYTHONPATH:', pythonpath
    for p in pythonpath:
      if p == '':
        continue
      try:
        if os.path.normpath( p ) in sys.path:
          # In case a given directory is twice in PYTHONPATH it has to removed only once
          sys.path.remove( os.path.normpath( p ) )
      except Exception, x:
        print x
        print "[EXCEPTION-info] Failing path:", p, os.path.normpath( p )
        print "[EXCEPTION-info] sys.path:", sys.path
        raise x
  except Exception, x:
    print x
    print "[EXCEPTION-info] sys.executable:", sys.executable
    print "[EXCEPTION-info] sys.version:", sys.version
    print "[EXCEPTION-info] os.uname():", os.uname()
    raise x

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

def retrieveUrlTimeout( url, fileName, log, timeout = 0 ):
  """
   Retrieve remote url to local file, with timeout wrapper
  """
  urlData = ''
  if timeout:
    signal.signal( signal.SIGALRM, alarmTimeoutHandler )
    # set timeout alarm
    signal.alarm( timeout + 5 )
  try:
    remoteFD = urllib2.urlopen( url )
    expectedBytes = 0
    # Sometimes repositories do not return Content-Length parameter
    try:
      expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
    except Exception as x:
      expectedBytes = 0
    data = remoteFD.read()
    if fileName:
      with open( fileName + '-local', "wb" ) as localFD:
        localFD.write( data )
    else:
      urlData += data
    remoteFD.close()
    if len( data ) != expectedBytes and expectedBytes > 0:
      log.error( 'URL retrieve: expected size does not match the received one' )
      return False

    if timeout:
      signal.alarm( 0 )
    if fileName:
      return True
    else:
      return urlData

  except urllib2.HTTPError, x:
    if x.code == 404:
      log.error( "URL retrieve: %s does not exist" % url )
      if timeout:
        signal.alarm( 0 )
      return False
  except urllib2.URLError:
    log.error( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
    return False
  except Exception, x:
    if x == 'Timeout':
      log.error( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
    if timeout:
      signal.alarm( 0 )
    raise x


class ObjectLoader( object ):
  """ Simplified class for loading objects from a DIRAC installation.

      Example:

      ol = ObjectLoader()
      object, modulePath = ol.loadObject( 'pilot', 'LaunchAgent' )
  """

  def __init__( self, baseModules, log ):
    """ init
    """
    self.__rootModules = baseModules
    self.log = log

  def loadModule( self, modName, hideExceptions = False ):
    """ Auto search which root module has to be used
    """
    for rootModule in self.__rootModules:
      impName = modName
      if rootModule:
        impName = "%s.%s" % ( rootModule, impName )
      self.log.debug( "Trying to load %s" % impName )
      module, parentPath = self.__recurseImport( impName, hideExceptions = hideExceptions )
      #Error. Something cannot be imported. Return error
      if module is None:
        return None, None
      #Huge success!
      else:
        return module, parentPath
      #Nothing found, continue
    #Return nothing found
    return None, None


  def __recurseImport( self, modName, parentModule = None, hideExceptions = False ):
    """ Internal function to load modules
    """
    if type( modName ) in types.StringTypes:
      modName = modName.split( '.' )
    try:
      if parentModule:
        impData = imp.find_module( modName[0], parentModule.__path__ )
      else:
        impData = imp.find_module( modName[0] )
      impModule = imp.load_module( modName[0], *impData )
      if impData[0]:
        impData[0].close()
    except ImportError, excp:
      if str( excp ).find( "No module named %s" % modName[0] ) == 0:
        return None, None
      errMsg = "Can't load %s in %s" % ( ".".join( modName ), parentModule.__path__[0] )
      if not hideExceptions:
        self.log.exception( errMsg )
      return None, None
    if len( modName ) == 1:
      return impModule, parentModule.__path__[0]
    return self.__recurseImport( modName[1:], impModule,
                                 hideExceptions = hideExceptions )


  def loadObject( self, package, moduleName, command ):
    """ Load an object from inside a module
    """
    loadModuleName = '%s.%s' % ( package, moduleName )
    module, parentPath = self.loadModule( loadModuleName )
    if module is None:
      return None, None

    try:
      commandObj = getattr( module, command )
      return commandObj, os.path.join( parentPath, moduleName )
    except AttributeError, e:
      self.log.error( 'Exception: %s' % str(e) )
      return None, None

def getCommand( params, commandName, log ):
  """ Get an instantiated command object for execution.
      Commands are looked in the following modules in the order:

      1. <CommandExtension>Commands
      2. pilotCommands
      3. <Extension>.WorkloadManagementSystem.PilotAgent.<CommandExtension>Commands
      4. <Extension>.WorkloadManagementSystem.PilotAgent.pilotCommands
      5. DIRAC.WorkloadManagementSystem.PilotAgent.<CommandExtension>Commands
      6. DIRAC.WorkloadManagementSystem.PilotAgent.pilotCommands

      Note that commands in 3.-6. can only be used of the the DIRAC installation
      has been done. DIRAC extensions are taken from -e ( --extraPackages ) option
      of the pilot script.
  """
  extensions = params.commandExtensions
  modules = [ m + 'Commands' for m in extensions + ['pilot'] ]
  commandObject = None

  # Look for commands in the modules in the current directory first
  for module in modules:
    try:
      impData = imp.find_module( module )
      commandModule = imp.load_module( module, *impData )
      commandObject = getattr( commandModule, commandName )
    except Exception, _e:
      pass
    if commandObject:
      return commandObject( params ), module

  if params.diracInstalled:
    diracExtensions = []
    for ext in params.extensions:
      if not ext.endswith( 'DIRAC' ):
        diracExtensions.append( ext + 'DIRAC' )
      else:
        diracExtensions.append( ext )
    diracExtensions += ['DIRAC']
    ol = ObjectLoader( diracExtensions, log )
    for module in modules:
      commandObject, modulePath = ol.loadObject( 'WorkloadManagementSystem.PilotAgent',
                                                 module,
                                                 commandName )
      if commandObject:
        return commandObject( params ), modulePath

  # No command could be instantitated
  return None, None

class Logger( object ):
  """ Basic logger object, for use inside the pilot. Just using print.
  """

  def __init__( self, name = 'Pilot', debugFlag = False, pilotOutput = 'pilot.out' ):
    self.debugFlag = debugFlag
    self.name = name
    self.out = pilotOutput

  def __outputMessage( self, msg, level, header ):
    if self.out:
      with open( self.out, 'a' ) as outputFile:
        for _line in msg.split( "\n" ):
          if header:
            outLine = "%s UTC %s [%s] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ),
                                              level,
                                              self.name,
                                              _line )
            print outLine
            if self.out:
              outputFile.write( outLine + '\n' )
          else:
            print _line
            outputFile.write( _line + '\n' )
    sys.stdout.flush()

  def setDebug( self ):
    self.debugFlag = True

  def debug( self, msg, header = True ):
    if self.debugFlag:
      self.__outputMessage( msg, "DEBUG", header )

  def error( self, msg, header = True ):
    self.__outputMessage( msg, "ERROR", header )

  def warn( self, msg, header = True ):
    self.__outputMessage( msg, "WARN", header )

  def info( self, msg, header = True ):
    self.__outputMessage( msg, "INFO", header )

class CommandBase( object ):
  """ CommandBase is the base class for every command in the pilot commands toolbox
  """

  def __init__( self, pilotParams, dummy='' ):
    """ c'tor

        Defines the logger and the pilot parameters
    """

    self.pp = pilotParams
    self.log = Logger( self.__class__.__name__ )
    self.debugFlag = False
    for o, _ in self.pp.optList:
      if o == '-d' or o == '--debug':
        self.log.setDebug()
        self.debugFlag = True
    self.log.debug( "\n\n Initialized command %s" % self.__class__ )

  def executeAndGetOutput( self, cmd, environDict = None ):
    """ Execute a command on the worker node and get the output
    """

    self.log.info( "Executing command %s" % cmd )
    try:
      import subprocess  # spawn new processes, connect to their input/output/error pipes, and obtain their return codes.
      _p = subprocess.Popen( "%s" % cmd, shell = True, env=environDict, stdout = subprocess.PIPE,
                             stderr = subprocess.PIPE, close_fds = False )

      # standard output
      outData = _p.stdout.read().strip()
      for line in outData:
        sys.stdout.write( line )
      sys.stdout.write( '\n' )

      for line in _p.stderr:
        sys.stdout.write( line )
      sys.stdout.write( '\n' )

      # return code
      returnCode = _p.wait()
      self.log.debug( "Return code of %s: %d" % ( cmd, returnCode ) )

      return (returnCode, outData)
    except ImportError:
      self.log.error( "Error importing subprocess" )

  def exitWithError( self, errorCode ):
    """ Wrapper around sys.exit()
    """
    self.log.info( "List of child processes of current PID:" )
    retCode, _outData = self.executeAndGetOutput( "ps --forest -o pid,%%cpu,%%mem,tty,stat,time,cmd -g %d" % os.getpid() )
    if retCode:
      self.log.error( "Failed to issue ps [ERROR %d] " % retCode )
    sys.exit( errorCode )

class PilotParams( object ):
  """ Class that holds the structure with all the parameters to be used across all the commands
  """

  MAX_CYCLES = 10

  def __init__( self ):
    """ c'tor

        param names and defaults are defined here
    """

    self.rootPath = os.getcwd()
    self.originalRootPath = os.getcwd()
    self.pilotRootPath = os.getcwd()
    self.workingDir = os.getcwd()

    self.optList = {}
    self.debugFlag = False
    self.local = False
    self.commandExtensions = []
    self.commands = ['GetPilotVersion', 'CheckWorkerNode', 'InstallDIRAC', 'ConfigureBasics', 'CheckCECapabilities',
                     'CheckWNCapabilities', 'ConfigureSite', 'ConfigureArchitecture', 'ConfigureCPURequirements',
                     'LaunchAgent']
    self.extensions = []
    self.tags = []
    self.site = ""
    self.setup = ""
    self.configServer = ""
    self.installation = ""
    self.ceName = ""
    self.ceType = ''
    self.queueName = ""
    self.platform = ""
    self.minDiskSpace = 2560 #MB
    self.jobCPUReq = 900
    self.pythonVersion = '27'
    self.userGroup = ""
    self.userDN = ""
    self.maxCycles = self.MAX_CYCLES
    self.flavour = 'DIRAC'
    self.gridVersion = ''
    self.pilotReference = ''
    self.releaseVersion = ''
    self.releaseProject = ''
    self.gateway = ""
    self.useServerCertificate = False
    self.pilotScriptName = ''
    self.genericOption = ''
    # DIRAC client installation environment
    self.diracInstalled = False
    self.diracExtensions = []
    # Some commands can define environment necessary to execute subsequent commands
    self.installEnv = os.environ
    # If DIRAC is preinstalled this file will receive the updates of the local configuration
    self.localConfigFile = ''
    self.executeCmd = False
    self.configureScript = 'dirac-configure'
    self.architectureScript = 'dirac-platform'
    self.certsLocation = '%s/etc/grid-security' % self.workingDir
    self.pilotCFGFile = 'pilot.json'
    self.pilotCFGFileLocation = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/defaults/'

    # Pilot command options
    self.cmdOpts = ( ( 'b', 'build', 'Force local compilation' ),
                     ( 'd', 'debug', 'Set debug flag' ),
                     ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
                     ( 'E:', 'commandExtensions=', 'Python module with extra commands' ),
                     ( 'X:', 'commands=', 'Pilot commands to execute commands' ),
                     ( 'g:', 'grid=', 'lcg tools package version' ),
                     ( 'h', 'help', 'Show this help' ),
                     ( 'i:', 'python=', 'Use python<26|27> interpreter' ),
                     ( 'l:', 'project=', 'Project to install' ),
                     ( 'p:', 'platform=', 'Use <platform> instead of local one' ),
                     ( 'u:', 'url=', 'Use <url> to download tarballs' ),
                     ( 'r:', 'release=', 'DIRAC release to install' ),
                     ( 'n:', 'name=', 'Set <Site> as Site Name' ),
                     ( 'D:', 'disk=', 'Require at least <space> MB available' ),
                     ( 'M:', 'MaxCycles=', 'Maximum Number of JobAgent cycles to run' ),
                     ( 'N:', 'Name=', 'CE Name' ),
                     ( 'Q:', 'Queue=', 'Queue name' ),
                     ( 'y:', 'CEType=', 'CE Type (normally InProcess)' ),
                     ( 'S:', 'setup=', 'DIRAC Setup to use' ),
                     ( 'C:', 'configurationServer=', 'Configuration servers to use' ),
                     ( 'T:', 'CPUTime', 'Requested CPU Time' ),
                     ( 'G:', 'Group=', 'DIRAC Group to use' ),
                     ( 'O:', 'OwnerDN', 'Pilot OwnerDN (for private pilots)' ),
                     ( 'U', 'Upload', 'Upload compiled distribution (if built)' ),
                     ( 'V:', 'installation=', 'Installation configuration file' ),
                     ( 'W:', 'gateway=', 'Configure <gateway> as DIRAC Gateway during installation' ),
                     ( 's:', 'section=', 'Set base section for relative parsed options' ),
                     ( 'o:', 'option=', 'Option=value to add' ),
                     ( 'c', 'cert', 'Use server certificate instead of proxy' ),
                     ( 'C:', 'certLocation=', 'Specify server certificate location' ),
                     ( 'L:', 'pilotCFGLocation=', 'Specify pilot CFG location' ),
                     ( 'F:', 'pilotCFGFile=', 'Specify pilot CFG file' ),
                     ( 'R:', 'reference=', 'Use this pilot reference' ),
                     ( 'x:', 'execute=', 'Execute instead of JobAgent' ),
                   )

    self.__initOptions()

  def __initOptions( self ):
    """ Parses and interpret options on the command line
    """

    self.optList, __args__ = getopt.getopt( sys.argv[1:],
                                            "".join( [ opt[0] for opt in self.cmdOpts ] ),
                                            [ opt[1] for opt in self.cmdOpts ] )
    for o, v in self.optList:
      if o == '-E' or o == '--commandExtensions':
        self.commandExtensions = v.split( ',' )
      elif o == '-X' or o == '--commands':
        self.commands = v.split( ',' )
      elif o == '-e' or o == '--extraPackages':
        self.extensions = v.split( ',' )
      elif o == '-n' or o == '--name':
        self.site = v
      elif o == '-N' or o == '--Name':
        self.ceName = v
      elif o == '-y' or o == '--CEType':
        self.ceType = v
      elif o == '-Q' or o == '--Queue':
        self.queueName = v
      elif o == '-R' or o == '--reference':
        self.pilotReference = v
      elif o == '-d' or o == '--debug':
        self.debugFlag = True
      elif o in ( '-S', '--setup' ):
        self.setup = v
      elif o in ( '-C', '--configurationServer' ):
        self.configServer = v
      elif o in ( '-G', '--Group' ):
        self.userGroup = v
      elif o in ( '-x', '--execute' ):
        self.executeCmd = v
      elif o in ( '-O', '--OwnerDN' ):
        self.userDN = v
      elif o in ( '-V', '--installation' ):
        self.installation = v
      elif o == '-p' or o == '--platform':
        self.platform = v
      elif o == '-D' or o == '--disk':
        try:
          self.minDiskSpace = int( v )
        except ValueError:
          pass
      elif o == '-r' or o == '--release':
        self.releaseVersion = v.split(',',1)[0]
      elif o in ( '-l', '--project' ):
        self.releaseProject = v
      elif o in ( '-W', '--gateway' ):
        self.gateway = v
      elif o == '-c' or o == '--cert':
        self.useServerCertificate = True
      elif o == '-C' or o == '--certLocation':
        self.certsLocation = v
      elif o == '-L' or o == '--pilotCFGLocation':
        self.pilotCFGFileLocation = v
      elif o == '-F' or o == '--pilotCFGFile':
        self.pilotCFGFile = v
      elif o == '-M' or o == '--MaxCycles':
        try:
          self.maxCycles = min( self.MAX_CYCLES, int( v ) )
        except ValueError:
          pass
      elif o in ( '-T', '--CPUTime' ):
        self.jobCPUReq = v
      elif o in ( '-o', '--option' ):
        self.genericOption = v
