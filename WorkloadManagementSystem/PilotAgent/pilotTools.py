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

__RCSID__ = '$Id$'

def printVersion( log ):

  log.info( "Running %s" % " ".join( sys.argv ) )
  try:
    fd = open( "%s.run" % sys.argv[0], "w" )
    pickle.dump( sys.argv[1:], fd )
    fd.close()
  except:
    pass
  log.info( "Version %s" % __RCSID__ )

def pythonPathCheck():

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

def getCommands( params ):
  
  extensions = params.commandExtensions
  modules = [ m + 'Commands' for m in ['pilot'] + extensions ]
  commandNames = params.commands
  commands = []
  for cName in commandNames:
    commandObject = None
    for module in modules:
      try:
        impData = imp.find_module( module )
        commandModule = imp.load_module( module, *impData )
        commandObject = getattr( commandModule, cName )
      except:
        pass  
    if commandObject is None:
      error = "Command %s is not found in all the locations: %s" % ( cName, str( extensions ) )
      return error
    else:
      commands.append( commandObject( params ) )  
  return commands

class Logger( object ):
  """ Basic logger object, for use inside the pilot. Just using print.
  """

  def __init__( self, name = 'Pilot', debugFlag = False ):
    self.debugFlag = debugFlag
    self.name = name

  def setDebug( self ):
    self.debugFlag = True

  def debug( self, msg ):
    if self.debugFlag:
      for _line in msg.split( "\n" ):
        print "%s UTC %s [DEBUG] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), self.name, _line )
      sys.stdout.flush()

  def error( self, msg ):
    for _line in msg.split( "\n" ):
      print "%s UTC %s [ERROR] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), self.name, _line )
    sys.stdout.flush()

  def warn( self, msg ):
    for _line in msg.split( "\n" ):
      print "%s UTC %s [WARN]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), self.name, _line )
    sys.stdout.flush()

  def info( self, msg ):
    for _line in msg.split( "\n" ):
      print "%s UTC %s [INFO]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), self.name, _line )
    sys.stdout.flush()

class CommandBase( object ):
  """ CommandBase is the base class for every command in the pilot commands toolbox
  """

  def __init__( self, pilotParams ):
    """ c'tor

        Defines the logger and the pilot parameters
    """

    self.pp = pilotParams
    self.log = Logger( self.__class__ )
    self.debugFlag = False
    for o, _v in self.pp.optList:
      if o == '-d' or o == '--debug':
        self.log.setDebug()
        self.debugFlag = True


  def executeAndGetOutput( self, cmd, environDict = None ):
    """ Execute a command on the worker node and get the output"""

    self.log.debug( 'Executing command %s' % cmd )
    try:
      import subprocess  # spawn new processes, connect to their input/output/error pipes, and obtain their return codes.
      self.log.debug( "executeAndGetOutput: %s" % cmd )
      _p = subprocess.Popen( "%s" % cmd, shell = True, env=environDict, stdout = subprocess.PIPE,
                        stderr = subprocess.PIPE, close_fds = False )
      outData = _p.stdout.read().strip()
      returnCode = _p.wait()
      return (returnCode, outData)
    except ImportError:
      self.log.error( "Error importing subprocess" )
      
class PilotParams:
  """ This is a structure to hold all the parameters to be used across 
      all the commands
  """

  MAX_CYCLES = 10

  def __init__( self ):
    self.debugFlag = False
    self.local = False
    self.dryRun = False
    self.commandExtensions = []
    self.commands = ['InstallDIRAC', 'ConfigureDIRAC', 'LaunchAgent']
    self.site = ""
    self.setup = ""
    self.configServer = ""
    self.installation = ""
    self.ceName = ""
    self.queueName = ""
    self.platform = ""
    self.minDiskSpace = 2560 #MB
    self.jobCPUReq = 900
    self.pythonVersion = '27'
    self.userGroup = ""
    self.userDN = ""
    self.maxCycles = self.MAX_CYCLES
    self.flavour = 'DIRAC'
    self.gridVersion = '2014-04-09'
    self.pilotReference = ''
    self.releaseVersion = ''
    self.releaseProject = ''
    self.gateway = ""
    self.useServerCertificate = False
    self.rootPath = ''
    self.pilotRootPath = ''
    self.pilotScriptName = ''
    self.workingDir = ''
    # DIRAC client installation environment
    self.installEnv = None
    self.executeCmd = False
    
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
                     ( 't', 'test', 'Make a dry run. Do not run JobAgent' ),
                     ( 'u:', 'url=', 'Use <url> to download tarballs' ),
                     ( 'r:', 'release=', 'DIRAC release to install' ),
                     ( 'n:', 'name=', 'Set <Site> as Site Name' ),
                     ( 'D:', 'disk=', 'Require at least <space> MB available' ),
                     ( 'M:', 'MaxCycles=', 'Maximum Number of JobAgent cycles to run' ),
                     ( 'N:', 'Name=', 'Use <CEName> to determine Site Name' ),
                     ( 'Q:', 'Queue', 'Queue name' ),
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

    self.__initOptions()
    
  def __initOptions( self ):
    
    self.optList, __args__ = getopt.getopt( sys.argv[1:],
                                            "".join( [ opt[0] for opt in self.cmdOpts ] ),
                                            [ opt[1] for opt in self.cmdOpts ] ) 
    for o, v in self.optList:
      if o == '-E' or o == '--commandExtensions':
        self.commandExtensions = v.split( ',' )
      elif o == '-X' or o == '--commands':
        self.commands = v.split( ',' )
      elif o == '-n' or o == '--name':
        self.site = v
      elif o == '-N' or o == '--Name':
        self.ceName = v
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
      elif o == '-t' or o == '--test':
        self.dryRun = True
        
      elif o in ( '-V', '--installation' ):
        self.installation = v
      elif o == '-p' or o == '--platform':
        self.platform = v
      elif o == '-D' or o == '--disk':
        try:
          self.minDiskSpace = int( v )
        except:
          pass
      elif o == '-r' or o == '--release':
        self.releaseVersion = v.split(',',1)[0]
      elif o in ( '-l', '--project' ):
        self.releaseProject = v
      elif o in ( '-W', '--gateway' ):
        self.gateway = v
      elif o == '-c' or o == '--cert':
        self.useServerCertificate = False
      elif o == '-M' or o == '--MaxCycles':
        try:
          self.maxCycles = min( self.MAX_CYCLES, int( v ) )
        except:
          pass  
      elif o in ( '-T', '--CPUTime' ):
        self.jobCPUReq = v
            
    self.rootPath = os.getcwd()
    self.originalRootPath = os.getcwd()
    self.pilotRootPath = os.getcwd()  
    self.workingDir = os.getcwd()  
    
