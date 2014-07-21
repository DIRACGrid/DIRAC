""" A set of common tools to be used in pilot scripts
"""

import sys
import time
import os
import pickle
import getopt

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

class Logger( object ):

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

  def info( self, msg ):
    for _line in msg.split( "\n" ):
      print "%s UTC %s [INFO]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), self.name, _line )
    sys.stdout.flush()

class CommandBase( object ):

  def __init__( self, pilotParams, name = 'Pilot' ):
    self.pp = pilotParams
    self.commandName = name
    self.log = Logger( name )
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

  MAX_CYCLES = 100

  def __init__( self ):
    self.debug = False
    self.local = False
    self.dryRun = False
    self.site = ""
    self.ceName = ""
    self.queueName = ""
    self.platform = ""
    self.minDiskSpace = 2560 #MB
    self.jobCPUReq = 900
    self.pythonVersion = '27'
    self.userGroup = ""
    self.userDN = ""
    self.maxCycles = PilotParams.MAX_CYCLES
    self.flavour = 'DIRAC'
    self.gridVersion = '2014-04-09'
    self.pilotReference = ''
    self.releaseVersion = ''
    self.releaseProject = ''
    self.rootPath = ''
    self.pilotRootPath = ''
    self.pilotScriptName = ''
    self.workingDir = ''
    # The following parameters are added for BOINC computing element with virtual machine.
    self.boincUserID = ''         #  The user ID in a BOINC computing element
    self.boincHostPlatform = ''   # The os type of the host machine running the pilot, not the virtual machine
    self.boincHostID = ''         # the host id in a  BOINC computing element
    self.boincHostName = ''       # the host name of the host machine running the pilot, not the virtual machine
    # DIRAC client installation environment
    self.installEnv = None
    self.executeCmd = False
    
    # Pilot command options
    self.cmdOpts = ( ( 'b', 'build', 'Force local compilation' ),
                ( 'd', 'debug', 'Set debug flag' ),
                ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
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

    self.__initOptions()
    
  def __initOptions( self ):
    
    self.optList, __args__ = getopt.getopt( sys.argv[1:],
                                            "".join( [ opt[0] for opt in self.cmdOpts ] ),
                                            [ opt[1] for opt in self.cmdOpts ] )   
    self.rootPath = os.getcwd()
    self.originalRootPath = os.getcwd()
    self.pilotRootPath = os.getcwd()  
    self.workingDir = os.getcwd()  