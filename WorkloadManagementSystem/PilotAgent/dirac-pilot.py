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
import popen2

#Check PYTHONPATH and LD_LIBARY_PATH
try:
  os.umask( 022 )

  pythonpath = os.getenv( 'PYTHONPATH', '' ).split( ':' )
  newpythonpath = []
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

class CliParams:

  MAX_CYCLES = 5

  def __init__( self ):
    self.debug = False
    self.local = False
    self.dryRun = False
    self.testVOMSOK = False
    self.site = ""
    self.ceName = ""
    self.platform = ""
    self.minDiskSpace = 2560 #MB
    self.jobCPUReq = 900
    self.pythonVersion = '25'
    self.userGroup = ""
    self.userDN = ""
    self.maxCycles = CliParams.MAX_CYCLES
    self.flavour = 'DIRAC'

###
# Helper functions
###

def logDEBUG( msg ):
  if cliParams.debug:
    print "[DEBUG] %s" % msg

def logERROR( msg ):
  print "[ERROR] %s" % msg

def logINFO( msg ):
  print "[INFO]  %s" % msg

# Version print

logINFO( "Running %s" % __RCSID__ )

###
# Checking scripts are ok
###
try:
  pilotScript = os.path.realpath( __file__ )
  # in old python versions __file__ is not defined
except:
  pilotScript = os.path.realpath( sys.argv[0] )

pilotScriptName = os.path.basename( pilotScript )
pilotRootPath = os.path.dirname( myFullName )
installScriptName = 'dirac-install'
installScript = os.path.join( rootPath, installScriptName )

rootPath = os.getcwd()

if not os.path.isfile( installScript ):
  logERROR( "%s requires %s in the same directory (%s)" % ( pilotScriptName, installScriptName, pilotRootPath ) )
  sys.exit( 1 )

os.chmod( installScript, stat.S_IRWXU )


###
# Option parsing
###

"""
 Flags not migrated from old dirac-pilot
   -r --repository=<rep>       Use <rep> as cvs repository              <--Not done
   -C --cvs                    Retrieve from CVS (implies -b) <--Not done
"""

cmdOpts = ( ( 'b', 'build', 'Force local compilation' ),
            ( 'd', 'debug', 'Set debug flag' ),
            ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'h', 'help', 'Show this help' ),
            ( 'i:', 'python=', 'Use python<24|25> interpreter' ),
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
            ( 'S:', 'setup=' 'DIRAC Setup to use' ),
            ( 'C:', 'configurationServer=', 'Configuration servers to use' ),
            ( 'T:', 'CPUTime', 'Requested CPU Time' ),
            ( 'G:', 'Group=', 'DIRAC Group to use' ),
            ( 'O:', 'OwnerDN', 'Pilot OwnerDN (for private pilots)' ),
            ( 'U', 'Upload', 'Upload compiled distribution (if built)' ),
            ( 'V:', 'VO=', 'Virtual Organization' ),
            ( 'W:', 'gateway=', 'Configure <gateway> as DIRAC Gateway during installation' ),
          )

cliParams = CliParams()

installOptions = []
configureOptions = []

optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )
for o, v in optList:
  if o in ( '-h', '--help' ):
    print "Usage %s <opts>" % sys.argv[0]
    for cmdOpt in cmdOpts:
      print "%s %s : %s" % ( cmdOpt[0].ljust( 4 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
    sys.exit( 1 )
  elif o in ( '-b', '--build' ):
    installOptions.append( '-b' )
  elif o == '-d' or o == '--debug':
    cliParams.debug = True
    installOpts.append( '-d' )
  elif o == '-e' or o == '--extraPackages':
    installOpts.append( '-e "%s"' % v )
  elif o == '-g' or o == '--grid':
    #TODO
    pass
  elif o == '-i' or o == '--python':
    installOpts.append( '-i "%s"' % v )
  elif o == '-n' or o == '--name':
    configureOptions.append( '-n "%s"' % v )
    cliParams.site = v
  elif o == '-p' or o == '--platform':
    installOpts.append( '-p "%s"' % v )
    cliParams.platform = v
  elif o == '-r' or o == '--release':
    installOpts.append( '-r "%s"' % v )
  elif o == '-t' or o == '--test':
    cliParams.dryRun = True
  elif o == '-u' or o == '--url':
    #TODO
    pass
  elif o == '-N' or o == '--Name':
    installOpts.append( '-N "%s"' % v )
    cliParams.ceName = v
  elif o == '-D' or o == '--disk':
    try:
      cliParams.minDiskSpace = int( v )
    except:
      pass
  elif o == '-M' or o == '--MaxCycles':
    try:
      cliParams.maxCycles = min( CliParams.MAX_CYCLES, int( v ) )
    except:
      pass
  elif o in ( '-S', '--setup' ):
    configureOptions.append( '-S "%s"' % v )
  elif o in ( '-C', '--configurationServer' ):
    configureOptions.append( '-C "%s"' % v )
  elif o in ( '-P', '--path' ):
    installOpts.append( '-P "%s"' % v )
    rootPath = v
  elif o in ( '-T', '--CPUTime' ):
    cliParams.jobCPUReq = v
  elif o in ( '-G', '--Group' ):
    cliParams.userGroup = v
  elif o in ( '-O', '--OwnerDN' ):
    cliParams.userDN = v
  elif o in ( '-U', '--Upload' ):
    #TODO
    pass
  elif o in ( '-V', '--VO' ):
    configureOptions.append( '-V "%s"' % v )
  elif o in ( '-W', '--gateway' ):
    configureOptions.append( '-W "%s"' % v )
  elif o == '-E' or o == '--server':
    installOpts.append( '-t "%s"' % v )
  elif o == '-o' or o == '--option':
    configureOptions.append( '-o "%s"' % v )
  elif o == '-s' or o == '--section':
    configureOptions.append( '-s "%s"' % v )
  elif o == '-c' or o == '--cert':
    configureOptions.append( '--UseServerCertificate' )

##
# Attempt to determine the flavour
##

pilotRef = 'Unknown'
if os.environ.has_key( 'EDG_WL_JOBID' ):
  cliParams.flavour = 'LCG'
  pilotRef = os.environ['EDG_WL_JOBID']

if os.environ.has_key( 'GLITE_WMS_JOBID' ):
  cliParams.flavour = 'gLite'
  pilotRef = os.environ['GLITE_WMS_JOBID']

configureOptions.append( '-o /LocalSite/GridMiddleware=%s' % cliParams.flavour )

###
# Try to get the CE name
###
if pilotRef != 'Unknown':
  ( child_stdout, child_stdin, child_stderr ) = popen2.popen3( 'edg-brokerinfo getCE || glite-brokerinfo getCE' )
  CE = child_stdout.read().strip()
  cliParams.ceName = CE.split( ':' )[0]
  child_stdout.close()
  child_stderr.close()
  configureOptions.append( '-o /LocalSite/PilotReference=%s' % pilotRef )
  configureOptions.append( '-N "%s"' % cliParams.ceName )
else:
  cliParams.ceName = 'Local'

###
# Set the platform if defined
###

if cliParams.platform:
  installOpts.append( '-p "%s"' % localPlatform )

###
# Set the group and the DN
###

if cliParams.userGroup:
  configureOptions.append( '-o /AgentJobRequirements/OwnerGroup="%s"' % cliParams.userGroup )

if cliParams.userDN:
  configureOptions.append( '-o /AgentJobRequirements/OwnerDN="%s"' % cliParams.userDN )

###
# Do the installation
###

installCmd = "%s %s" % ( installScriptPath, " ".join( installOpts ) )

logDEBUG( "Installing with: %s" % installCmd )

if os.system( installCmd ):
  logERROR( "Could not make a proper DIRAC installation" )
  sys.exit( 1 )

###
# Set the env to use the recently installed DIRAC
###

diracScriptsPath = os.path.join( rootPath, 'scripts' )
sys.path.insert( 0, diracScriptsPath )

###
# Configure DIRAC
###

configureCmd = "%s %s" % ( os.path.join( diracScriptsPath, "dirac-configure" ), " ".join( configureOptions ) )

logDEBUG( "Configuring DIRAC with: %s" )

if os.system( configureCmd ):
  logERROR( "Could not configure DIRAC" )
  sys.exit( 1 )

###
# Dump the CS to cache in file
###

cfgFile = os.path.join( rootPath, "etc", "dirac.cfg" ) 
cacheScript = os.path.join( diracScriptsPath, "dirac-configuration-dump-local-cache" )
if os.system( "%s -f %s" % ( cacheScript, cfgFile ) ):
  logERROR( "Could not dump the CS to %s" % cfgFile )
  
###
# Set the LD_LIBRARY_PATH and PATH
###
if not cliParams.platform:
  platformPath = os.path.join( rootPath, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  PlatformModule = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  cliParams.platform = PlatformModule.getPlatformString()

diracLibPath = os.path.join( rootPath, cliParams.platform, 'lib' )
diracBinPath = os.path.join( rootPath, cliParams.platform, 'bin' )
os.environ['LD_LIBRARY_PATH_SAVE'] = os.environ['LD_LIBRARY_PATH']
os.environ['LD_LIBRARY_PATH'] = "%s" % ( diracLibPath )
os.environ['PATH'] = '%s:%s:%s' % (diracBinPath,diracScriptsPath,os.getenv('PATH'))

###
# End of initialisation
###