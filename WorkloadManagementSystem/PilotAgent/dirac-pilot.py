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
    self.pythonVersion = '26'
    self.userGroup = ""
    self.userDN = ""
    self.maxCycles = CliParams.MAX_CYCLES
    self.flavour = 'DIRAC'
    self.gridVersion = '2013-04-22'
    self.pilotReference = ''
    self.releaseVersion = ''
    self.releaseProject = ''
    # The following parameters are added for BOINC computing element with virtual machine.
    self.boincUserID = ''         #  The user ID in a BOINC computing element
    self.boincHostPlatform = ''   # The os type of the host machine running the pilot, not the virtual machine
    self.boincHostID = ''         # the host id in a  BOINC computing element
    self.boincHostName = ''       # the host name of the host machine running the pilot, not the virtual machine

cliParams = CliParams()

###
# Helper functions
###

def logDEBUG( msg ):
  if cliParams.debug:
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

def executeAndGetOutput( cmd ):
  try:
    import subprocess
    _p = subprocess.Popen( "%s" % cmd, shell = True, stdout = subprocess.PIPE,
                          stderr = subprocess.PIPE, close_fds = True )
    outData = _p.stdout.read().strip()
    returnCode = _p.wait()
  except ImportError:
    import popen2
    _p3 = popen2.Popen3( "%s" % cmd )
    outData = _p3.fromchild.read().strip()
    returnCode = _p3.wait()
  return ( returnCode, outData )

# Version print

logINFO( "Running %s" % " ".join( sys.argv ) )
try:
  fd = open( "%s.run" % sys.argv[0], "w" )
  pickle.dump( sys.argv[1:], fd )
  fd.close()
except:
  pass
logINFO( "Version %s" % __RCSID__ )

###
# Checking scripts are ok
###
try:
  pilotScript = os.path.realpath( __file__ )
  # in old python versions __file__ is not defined
except:
  pilotScript = os.path.realpath( sys.argv[0] )

pilotScriptName = os.path.basename( pilotScript )
pilotRootPath = os.path.dirname( pilotScript )

installScriptName = 'dirac-install.py'
originalRootPath = os.getcwd()
rootPath = os.getcwd()


###
# Option parsing
###

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
            ( 'Q:', 'queue=', 'Queue name' ),
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

installOpts = []
configureOpts = []
executeCmd = False

optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )
for o, v in optList:
  if o in ( '-h', '--help' ):
    print "Usage %s <opts>" % sys.argv[0]
    for cmdOpt in cmdOpts:
      print "%s %s : %s" % ( cmdOpt[0].ljust( 4 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
    sys.exit( 1 )
  elif o in ( '-x', '--execute' ):
    executeCmd = v
  elif o in ( '-b', '--build' ):
    installOpts.append( '-b' )
  elif o == '-d' or o == '--debug':
    cliParams.debug = True
    installOpts.append( '-d' )
  elif o == '-e' or o == '--extraPackages':
    installOpts.append( '-e "%s"' % v )
  elif o == '-g' or o == '--grid':
    cliParams.gridVersion = v
  elif o == '-i' or o == '--python':
    cliParams.pythonVersion = v
  elif o in ( '-l', '--project' ):
    installOpts.append( "-l '%s'" % v )
    cliParams.releaseProject = v
  elif o == '-n' or o == '--name':
    configureOpts.append( '-n "%s"' % v )
    cliParams.site = v
  elif o == '-p' or o == '--platform':
    installOpts.append( '-p "%s"' % v )
    cliParams.platform = v
  elif o == '-r' or o == '--release':
    installOpts.append( '-r "%s"' % v )
    cliParams.releaseVersion = v
  elif o == '-t' or o == '--test':
    cliParams.dryRun = True
  elif o == '-u' or o == '--url':
    installOpts.append( '-u "%s"' % v )
  elif o == '-N' or o == '--Name':
    configureOpts.append( '-N "%s"' % v )
    cliParams.ceName = v
  elif o == '-Q' or o == '--queue':
    cliParams.queueName = v  
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
  elif o == '-R' or o == '--reference':
    cliParams.pilotReference = v
  elif o in ( '-S', '--setup' ):
    configureOpts.append( '-S "%s"' % v )
  elif o in ( '-C', '--configurationServer' ):
    configureOpts.append( '-C "%s"' % v )
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
  elif o in ( '-V', '--installation' ):
    installOpts.append( '-V "%s"' % v )
    configureOpts.append( 'defaults-%s.cfg' % v )
  elif o in ( '-W', '--gateway' ):
    configureOpts.append( '-W "%s"' % v )
  elif o == '-E' or o == '--server':
    installOpts.append( '-t "server"' )
  elif o == '-o' or o == '--option':
    configureOpts.append( '-o "%s"' % v )
  elif o == '-s' or o == '--section':
    configureOpts.append( '-s "%s"' % v )
  elif o == '-c' or o == '--cert':
    configureOpts.append( '--UseServerCertificate' )

############################################################################
# Locate installation script
for path in ( pilotRootPath, originalRootPath, rootPath ):
  installScript = os.path.join( path, installScriptName )
  if os.path.isfile( installScript ):
    break

if not os.path.isfile( installScript ):
  logERROR( "%s requires %s to exist in one of: %s, %s, %s" % ( pilotScriptName, installScriptName,
                                                            pilotRootPath, originalRootPath, rootPath ) )
  logINFO( "Trying to download it to %s..." % originalRootPath )
  try:
    remoteLocation = "http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/dirac-install.py"
    remoteFD = urllib2.urlopen( remoteLocation )
    installScript = os.path.join( originalRootPath, installScriptName )
    localFD = open( installScript, "w" )
    localFD.write( remoteFD.read() )
    localFD.close()
    remoteFD.close()
  except Exception, e:
    logERROR( "Could not download %s..: %s" % ( remoteLocation, str( e ) ) )
    sys.exit( 1 )

try:
  os.chmod( installScript, stat.S_IRWXU )
except:
  pass

######################################################################

if cliParams.gridVersion:
  installOpts.append( "-g '%s'" % cliParams.gridVersion )

if cliParams.pythonVersion:
  installOpts.append( '-i "%s"' % cliParams.pythonVersion )

######################################################################
# Attempt to determine the flavour
##

pilotRef = 'Unknown'

# Pilot reference is specified at submission
if cliParams.pilotReference:
  cliParams.flavour = 'DIRAC'
  pilotRef = cliParams.pilotReference

# Take the reference from the Torque batch system
if os.environ.has_key( 'PBS_JOBID' ):
  cliParams.flavour = 'SSHTorque'
  pilotRef = 'sshtorque://'+cliParams.ceName+'/'+os.environ['PBS_JOBID']
  cliParams.queueName = os.environ['PBS_QUEUE']

# Grid Engine
if os.environ.has_key( 'JOB_ID' ):
    cliParams.flavour = 'SSHGE'
    pilotRef = 'sshge://'+cliParams.ceName+'/'+os.environ['JOB_ID']

# Condor
if os.environ.has_key( 'CONDOR_JOBID' ):
  cliParams.flavour = 'SSHCondor'
  pilotRef = 'sshcondor://'+cliParams.ceName+'/'+os.environ['CONDOR_JOBID']

# LSF
if os.environ.has_key( 'LSB_BATCH_JID' ):
  cliParams.flavour = 'SSHLSF'
  pilotRef = 'sshlsf://'+cliParams.ceName+'/'+os.environ['LSB_BATCH_JID']

# This is the CREAM direct submission case
if os.environ.has_key( 'CREAM_JOBID' ):
  cliParams.flavour = 'CREAM'
  pilotRef = os.environ['CREAM_JOBID']

# If we still have the GLITE_WMS_JOBID, it means that the submission
# was through the WMS, take this reference then
if os.environ.has_key( 'EDG_WL_JOBID' ):
  cliParams.flavour = 'LCG'
  pilotRef = os.environ['EDG_WL_JOBID']

if os.environ.has_key( 'GLITE_WMS_JOBID' ):
  if os.environ['GLITE_WMS_JOBID'] != 'N/A':
    cliParams.flavour = 'gLite'
    pilotRef = os.environ['GLITE_WMS_JOBID']

if os.environ.has_key( 'OSG_WN_TMP' ):
  cliParams.flavour = 'OSG'

# Direct SSH tunnel submission
if os.environ.has_key( 'SSHCE_JOBID' ):
  cliParams.flavour = 'SSH'
  pilotRef = 'ssh://'+cliParams.ceName+'/'+os.environ['SSHCE_JOBID']  
  
# ARC case  
if os.environ.has_key( 'GRID_GLOBAL_JOBID' ):
  cliParams.flavour = 'ARC'
  pilotRef = os.environ['GRID_GLOBAL_JOBID']     

# This is for BOINC case
if os.environ.has_key( 'BOINC_JOB_ID' ):
  cliParams.flavour = 'BOINC'
  pilotRef = os.environ['BOINC_JOB_ID']

if cliParams.flavour == 'BOINC':
  if os.environ.has_key('BOINC_USER_ID'):
    cliParams.boincUserID = os.environ['BOINC_USER_ID']
  if os.environ.has_key('BOINC_HOST_ID'):
    cliParams.boincHostID = os.environ['BOINC_HOST_ID']
  if os.environ.has_key('BOINC_HOST_PLATFORM'):
    cliParams.boincHostPlatform = os.environ['BOINC_HOST_PLATFORM']
  if os.environ.has_key('BOINC_HOST_NAME'):
    cliParams.boincHostName = os.environ['BOINC_HOST_NAME']

logDEBUG( "Flavour: %s; pilot reference: %s " % ( cliParams.flavour, pilotRef ) )

configureOpts.append( '-o /LocalSite/GridMiddleware=%s' % cliParams.flavour )
if pilotRef != 'Unknown':
  configureOpts.append( '-o /LocalSite/PilotReference=%s' % pilotRef )

# add options for BOINc
if cliParams.boincUserID:
  configureOpts.append( '-o /LocalSite/BoincUserID=%s' % cliParams.boincUserID )
if cliParams.boincHostID:
  configureOpts.append( '-o /LocalSite/BoincHostID=%s' % cliParams.boincHostID)
if cliParams.boincHostPlatform:
  configureOpts.append( '-o /LocalSite/BoincHostPlatform=%s' % cliParams.boincHostPlatform)
if cliParams.boincHostName:
  configureOpts.append( '-o /LocalSite/BoincHostName=%s' % cliParams.boincHostName )

###
# Try to get the CE name
###
#cliParams.ceName = 'Local'
if cliParams.flavour in ['LCG','gLite','OSG']:
  retCode, CE = executeAndGetOutput( 'glite-brokerinfo getCE || edg-brokerinfo getCE' )
  if not retCode:
    cliParams.ceName = CE.split( ':' )[0]
    if len( CE.split( '/' ) ) > 1:
      cliParams.queueName = CE.split( '/' )[1]
    configureOpts.append( '-N "%s"' % cliParams.ceName )
  elif os.environ.has_key( 'OSG_JOB_CONTACT' ):
    # OSG_JOB_CONTACT String specifying the endpoint to use within the job submission
    #                 for reaching the site (e.g. manager.mycluster.edu/jobmanager-pbs )
    CE = os.environ['OSG_JOB_CONTACT']
    cliParams.ceName = CE.split( '/' )[0]
    if len( CE.split( '/' ) ) > 1:
      cliParams.queueName = CE.split( '/' )[1]
    configureOpts.append( '-N "%s"' % cliParams.ceName )
  else:
    logERROR( "There was an error executing brokerinfo. Setting ceName to local " )
elif cliParams.flavour == "CREAM":
  if os.environ.has_key( 'CE_ID' ):
    cliParams.ceName = os.environ['CE_ID'].split( ':' )[0]
    if os.environ['CE_ID'].count( "/" ):
      cliParams.queueName = os.environ['CE_ID'].split( '/' )[1]
    configureOpts.append( '-N "%s"' % cliParams.ceName )
    #if cliParams.queueName:
    #  configureOpts.append( '-o /LocalSite/CEQueue="%s"' % cliParams.queueName )

if cliParams.queueName:
  configureOpts.append( '-o /LocalSite/CEQueue=%s' % cliParams.queueName )
if cliParams.ceName:
  configureOpts.append( '-o /LocalSite/GridCE=%s' % cliParams.ceName )
if cliParams.releaseVersion:
  configureOpts.append( '-o /LocalSite/ReleaseVersion=%s' % cliParams.releaseVersion )
if cliParams.releaseProject:
  configureOpts.append( '-o /LocalSite/ReleaseProject=%s' % cliParams.releaseProject )

###
# Set the platform if defined
###

if cliParams.platform:
  installOpts.append( '-p "%s"' % cliParams.platform )

###
# Set the group and the DN
###

if cliParams.userGroup:
  configureOpts.append( '-o /AgentJobRequirements/OwnerGroup="%s"' % cliParams.userGroup )

if cliParams.userDN:
  configureOpts.append( '-o /AgentJobRequirements/OwnerDN="%s"' % cliParams.userDN )

#############################################################################
# Treat the OSG case

osgDir = ''
if cliParams.flavour == "OSG":
  vo = cliParams.releaseProject.replace( 'DIRAC', '' ).upper()
  if not vo:
    vo = 'DIRAC'
  osgDir = os.environ['OSG_WN_TMP']
  # Make a separate directory per Project if it is defined
  jobDir = os.path.basename( pilotRef )
  if not jobDir:   # just in case
    import random
    jobDir = str( random.randint( 1000, 10000 ) )
  osgDir = os.path.join( osgDir, vo, jobDir )
  if not os.path.isdir(osgDir):
    os.makedirs(osgDir)
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

if rootPath == originalRootPath:
  # No special root path was requested
  rootPath = os.getcwd()

###
# Do the installation
###

installCmd = "%s %s" % ( installScript, " ".join( installOpts ) )

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

# Instead of dumping the Full configuration, include all Server in dirac.cfg
configureOpts.append( '-I' )
if cliParams.debug:
  configureOpts.append( '-d' )
configureCmd = "%s %s" % ( os.path.join( diracScriptsPath, "dirac-configure" ), " ".join( configureOpts ) )

logDEBUG( "Configuring DIRAC with: %s" % configureCmd )

if os.system( configureCmd ):
  logERROR( "Could not configure DIRAC" )
  sys.exit( 1 )

###
# Dump the CS to cache in file
###

# cfgFile = os.path.join( rootPath, "etc", "dirac.cfg" )
# cacheScript = os.path.join( diracScriptsPath, "dirac-configuration-dump-local-cache" )
# if os.system( "%s -f %s" % ( cacheScript, cfgFile ) ):
#   logERROR( "Could not dump the CS to %s" % cfgFile )
configureScript = os.path.join( diracScriptsPath, "dirac-configure" )

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
for envVarName in ( 'LD_LIBRARY_PATH', 'PYTHONPATH' ):
  if envVarName in os.environ:
    os.environ[ '%s_SAVE' % envVarName ] = os.environ[ envVarName ]
    del( os.environ[ envVarName ] )
  else:
    os.environ[ '%s_SAVE' % envVarName ] = ""
os.environ['LD_LIBRARY_PATH'] = "%s" % ( diracLibPath )
os.environ['PATH'] = '%s:%s:%s' % ( diracBinPath, diracScriptsPath, os.getenv( 'PATH' ) )

###
# End of initialisation
###

#
# Set the local architecture
#

architectureScriptName = "dirac-architecture"
architectureScript = ""
candidate = os.path.join( rootPath, "scripts", architectureScriptName )
if os.path.isfile( candidate ):
  architectureScript = candidate
else:
  # If the extension does not provide a dirac-architecture, use dirac-platform as default value
  candidate = os.path.join( rootPath, "scripts", "dirac-platform" )
  if os.path.isfile( candidate ):
    architectureScript = candidate

if architectureScript:
  retCode, localArchitecture = executeAndGetOutput( architectureScript )
  if not retCode:
    localArchitecture = localArchitecture.strip()
    # os.system( "%s -f %s -o '/LocalSite/Architecture=%s'" % ( cacheScript, cfgFile, localArchitecture ) )
    # dirac-configure will not change existing cfg unless -U option is used.
    os.system( "%s -F -o '/LocalSite/Architecture=%s'" % ( configureScript, localArchitecture ) )
  else:
    logERROR( "There was an error calling %s: %s" % ( architectureScript, localArchitecture ) )
#
# Get host and local user info
#

localUid = os.getuid()
try:
  import pwd
  localUser = pwd.getpwuid( localUid )[0]
except:
  localUser = 'Unknown'

logINFO( 'Uname      = %s' % " ".join( os.uname() ) )
logINFO( 'Host Name  = %s' % socket.gethostname() )
logINFO( 'Host FQDN  = %s' % socket.getfqdn() )
logINFO( 'User Name  = %s' % localUser )
logINFO( 'User Id    = %s' % localUid )
logINFO( 'CurrentDir = %s' % rootPath )

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

#
# Disk space check
#

fs = os.statvfs( rootPath )
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

if diskSpace < cliParams.minDiskSpace:
  logERROR( '%s MB < %s MB, not enough local disk space available, exiting'
                  % ( diskSpace, cliParams.minDiskSpace ) )
  sys.exit( 1 )

#
# Get job CPU requirement and queue normalization
#

if cliParams.flavour in ['LCG','gLite','OSG']:
  logINFO( 'CE = %s' % CE )
  logINFO( 'LCG_SITE_CE = %s' % cliParams.ceName )

  retCode, queueNormList = executeAndGetOutput( 'dirac-wms-get-queue-normalization %s' % CE )
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
        os.system( "%s -F -o /LocalSite/CPUScalingFactor=%s -o /LocalSite/CPUNormalizationFactor=%s" % ( configureScript,
                                                                                                      queueNorm / 250.,
                                                                                                      queueNorm / 250. ) )
    else:
      logERROR( 'Fail to get Normalization of the Queue' )
  else:
    logERROR( "There was an error calling dirac-wms-get-queue-normalization" )


  retCode, queueLength = executeAndGetOutput( 'dirac-wms-get-normalized-queue-length %s' % CE )
  if not retCode:
    queueLength = queueLength.strip().split( ' ' )
    if len( queueLength ) == 2:
      cliParams.jobCPUReq = float( queueLength[1] )
      logINFO( 'Normalized Queue Length = %s' % cliParams.jobCPUReq )
    else:
      logERROR( 'Failed to get Normalized length of the Queue' )
  else:
    logERROR( "There was an error calling dirac-wms-get-normalized-queue-length" )

# Instead of using the Average reported by the Site, determine a Normalization
#os.system( "dirac-wms-cpu-normalization -U" )

#
# further local configuration
#

inProcessOpts = ['-s /Resources/Computing/CEDefaults' ]
inProcessOpts .append( '-o WorkingDirectory=%s' % rootPath )
inProcessOpts .append( '-o GridCE=%s' % cliParams.ceName )
if cliParams.flavour in ['LCG','gLite','OSG']:
  inProcessOpts .append( '-o GridCEQueue=%s' % CE )
inProcessOpts .append( '-o LocalAccountString=%s' % localUser )
inProcessOpts .append( '-o TotalCPUs=%s' % 1 )
inProcessOpts .append( '-o MaxCPUTime=%s' % ( int( cliParams.jobCPUReq ) ) )
inProcessOpts .append( '-o CPUTime=%s' % ( int( cliParams.jobCPUReq ) ) )
inProcessOpts .append( '-o MaxRunningJobs=%s' % 1 )
# To prevent a wayward agent picking up and failing many jobs.
inProcessOpts .append( '-o MaxTotalJobs=%s' % 10 )


jobAgentOpts = [ '-o MaxCycles=%s' % cliParams.maxCycles ]
# jobAgentOpts.append( '-o CEUniqueID=%s' % JOB_AGENT_CE )
if cliParams.debug:
  jobAgentOpts.append( '-o LogLevel=DEBUG' )

if cliParams.userGroup:
  logINFO( 'Setting DIRAC Group to "%s"' % cliParams.userGroup )
  inProcessOpts .append( '-o OwnerGroup="%s"' % cliParams.userGroup )

if cliParams.userDN:
  logINFO( 'Setting Owner DN to "%s"' % cliParams.userDN )
  inProcessOpts .append( '-o OwnerDN="%s"' % cliParams.userDN )

# Find any .cfg file uploaded with the sandbox
extraCFG = []
for i in os.listdir( rootPath ):
  cfg = os.path.join( rootPath, i )
  if os.path.isfile( cfg ) and re.search( '.cfg&', cfg ):
    extraCFG.append( cfg )

if executeCmd:
  #Execute user command
  logINFO( "Executing user defined command: %s" % executeCmd )
  sys.exit( os.system( "source bashrc; %s" % executeCmd ) / 256 )

#
# Start the job agent
#
logINFO( 'Starting JobAgent' )
os.environ['PYTHONUNBUFFERED'] = 'yes'

diracAgentScript = os.path.join( rootPath, "scripts", "dirac-agent" )
jobAgent = '%s WorkloadManagement/JobAgent %s %s %s' % ( diracAgentScript,
                                                         " ".join( jobAgentOpts ),
                                                         " ".join( inProcessOpts ),
                                                         " ".join( extraCFG ) )

logINFO( "JobAgent execution command:\n%s" % jobAgent )

if not cliParams.dryRun:
  os.system( jobAgent )

fs = os.statvfs( rootPath )
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
ret = os.system( 'dirac-proxy-info' )

# Do some cleanup
if os.environ.has_key( 'OSG_WN_TMP' ) and osgDir:
  os.chdir( originalRootPath )
  import shutil
  shutil.rmtree( osgDir )

sys.exit( 0 )
