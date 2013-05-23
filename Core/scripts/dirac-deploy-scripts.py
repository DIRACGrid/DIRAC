#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-deploy-scripts
# Author : Adria Casajus
########################################################################
"""
Deploy all scripts and extensions
"""
__RCSID__ = "$Id$"

import os
import shutil
import stat
import re
import time
import sys

def logDEBUG( msg ):
  if False:
    for line in msg.split( "\n" ):
      print "%s UTC dirac-deploy-scripts [DEBUG] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
    sys.stdout.flush()

def logERROR( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-deploy-scripts [ERROR] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def logNOTICE( msg ):
  for line in msg.split( "\n" ):
    print " ", line
    # print "%s UTC dirac-deploy-scripts [NOTICE]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

moduleSuffix = "DIRAC"
gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
excludeMask = [ '__init__.py' ]
simpleCopyMask = [ os.path.basename( __file__ ), 'dirac-compile-externals.py', 'dirac-install.py', 'dirac-platform.py' ]

wrapperTemplate = """#!/usr/bin/env python
#
import os,sys,imp
#
DiracRoot = os.path.dirname(os.path.dirname( os.path.realpath( sys.argv[0] ) ))
if 'DIRACPLAT' in os.environ:
  DiracPlatform = os.environ['DIRACPLAT']
else:
  platformPath = os.path.join( DiracRoot, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  DiracPlatform = Platform.getPlatformString()
  if not DiracPlatform or DiracPlatform == "ERROR":
    print >> sys.stderr, "Can not determine local platform"
    sys.exit(-1)
DiracPath        = '%s' % ( os.path.join(DiracRoot,DiracPlatform,'bin'), )
DiracPythonPath  = '%s' % ( DiracRoot, )
DiracLibraryPath      = '%s' % ( os.path.join(DiracRoot,DiracPlatform,'lib'), )

baseLibPath = DiracLibraryPath
if os.path.exists( baseLibPath ):
  for entry in os.listdir( baseLibPath ):
    if os.path.isdir( entry ):
      DiracLibraryPath = '%s:%s' % ( DiracLibraryPath, os.path.join( baseLibPath, entry ) )


os.environ['PATH'] = '%s:%s' % ( DiracPath, os.environ['PATH'] )

for varName in ( 'LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH'):
  if varName not in os.environ:
    os.environ[varName] = DiracLibraryPath
  else:
    os.environ[varName] = '%s:%s' % ( DiracLibraryPath, os.environ[varName] )

if 'PYTHONPATH' not in os.environ:
  os.environ['PYTHONPATH'] = DiracPythonPath
else:
  os.environ['PYTHONPATH'] = '%s:%s' % ( DiracPythonPath, os.environ['PYTHONPATH'] )

DiracScript = os.path.join( DiracRoot, '$SCRIPTLOCATION$' )

certDir = os.path.join( "etc", "grid-security", "certificates" )
if 'X509_CERT_DIR' not in os.environ and \
  not os.path.isdir( os.path.join( "/", certDir ) ) and \
  os.path.isdir( os.path.join( DiracRoot, certDir ) ):
  os.environ[ 'X509_CERT_DIR' ] = os.path.join( DiracRoot, certDir )

# DCommands special
os.environ['DCOMMANDS_PPID'] = str( os.getppid( ) )

if sys.argv[1:]:
  args = ' "%s"' % '" "'.join( sys.argv[1:] )
else:
  args = ''
sys.exit( os.system('python "%s"%s' % ( DiracScript, args )  ) / 256 )
"""


def lookForScriptsInPath( basePath, rootModule ):
  isScriptsDir = os.path.split( rootModule )[1] == "scripts"
  scriptFiles = []
  for entry in os.listdir( basePath ):
    absEntry = os.path.join( basePath, entry )
    if os.path.isdir( absEntry ):
      scriptFiles.extend( lookForScriptsInPath( absEntry, os.path.join( rootModule, entry ) ) )
    elif isScriptsDir and os.path.isfile( absEntry ):
      scriptFiles.append( ( os.path.join( rootModule, entry ), entry ) )
  return scriptFiles

def findDIRACRoot( path ):
  dirContents = os.listdir( path )
  if 'DIRAC' in dirContents and os.path.isdir( os.path.join( path, 'DIRAC' ) ):
    return path
  parentPath = os.path.dirname( path )
  if parentPath == path or len( parentPath ) == 1:
    return False
  return findDIRACRoot( os.path.dirname( path ) )


rootPath = findDIRACRoot( os.path.dirname( os.path.realpath( __file__ ) ) )
if not rootPath:
  logERROR( "Error: Cannot find DIRAC root!" )
  sys.exit( 1 )

targetScriptsPath = os.path.join( rootPath, "scripts" )
pythonScriptRE = re.compile( "(.*/)*([a-z]+-[a-zA-Z0-9-]+|d[a-zA-Z0-9-]+).py" )
logNOTICE( "Scripts will be deployed at %s" % targetScriptsPath )

if not os.path.isdir( targetScriptsPath ):
  os.mkdir( targetScriptsPath )

for rootModule in os.listdir( rootPath ):
  modulePath = os.path.join( rootPath, rootModule )
  if not os.path.isdir( modulePath ):
    continue
  extSuffixPos = rootModule.find( moduleSuffix )
  if extSuffixPos == -1 or extSuffixPos != len( rootModule ) - len( moduleSuffix ):
    continue
  logNOTICE( "Inspecting %s module" % rootModule )
  scripts = lookForScriptsInPath( modulePath, rootModule )
  for script in scripts:
    scriptPath = script[0]
    scriptName = script[1]
    if scriptName in excludeMask:
      continue
    scriptLen = len( scriptName )
    if scriptName not in simpleCopyMask and pythonScriptRE.match( scriptName ):
      logDEBUG( " Wrapping %s" % scriptName[:-3] )
      fakeScriptPath = os.path.join( targetScriptsPath, scriptName[:-3] )
      fd = open( fakeScriptPath, "w" )
      fd.write( wrapperTemplate.replace( '$SCRIPTLOCATION$', scriptPath ) )
      fd.close()
      os.chmod( fakeScriptPath, gDefaultPerms )
    else:
      logDEBUG( " Copying %s" % scriptName )
      shutil.copy( os.path.join( rootPath, scriptPath ), targetScriptsPath )
      copyPath = os.path.join( targetScriptsPath, scriptName )
      os.chmod( copyPath, gDefaultPerms )
      cLen = len( copyPath )
      reFound = pythonScriptRE.match( copyPath )
      if reFound:
        destPath = "".join( list( reFound.groups() ) )
        os.rename( copyPath, destPath )
