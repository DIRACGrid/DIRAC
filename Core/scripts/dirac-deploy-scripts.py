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
import sys
import platform

DEBUG = False

moduleSuffix = "DIRAC"
gDefaultPerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
excludeMask = [ '__init__.py' ]
simpleCopyMask = [ os.path.basename( __file__ ),
                   'dirac-compile-externals.py',
                   'dirac-install.py',
                   'dirac-platform.py',
                   'dirac_compile_externals.py',
                   'dirac_install.py',
                   'dirac_platform.py']

wrapperTemplate = """#!$PYTHONLOCATION$
#
import os,sys,imp
#
DiracRoot = os.path.dirname(os.path.dirname( os.path.realpath( sys.argv[0] ) ))
if 'DIRACPLAT' in os.environ:
  DiracPlatform = os.environ['DIRACPLAT']
else:
  platformPath = os.path.join( DiracRoot, "DIRAC", "Core", "Utilities", "Platform.py" )
  with open( platformPath, "r" ) as platFD:
    Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
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
"""

# Python interpreter location can be specified as an argument
pythonLocation = "/usr/bin/env python"
if len( sys.argv ) == 2:
  pythonLocation = os.path.join( sys.argv[1], 'bin', 'python' )
wrapperTemplate = wrapperTemplate.replace( '$PYTHONLOCATION$', pythonLocation )

# On the newest MacOS the DYLD_LIBRARY_PATH variable is not passed to the shell of
# the os.system() due to System Integrity Protection feature
if platform.system() == "Darwin":
  wrapperTemplate += """
sys.exit( os.system( 'DYLD_LIBRARY_PATH=%s python "%s"%s' % ( DiracLibraryPath, DiracScript, args )  ) / 256 )
"""
else:
  wrapperTemplate += """
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
  print "Error: Cannot find DIRAC root!"
  sys.exit( 1 )

targetScriptsPath = os.path.join( rootPath, "scripts" )
pythonScriptRE = re.compile( "(.*/)*([a-z]+-[a-zA-Z0-9-]+|[a-z]+_[a-zA-Z0-9_]+|d[a-zA-Z0-9-]+).py$" )
print "Scripts will be deployed at %s" % targetScriptsPath

if not os.path.isdir( targetScriptsPath ):
  os.mkdir( targetScriptsPath )


# DIRAC scripts need to be treated first, so that its scripts
# can be overwritten by the extensions
listDir = os.listdir( rootPath )
if 'DIRAC' in listDir:  # should always be true...
  listDir.remove( 'DIRAC' )
  listDir.insert( 0, 'DIRAC' )

for rootModule in listDir:
  modulePath = os.path.join( rootPath, rootModule )
  if not os.path.isdir( modulePath ):
    continue
  extSuffixPos = rootModule.find( moduleSuffix )
  if extSuffixPos == -1 or extSuffixPos != len( rootModule ) - len( moduleSuffix ):
    continue
  print "Inspecting %s module" % rootModule
  scripts = lookForScriptsInPath( modulePath, rootModule )
  for script in scripts:
    scriptPath = script[0]
    scriptName = script[1]
    if scriptName in excludeMask:
      continue
    scriptLen = len( scriptName )
    if scriptName not in simpleCopyMask and pythonScriptRE.match( scriptName ):
      newScriptName = scriptName[:-3].replace( '_', '-' )
      if DEBUG:
        print " Wrapping %s as %s" % ( scriptName, newScriptName )
      fakeScriptPath = os.path.join( targetScriptsPath, newScriptName )
      with open( fakeScriptPath, "w" ) as fd:
        fd.write( wrapperTemplate.replace( '$SCRIPTLOCATION$', scriptPath ) )
      os.chmod( fakeScriptPath, gDefaultPerms )
    else:
      if DEBUG:
        print " Copying %s" % scriptName
      shutil.copy( os.path.join( rootPath, scriptPath ), targetScriptsPath )
      copyPath = os.path.join( targetScriptsPath, scriptName )
      if platform.system() == 'Darwin':
        with open( copyPath, 'r+' ) as script:
          scriptStr = script.read()
          script.seek( 0 )
          script.write( scriptStr.replace( '/usr/bin/env python', pythonLocation ) )
      os.chmod( copyPath, gDefaultPerms )
      cLen = len( copyPath )
      reFound = pythonScriptRE.match( copyPath )
      if reFound:
        pathList = list( reFound.groups() )
        pathList[-1] = pathList[-1].replace( '_', '-' )
        destPath = "".join( pathList )
        if DEBUG:
          print " Renaming %s as %s" % ( copyPath, destPath )
        os.rename( copyPath, destPath )
