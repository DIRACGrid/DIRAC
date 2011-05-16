#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-compile-externals
# Author : Adria Casajus
########################################################################
"""
Compile DIRAC externals (does not require DIRAC code)
"""
__RCSID__ = "$Id$"

import tempfile
import urllib2
import os
import tarfile
import getopt
import sys
import stat
import imp
import shutil

svnPublicRoot = "http://svnweb.cern.ch/guest/dirac/Externals/%s"
tarWebRoot = "http://svnweb.cern.ch/world/wsvn/dirac/Externals/%s/?op=dl&rev=0&isdir=1"

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

def downloadExternalsSVN( destPath, version = False ):
  if version and not version.lower() in ( "trunk", "head" ):
    snapshotPath = "tags/%s" % version
  else:
    snapshotPath = "trunk"
    version = "trunk"
  extPath = os.path.join( destPath, "Externals" )
  osCmd = "svn export http://svnweb.cern.ch/guest/dirac/Externals/%s '%s'" % ( snapshotPath, extPath )
  return not os.system( osCmd )

def downloadExternalsTar( destPath, version = False ):
  netReadSize = 1024 * 1024
  if version and not version.lower() in ( "trunk", "head" ):
    snapshotPath = "tags/%s" % version
  else:
    snapshotPath = "trunk"
    version = "trunk"
  print "Requesting externals..."
  remoteDesc = urllib2.urlopen( tarWebRoot % snapshotPath )
  fd, filePath = tempfile.mkstemp()
  data = remoteDesc.read( netReadSize )
  print "Downloading..."
  sizeDown = 0
  while data:
    os.write( fd, data )
    sizeDown += len( data )
    data = remoteDesc.read( netReadSize )
  ret = os.system( "cd '%s'; tar xzf '%s'" % ( destPath, filePath ) )
  os.unlink( filePath )
  if ret:
    return False
  print "Downloaded %s bytes" % sizeDown
  for entry in os.listdir( destPath ):
    if entry.find( version ) == 0:
      os.rename( os.path.join( destPath, entry ), os.path.join( destPath, "Externals" ) )
      break
  return True

def downloadFileFromSVN( filePath, destPath, isExecutable = False, filterLines = [] ):
  fileName = os.path.basename( filePath )
  print " - Downloading %s" % fileName
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/DIRAC/trunk/%s?op=dl&rev=0" % filePath
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/%s' % filePath
  downOK = False
  localPath = os.path.join( destPath, fileName )
  for remoteLocation in ( viewSVNLocation, anonymousLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      continue
    remoteData = remoteFile.read()
    remoteFile.close()
    if remoteData:
      localFile = open( localPath , "wb" )
      localFile.write( remoteData )
      localFile.close()
      downOK = True
      break
  if not downOK:
    osCmd = "svn cat 'http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/%s' > %s" % ( filePath, localPath )
    if os.system( osCmd ):
      print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % fileName
      sys.exit( 1 )
  if filterLines:
    fd = open( localPath, "rb" )
    fileContents = fd.readlines()
    fd.close()
    fd = open( localPath, "wb" )
    for line in fileContents:
      isFiltered = False
      for filter in filterLines:
        if line.find( filter ) > -1:
          isFiltered = True
          break
      if not isFiltered:
        fd.write( line )
    fd.close()
  if isExecutable:
    os.chmod( localPath , executablePerms )

def findDIRACRoot( path ):
  dirContents = os.listdir( path )
  if 'DIRAC' in dirContents and os.path.isdir( os.path.join( path, 'DIRAC' ) ):
    return path
  parentPath = os.path.dirname( path )
  if parentPath == path or len( parentPath ) == 1:
    return False
  return findDIRACRoot( os.path.dirname( path ) )

def resolvePackagesToBuild( compType, buildCFG, alreadyExplored = [] ):
  explored = list( alreadyExplored )
  packagesToBuild = []
  if compType not in buildCFG.listSections():
    return []
  typeCFG = buildCFG[ compType ]
  for type in typeCFG.getOption( 'require', [] ):
    if type in explored:
      continue
    explored.append( type )
    newPackages = resolvePackagesToBuild( type, buildCFG, explored )
    for pkg in newPackages:
      if pkg not in packagesToBuild:
        packagesToBuild.append( pkg )
  for pkg in typeCFG.getOption( 'buildOrder', [] ):
    if pkg not in packagesToBuild:
      packagesToBuild.append( pkg )
  return packagesToBuild

def fixAbsoluteLinks( path ):
  for entry in os.listdir( path ):
    entryPath = os.path.join( path, entry )
    if os.path.islink( entryPath ):
      destPath = os.readlink( entryPath )
      if os.path.isabs( destPath ):
        absLinkDirSplit = [ d for d in os.path.abspath( path ).split( "/" ) if d.strip() ]
        absDestDirSplit = [ d for d in destPath.split( "/" ) if d.strip() ]
        common = -1
        for i in range( min( len( absLinkDirSplit ), len( absDestDirSplit ) ) ):
          if absLinkDirSplit[ i ] == absDestDirSplit[ i ]:
            common = i
          else:
            break
        absLinkDirSplit = absLinkDirSplit[ common + 1: ]
        absDestDirSplit = absDestDirSplit[ common + 1: ]
        finalDestination = [ ".." for d in absLinkDirSplit ]
        finalDestination.extend( absDestDirSplit )
        finalDestination = os.path.join( *finalDestination )
        print "Relinking %s" % entryPath
        print "    %s -> %s" % ( destPath, finalDestination )
        os.unlink( entryPath )
        os.symlink( finalDestination, entryPath )
    elif os.path.isdir( entryPath ):
      fixAbsoluteLinks( entryPath )

cmdOpts = ( ( 'D:', 'destination=', 'Destination where to build the externals' ),
            ( 't:', 'type=', 'Type of compilation (default: client)' ),
            ( 'e:', 'externalsPath=', 'Path to the externals sources' ),
            ( 'v:', 'version=', 'Version of the externals to compile (default will be trunk)' ),
            ( 'h', 'help', 'Show this help' ),
            ( 'i:', 'pythonVersion=', 'Python version to compile (25/24)' ),
            ( 'f', 'fixLinksOnly', 'Only fix absolute soft links' ),
            ( 'j:', 'makeJobs=', 'Number of make jobs, by default is 1' )
          )

compExtVersion = False
compType = 'client'
compDest = False
compExtSource = False
onlyFixLinks = False
makeArgs = []
compVersionDict = { 'PYTHONVERSION' : '2.5' }

optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )
for o, v in optList:
  if o in ( '-h', '--help' ):
    print __doc__.split( '\n' )[1]
    print "Usage:\n  %s [options]..." % sys.argv[0]
    print "Options:"
    for cmdOpt in cmdOpts:
      print "-%s --%s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 15 ), cmdOpt[2] )
    sys.exit( 1 )
  elif o in ( '-t', '--type' ):
    compType = v.lower()
  elif o in ( '-e', '--externalsPath' ):
    compExtSource = v
  elif o in ( '-D', '--destination' ):
    compDest = v
  elif o in ( '-v', '--version' ):
    compExtVersion = v
  elif o in ( '-i', '--pythonversion' ):
    compVersionDict[ 'PYTHONVERSION' ] = ".".join( [ c for c in v ] )
  elif o in ( '-f', '--fixLinksOnly' ):
    onlyFixLinks = True
  elif o in ( '-j', '--makeJobs' ):
    try:
      v = int( v )
    except:
      print "Value for makeJobs is not an integer (%s)" % v
      sys.exit( 1 )
    if v < 1:
      print "Value for makeJobs mas to be greater than 0 (%s)" % v
      sys.exit( 1 )
    makeArgs.append( "-j %d" % int( v ) )

#Find platform
basePath = os.path.dirname( os.path.realpath( __file__ ) )
diracRoot = findDIRACRoot( basePath )
if diracRoot:
  platformPath = os.path.join( diracRoot, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  platform = Platform.getPlatformString()

if not compDest:
  if not diracRoot:
    print "Error: Could not find DIRAC root"
    sys.exit( 1 )
  print "Using platform %s" % platform
  if not platform or platform == "ERROR":
    print >> sys.stderr, "Can not determine local platform"
    sys.exit( -1 )
  compDest = os.path.join( diracRoot, platform )

if onlyFixLinks:
  print "Fixing absolute links"
  fixAbsoluteLinks( compDest )
  sys.exit( 0 )

if compDest:
  if os.path.isdir( compDest ):
    oldCompDest = compDest + '.old'
    print "Warning: %s already exists! Backing it up to %s" % ( compDest, oldCompDest )
    if os.path.exists( oldCompDest ):
      shutil.rmtree( oldCompDest )
    os.rename( compDest, oldCompDest )

if not compExtSource:
  workDir = tempfile.mkdtemp( prefix = "ExtDIRAC" )
  print "Creating temporary work dir at %s" % workDir
  downOK = False
  for fnc in ( downloadExternalsSVN, downloadExternalsTar ):
    if fnc( workDir, compExtVersion ):
      downOK = True
      break
  if not downOK:
    print "Oops! Could not download Externals!"
    sys.exit( 1 )
  externalsDir = os.path.join( workDir, "Externals" )
else:
  externalsDir = compExtSource

downloadFileFromSVN( "DIRAC/Core/scripts/dirac-platform.py", externalsDir, True )
downloadFileFromSVN( "DIRAC/Core/Utilities/CFG.py", externalsDir, False, [ '@gCFGSynchro' ] )

#Load CFG
cfgPath = os.path.join( externalsDir, "CFG.py" )
cfgFD = open( cfgPath, "r" )
CFG = imp.load_module( "CFG", cfgFD, cfgPath, ( "", "r", imp.PY_SOURCE ) )
cfgFD.close()

buildCFG = CFG.CFG().loadFromFile( os.path.join( externalsDir, "builds.cfg" ) )

if compType not in buildCFG.listSections():
  print "Invalid compilation type %s" % compType
  print " Valid ones are: %s" % ", ".join( buildCFG.listSections() )
  sys.exit( 1 )

packagesToBuild = resolvePackagesToBuild( compType, buildCFG )


if compDest:
  makeArgs .append( "-p '%s'" % os.path.realpath( compDest ) )

#Substitution of versions 
finalPackages = []
for prog in packagesToBuild:
  for k in compVersionDict:
    finalPackages.append( prog.replace( "$%s$" % k, compVersionDict[k] ) )

print "Trying to get a raw environment"
patDet = os.path.join( diracRoot, platform )
for envVar in ( 'LD_LIBRARY_PATH', 'PATH' ):
  if envVar not in os.environ:
    continue
  envValue = os.environ[ envVar ]
  valList = [ val.strip() for val in envValue.split( ":" ) if envValue.strip() ]
  fixedValList = []
  for value in valList:
    print patDet
    if value.find( patDet ) != 0:
      fixedValList.append( value )
  os.environ[ envVar ] = ":".join( fixedValList )

makeArgs = " ".join( makeArgs )
print "Building %s" % ", ".join ( finalPackages )
for prog in finalPackages:
  print "== BUILDING %s == " % prog
  progDir = os.path.join( externalsDir, prog )
  makePath = os.path.join( progDir, "dirac-make.py" )
  buildOutPath = os.path.join( progDir, "build.out" )
  os.chmod( makePath, executablePerms )
  instCmd = "'%s' %s" % ( makePath, makeArgs )
  print " - Executing %s" % instCmd
  ret = os.system( "%s  > '%s' 2>&1" % ( instCmd, buildOutPath ) )
  if ret:
    print "Oops! Error while compiling %s" % prog
    print "Take a look at %s for more info" % buildOutPath
    sys.exit( 1 )

print "Fixing absolute links"
fixAbsoluteLinks( compDest )



