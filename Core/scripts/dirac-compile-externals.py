#!/usr/bin/env python
# $HeadURL$
"""
Compile the externals
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

svnPublicRoot = "http://svnweb.cern.ch/guest/dirac/Externals/%s"
tarWebRoot = "http://svnweb.cern.ch/world/wsvn/dirac/Externals/%s/?op=dl&rev=0&isdir=1"

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

def downloadExternalsSVN( destPath, version = False ):
  if version:
    snapshotPath = "tags/%s" % version
  else:
    snapshotPath = "trunk"
    version = "trunk"
  extPath = os.path.join( destPath, "Externals" )
  osCmd = "svn export http://svnweb.cern.ch/guest/dirac/Externals/%s '%s'" % ( snapshotPath, extPath )
  return not os.system( osCmd )          
  
def downloadExternalsTar( destPath, version = False ):
  netReadSize = 1024*1024
  if version:
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
  
def downloadFileFromSVN( filePath, destPath, isExecutable = False ):
  fileName = os.path.basename( filePath )
  print " - Downloading %s" % fileName 
  remoteFile = urllib2.urlopen( "http://svnweb.cern.ch/world/wsvn/dirac/DIRAC/trunk/%s?op=dl&rev=0" % filePath )
  localPath = os.path.join( destPath, fileName )
  localFile = open( localPath , "wb" )
  localFile.write( remoteFile.read() )
  localFile.close()
  remoteFile.close()
  if isExecutable:
    os.chmod(localPath , executablePerms )
  
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
  
cmdOpts = ( ( 'd:', 'destination=',   'Destination where to build the externals' ),
            ( 't:', 'type=',          'Type of compilation (default: client)' ),
            ( 'e:', 'externalsPath=', 'Path to the externals sources' ),
            ( 'h',  'help',           'Show this help' ),
          )

compType = 'client'
compDest = False
compExtSource = False
compVersionDict = { 'PYTHONVERSION' : '2.5' }
  
optList, args = getopt.getopt( sys.argv[1:], 
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )
for o, v in optList:
  if o in ( '-h', '--help' ):
    print "Usage %s <opts>" % sys.argv[0]
    for cmdOpt in cmdOpts:
      print "%s %s : %s" % ( cmdOpt[0].ljust(4), cmdOpt[1].ljust(15), cmdOpt[2] )
    sys.exit(1)
  elif o in ( '-t', '--type' ):
    compType = v.lower()
  elif o in ( '-e', '--externalsPath' ):
    compExtSource = v
  elif o in ( '-d', '--destination' ):
    compDest = v

if not compDest:
  basePath = os.path.dirname( os.path.realpath( __file__ ) )
  diracRoot = findDIRACRoot( basePath )
  if not diracRoot:
    print "Error: Could not find DIRAC root"
    sys.exit(1)
  import popen2
  try:
    p3 = popen2.Popen3( os.path.join( basePath, 'dirac-platform.py' ) )
  except AttributeError:
    print "Error: Cannot find dirac-platform.py!"
    sys.exit(1)
  platform = p3.fromchild.read().strip()
  p3.wait()
  if not platform or platform == "ERROR":
    print >> sys.stderr, "Can not determine local platform"
    sys.exit(-1)
  compDest = os.path.join( diracRoot, platform )

if compDest:  
  if os.path.isdir( compDest ):
    print "Error: %s already exists! Please make sure target dir does not exist" % compDest
    sys.exit(1)
    
if not compExtSource:
  workDir = tempfile.mkdtemp( prefix = "ExtDIRAC" )
  print "Creating temporary work dir at %s" % workDir
  downOK = False
  for fnc in ( downloadExternalsTar, downloadExternalsSVN ):
    if fnc( workDir ):
      downOK = True
      break
  if not downOK:
    print "Oops! Could not download Externals!"
    sys.exit(1)
  externalsDir = os.path.join( workDir, "Externals" )
else:
  externalsDir = compExtSource
  
downloadFileFromSVN( "DIRAC/Core/scripts/dirac-platform.py", externalsDir, True )
downloadFileFromSVN( "DIRAC/Core/Utilities/CFG.py", externalsDir, False )

#Load CFG
cfgPath = os.path.join( externalsDir, "CFG.py" )
cfgFD = open( cfgPath, "r" )
CFG = imp.load_module( "CFG", cfgFD, cfgPath, ( "", "r", imp.PY_SOURCE ) )
cfgFD.close()

buildCFG = CFG.CFG().loadFromFile( os.path.join( externalsDir, "builds.cfg" ) )

if compType not in buildCFG.listSections():
  print "Invalid compilation type %s" % compType
  print " Valid ones are: %s" % ", ".join( compilationTypes )
  sys.exit(1)

packagesToBuild = resolvePackagesToBuild( compType, buildCFG )

if compDest:
  makeArgs = compDest
else:
  makeArgs = ""

print "Building %s" % ", ".join ( packagesToBuild )
for prog in packagesToBuild:
  for k in compVersionDict:
    prog = prog.replace( "$%s$" % k, compVersionDict[k] )
  print "== BUILDING %s == " % prog
  makePath = os.path.join( externalsDir, prog, "dirac-make" )
  os.chmod( makePath, executablePerms )
  instCmd = "'%s' '%s'" % ( makePath, makeArgs )
  print " - Executing %s" % instCmd
  ret = os.system( instCmd )
  if ret:
    print "Oops! Error while compiling %s" % prog
    sys.exit(1)




  