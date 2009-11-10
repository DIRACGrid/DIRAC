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

svnPublicRoot = "http://svnweb.cern.ch/guest/dirac/Externals/%s"
tarWebRoot = "http://svnweb.cern.ch/world/wsvn/dirac/Externals/%s/?op=dl&rev=0&isdir=1"

compilationTypes = { 'client' : [ 'pythonRequirements', 
                                  'Python-$PYTHONVERSION%', 
                                  'pyGSI', 'runit', 'ldap' ],
                     'server' : [ 'pythonRequirements', 
                                  'Python-$PYTHONVERSION%', 
                                  'pyGSI', 'runit', 'ldap',
                                  'MySQL', 'MySQL-python',
                                  'Pylons', 'pyPlotTools',
                                  'rrdtool' ]  }

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
  while data:
    os.write( fd, data )
    data = remoteDesc.read( netReadSize )
  ret = os.system( "cd '%s'; tar xzf '%s'" % ( destPath, filePath ) )
  os.unlink( filePath )
  if ret:
    return False
  print "Downloaded"
  for entry in os.listdir( destPath ):
    if entry.find( version ) == 0:
      os.rename( os.path.join( destPath, entry ), os.path.join( destPath, "Externals" ) )
      break
  return True
  
def downloadPlatformScript( destPath ):
  print "Downloading dirac-platform file..."
  platformFile = open( os.path.join( destPath, "dirac-platform.py" ) , "wb" )
  remoteFile = urllib2.urlopen( "http://svnweb.cern.ch/world/wsvn/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-platform.py" )
  platformFile.write( remoteFile.read() )
  platformFile.close()
  remoteFile.close()
  os.chmod( os.path.join( destPath, "dirac-platform.py" ) , executablePerms )
  print "Downloaded"
  
def findDIRACRoot( path ):
  dirContents = os.listdir( path )
  if 'DIRAC' in dirContents and os.path.isdir( os.path.join( path, 'DIRAC' ) ):
    return path
  parentPath = os.path.dirname( path )
  if parentPath == path or len( parentPath ) == 1:
    return False
  return findDIRACRoot( os.path.dirname( path ) )
  
cmdOpts = ( ( 'd:', 'destination=',   'Destination where to build the externals' ),
            ( 't:', 'type=',          'Type of compilation (default: client)' ),
            ( 'e:', 'externalsPath=', 'Path to the externals sources' ),
            ( 'h',  'help',           'Show this help' ),
            ( 'u',  'directUse',      'Compile in <diracroot>/<platform>' )
          )

compType = 'client'
compDest = False
compExtSource = False
compAutoDest = False
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
    compType = v
  elif o in ( '-e', '--externalsPath' ):
    compExtSource = v
  elif o in ( '-d', '--destination' ):
    compDest = v
  elif o in ( '-u', '--directUse' ):
    compAutoDest = True

if compType not in compilationTypes:
  print "Invalid compilation type %s" % compType
  print " Valid ones are: %s" % ", ".join( compilationTypes )
  sys.exit(1)

if compAutoDest:
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
    
workDir = tempfile.mkdtemp( prefix = "ExtDIRAC" )
print "Creating temporary work dir at %s" % workDir
    
if not compExtSource:
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
  
downloadPlatformScript( externalsDir )

if compDest:
  makeArgs = compDest
else:
  makeArgs = ""

for prog in compilationTypes[ compType ]:
  for k in compVersionDict:
    prog = prog.replace( "$%s$" % k, compVersionDict[k] )
  print "== BUILDING %s == " % prog
  makePath = os.path.join( externalsDir, prog, "dirac-make" )
  os.chmod( makePath, executablePerms )
  ret = os.system( "'%s' %s" % ( makePath, makeArgs ) )
  if ret:
    print "Oops! Error while compiling %s" % prog
    sys.exit(1)




  