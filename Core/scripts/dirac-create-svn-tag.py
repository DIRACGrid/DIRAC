#!/usr/bin/env python
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-deploy-scripts.py $
"""
Tag a new release in SVN
"""
__RCSID__ = "$Id: dirac-deploy-scripts.py 18378 2009-11-18 19:24:03Z acasajus $"
import DIRAC
from DIRAC.Core.Base                                         import Script

import sys, os, tempfile, shutil, getpass

svnProject = 'DIRAC'
svnVersion = False

svnSshRoot    = "svn+ssh://%s@svn.cern.ch/reps/dirac/%s"

def setVersion( optionValue ):
  global svnVersion
  svnVersion = optionValue
  return DIRAC.S_OK()

def setProject( optionValue ):
  global svnProject
  svnProject = optionValue
  return DIRAC.S_OK()

Script.disableCS()

Script.registerSwitch( "v:", "version=",                "version to tag (mandatory)", setVersion )
Script.registerSwitch( "p:", "project=",                "project to tag (default = DIRAC)", setProject )

Script.parseCommandLine( ignoreErrors = False )

DIRAC.gLogger.info( 'Executing: %s ' % ( ' '.join(sys.argv) ) )

def usage():
  Script.showHelp()
  DIRAC.exit(2)
  
if not svnVersion:
  usage()

def downloadFileFromSVN( projectName, filePath, destPath, isExecutable = False, filterLines = [] ):
  import urllib2, stat
  executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
  fileName = os.path.basename( filePath )
  print " - Downloading %s" % fileName 
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s/trunk/%s?op=dl&rev=0" % ( projectName, filePath )
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s/trunk/%s' % ( projectName, filePath )
  downOK = False
  localPath = os.path.join( destPath, fileName )
  for remoteLocation in ( viewSVNLocation, anonymousLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      DIRAC.gLogger.exception()
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
    osCmd = "svn cat 'http://svnweb.cern.ch/guest/dirac/%s/trunk/%s' > %s" % ( svnProject, filePath, localPath )
    if os.system( osCmd ):
      print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % fileName
      sys.exit(1)
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
    os.chmod(localPath , executablePerms )

  return localPath

tmpDir = tempfile.mkdtemp( )
try:
  versionsFile = downloadFileFromSVN( svnProject, "%s/versions.cfg" % svnProject, tmpDir, False )
  
  buildCFG = DIRAC.Core.Utilities.CFG.CFG().loadFromFile( versionsFile )

  if not svnVersion in buildCFG.listSections():
    DIRAC.gLogger.error( 'Version does not exist:', svnVersion )
    DIRAC.gLogger.error( 'Available versions:', ', '.join( buildCFG.listSections() ) )
    DIRAC.exit(-1)


  userName = raw_input( "SVN User Name[%s]: " % getpass.getuser() )
  if not userName:
    userName = getpass.getuser()

  packageList = buildCFG[svnVersion].listOptions()
  print packageList
  msg = '"Release %s"' % svnVersion
  dest = svnSshRoot % ( userName, '%s/tags/%s/%s_%s/%s' % ( svnProject, svnProject, svnProject, svnVersion, svnProject ) )
  cmd = 'svn --parents -m %s mkdir %s' % ( msg, dest )
  source = []
  for extra in ['__init__.py', 'versions.cfg']:
    source.append( svnSshRoot % ( userName, '%s/trunk/%s/%s'  % ( svnProject, svnProject, extra ) ) )
  for pack in packageList:
    packVer = buildCFG[svnVersion].getOption(pack,'')
    if packVer in ['trunk', '', 'HEAD']:
      source.append( svnSshRoot % ( userName, '%s/trunk/%s/%s'  % ( svnProject, svnProject, pack ) ) )
    else:
      source.append( svnSshRoot % ( userName, '%s/tags/%s/%s/%s' % ( svnProject, svnProject, pack, packVer ) ) )
  if not source:
    DIRAC.gLogger.error( 'No packages to be included' )
    DIRAC.exit( -1 )
  DIRAC.gLogger.info( 'Creating SVN Dir:', dest )
  ret = os.system( cmd )
  if ret:
    DIRAC.exit( -1 )
  DIRAC.gLogger.info( 'Copying packages:', packageList )
  cmd = 'svn -m %s copy %s %s' % ( msg, ' '.join( source ), dest )
  ret = os.system( cmd )
  if ret:
    DIRAC.gLogger.error( 'Failed to create tag' )
finally:
  shutil.rmtree( tmpDir )

  