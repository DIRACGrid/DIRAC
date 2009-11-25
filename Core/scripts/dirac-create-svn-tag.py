#!/usr/bin/env python
# $HeadURL$
"""
Tag a new release in SVN
"""
__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, CFG

import sys, os, tempfile, shutil, getpass, subprocess

svnProjects = 'DIRAC'
svnVersions = ""
svnUsername = ""

svnSshRoot    = "svn+ssh://%s@svn.cern.ch/reps/dirac/%s"

def setVersion( optionValue ):
  global svnVersions
  svnVersions = optionValue
  return S_OK()

def setProject( optionValue ):
  global svnProjects
  svnProjects = optionValue
  return S_OK()

def setUsername( optionValue ):
  global svnUsername
  svnUsername = optionValue
  return S_OK()

Script.disableCS()

Script.registerSwitch( "v:", "version=", "versions to tag comma separated (mandatory)", setVersion )
Script.registerSwitch( "p:", "project=", "projects to tag comma separated (default = DIRAC)", setProject )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )

Script.parseCommandLine( ignoreErrors = False )

gLogger.info( 'Executing: %s ' % ( ' '.join(sys.argv) ) )

def usage():
  Script.showHelp()
  exit(2)
  
if not svnVersions:
  usage()
  
def execAndGetOutput( cmd ):
  p = subprocess.Popen( cmd, 
                        shell = True, stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE, close_fds = True )
  stdData = p.stdout.read()
  p.wait()
  return ( p.returncode, stdData )

def getSVNFileContents( projectName, filePath ):
  import urllib2, stat
  gLogger.info( "Reading %s/trunk/%s" % ( projectName, filePath ) ) 
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s/trunk/%s?op=dl&rev=0" % ( projectName, filePath )
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s/trunk/%s' % ( projectName, filePath )
  for remoteLocation in ( viewSVNLocation, anonymousLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      gLogger.exception()
      continue
    remoteData = remoteFile.read()
    remoteFile.close()      
    if remoteData:
      return remoteData
  #Web cat failed. Try directly with svn
  exitStatus, remoteData = execAndGetOutput( "svn cat 'http://svnweb.cern.ch/guest/dirac/%s/trunk/%s'" % ( projectName, filePath ) )
  if exitStatus:
    print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % fileName
    sys.exit(1)
  return remoteData

##
#End of helper functions
##

#Get username
if not svnUsername:
  svnUsername = raw_input( "SVN User Name[%s]: " % getpass.getuser() )
  if not svnUsername:
    svnUsername = getpass.getuser()

#Start the magic!
for svnProject in List.fromChar( svnProjects ):
    
  versionsData = getSVNFileContents( svnProject, "%s/versions.cfg" % svnProject )
  
  buildCFG = CFG.CFG().loadFromBuffer( versionsData )
  
  if 'Versions' not in buildCFG.listSections():
    gLogger.error( "versions.cfg file in project %s does not contain a Versions top section" % svnProject )
    continue

  versionsRoot = svnSshRoot % ( svnUsername, '%s/tags/%s' % ( svnProject, svnProject ) )
  exitStatus, data = execAndGetOutput( "svn ls '%s'" % ( versionsRoot ) )
  if exitStatus:
    createdVersions = []
  else:
    createdVersions = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]
  

  for svnVersion in List.fromChar( svnVersions ):
    
    gLogger.info( "Start tagging for project %s version %s " %  ( svnProject, svnVersion ) )
    
    if "%s_%s" % ( svnProject, svnVersion ) in createdVersions:
      gLogger.error( "Version %s is already there for package %s :P" % ( svnVersion, svnProject ) )
      continue
    
    if not svnVersion in buildCFG[ 'Versions' ].listSections():
      gLogger.error( 'Version does not exist:', svnVersion )
      gLogger.error( 'Available versions:', ', '.join( buildCFG.listSections() ) )
      continue
  
    versionCFG = buildCFG[ 'Versions' ][svnVersion]
    packageList = versionCFG.listOptions()
    gLogger.info( "Tagging packages: %s" % ", ".join( packageList ) )
    msg = '"Release %s"' % svnVersion
    versionPath = svnSshRoot % ( svnUsername, '%s/tags/%s/%s_%s' % ( svnProject, svnProject, svnProject, svnVersion ) )
    mkdirCmd = "svn --parents -m %s mkdir '%s'" % ( msg, versionPath )
    cpCmds = []
    for extra in buildCFG.getOption( 'packageExtraFiles', ['__init__.py', 'versions.cfg'] ):
      source = svnSshRoot % ( svnUsername, '%s/trunk/%s/%s'  % ( svnProject, svnProject, extra ) )
      cpCmds.append( "svn -m '%s' copy '%s' '%s/%s'" % ( msg, source, versionPath, extra ) )
    for pack in packageList:
      packVer = versionCFG.getOption(pack,'')
      if packVer in ['trunk', '', 'HEAD']:
        source = svnSshRoot % ( svnUsername, '%s/trunk/%s/%s'  % ( svnProject, svnProject, pack ) )
      else:
        source = svnSshRoot % ( svnUsername, '%s/tags/%s/%s/%s' % ( svnProject, svnProject, pack, packVer ) )
      cpCmds.append( "svn -m '%s' copy '%s' '%s/%s'" % ( msg, source, versionPath, pack ) )
    if not cpCmds:
      gLogger.error( 'No packages to be included' )
      exit( -1 )
    gLogger.info( 'Creating SVN Dir:', versionPath )
    ret = os.system( mkdirCmd )
    if ret:
      exit( -1 )
    gLogger.info( 'Copying packages: %s' % ", ".join( packageList ) )
    for cpCmd in cpCmds:
      ret = os.system( cpCmd )
      if ret:
        gLogger.error( 'Failed to create tag' )


  