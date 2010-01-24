#!/usr/bin/env python
# $HeadURL$
"""
Tag a new release in SVN
"""
__RCSID__ = "$Id$"
from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, Distribution

import sys, os, tempfile, shutil, getpass

svnProjects = 'DIRAC'
svnVersions = ""
svnUsername = ""
onlyReleaseNotes = False

svnSshRoot = "svn+ssh://%s@svn.cern.ch/reps/dirac/%s"

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

def setOnlyReleaseNotes( optionValue ):
  global onlyReleaseNotes
  gLogger.info( "Only updating release notes!" )
  onlyReleaseNotes = True
  return S_OK()

Script.disableCS()

Script.registerSwitch( "v:", "version=", "versions to tag comma separated (mandatory)", setVersion )
Script.registerSwitch( "p:", "project=", "projects to tag comma separated (default = DIRAC)", setProject )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )
Script.registerSwitch( "n", "releaseNotes", "Only refresh release notes", setOnlyReleaseNotes )

Script.parseCommandLine( ignoreErrors = False )

gLogger.info( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )

def usage():
  Script.showHelp()
  exit( 2 )

if not svnVersions:
  usage()

def generateAndUploadReleaseNotes( projectName, svnPath, versionReleased ):
    tmpDir = tempfile.mkdtemp()
    gLogger.info( "Generating release notes for %s under %s" % ( projectName, tmpDir ) )
    filesToUpload = []
    for suffix, singleVersion in ( ( "history", False ), ( "notes", True ) ):
      gLogger.info( "Generating %s rst" % suffix )
      rstHistory = os.path.join( tmpDir, "release%s.rst" % suffix )
      Distribution.generateReleaseNotes( projectName, rstHistory, versionReleased, singleVersion )
      filesToUpload.append( rstHistory )

    svnCmd = "svn import '%s' '%s' -m 'Release notes for version %s'" % ( tmpDir, svnPath, versionReleased )
    if os.system( svnCmd ):
      gLogger.error( "Could not upload release notes" )
      sys.exit( 1 )

    os.system( "rm -rf '%s'" % tmpDir )
    gLogger.info( "Release notes committed" )

##
#End of helper functions
##

#Get username
if not svnUsername:
  svnUsername = getpass.getuser()
gLogger.info( "Using %s as username" % svnUsername )

#Start the magic!
for svnProject in List.fromChar( svnProjects ):

  buildCFG = Distribution.loadCFGFromRepository( "%s/trunk/%s/versions.cfg" % ( svnProject, svnProject ) )

  if 'Versions' not in buildCFG.listSections():
    gLogger.error( "versions.cfg file in project %s does not contain a Versions top section" % svnProject )
    continue

  versionsRoot = svnSshRoot % ( svnUsername, '%s/tags/%s' % ( svnProject, svnProject ) )
  exitStatus, data = Distribution.execAndGetOutput( "svn ls '%s'" % ( versionsRoot ) )
  if exitStatus:
    createdVersions = []
  else:
    createdVersions = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

  for svnVersion in List.fromChar( svnVersions ):

    gLogger.info( "Start tagging for project %s version %s " % ( svnProject, svnVersion ) )

    if svnVersion in createdVersions:
      if not onlyReleaseNotes:
        gLogger.error( "Version %s is already there for package %s :P" % ( svnVersion, svnProject ) )
        continue
      else:
        gLogger.info( "Generating release notes for version %s" % svnVersion )
        generateAndUploadReleaseNotes( svnProject,
                                       "%s/%s" % ( versionsRoot, svnVersion ),
                                       svnVersion )
        continue

    if onlyReleaseNotes:
      gLogger.error( "Version %s is not tagged for %s. Can't refresh the release notes" % ( svnVersion, svnProject ) )
      continue

    if not svnVersion in buildCFG[ 'Versions' ].listSections():
      gLogger.error( 'Version does not exist:', svnVersion )
      gLogger.error( 'Available versions:', ', '.join( buildCFG.listSections() ) )
      continue

    versionCFG = buildCFG[ 'Versions' ][svnVersion]
    packageList = versionCFG.listOptions()
    gLogger.info( "Tagging packages: %s" % ", ".join( packageList ) )
    msg = '"Release %s"' % svnVersion
    versionPath = "%s/%s" % ( versionsRoot, svnVersion )
    mkdirCmd = "svn -m %s mkdir '%s'" % ( msg, versionPath )
    cpCmds = []
    for extra in buildCFG.getOption( 'packageExtraFiles', [ '__init__.py', 'versions.cfg' ] ):
      source = svnSshRoot % ( svnUsername, '%s/trunk/%s/%s' % ( svnProject, svnProject, extra ) )
      cpCmds.append( "svn -m '%s' copy '%s' '%s/%s'" % ( msg, source, versionPath, extra ) )
    for pack in packageList:
      packVer = versionCFG.getOption( pack, '' )
      if packVer.lower() in ( 'trunk', '', 'head' ):
        source = svnSshRoot % ( svnUsername, '%s/trunk/%s/%s' % ( svnProject, svnProject, pack ) )
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

    #Generate release notes for version
    generateAndUploadReleaseNotes( svnProject, versionPath, svnVersion )

