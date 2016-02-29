#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-create-svn-tag
# Author :  Adria Casajus
########################################################################
"""
  Tag a new release in SVN
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, Distribution

import sys, os, tempfile, shutil, getpass

svnPackages = 'DIRAC'
svnVersions = ""
svnUsername = ""
onlyReleaseNotes = False

def setVersion( optionValue ):
  global svnVersions
  svnVersions = optionValue
  return S_OK()

def setPackage( optionValue ):
  global svnPackages
  svnPackages = optionValue
  return S_OK()

def setUsername( optionValue ):
  global svnUsername
  svnUsername = optionValue
  return S_OK()

def setOnlyReleaseNotes( optionValue ):
  global onlyReleaseNotes
  gLogger.notice( "Only updating release notes!" )
  onlyReleaseNotes = True
  return S_OK()

Script.disableCS()

Script.registerSwitch( "v:", "version=", "versions to tag comma separated (mandatory)", setVersion )
Script.registerSwitch( "p:", "package=", "packages to tag comma separated (default = DIRAC)", setPackage )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )
Script.registerSwitch( "n", "releaseNotes", "Only refresh release notes", setOnlyReleaseNotes )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName ] ) )

Script.parseCommandLine( ignoreErrors = False )

gLogger.notice( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )

if not svnVersions:
  Script.showHelp()

def generateAndUploadReleaseNotes( packageDistribution, svnPath, versionReleased ):
    tmpDir = tempfile.mkdtemp()
    packageName = packageDistribution.getPackageName()
    gLogger.notice( "Generating release notes for %s under %s" % ( packageName, tmpDir ) )
    for suffix, singleVersion in ( ( "history", False ), ( "notes", True ) ):
      gLogger.notice( "Generating %s rst" % suffix )
      rstHistory = os.path.join( tmpDir, "release%s.rst" % suffix )
      htmlHistory = os.path.join( tmpDir, "release%s.html" % suffix )
      Distribution.generateReleaseNotes( packageName, rstHistory, versionReleased, singleVersion )
      try:
        Distribution.generateHTMLReleaseNotesFromRST( rstHistory, htmlHistory )
      except Exception, x:
        print "Failed to generate html version of the notes:", str( x )
      # Attempt to generate pdf as well  
      os.system( 'rst2pdf %s' % rstHistory )

    packageDistribution.queueImport( tmpDir, svnPath, 'Release notes for version %s' % versionReleased )
    if not packageDistribution.executeCommandQueue():
      gLogger.error( "Could not upload release notes" )
      sys.exit( 1 )

    os.system( "rm -rf '%s'" % tmpDir )
    gLogger.notice( "Release notes committed" )

##
#End of helper functions
##

#Get username
if not svnUsername:
  svnUsername = getpass.getuser()
gLogger.notice( "Using %s as username" % svnUsername )

#Start the magic!
for svnPackage in List.fromChar( svnPackages ):

  packageDistribution = Distribution.Distribution( svnPackage )
  packageDistribution.setSVNUser( svnUsername )
  buildCFG = packageDistribution.getVersionsCFG()

  if 'Versions' not in buildCFG.listSections():
    gLogger.error( "versions.cfg file in package %s does not contain a Versions top section" % svnPackage )
    continue

  versionsRoot = '%s/tags/%s' % ( svnPackage, svnPackage )

  if packageDistribution.getDevPath().find( "https" ) == 0:
    password = getpass.getpass( "Insert password for %s: " % versionsRoot )
    packageDistribution.setSVNPassword( password )

  exitStatus, data = packageDistribution.doLS( '%s/tags/%s' % ( svnPackage, svnPackage ) )
  if exitStatus:
    createdVersions = []
  else:
    createdVersions = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

  for svnVersion in List.fromChar( svnVersions ):

    gLogger.notice( "Start tags for package %s version %s " % ( svnPackage, svnVersion ) )

    if svnVersion in createdVersions:
      if not onlyReleaseNotes:
        gLogger.error( "Version %s is already there for package %s :P" % ( svnVersion, svnPackage ) )
        continue
      else:
        gLogger.notice( "Generating release notes for version %s" % svnVersion )
        generateAndUploadReleaseNotes( packageDistribution,
                                       "%s/%s" % ( versionsRoot, svnVersion ),
                                       svnVersion )
        continue

    if onlyReleaseNotes:
      gLogger.error( "Version %s is not tagged for %s. Can't refresh the release notes" % ( svnVersion, svnPackage ) )
      continue

    if not svnVersion in buildCFG[ 'Versions' ].listSections():
      gLogger.error( 'Version does not exist:', svnVersion )
      gLogger.error( 'Available versions:', ', '.join( buildCFG[ 'Versions' ].listSections() ) )
      continue

    versionCFG = buildCFG[ 'Versions' ][svnVersion]
    packageList = versionCFG.listOptions()
    gLogger.notice( "Tagging packages: %s" % ", ".join( packageList ) )
    msg = 'Release %s' % svnVersion
    versionPath = "%s/%s" % ( versionsRoot, svnVersion )
    packageDistribution.queueMakeDir( versionPath, msg )
    tmpFilesToDelete = []
    for extra in buildCFG.getOption( 'packageExtraFiles', [ '__init__.py', 'versions.cfg' ] ):
      if extra != "__init__.py":
        packageDistribution.queueCopy( '%s/trunk/%s/%s' % ( svnPackage, svnPackage, extra ),
                                       '%s/%s' % ( versionPath, extra ),
                                       msg )
      else:
        versionInitFile = packageDistribution.writeVersionToTmpInit( svnVersion )
        packageDistribution.queueImport( versionInitFile,
                                         "%s/%s" % ( versionPath, extra ),
                                         msg )
        tmpFilesToDelete.append( versionInitFile )

    for pack in packageList:
      packVer = versionCFG.getOption( pack, '' )
      if packVer.lower() in ( 'trunk', '', 'head' ):
        source = '%s/trunk/%s/%s' % ( svnPackage, svnPackage, pack )
      else:
        source = '%s/tags/%s/%s/%s' % ( svnPackage, svnPackage, pack, packVer )
      packageDistribution.queueCopy( source, '%s/%s' % ( versionPath, pack ), msg )
    if packageDistribution.emptyQueue():
      gLogger.error( 'No packages to be included' )
      exit( -1 )
    gLogger.notice( 'Copying packages: %s' % ", ".join( packageList ) )
    if not packageDistribution.executeCommandQueue():
      gLogger.error( 'Failed to create tag' )

    for tmpFile in tmpFilesToDelete:
      os.unlink( tmpFile )

    #Generate release notes for version
    generateAndUploadReleaseNotes( packageDistribution, versionPath, svnVersion )

