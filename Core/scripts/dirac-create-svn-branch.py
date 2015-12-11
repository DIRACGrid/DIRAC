#!/usr/bin/env python
########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/DIRAC/trunk/DIRAC/Core/scripts/dirac-create-svn-tag.py $
# File :    dirac-create-svn-tag
# Author :  Adria Casajus
########################################################################
"""
  Create a new branch in svn
"""
__RCSID__ = "$Id: dirac-create-svn-tag.py 31857 2010-12-10 10:04:19Z rgracian $"

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, Distribution

import sys, os, getpass

svnVersion = ""
svnPackages = 'DIRAC'
svnUsername = ""
branchPrefix = ""
branchName = ""

def setVersion( optionValue ):
  global svnVersion
  svnVersion = optionValue
  return S_OK()

def setPackage( optionValue ):
  global svnPackages
  svnPackages = optionValue
  return S_OK()

def setUsername( optionValue ):
  global svnUsername
  svnUsername = optionValue
  return S_OK()

def setDevelBranch( optionValue ):
  global branchPrefix, branchName
  branchPrefix = "dev"
  branchName = optionValue
  return S_OK()

def setPreBranch( optionValue ):
  global branchPrefix, branchName
  branchPrefix = "pre"
  branchName = optionValue
  return S_OK()

def setReleaseBranch( optionValue ):
  global branchPrefix, branchName
  branchPrefix = "rel"
  branchName = optionValue
  return S_OK()


Script.disableCS()

Script.registerSwitch( "p:", "package=", "package to branch (default = DIRAC)", setPackage )
Script.registerSwitch( "v:", "version=", "version to branch from", setVersion )
Script.registerSwitch( "u:", "username=", "svn username to use", setUsername )
Script.registerSwitch( "l:", "devel=", "Create a development branch with name", setDevelBranch )
Script.registerSwitch( "e:", "pre=", "Create a pre branch with name", setPreBranch )
Script.registerSwitch( "r:", "release=", "Create a release branch with name", setReleaseBranch )

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ...' % Script.scriptName,
                                     '  Specifying a version is mandatory' ] ) )

Script.parseCommandLine( ignoreErrors = False )

gLogger.notice( 'Executing: %s ' % ( ' '.join( sys.argv ) ) )

if not svnVersion:
  gLogger.error( "Need to specify only one version from which to spawn the branch" )
  Script.showHelp()
  sys.exit( 1 )

if not branchPrefix:
  gLogger.error( "No branch type/name defined!" )
  sys.exit( 1 )

if not branchName:
  vTuple = Distribution.parseVersionString( svnVersion )
  if not vTuple:
    gLogger.error( "%s is not a valid version" % svnVersion )
    sys.exit( 1 )
  branchName = "v%d" % vTuple[0]
  if vTuple[1]:
    branchName += "r%d" % vTuple[1]

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

  branchBasePath = '%s/branches/%s/%s' % ( svnPackage, svnPackage, branchPrefix )
  branchPath = '%s/%s' % ( branchBasePath, branchName )
  gLogger.notice( "Branch path will be %s" % ( branchPath ) )

  if packageDistribution.getDevPath().find( "https" ) == 0:
    password = getpass.getpass( "Insert password for %s: " % versionsRoot )
    packageDistribution.setSVNPassword( password )

  if svnVersion.lower() in ( 'head', 'trunk' ):
    packageDistribution.queueCopy( '%s/trunk/%s' % ( svnPackage, svnPackage ),
                                   '%s' % ( branchPath ),
                                   'Branch from trunk to %s' % branchBasePath )
    if not packageDistribution.executeCommandQueue():
      gLogger.error( 'Failed to create branch' )
    else:
      gLogger.notice( "Branch %s/%s done for %s" % ( branchPrefix, branchName, svnPackage ) )
    continue

  #HERE!

  exitStatus, data = packageDistribution.doLS( branchBasePath )
  if exitStatus:
    createdBranches = []
  else:
    createdBranches = [ v.strip( "/" ) for v in data.split( "\n" ) if v.find( "/" ) > -1 ]

  if branchName in createdBranches:
    gLogger.error( "Branch %s is already there for package %s :P" % ( branchName, svnPackage ) )
    continue

  if not svnVersion in buildCFG[ 'Versions' ].listSections():
    gLogger.error( 'Version does not exist:', svnVersion )
    gLogger.error( 'Available versions:', ', '.join( buildCFG.listSections() ) )
    continue

  versionCFG = buildCFG[ 'Versions' ][ svnVersion ]
  packageList = versionCFG.listOptions()
  gLogger.notice( "Branching packages: %s" % ", ".join( packageList ) )

  msg = 'Branch %s/%s from %s' % ( branchPrefix, branchName, svnVersion )
  packageDistribution.queueMakeDir( branchPath, msg )

  #Systems
  svnCPOrigins = []
  for pack in packageList:
    packVer = versionCFG.getOption( pack, '' )
    source = '%s/trunk/%s/%s' % ( svnPackage, svnPackage, pack )
    if packVer.lower() in ( 'trunk', '', 'head' ):
      gLogger.notice( "%s comes straight from the trunk" % pack )
    else:
      tagSource = '%s/tags/%s/%s/%s' % ( svnPackage, svnPackage, pack, packVer )
      revNum = packageDistribution.getCopyRevision( tagSource )
      source = "%s@%s" % ( source, revNum )
      gLogger.notice( "Tag %s/%s was created in rev %s" % ( pack, packVer, revNum ) )
    svnCPOrigins.append( source )

  packageDistribution.queueMultiCopy( svnCPOrigins, branchPath, msg )

  tmpFilesToDelete = []
  #Package files
  for extra in buildCFG.getOption( 'packageExtraFiles', [ '__init__.py', 'versions.cfg' ] ):
    if extra != "__init__.py":
      packageDistribution.queueCopy( '%s/trunk/%s/%s' % ( svnPackage, svnPackage, extra ),
                                     '%s/%s' % ( branchPath, extra ),
                                     msg )
    else:
      versionInitFile = packageDistribution.writeVersionToTmpInit( svnVersion )
      packageDistribution.queueImport( versionInitFile,
                                       "%s/%s" % ( branchPath, extra ),
                                       msg )
      tmpFilesToDelete.append( versionInitFile )

  if packageDistribution.emptyQueue():
    gLogger.error( 'No packages to be included' )
    exit( -1 )

  gLogger.notice( 'Copying packages: %s' % ", ".join( packageList ) )
  if not packageDistribution.executeCommandQueue():
    gLogger.error( 'Failed to create branch' )
  else:
    gLogger.notice( "Branch %s/%s done for %s" % ( branchPrefix, branchName, svnPackage ) )

  for tmpFile in tmpFilesToDelete:
    os.unlink( tmpFile )
