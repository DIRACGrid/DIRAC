#!/usr/bin/env python
# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, File, Distribution, Platform, Subprocess

import sys, os, re, urllib2, tempfile, getpass

globalDistribution = Distribution.Distribution()

class Params:

  def __init__( self ):
    self.releasesToBuild = []
    self.userName = ""
    self.forceSVNLinks = False
    self.ignoreSVNLinks = False
    self.debug = False
    self.externalsBuildType = [ 'client' ]
    self.forceExternals = False
    self.ignoreExternals = False
    self.ignorePackages = False
    self.externalsPython = '25'
    self.destination = ""

  def setReleases( self, optionValue ):
    self.releasesToBuild = List.fromChar( optionValue )
    return S_OK()

  def setUserName( self, optionValue ):
    self.userName = optionValue
    globalDistribution.setSVNUser( optionValue )
    return S_OK()

  def setForceSVNLink( self, optionValue ):
    self.forceSVNLinks = True
    return S_OK()

  def setIgnoreSVNLink( self, optionValue ):
    self.ignoreSVNLinks = True
    return S_OK()

  def setDebug( self, optionValue ):
    self.debug = True
    return S_OK()

  def setExternalsBuildType( self, optionValue ):
    self.externalsBuildType = List.fromChar( optionValue )
    return S_OK()

  def setForceExternals( self, optionValue ):
    self.forceExternals = True
    return S_OK()

  def setIgnoreExternals( self, optionValue ):
    self.ignoreExternals = True
    return S_OK()

  def setDestination( self, optionValue ):
    self.destination = optionValue
    return S_OK()

  def setPythonVersion( self, optionValue ):
    self.pythonVersion = optionValue
    return S_OK()

  def setIgnorePackages( self, optionValue ):
    self.ignorePackages = True
    return S_OK()

cliParams = Params()

Script.disableCS()
Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
Script.registerSwitch( "r:", "releases=", "reseases to build (mandatory, comma separated)", cliParams.setReleases )
Script.registerSwitch( "u:", "username=", "svn username to use", cliParams.setUserName )
Script.registerSwitch( "l", "forceSVNLinks", "Redo the svn links even if the release exists", cliParams.setForceSVNLink )
Script.registerSwitch( "L", "ignoreSVNLinks", "Do not do the svn links for the release", cliParams.setIgnoreSVNLink )
Script.registerSwitch( "D", "debug", "Debug mode", cliParams.setDebug )
Script.registerSwitch( "t:", "buildType=", "External type to build (client/server)", cliParams.setExternalsBuildType )
Script.registerSwitch( "e", "forceExternals", "Force externals compilation even if already compiled", cliParams.setForceExternals )
Script.registerSwitch( "E", "ignoreExternals", "Do not compile externals", cliParams.setIgnoreExternals )
Script.registerSwitch( "d:", "destination", "Destination where to build the tar files", cliParams.setDestination )
Script.registerSwitch( "i:", "pythonVersion", "Python version to use (24/25)", cliParams.setPythonVersion )
Script.registerSwitch( "P", "ignorePackages", "Do not make tars of python packages", cliParams.setIgnorePackages )

Script.parseCommandLine( ignoreErrors = False )

def usage():
  Script.showHelp()
  exit( 2 )

if not cliParams.releasesToBuild:
  usage()
  exit( 2 )

##
#Helper functions
##

def getVersionPath( package, version ):
  if version.strip().lower() in ( "trunk", "", "head" ):
    version = "%s/trunk/%s" % ( package, package )
  else:
    version = "%s/tags/%s/%s" % ( package, package, version )
  return version

def tagSVNReleases( mainCFG, taggedReleases ):
  global cliParams

  releasesCFG = mainCFG[ 'Releases' ]

  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )

  for releaseVersion in cliParams.releasesToBuild:
    if not cliParams.forceSVNLinks and releaseVersion in taggedReleases:
      gLogger.info( "Release %s is already tagged, skipping" % releaseVersion )
      continue
    if releaseVersion not in releasesCFG.listSections():
      gLogger.error( "Release %s not defined in releases.cfg" % releaseVersion )
      continue
    releaseSVNPath = "tags/%s" % releaseVersion
    if releaseVersion not in taggedReleases:
      gLogger.info( "Creating global release dir %s" % releaseVersion )
      if not globalDistribution.doMakeDir( releaseSVNPath, "Release %s" % releaseVersion ):
        gLogger.error( "Error while generating release tag" )
        sys.exit( 1 )
      exitStatus, stdData, errData = result[ 'Value' ]
      if exitStatus:
        gLogger.error( "Error while generating release tag", "\n".join( [ stdData, errData ] ) )
        sys.exit( 1 )
    svnLinks = []
    packages = releasesCFG[ releaseVersion ].listOptions()
    packages.sort()
    for p in packages:
      if p not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( p, "" )
      versionPath = getVersionPath( p, version )
      svnLinks.append( "%s %s" % ( p, globalDistribution.getSVNPathForPackage( versionPath ) ) )
    tmpPath = tempfile.mkdtemp()
    fd = open( os.path.join( tmpPath, "extProp" ), "wb" )
    fd.write( "%s\n" % "\n".join( svnLinks ) )
    fd.close()
    checkOutPath = os.path.join( tmpPath, "svnco" )
    releasesFilePath = os.path.join( tmpPath, "releases.cfg" )
    releasesTmpFilePath = os.path.join( tmpPath, "releases.cfg" )
    releasesFinalFilePath = os.path.join( checkOutPath, "releases.cfg" )
    if not mainCFG.writeToFile( releasesTmpFilePath ):
      gLogger.error( "Could not write releases.cfg file to %s" % releasesTmpFilePath )
      sys.exit( 1 )
    if not glocalDistribution.doCheckout( releaseSVNPath, checkOutPath ):
      gLogger.error( "Could not check out %s to  %s" % ( releaseSVNPath, checkOutPath ) )
      sys.exit( 1 )
    svnCmds = []
    #svnCmds.append( "svn co -N '%s' '%s'" % ( releaseSVNPath, checkOutPath ) )
    svnCmds.append( "mv '%s' '%s'" % ( releasesTmpFilePath, releasesFinalFilePath ) )
    svnCmds.append( "svn add '%s'" % releasesFinalFilePath )
    svnCmds.append( "svn propset svn:externals -F '%s/extProp' '%s'" % ( tmpPath, checkOutPath ) )
    #svnCmds.append( "svn ci -m 'Release %s svn:externals' '%s'" % ( releaseVersion, checkOutPath ) )
    gLogger.info( "Creating svn:externals in %s..." % releaseVersion )
    for cmd in svnCmds:
      result = Subprocess.shellCall( 900, cmd )
      if not result[ 'OK' ]:
        gLogger.error( "Error while adding externals to tag", result[ 'Message' ] )
        sys.exit( 1 )
      exitStatus, stdData, errData = result[ 'Value' ]
      if exitStatus:
        gLogger.error( "Error while adding externals to tag", "\n".join( [ stdData, errData ] ) )
        sys.exit( 1 )
    if not globalDistribution.doCommit( checkOutPath, "Release %s svn:externals" % releaseVersion ):
      gLogger.error( "Could not commit %s to  %s" % ( releaseSVNPath, checkOutPath ) )
      sys.exit( 1 )
    os.system( "rm -rf '%s'" % tmpPath )

def autoTarPackages( mainCFG, targetDir ):
  global cliParams

  releasesCFG = mainCFG[ 'Releases' ]
  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )
  for releaseVersion in cliParams.releasesToBuild:
    releaseTMPPath = os.path.join( targetDir, releaseVersion )
    gLogger.info( "Getting %s release to %s" % ( releaseVersion, targetDir ) )
    os.mkdir( releaseTMPPath )
    for package in releasesCFG[ releaseVersion ].listOptions():
      if package not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( package, "" )
      versionPath = getVersionPath( package, version )
      pkgSVNPath = globalDistribution.getSVNPathForPackage( versionPath )
      pkgHDPath = os.path.join( releaseTMPPath, package )
      gLogger.info( " Getting %s" % pkgSVNPath )
      svnCmd = "svn export '%s' '%s'" % ( pkgSVNPath, pkgHDPath )
      result = Subprocess.shellCall( 900, svnCmd )
      if not result[ 'OK' ]:
        gLogger.error( "Error while retrieving %s package" % package, result[ 'Message' ] )
        sys.exit( 1 )
      exitStatus, stdData, errData = result[ 'Value' ]
      if exitStatus:
        gLogger.error( "Error while retrieving %s package" % package, "\n".join( [ stdData, errData ] ) )
        sys.exit( 1 )
      gLogger.info( "Taring %s..." % package )
      tarfilePath = os.path.join( targetDir, "%s-%s.tar.gz" % ( package, version ) )
      result = Distribution.createTarball( tarfilePath, pkgHDPath )
      if not result[ 'OK' ]:
        gLogger.error( "Could not generate tarball for package %s" % package, result[ 'Error' ] )
        sys.exit( 1 )
      #Remove package dir
      os.system( "rm -rf '%s'" % os.path.join( targetDir, package ) )

def getAvailableExternals():
  packagesURL = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/tars/tars.list"
  try:
    remoteFile = urllib2.urlopen( packagesURL )
  except urllib2.URLError:
    gLogger.exception()
    return []
  remoteData = remoteFile.read()
  remoteFile.close()
  versionRE = re.compile( "Externals-([a-zA-Z]*)-([a-zA-Z0-9]*(?:-pre[0-9]+)*)-(.*)-(python[0-9]+)\.tar\.gz" )
  availableExternals = []
  for line in remoteData.split( "\n" ):
    res = versionRE.search( line )
    if res:
      availableExternals.append( res.groups() )
  return availableExternals

def tarExternals( mainCFG, targetDir ):
  global cliParams

  releasesCFG = mainCFG[ 'Releases' ]
  platform = Platform.getPlatformString()
  availableExternals = getAvailableExternals()
  for releaseVersion in cliParams.releasesToBuild:
    externalsVersion = releasesCFG[ releaseVersion ].getOption( "Externals", "" )
    if not externalsVersion:
      gLogger.info( "Externals is not defined for release %s" % releaseVersion )
      continue
    for externalType in cliParams.externalsBuildType:
      requestedExternals = ( externalType, externalsVersion, platform, 'python%s' % cliParams.externalsPython )
      requestedExternalsString = "-".join( list( requestedExternals ) )
      gLogger.info( "Trying to compile %s externals..." % requestedExternalsString )
      if not cliParams.forceExternals and requestedExternals in availableExternals:
        gLogger.info( "Externals %s is already compiled, skipping..." % ( requestedExternalsString ) )
        continue
      compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals.py" )
      compileTarget = os.path.join( targetDir, platform )
      compileCmd = "%s -d '%s' -t '%s' -v '%s' -i '%s'" % ( compileScript, compileTarget,
                                                            externalType,
                                                            externalsVersion, cliParams.externalsPython )
      gLogger.debug( compileCmd )
      if os.system( compileCmd ):
        gLogger.error( "Error while compiling externals!" )
        sys.exit( 1 )
      tarfilePath = os.path.join( targetDir, "Externals-%s.tar.gz" % ( requestedExternalsString ) )
      result = Distribution.createTarball( tarfilePath,
                                           compileTarget,
                                           os.path.join( targetDir, "mysql" ) )
      if not result[ 'OK' ]:
        gLogger.error( "Could not generate tarball for package %s" % package, result[ 'Error' ] )
        sys.exit( 1 )
      os.system( "rm -rf '%s'" % compileTarget )

mainCFG = globalDistribution.loadCFGFromRepository( "/trunk/releases.cfg" )
if 'Releases' not in mainCFG.listSections():
  gLogger.fatal( "releases.cfg file does not have a Releases section" )
  exit( 1 )
releasesCFG = mainCFG[ 'Releases' ]

for release in cliParams.releasesToBuild:
  if release not in releasesCFG.listSections():
    gLogger.error( "Release %s is not defined in the releases.cfg" % release )
    sys.exit( 1 )

if not cliParams.destination:
  targetPath = tempfile.mkdtemp( 'DiracDist' )
else:
  targetPath = cliParams.destination
  try:
    os.makedirs( targetPath )
  except:
    pass
gLogger.info( "Will generate tarballs in %s" % targetPath )

doneSomeTars = False

if not cliParams.ignoreSVNLinks:
  taggedReleases = globalDistribution.getRepositoryVersions()
  tagSVNReleases( mainCFG, taggedReleases )
  doneSomeTars = True

if not cliParams.ignoreExternals:
  tarExternals( mainCFG, targetPath )
  doneSomeTars = True

if not cliParams.ignorePackages:
  autoTarPackages( mainCFG, targetPath )
  doneSomeTars = True

if not doneSomeTars:
  gLogger.info( "No packages were tared" )
else:
  for release in cliParams.releasesToBuild:
    if not mainCFG.writeToFile( os.path.join( targetPath, "releases-%s.cfg" % release ) ):
      gLogger.error( "Could not write releases.cfg file to %s" % targetPath )
      sys.exit( 1 )
  gLogger.info( "Everything seems ok" )
  gLogger.info( "Please upload the tarballs by executing:" )
  gLogger.info( "( cd %s ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'" % targetPath )
