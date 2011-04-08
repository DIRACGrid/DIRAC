#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-distribution
# Author :  Adria Casajus
########################################################################
"""
  Create tarballs for a given DIRAC release
"""
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, File, Distribution, Platform, Subprocess, CFG

import sys, os, re, urllib2, tempfile, getpass, ConfigParser

globalDistribution = Distribution.Distribution()

projectMapping = {
  'DIRAC' : "https://github.com/DIRACGrid/DIRAC/raw/master/releases.ini"
  }

class Params:

  def __init__( self ):
    self.releasesToBuild = []
    self.projectName = 'DIRAC'
    self.debug = False
    self.externalsBuildType = [ 'client' ]
    self.buildExternals = False
    self.ignorePackages = False
    self.relini = False
    self.externalsPython = '26'
    self.destination = ""
    self.externalsLocation = ""
    self.makeJobs = 1

  def setReleases( self, optionValue ):
    self.releasesToBuild = List.fromChar( optionValue )
    return S_OK()

  def setDebug( self, optionValue ):
    self.debug = True
    return S_OK()

  def setExternalsBuildType( self, optionValue ):
    self.externalsBuildType = List.fromChar( optionValue )
    return S_OK()

  def setBuildExternals( self, optionValue ):
    self.buildExternals = True
    return S_OK()

  def setDestination( self, optionValue ):
    self.destination = optionValue
    return S_OK()

  def setPythonVersion( self, optionValue ):
    self.externalsPython = optionValue
    return S_OK()

  def setIgnorePackages( self, optionValue ):
    self.ignorePackages = True
    return S_OK()

  def setExternalsLocation( self, optionValue ):
    self.externalsLocation = optionValue
    return S_OK()

  def setMakeJobs( self, optionValue ):
    self.makeJobs = max( 1, int( optionValue ) )
    return S_OK()

  def setReleasesINI( self, optionValue ):
    self.relini = optionValue
    return S_OK()

  def registerSwitches( self ):
    Script.registerSwitch( "r:", "releases=", "releases to build (mandatory, comma separated)", cliParams.setReleases )
    Script.registerSwitch( "p:", "project=", "Project to build the release for (DIRAC by default)", cliParams.setReleases )
    Script.registerSwitch( "D:", "destination", "Destination where to build the tar files", cliParams.setDestination )
    Script.registerSwitch( "i:", "pythonVersion", "Python version to use (25/26)", cliParams.setPythonVersion )
    Script.registerSwitch( "P", "ignorePackages", "Do not make tars of python packages", cliParams.setIgnorePackages )
    Script.registerSwitch( "C:", "relcfg=", "Use <file> as the releases.ini", cliParams.setReleasesINI )
    Script.registerSwitch( "b", "buildExternals", "Force externals compilation even if already compiled", cliParams.setBuildExternals )
    Script.registerSwitch( "t:", "buildType=", "External type to build (client/server)", cliParams.setExternalsBuildType )
    Script.registerSwitch( "x:", "externalsLocation=", "Use externals location instead of downloading them", cliParams.setExternalsLocation )
    Script.registerSwitch( "j:", "makeJobs=", "Make jobs (default is 1)", cliParams.setMakeJobs )

    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )

class ReleaseConfig:

  def __init__( self ):
    self.__configs = {}
    self.__projects = []

  def loadProject( self, projectName ):
    if projectName in self.__projects:
      return True
    try:
      releasesLocation = projectMapping[ self.cliParams.projectName ]
      remoteFile = urllib2.urlopen( releasesLocation )
    except urllib2.URLError:
      gLogger.exception( "Could not open %s" % releasesLocation )
      return False
    try:
      config = ConfigParser.SafeConfigParser()
      config.readfp( remoteFile )
    except:
      gLogger.exception( "Could not parse %s" % releasesLocation )
      return False
    remoteFile.close()
    if 'config' not in config.sections():
      gLogger.error( "'config' section missing in %s" % releasesLocation )
      return False
    self.__projects.append( projectName )
    self.__configs[ projectName ] = config
    return True

  def __getOpt( self, projectName, option, section = False ):
    if not section:
      section = "Config"
    try:
      return self.__configs[ projectName ].get( section, option )
    except ConfigParser.NoOptionError:
      gLogger.error( "Missing option %s/%s for project %s" % ( section, option, projectName ) )
      return False

  def getExtensionsForProjectRelease( self, projectName, releaseVersion ):
    if not projectName in self.__projects:
      gLogger.error( "[GEFPR] Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % projectName )
      return False
    config = self.__configs[ projectName ]
    if releaseVersion not in config.sections():
      gLogger.error( "Release %s is not defined for project %s" % ( releaseVersion, projectName ) )
      return False
    #Defined extensions explicitly in the release
    extensions = self.__getOpt( projectName, "extensions", releaseVersion )
    if extensions:
      #I know I shouldn't be doing lines this complex, but I've always been a oneliner guy :P
      extensions = dict( [ [ f.strip() for f in entry.split( ":" ) ] for entry in extensions.split( "," ) if entry.strip() ] )
    else:
      #Default extensions with the same version as the release version
      extensions = self._getOpt( projectName, "defaultExtensions" )
      if extensions:
        extensions = dict( [ ( extName.strip() , releaseVersion ) for extName in extensions.split( "," ) if extName.strip() ] )
      else:
        #Ext = projectName and same version
        extensions = { projectName : releaseVersion }
    #Check projectName is in the extNames if not DIRAC
    if projectName != "DIRAC":
      for extName in extensions:
        if extName.find( projectName ) != 0:
          gLogger.error( "Extension %s does not start with the project name %s" ( extName, projectName ) )
          return False
    return extensions

  def getExtSource( self, projectName, extName ):
    if not projectName in self.__projects:
      gLogger.error( "[GETEXTSOURCE] Project %s has not been loaded. I¡'m a MEGA BUG! Please report me!" % projectName )
      return False
    extLocation = self.__getOpt( projectName, "src_%s" % extName )
    if not extLocation:
      return False
    extTpl = [ field.strip() for field in extLocation.split( "|" ) if field.strip() ]
    if extTpl == 1:
      return ( False, extTpl )
    return ( extTpl[0], extTpl[1] )


class DistributionMaker:

  def __init__( self, cliParams ):
    self.cliParams = cliParams
    self.relConf = ReleaseConfig()

  def isOK( self ):
    if not self.cliParams.releasesToBuild:
      gLogger.error( "Missing releases to build!" )
      Script.showHelp()
      sys.exit( 1 )

    if self.cliParams.projectName not in projectMapping:
      gLogger.error( "Don't know how to find releases.ini for %s" % self.cliParams.projectName )
      sys.exit( 1 )

    if not self.cliParams.destination:
      self.cliParams.destination = tempfile.mkdtemp( 'DiracDist' )
    else:
      try:
        os.makedirs( self.cliParams.destination )
      except:
        pass
    gLogger.notice( "Will generate tarballs in %s" % self.cliParams.destination )


  def loadReleases( self ):
    return self.relConf.loadProject( self.cliParams.projectName )

  def createExtTarballs( self, releaseVersion ):
    extsToTar = self.relConf.getExtensionsForProjectRelease( self.cliParams.projectName, releaseVersion )
    if not extToTar:
      return False
    for extName in extToTar:
      extVersion = extToTar[ extName ]
      dctArgs = []
      #Version
      dctArgs.append( "-v '%s" % extVersion )
      gLogger.notice( "Creating tar for %s version %s" % { extName, extVersion } )
      #Source
      extSrcTuple = self.relConf.getExtSource( projectName, extName )
      if not extSrcTuple:
        return False
      if extSrcTuple[1]:
        logMsgVCS = extSrcTuple[0]
        dctArgs.append( "-z '%s'" % extSrcTuple[0] )
      else:
        logMsgVCS = "autodiscover"
      args.append( "-u '%s'" % extSrcTuple[1] )
      gLogger.info( "Sources will be retrieved from %s (%s)" % ( extSrcTuple[1], logMsgVCS ) )
      #Tar destination
      dctArgs.append( "-D '%s'" % self.cliParams.destination )
      #Script location discovery
      scriptName = os.path.join( os.path.dirname( __FILE__ ), "dirac-create-distribution-tarball.py" )
      cmd = "'%s' %s" % ( scriptName, " ".join( dctArgs ) )
      gLogger.verbose( "Executing %s" % cmd )
      if os.system( cmd ):
        gLogger.error( "Failed creating tarball for extension %s. Aborting" % extName )
        return False
      gLogger.info( "Tarball for %s version %s created" % ( extName, extVersion ) )
    return True



if __name__ == "__main__":
  cliParams = Params()
  Script.disableCS()
  Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )

























if not cliParams.releasesToBuild:
  Script.showHelp()


##
#Helper functions
##

def autoTarPackages( mainCFG, targetDir ):
  global cliParams

  releasesCFG = mainCFG[ 'Releases' ]
  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )
  for releaseVersion in cliParams.releasesToBuild:
    releaseTMPPath = os.path.join( targetDir, releaseVersion )
    gLogger.notice( "Getting %s release to %s" % ( releaseVersion, targetDir ) )
    os.mkdir( releaseTMPPath )
    for package in releasesCFG[ releaseVersion ].listOptions():
      if package not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( package, "" )
      versionPath = getVersionPath( package, version )
      pkgSVNPath = globalDistribution.getSVNPathForPackage( package, versionPath )
      pkgHDPath = os.path.join( releaseTMPPath, package )
      gLogger.notice( " Getting %s" % pkgSVNPath )
      svnCmd = "svn export '%s' '%s'" % ( pkgSVNPath, pkgHDPath )
      result = Subprocess.shellCall( 900, svnCmd )
      if not result[ 'OK' ]:
        gLogger.error( "Error while retrieving %s package" % package, result[ 'Message' ] )
        sys.exit( 1 )
      exitStatus, stdData, errData = result[ 'Value' ]
      if exitStatus:
        gLogger.error( "Error while retrieving %s package" % package, "\n".join( [ stdData, errData ] ) )
        sys.exit( 1 )
      gLogger.notice( "Taring %s..." % package )
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
      gLogger.notice( "Externals is not defined for release %s" % releaseVersion )
      continue
    for externalType in cliParams.externalsBuildType:
      requestedExternals = ( externalType, externalsVersion, platform, 'python%s' % cliParams.externalsPython )
      requestedExternalsString = "-".join( list( requestedExternals ) )
      gLogger.notice( "Trying to compile %s externals..." % requestedExternalsString )
      if not cliParams.forceExternals and requestedExternals in availableExternals:
        gLogger.notice( "Externals %s is already compiled, skipping..." % ( requestedExternalsString ) )
        continue
      compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals" )
      if not os.path.isfile( compileScript ):
        compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals.py" )
      compileTarget = os.path.join( targetDir, platform )
      cmdArgs = []
      cmdArgs.append( "-D '%s'" % compileTarget )
      cmdArgs.append( "-t '%s'" % externalType )
      cmdArgs.append( "-v '%s'" % externalsVersion )
      cmdArgs.append( "-i '%s'" % cliParams.externalsPython )
      if cliParams.externalsLocation:
        cmdArgs.append( "-e '%s'" % cliParams.externalsLocation )
      if cliParams.makeJobs:
        cmdArgs.append( "-j '%s'" % cliParams.makeJobs )
      compileCmd = "%s %s" % ( compileScript, " ".join( cmdArgs ) )
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

if cliParams.relcfg:
  try:
    mainCFG = CFG.CFG().loadFromFile( cliParams.relcfg )
  except IOError, e:
    gLogger.fatal( "Can not open %s: %s" % ( cliParams.relcfg, e ) )
    sys.exit( 1 )
else:
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
gLogger.notice( "Will generate tarballs in %s" % targetPath )

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
  gLogger.notice( "No packages were tared" )
else:
  for release in cliParams.releasesToBuild:
    if not mainCFG.writeToFile( os.path.join( targetPath, "releases-%s.cfg" % release ) ):
      gLogger.error( "Could not write releases.cfg file to %s" % targetPath )
      sys.exit( 1 )
  gLogger.notice( "Everything seems ok" )
  gLogger.notice( "Please upload the tarballs by executing:" )
  gLogger.notice( "( cd %s ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'" % targetPath )
