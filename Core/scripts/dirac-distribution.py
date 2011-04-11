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

import sys, os, re, urllib2, tempfile, getpass, imp

globalDistribution = Distribution.Distribution()

projectMapping = {
  'DIRAC' : "https://github.com/DIRACGrid/DIRAC/raw/master/releases.ini"
  }

###
# Load release manager from dirac-install
##
diracInstallLocation = os.path.join( os.path.dirname( __file__ ), "dirac-install.py" )
try:
  diFile = open( diracInstallLocation, "r" )
  ReleaseConfig = imp.load_module( "ReleaseConfig", diFile, diracInstallLocation, ( "", "r", imp.PY_SOURCE ) )
  diFile.close()
except Exception, excp:
  raise
  gLogger.fatal( "Cannot find dirac-install! Aborting (%s)" % str( excp ) )
  sys.exit( 1 )
##END OF LOAD


class Params:

  def __init__( self ):
    self.releasesToBuild = []
    self.projectName = 'DIRAC'
    self.debug = False
    self.externalsBuildType = [ 'client' ]
    self.ignoreExternals = False
    self.forceExternals = False
    self.ignorePackages = False
    self.relcfg = False
    self.externalsPython = '26'
    self.destination = ""
    self.externalsLocation = ""
    self.makeJobs = 1

  def setReleases( self, optionValue ):
    self.releasesToBuild = List.fromChar( optionValue )
    return S_OK()

  def setProject( self, optionValue ):
    self.projectName = optionValue
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

  def setReleasesCFG( self, optionValue ):
    self.relcfg = optionValue
    return S_OK()

  def registerSwitches( self ):
    Script.registerSwitch( "r:", "releases=", "releases to build (mandatory, comma separated)", cliParams.setReleases )
    Script.registerSwitch( "t:", "project=", "Project to build the release for (DIRAC by default)", cliParams.setProject )
    Script.registerSwitch( "D:", "destination", "Destination where to build the tar files", cliParams.setDestination )
    Script.registerSwitch( "i:", "pythonVersion", "Python version to use (25/26)", cliParams.setPythonVersion )
    Script.registerSwitch( "P", "ignorePackages", "Do not make tars of python packages", cliParams.setIgnorePackages )
    Script.registerSwitch( "C:", "relcfg=", "Use <file> as the releases.cfg", cliParams.setReleasesCFG )
    Script.registerSwitch( "b", "buildExternals", "Force externals compilation even if already compiled", cliParams.setForceExternals )
    Script.registerSwitch( "B", "ignoreExternals", "Skip externals compilation", cliParams.setIgnoreExternals )
    Script.registerSwitch( "t:", "buildType=", "External type to build (client/server)", cliParams.setExternalsBuildType )
    Script.registerSwitch( "x:", "externalsLocation=", "Use externals location instead of downloading them", cliParams.setExternalsLocation )
    Script.registerSwitch( "j:", "makeJobs=", "Make jobs (default is 1)", cliParams.setMakeJobs )

    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )


class DistributionMaker:

  def __init__( self, cliParams ):
    self.cliParams = cliParams
    self.relConf = ReleaseConfig()

  def isOK( self ):
    if not self.cliParams.releasesToBuild:
      gLogger.error( "Missing releases to build!" )
      Script.showHelp()
      return False

    if self.cliParams.projectName not in projectMapping:
      gLogger.error( "Don't know how to find releases.ini for %s" % self.cliParams.projectName )
      return False

    if not self.cliParams.destination:
      self.cliParams.destination = tempfile.mkdtemp( 'DiracDist' )
    else:
      try:
        os.makedirs( self.cliParams.destination )
      except:
        pass
    gLogger.notice( "Will generate tarballs in %s" % self.cliParams.destination )
    return True

  def loadReleases( self ):
    gLogger.notice( "Loading releases.ini" )
    return self.relConf.loadProject( self.cliParams.projectName, self.cliParams.relcfg )

  def createModuleTarballs( self ):
    for version in self.cliParams.releasesToBuild:
      if not self.__createReleaseTarballs( version ):
        return False
    return True

  def __createReleaseTarballs( self, releaseVersion ):
    modsToTar = self.relConf.getModulesForProjectRelease( self.cliParams.projectName, releaseVersion )
    if not modsToTar:
      return False
    for modName in modsToTar:
      modVersion = modsToTar[ modName ]
      dctArgs = []
      #Version
      dctArgs.append( "-n '%s'" % modName )
      dctArgs.append( "-v '%s'" % modVersion )
      gLogger.notice( "Creating tar for %s version %s" % ( modName, modVersion ) )
      #Source
      modSrcTuple = self.relConf.getModSource( self.cliParams.projectName, modName )
      if not modSrcTuple:
        return False
      if modSrcTuple[0]:
        logMsgVCS = modSrcTuple[0]
        dctArgs.append( "-z '%s'" % modSrcTuple[0] )
      else:
        logMsgVCS = "autodiscover"
      dctArgs.append( "-u '%s'" % modSrcTuple[1] )
      gLogger.info( "Sources will be retrieved from %s (%s)" % ( modSrcTuple[1], logMsgVCS ) )
      #Tar destination
      dctArgs.append( "-D '%s'" % self.cliParams.destination )
      #Script location discovery
      scriptName = os.path.join( os.path.dirname( __file__ ), "dirac-create-distribution-tarball.py" )
      cmd = "'%s' %s" % ( scriptName, " ".join( dctArgs ) )
      gLogger.verbose( "Executing %s" % cmd )
      if os.system( cmd ):
        gLogger.error( "Failed creating tarball for module %s. Aborting" % modName )
        return False
      gLogger.info( "Tarball for %s version %s created" % ( modName, modVersion ) )
    return True


  def getAvailableExternals( self ):
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

  def createExternalsTarballs( self ):
    extDone = []
    for releaseVersion in self.cliParams.releasesToBuild:
      if releaseVersion in extDone:
        continue
      if not self.tarExternals( releaseVersion ):
        return False
      extDone.append( releaseVersion )
    return True

  def tarExternals( self, releaseVersion ):
    externalsVersion = self.relConf.getExtenalsVersion( releaseVersion )
    platform = Platform.getPlatformString()
    availableExternals = self.getAvailableExternals()

    if not externalsVersion:
      gLogger.notice( "Externals is not defined for release %s" % releaseVersion )
      return False

    for externalType in self.cliParams.externalsBuildType:
      requestedExternals = ( externalType, externalsVersion, platform, 'python%s' % self.cliParams.externalsPython )
      requestedExternalsString = "-".join( list( requestedExternals ) )
      gLogger.notice( "Trying to compile %s externals..." % requestedExternalsString )
      if not self.cliParams.forceExternals and requestedExternals in availableExternals:
        gLogger.notice( "Externals %s is already compiled, skipping..." % ( requestedExternalsString ) )
        continue
      compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals" )
      if not os.path.isfile( compileScript ):
        compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals.py" )
      compileTarget = os.path.join( self.cliParams.destination, platform )
      cmdArgs = []
      cmdArgs.append( "-D '%s'" % compileTarget )
      cmdArgs.append( "-t '%s'" % externalType )
      cmdArgs.append( "-v '%s'" % externalsVersion )
      cmdArgs.append( "-i '%s'" % self.cliParams.externalsPython )
      if cliParams.externalsLocation:
        cmdArgs.append( "-e '%s'" % self.cliParams.externalsLocation )
      if cliParams.makeJobs:
        cmdArgs.append( "-j '%s'" % self.cliParams.makeJobs )
      compileCmd = "%s %s" % ( compileScript, " ".join( cmdArgs ) )
      gLogger.info( compileCmd )
      if os.system( compileCmd ):
        gLogger.error( "Error while compiling externals!" )
        sys.exit( 1 )
      tarfilePath = os.path.join( self.cliParams.destination, "Externals-%s.tar.gz" % ( requestedExternalsString ) )
      result = Distribution.createTarball( tarfilePath,
                                           compileTarget,
                                           os.path.join( self.cliParams.destination, "mysql" ) )
      if not result[ 'OK' ]:
        gLogger.error( "Could not generate tarball for package %s" % package, result[ 'Error' ] )
        sys.exit( 1 )
      os.system( "rm -rf '%s'" % compileTarget )

    return True

  def doTheMagic( self ):
    if not distMaker.isOK():
      gLogger.fatal( "There was an error with the release description" )
      return False
    if not distMaker.loadReleases():
      gLogger.fatal( "There was an error when loading the release.ini file" )
      return False
    #Module tars
    if self.cliParams.ignorePackages:
      gLogger.notice( "Skipping creating module tarballs" )
    else:
      if not self.createModuleTarballs():
        gLogger.fatal( "There was a problem when creating the module tarballs" )
        return False
    #Externals
    if self.cliParams.ignoreExternals:
      gLogger.notice( "Skipping creating externals tarball" )
    else:
      if not self.createExternalsTarballs():
        gLogger.fatal( "There was a problem when creating the Externals tarballs" )
        return False
    #Write the releases files
    relcfgData = self.relConf.getCFG( self.cliParams.projectName ).toString()
    for relVersion in self.cliParams.releasesToBuild:
      try:
        relFile = file( os.path.join( self.cliParams.destination, "releases-%s-%s.cfg" % ( self.cliParams.projectName, relVersion ) ), "w" )
        relFile.write( relcfgData )
        relFile.close()
      except Exception, exc:
        gLogger.fatal( "Could not write the release info: %s" % str( exc ) )
        return False
    return True

if __name__ == "__main__":
  cliParams = Params()
  Script.disableCS()
  Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  distMaker = DistributionMaker( cliParams )
  if not distMaker.doTheMagic():
    sys.exit( 1 )
  gLogger.notice( "Everything seems ok. Tarballs generated in %s" % cliParams.destination )
  if cliParams.projectName in ( "DIRAC", "LHCb" ):
    gLogger.notice( "( cd %s ; tar -cf - *.tar.gz *.md5 *.ini ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'" % cliParams.destination )
