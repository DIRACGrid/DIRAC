#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

try:
  import hashlib as md5
except ImportError:
  import md5

globalDistribution = Distribution.Distribution()

g_uploadCmd = {
  'DIRAC' : "( cd %OUTLOCATION% ; tar -cf - *.tar.gz *.md5 *.cfg *.pdf *.html ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/installSource &&  tar -xvf - && ls *.tar.gz > tars.list'",
  'LHCb' : "( cd %OUTLOCATION% ; tar -cf - *.tar.gz *.md5 *.cfg *.pdf *.html ) | ssh lhcbprod@lxplus.cern.ch 'cd  /afs/cern.ch/lhcb/distribution/LHCbDirac_project &&  tar -xvf - && ls *.tar.gz > tars.list'",
  'ILC' : "( cd %OUTLOCATION% ; tar -cf - *.tar.gz *.md5 *.cfg *.pdf *.html ) | ssh lhcbprod@lxplus.cern.ch 'cd  /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'",
}

###
# Load release manager from dirac-install
##
diracInstallLocation = os.path.join( os.path.dirname( __file__ ), "dirac-install" )
if not os.path.isfile( diracInstallLocation ):
  diracInstallLocation = os.path.join( os.path.dirname( __file__ ), "dirac-install.py" )
try:
  diFile = open( diracInstallLocation, "r" )
  DiracInstall = imp.load_module( "DiracInstall", diFile, diracInstallLocation, ( "", "r", imp.PY_SOURCE ) )
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
    self.globalDefaults = ""
    self.forcedLocations = {}

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

  def setGlobalDefaults( self, value ):
    self.globalDefaults = value
    return S_OK()

  def overWriteLocation( self, value ):
    locSplit = value.split( ":" )
    if len( locSplit ) < 2:
      return S_ERROR( "Invalid location. It has to have format <moduleName>:<url> insteaf of %s" % value )
    modName = locSplit[0]
    location = ":".join( locSplit[1:] )
    gLogger.notice( "Forcing location of %s to %s" % ( modName, location ) )
    self.forcedLocations[ modName ] = location
    return S_OK()


  def registerSwitches( self ):
    Script.registerSwitch( "r:", "releases=", "releases to build (mandatory, comma separated)", cliParams.setReleases )
    Script.registerSwitch( "l:", "project=", "Project to build the release for (DIRAC by default)", cliParams.setProject )
    Script.registerSwitch( "D:", "destination", "Destination where to build the tar files", cliParams.setDestination )
    Script.registerSwitch( "i:", "pythonVersion", "Python version to use (25/26)", cliParams.setPythonVersion )
    Script.registerSwitch( "P", "ignorePackages", "Do not make tars of python packages", cliParams.setIgnorePackages )
    Script.registerSwitch( "C:", "relcfg=", "Use <file> as the releases.cfg", cliParams.setReleasesCFG )
    Script.registerSwitch( "b", "buildExternals", "Force externals compilation even if already compiled", cliParams.setForceExternals )
    Script.registerSwitch( "B", "ignoreExternals", "Skip externals compilation", cliParams.setIgnoreExternals )
    Script.registerSwitch( "t:", "buildType=", "External type to build (client/server)", cliParams.setExternalsBuildType )
    Script.registerSwitch( "x:", "externalsLocation=", "Use externals location instead of downloading them", cliParams.setExternalsLocation )
    Script.registerSwitch( "j:", "makeJobs=", "Make jobs (default is 1)", cliParams.setMakeJobs )
    Script.registerSwitch( 'M:', 'defaultsURL=', 'Where to retrieve the global defaults from', cliParams.setGlobalDefaults )
    Script.registerSwitch( 'O:', 'overwriteLocation=', 'Force location of modules from where to make the release. Format <moduleName>:<url>', cliParams.overWriteLocation )

    Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                        '\nUsage:',
                                        '  %s [option|cfgfile] ...\n' % Script.scriptName ] ) )


class DistributionMaker:

  def __init__( self, cliParams ):
    self.cliParams = cliParams
    self.relConf = DiracInstall.ReleaseConfig( projectName = cliParams.projectName,
                                               globalDefaultsURL = cliParams.globalDefaults )
    self.relConf.setDebugCB( gLogger.info )
    self.relConf.loadProjectDefaults()

  def isOK( self ):
    if not self.cliParams.releasesToBuild:
      gLogger.error( "Missing releases to build!" )
      Script.showHelp()
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
    gLogger.notice( "Loading releases.cfg" )
    return self.relConf.loadProjectRelease( self.cliParams.releasesToBuild, releaseMode = True, relLocation = self.cliParams.relcfg )

  def createModuleTarballs( self ):
    for version in self.cliParams.releasesToBuild:
      result = self.__createReleaseTarballs( version )
      if not result[ 'OK' ]:
        return result
    return S_OK()

  def __createReleaseTarballs( self, releaseVersion ):
    result = self.relConf.getModulesForRelease( releaseVersion )
    if not result[ 'OK' ]:
      return result
    modsToTar = result[ 'Value' ]
    for modName in modsToTar:
      modVersion = modsToTar[ modName ]
      dctArgs = [ '-A' ] #Leave a copy of the release notes outside the tarballs
      #Version
      dctArgs.append( "-n '%s'" % modName )
      dctArgs.append( "-v '%s'" % modVersion )
      gLogger.notice( "Creating tar for %s version %s" % ( modName, modVersion ) )
      #Source
      if modName in cliParams.forcedLocations:
        location = cliParams.forcedLocations[ modName ]
        gLogger.notice( "Source is forced to %s" % location )
        dctArgs.append( "-u '%s'" % location )
      else:
        result = self.relConf.getModSource( releaseVersion, modName )
        if not result[ 'OK' ]:
          return result
        modSrcTuple = result[ 'Value' ]
        if modSrcTuple[0]:
          logMsgVCS = modSrcTuple[0]
          dctArgs.append( "-z '%s'" % modSrcTuple[0] )
        else:
          logMsgVCS = "autodiscover"
        dctArgs.append( "-u '%s'" % modSrcTuple[1] )
        gLogger.notice( "Sources will be retrieved from %s (%s)" % ( modSrcTuple[1], logMsgVCS ) )
      #Tar destination
      dctArgs.append( "-D '%s'" % self.cliParams.destination )
      if cliParams.debug:
        dctArgs.append( "-dd" )
      #Script location discovery
      scriptName = os.path.join( os.path.dirname( __file__ ), "dirac-create-distribution-tarball" )
      if not os.path.isfile( scriptName ):
        scriptName = os.path.join( os.path.dirname( __file__ ), "dirac-create-distribution-tarball.py" )
      cmd = "'%s' %s" % ( scriptName, " ".join( dctArgs ) )
      gLogger.verbose( "Executing %s" % cmd )
      if os.system( cmd ) != 0:
        return S_ERROR( "Failed creating tarball for module %s. Aborting" % modName )
      gLogger.notice( "Tarball for %s version %s created" % ( modName, modVersion ) )
    return S_OK()


  def getAvailableExternals( self ):
    packagesURL = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/installSource/tars.list"
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
        gLogger.error( "Could not generate tarball for package %s" % requestedExternalsString, result[ 'Error' ] )
        sys.exit( 1 )
      os.system( "rm -rf '%s'" % compileTarget )

    return True

  def doTheMagic( self ):
    if not distMaker.isOK():
      gLogger.fatal( "There was an error with the release description" )
      return False
    result = distMaker.loadReleases()
    if not result[ 'OK' ]:
      gLogger.fatal( "There was an error when loading the release.cfg file: %s" % result[ 'Message' ] )
      return False
    #Module tars
    if self.cliParams.ignorePackages:
      gLogger.notice( "Skipping creating module tarballs" )
    else:
      result = self.createModuleTarballs()
      if not result[ 'OK' ]:
        gLogger.fatal( "There was a problem when creating the module tarballs: %s" % result[ 'Message' ] )
        return False
    #Externals
    if self.cliParams.ignoreExternals or cliParams.projectName != "DIRAC":
      gLogger.notice( "Skipping creating externals tarball" )
    else:
      if not self.createExternalsTarballs():
        gLogger.fatal( "There was a problem when creating the Externals tarballs" )
        return False
    #Write the releases files
    for relVersion in self.cliParams.releasesToBuild:
      projectCFG = self.relConf.getReleaseCFG( self.cliParams.projectName, relVersion )
      projectCFGData = projectCFG.toString() + "\n"
      try:
        relFile = file( os.path.join( self.cliParams.destination, "release-%s-%s.cfg" % ( self.cliParams.projectName, relVersion ) ), "w" )
        relFile.write( projectCFGData )
        relFile.close()
      except Exception, exc:
        gLogger.fatal( "Could not write the release info: %s" % str( exc ) )
        return False
      try:
        relFile = file( os.path.join( self.cliParams.destination, "release-%s-%s.md5" % ( self.cliParams.projectName, relVersion ) ), "w" )
        relFile.write( md5.md5( projectCFGData ).hexdigest() )
        relFile.close()
      except Exception, exc:
        gLogger.fatal( "Could not write the release info: %s" % str( exc ) )
        return False
      #Check deps
      if 'DIRAC' != self.cliParams.projectName:
        deps = self.relConf.getReleaseDependencies( self.cliParams.projectName, relVersion )
        if 'DIRAC' not in deps:
          gLogger.notice( "Release %s doesn't depend on DIRAC. Check it's what you really want" % relVersion )
        else:
          gLogger.notice( "Release %s depends on DIRAC %s" % ( relVersion, deps[ 'DIRAC'] ) )
    return True

  def getUploadCmd( self ):
    result = self.relConf.getUploadCommand()
    upCmd = False
    if not result['OK']:
      if self.cliParams.projectName in g_uploadCmd:
        upCmd = g_uploadCmd[ self.cliParams.projectName ]
    else:
      upCmd = result[ 'Value' ]

    filesToCopy = []
    for fileName in os.listdir( cliParams.destination ):
      for ext in ( ".tar.gz", ".md5", ".cfg", ".html", ".pdf" ):
        if fileName.find( ext ) == len( fileName ) - len( ext ):
          filesToCopy.append( os.path.join( cliParams.destination, fileName ) )
    outFiles = " ".join( filesToCopy )
    outFileNames = " ".join( [ os.path.basename( filePath ) for filePath in filesToCopy ] )

    if not upCmd:
      return "Upload to your installation source:\n'%s'\n" % "' '".join( filesToCopy )
    for inRep, outRep in ( ( "%OUTLOCATION%", self.cliParams.destination ),
                           ( "%OUTFILES%", outFiles ),
                           ( "%OUTFILENAMES%", outFileNames ) ):
      upCmd = upCmd.replace( inRep, outRep )
    return upCmd

if __name__ == "__main__":
  cliParams = Params()
  Script.disableCS()
  Script.addDefaultOptionValue( "/DIRAC/Setup", "Dummy" )
  cliParams.registerSwitches()
  Script.parseCommandLine( ignoreErrors = False )
  if Script.localCfg.getDebugMode():
    cliParams.debug = True
  distMaker = DistributionMaker( cliParams )
  if not distMaker.doTheMagic():
    sys.exit( 1 )
  gLogger.notice( "Everything seems ok. Tarballs generated in %s" % cliParams.destination )
  upCmd = distMaker.getUploadCmd()
  gLogger.always( upCmd )

