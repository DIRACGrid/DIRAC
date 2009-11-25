# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, CFG, File

import sys, os, re, urllib2, tempfile, getpass, subprocess

class Params:
  
  def __init__( self ):
    self.releasesToBuild = []
    self.userName = ""
    self.forceSVNLinks = False
    self.ignoreSVNLinks = False
    self.debug = False
    self.externalsBuildType = 'client'
    self.forceExternals = False
    self.ignoreExternals = False
    self.externalsPython = '25'
    self.svnRoot = "svn+ssh://svn.cern.ch/reps/dirac"
    self.destination = ""
    
  def setReleases( self, optionValue ):
    self.releasesToBuild  = List.fromChar( optionValue )
    return S_OK()
  
  def setUserName( self, optionValue ):
    self.userName = optionValue
    self.svnRoot = "svn+ssh://%s@svn.cern.ch/reps/dirac" % optionValue
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
    self.externalsBuildType = optionValue
    return S_OK()
  
  def setForceExternals( self, optionValue ):
    self.forceExternals = True
    return S_O
  
  def setIgnoreExternals( self, optionValue ):
    self.ignoreExternals = True
    return S_OK()
  
  def setDestination( self, optionValue ):
    self.destination = optionValue
    return S_OK()
  
cliParams = Params()

Script.disableCS()
Script.registerSwitch( "r:", "releases=", "reseases to build (mandatory, comma separated)", cliParams.setReleases )
Script.registerSwitch( "u:", "username=", "svn username to use", cliParams.setUserName )
Script.registerSwitch( "l", "forceSVNLinks", "Redo the svn links even if the release exists", cliParams.setForceSVNLink )
Script.registerSwitch( "L", "ignoreSVNLinks", "Do not do the svn links for the release", cliParams.setIgnoreSVNLink )
Script.registerSwitch( "D", "debug", "Debug mode", cliParams.setDebug )
Script.registerSwitch( "t:", "buildType=", "External type to build (client/server)", cliParams.setExternalsBuildType )
Script.registerSwitch( "e", "forceExternals", "Force externals compilation even if already compiled", cliParams.setExternalsBuildType )
Script.registerSwitch( "E", "ignoreExternals", "Do not compile externals", cliParams.setIgnoreExternals )
Script.registerSwitch( "d:", "destination", "Destination where to build the tar files", cliParams.setDestination )

Script.parseCommandLine( ignoreErrors = False )

def usage():
  Script.showHelp()
  exit(2)
  
if not cliParams.releasesToBuild:
  usage()
  exit(2)

##
#Helper functions
##
def execAndGetOutput( cmd ):
  if cliParams.debug:
    print "EXECUTING: %s" % cmd 
  p = subprocess.Popen( cmd, 
                        shell = True, stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE, close_fds = True )
  stdData = p.stdout.read()
  errData = p.stderr.read()
  p.wait()
  return ( p.returncode, stdData, errData )

def getSVNVersions( package = False, isCMTCompatible = False ):
  if package:
    webLocation = 'http://svnweb.cern.ch/guest/dirac/%s/tags/%s' % ( package, package )
  else:
    webLocation = 'http://svnweb.cern.ch/guest/dirac/tags'
    package = "global release"
  try:
    remoteFile = urllib2.urlopen( webLocation )
  except urllib2.URLError:
    gLogger.exception()
    sys.exit(2)
  remoteData = remoteFile.read()
  remoteFile.close()      
  if not remoteData:
    gLogger.error( "Could not retrieve versions for package %s" % package )
    sys.exit(1)
  versions = []
  if isCMTCompatible and package:
    rePackage = "%s_.*" % package
  else:
    rePackage = ".*"
  versionRE = re.compile( "<li> *<a *href=.*> *(%s)/ *</a> *</li>" % rePackage )
  for line in remoteData.split( "\n" ):
    res = versionRE.search( line )
    if res:
      versions.append( res.groups()[0] )
  return versions

def parseCFGFromSVN( svnPath ):
  import urllib2, stat
  gLogger.info( "Reading %s" % ( svnPath ) ) 
  if svnPath[0] == "/":
    svnPath = svnPath[1:]
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s?op=dl&rev=0" % ( svnPath )
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s' % ( svnPath )
  for remoteLocation in ( anonymousLocation, viewSVNLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      gLogger.exception()
      continue
    remoteData = remoteFile.read()
    remoteFile.close()      
    if remoteData:
      return CFG.CFG().loadFromBuffer( remoteData )
  #Web cat failed. Try directly with svn
  exitStatus, remoteData = execAndGetOutput( "svn cat 'http://svnweb.cern.ch/guest/dirac/%s'" % ( svnPath ) )
  if exitStatus:
    print "Error: Could not retrieve %s from the web nor via SVN. Aborting..." % svnPath
    sys.exit(1)
  return CFG.CFG().loadFromBuffer( remoteData )

def tagSVNReleases( mainCFG, taggedReleases ):
  global cliParams
  
  releasesCFG = mainCFG[ 'Releases' ]
  cmtCompatiblePackages = mainCFG.getOption( 'CMTCompatiblePackages', [] )
  
  if not cliParams.userName:
    cliParams.discoverUserName()
    
  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )
  
  for releaseVersion in cliParams.releasesToBuild:
    if not cliParams.forceSVNLinks and releaseVersion in taggedReleases:
      gLogger.info( "Release %s is already tagged, skipping" % releaseVersion )
      continue
    if releaseVersion not in releasesCFG.listSections():
      gLogger.error( "Release %s not defined in releases.cfg" % releaseVersion )
      continue
    releaseSVNPath = "%s/tags/%s" % ( cliParams.svnRoot, releaseVersion )
    if releaseVersion not in taggedReleases:
      gLogger.info( "Creating global release dir %s" % releaseVersion )
      svnCmd = "svn --parents -m 'Release %s' mkdir '%s'" % ( releaseVersion, releaseSVNPath )
      exitStatus, stdData, errData = execAndGetOutput( svnCmd )
      if exitStatus:
        gLogger.error( "Error while generating release tag", "\n".join( [ stdData, errData ] ) )
        continue
    svnLinks = []
    packages = releasesCFG[ releaseVersion ].listOptions()
    packages.sort()
    for p in packages:
      if p not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( p, "" )
      if version.strip().lower() in ( "trunk", "", "head" ):
        version = "trunk/%s" % ( p )
      else:
        if p in cmtCompatiblePackages:
          version = "tags/%s/%s_%s" % ( p, p, version )
        else:
          version = "tags/%s" % ( version )
      svnLinks.append( "%s http://svnweb.cern.ch/guest/dirac/%s/%s" % ( p, p, version ) )
    print svnLinks
    tmpPath = tempfile.mkdtemp()
    fd = open( os.path.join( tmpPath, "extProp" ), "wb" )
    fd.write( "%s\n" % "\n".join( svnLinks ) )
    fd.close()
    svnCmds = []
    svnCmds.append( "svn co -N '%s' '%s/svnco'" % ( releaseSVNPath, tmpPath ) )
    svnCmds.append( "svn propset svn:externals -F '%s/extProp' '%s/svnco'" % ( tmpPath, tmpPath ) )
    svnCmds.append( "svn ci -m 'Release %s svn:externals' '%s/svnco'" % ( releaseVersion, tmpPath ) )
    gLogger.info( "Creating svn:externals in %s..." % releaseVersion )
    for cmd in svnCmds:
      exitStatus, stdData, errData = execAndGetOutput( cmd )
      if exitStatus:
        gLogger.error( "Error while generating release tag", "\n".join( [ stdData, errData ] ) )
        continue
    os.system( "rm -rf '%s'" % tmpPath )
  
def autoTarPackages( mainCFG, targetDir ):
  global cliParams
  
  releasesCFG = mainCFG[ 'Releases' ]
  tmpPath = tempfile.mkdtemp()
  cmtCompatiblePackages = mainCFG.getOption( 'CMTCompatiblePackages', [] )
  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )
  for releaseVersion in cliParams.releasesToBuild:
    releaseTMPPath = os.path.join( tmpPath, releaseVersion )
    gLogger.info( "Getting %s release to tmp dir %s" % ( releaseVersion, releaseTMPPath ) )
    os.mkdir( releaseTMPPath )
    for package in releasesCFG[ releaseVersion ].listOptions():
      if package not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( package, "" )
      if version.strip().lower() in ( "trunk", "", "head" ):
        svnVersion = "trunk/%s" % ( package )
      else:
        if package in cmtCompatiblePackages:
          svnVersion = "tags/%s/%s_%s" % ( package, package, version )
        else:
          svnVersion = "tags/%s" % ( version )
      pkgSVNPath = "http://svnweb.cern.ch/guest/dirac/%s/%s" % ( package, svnVersion ) 
      gLogger.info( " Getting %s" % pkgSVNPath )
      svnCmd = "svn export '%s' '%s/%s'" % ( pkgSVNPath, releaseTMPPath, package )
      exitStatus, stdData, errData = execAndGetOutput( svnCmd )
      if exitStatus:
        gLogger.error( "Error while generating release tag", "\n".join( [ stdData, errData ] ) )
        continue
      gLogger.info( "Taring %s..." % package )
      tarfilePath = os.path.join( targetDir, "%s-%s.tar.gz" % ( package, version ) )
      cmd = "cd '%s'; tar czf '%s' %s" % ( releaseTMPPath, tarfilePath, package )
      if os.system( cmd ):
        gLogger.error( "Could not tar %s into %s" % ( package, tarfilePath ) )
        sys.exit(1)
      md5str = File.getMD5ForFiles( [ tarfilePath ] )
      md5FilePath = os.path.join( targetDir, "%s-%s.md5" % ( package, version ) )
      fd = open( md5FilePath, "w" )
      fd.write( md5str )
      fd.close()
  #Remove tmp dir
  os.system( "rm -rf '%s'" % tmpPath )

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

def getPlatform():
  platformFile = os.path.join( os.path.dirname( __file__ ), "dirac-platform.py" )
  exitCode, stdData, errData = execAndGetOutput( platformFile )
  platform = stdData.strip()
  if exitCode or not platform:
    gLogger.error( "Could not retrieve platform!" )
    sys.exit( 1 )
  return platform

def tarExternals( mainCFG, targetDir ):
  global cliParams
  
  releasesCFG = mainCFG[ 'Releases' ]
  platform = getPlatform()
  availableExternals = getAvailableExternals()
  for releaseVersion in cliParams.releasesToBuild:
    externalsVersion = releasesCFG[ releaseVersion ].getOption( "Externals", "" )
    if not externalsVersion:
      gLogger.info( "Externals is not defined for release %s" % releaseVersion)
      continue
    requestedExternals = ( cliParams.externalsBuildType, externalsVersion, platform, 'python%s' % cliParams.externalsPython )
    requestedExternalsString = "-".join( list( requestedExternals ) ) 
    if not cliParams.forceExternals and requestedExternals in availableExternals:
      gLogger.info( "Externals %s is already compiled, skipping..." % ( requestedExternalsString ) )
      continue
    gLogger.info( "Compiling externals..." )
    compileScript = os.path.join( os.path.dirname( __file__ ), "dirac-compile-externals.py" )
    compileTarget = os.path.join( targetDir, platform )
    compileCmd = "%s -d '%s' -t '%s' -v '%s' -p '%s'" % ( compileScript, compileTarget, cliParams.externalsBuildType,
                                                          externalsVersion, cliParams.externalsPython )
    print compileCmd
    if os.system( compileCmd ):
      gLogger.error( "Error while compiling externals!" )
      sys.exit(1)
    tarfilePath = os.path.join( targetDir, "Externals-%s.tar.gz" % ( requestedExternalsString ) )
    cmd = "cd '%s'; tar czf '%s' %s" % ( targetDir, tarfilePath, platform )
    if os.system( cmd ):
      gLogger.error( "Could not tar %s into %s" % ( package, tarfilePath ) )
      sys.exit(1)
    os.system( "rm -rf '%s'" % compileTarget )
    md5str = File.getMD5ForFiles( [ tarfilePath ] )
    md5FilePath = os.path.join( targetDir, "Externals-%s.md5" % ( requestedExternalsString ) )
    fd = open( md5FilePath, "w" )
    fd.write( md5str )
    fd.close()
    


mainCFG = parseCFGFromSVN( "/trunk/releases.cfg" )
if 'Releases' not in mainCFG.listSections():
  gLogger.fatal( "releases.cfg file does not have a Releases section" )
  exit(1)
releasesCFG = mainCFG[ 'Releases' ]

if not cliParams.destination:
  targetPath = tempfile.mkdtemp()
else:
  targetPath = cliParams.destination
  try:
    os.makedirs( targetPath )
  except:
    pass
gLogger.info( "Will generate tarballs in %s" % targetPath )

if not cliParams.ignoreSVNLinks:
  taggedReleases = getSVNVersions()
  tagSVNReleases( mainCFG, taggedReleases )
  
if not cliParams.ignoreExternals:
  tarExternals( mainCFG, targetPath )
  
autoTarPackages( mainCFG, targetPath )

gLogger.info( "Everything seems ok" )
gLogger.info( "Please upload the tarballs by executing:")
gLogger.info( "( cd %s ; tar -cf - *.tar.gz *.md5 ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'" % targetPath )
