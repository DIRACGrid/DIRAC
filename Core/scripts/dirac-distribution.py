# $HeadURL$
__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base      import Script
from DIRAC.Core.Utilities import List, CFG

import sys, os, re, urllib2, tempfile, getpass, subprocess

svnSshRoot    = "svn+ssh://%s@svn.cern.ch/reps/dirac/%s"

class Params:
  
  def __init__( self ):
    self.releasesToBuild = []
    self.userName = ""
    self.forceSVNLinks = False
    self.debug = False

  def setReleases( self, optionValue ):
    self.releasesToBuild  = List.fromChar( optionValue )
    return S_OK()
  
  def setUserName( self, optionValue ):
    self.userName = optionValue
    return S_OK()
  
  def discoverUserName( self ):
    if self.userName:
      return
    self.userName = raw_input( "SVN User Name[%s]: " % getpass.getuser() )
    if not self.userName:
      self.userName = getpass.getuser()
      
  def setForceSVNLink( self, optionValue ):
    self.forceSVNLinks = True
    return S_OK()
  
  def setDebug( self, optionValue ):
    self.debug = True
    return S_OK()
  
cliParams = Params()

Script.disableCS()
Script.registerSwitch( "r:", "releases=", "reseases to build (mandatory, comma separated)", cliParams.setReleases )
Script.registerSwitch( "u:", "username=", "svn username to use", cliParams.setUserName )
Script.registerSwitch( "l", "forceSVNLinks", "Redo the svn links even if the release exists", cliParams.setForceSVNLink )
Script.registerSwitch( "d", "debug", "Debug mode", cliParams.setDebug )

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
  
  for releaseVersion in cliParams.releasesToBuild:
    if not cliParams.forceSVNLinks and releaseVersion in taggedReleases:
      gLogger.info( "Release %s is already tagged, skipping" % releaseVersion )
      continue
    if releaseVersion not in releasesCFG.listSections():
      gLogger.error( "Release %s not defined in releases.cfg" % releaseVersion )
      continue
    releaseSVNPath = svnSshRoot % ( cliParams.userName, "/tags/%s" % ( releaseVersion ) )
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
      version = releasesCFG[ releaseVersion ].getOption( p, "" )
      if version.strip().lower() in ( "trunk", "", "head" ):
        version = "trunk"
      else:
        if p in cmtCompatiblePackages:
          version = "tags/%s/%s_%s/%s" % ( p, p, version, p )
        else:
          version = "tags/%s" % ( version )
      svnLinks.append( "%s http://svnweb.cern.ch/guest/dirac/%s/%s" % ( p, p, version ) )
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
  
  
def autoTarPackages( mainCFG, targetDir ):
  global cliParams
  
  releasesCFG = mainCFG[ 'Releases' ]
  tmpPath = tempfile.mkdtemp()
  cmtCompatiblePackages = mainCFG.getOption( 'CMTCompatiblePackages', [] )
  autoTarPackages = mainCFG.getOption( 'AutoTarPackages', [] )
  for releaseVersion in cliParams.releasesToBuild:
    releaseTMPPath = os.path.join( tmpPath, releaseVersion )
    gLogger.info( "Taring %s release in %s" % ( releaseVersion, releaseTMPPath ) )
    os.mkdir( releaseTMPPath )
    for package in releasesCFG[ releaseVersion ].listOptions():
      if package not in autoTarPackages:
        continue
      version = releasesCFG[ releaseVersion ].getOption( package, "" )
      if version.strip().lower() in ( "trunk", "", "head" ):
        svnVersion = "trunk"
      else:
        if package in cmtCompatiblePackages:
          svnVersion = "tags/%s/%s_%s/%s" % ( package, package, version, package )
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
  #Remove tmp dir
  os.system( "rm -rf '%s'" % tmpPath )
      
      
mainCFG = parseCFGFromSVN( "/trunk/releases.cfg" )
if 'Releases' not in mainCFG.listSections():
  gLogger.fatal( "releases.cfg file does not have a Releases section" )
  exit(1)
releasesCFG = mainCFG[ 'Releases' ]
taggedReleases = getSVNVersions()

#tagSVNReleases( mainCFG, taggedReleases )
autoTarPackages( mainCFG, "/tmp/adritest" )