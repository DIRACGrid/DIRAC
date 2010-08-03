#!/usr/bin/env python
# $HeadURL$
"""
Compile the externals
"""
__RCSID__ = "$Id$"

import sys, os, getopt, tarfile, urllib2, imp, signal, re, time, stat

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

try:
  from hashlib import md5
except:
  from md5 import md5


class Params:

  def __init__( self ):
    self.packagesToInstall = [ 'DIRAC' ]
    self.release = ''
    self.externalsType = 'client'
    self.pythonVersion = '25'
    self.platform = ''
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.buildIfNotAvailable = False
    self.debug = False
    self.lcgVer = ''
    self.useVersionsDir = False
    self.downBaseURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3'

cliParams = Params()

#platformAlias = { 'Darwin_i386_10.6' : 'Darwin_i386_10.5' }
platformAlias = {}

####
# Start of helper functions
####

def logDEBUG( msg ):
  if cliParams.debug:
    for line in msg.split( "\n" ):
      print "%s UTC dirac-install [DEBUG] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
    sys.stdout.flush()

def logERROR( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [ERROR] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def logINFO( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [INFO]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

def urlretrieveTimeout( url, fileName, timeout = 0 ):
  """
   Retrive remore url to local file, with timeout wrapper
  """
  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
  logDEBUG( 'Retrieving remote file "%s"' % url )

  if timeout:
    signal.signal( signal.SIGALRM, alarmTimeoutHandler )
    # set timeout alarm
    signal.alarm( timeout )
  try:
    remoteFD = urllib2.urlopen( url )
    expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
    localFD = open( fileName, "wb" )
    receivedBytes = 0L
    data = remoteFD.read( 16384 )
    while data:
      receivedBytes += len( data )
      localFD.write( data )
      data = remoteFD.read( 16384 )
    localFD.close()
    remoteFD.close()
    if receivedBytes != expectedBytes:
      logERROR( "File should be %s bytes but received %s" % ( expectedBytes, receivedBytes ) )
      return False
  except urllib2.HTTPError, x:
    if x.code == 404:
      logERROR( "%s does not exist" % url )
      return False
  except Exception, x:
    if x == 'TimeOut':
      logERROR( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
    if timeout:
      signal.alarm( 0 )
    raise x

  if timeout:
    signal.alarm( 0 )
  return True

def downloadFileFromSVN( filePath, destPath, isExecutable = False, filterLines = [] ):
  fileName = os.path.basename( filePath )
  logINFO( "Downloading %s" % fileName )
  viewSVNLocation = "http://svnweb.cern.ch/world/wsvn/dirac/%s?op=dl&rev=0" % filePath
  anonymousLocation = 'http://svnweb.cern.ch/guest/dirac/%s' % filePath
  downOK = False
  localPath = os.path.join( destPath, fileName )
  for remoteLocation in ( anonymousLocation, viewSVNLocation ):
    try:
      remoteFile = urllib2.urlopen( remoteLocation )
    except urllib2.URLError:
      continue
    remoteData = remoteFile.read()
    remoteFile.close()
    if remoteData:
      localFile = open( localPath , "wb" )
      localFile.write( remoteData )
      localFile.close()
      downOK = True
      break
  if not downOK:
    osCmd = "svn cat 'http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/%s' > %s" % ( filePath, localPath )
    if os.system( osCmd ):
      logERROR( "Could not retrieve %s from the web nor via SVN. Aborting..." % fileName )
      sys.exit( 1 )
  if filterLines:
    fd = open( localPath, "rb" )
    fileContents = fd.readlines()
    fd.close()
    fd = open( localPath, "wb" )
    for line in fileContents:
      isFiltered = False
      for filter in filterLines:
        if line.find( filter ) > -1:
          isFiltered = True
          break
      if not isFiltered:
        fd.write( line )
    fd.close()
  if isExecutable:
    os.chmod( localPath , executablePerms )

def downloadAndExtractTarball( pkgVer, targetPath, subDir = False, checkHash = True ):
  if not subDir:
    subDir = "tars"
  tarName = "%s.tar.gz" % ( pkgVer )
  tarPath = os.path.join( cliParams.targetPath, tarName )
  try:
    if not urlretrieveTimeout( "%s/%s/%s" % ( cliParams.downBaseURL, subDir, tarName ), tarPath, 300 ):
      logERROR( "Cannot download %s" % tarName )
      return False
  except Exception, e:
    logERROR( "Cannot download %s: %s" % ( tarName, str( e ) ) )
    sys.exit( 1 )
  if checkHash:
    md5Name = "%s.md5" % ( pkgVer )
    md5Path = os.path.join( cliParams.targetPath, md5Name )
    try:
      if not urlretrieveTimeout( "%s/%s/%s" % ( cliParams.downBaseURL, subDir, md5Name ), md5Path, 300 ):
        logERROR( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      logERROR( "Cannot download %s: %s" % ( md5Name, str( e ) ) )
      sys.exit( 1 )
    #Read md5  
    fd = open( os.path.join( cliParams.targetPath, md5Name ), "r" )
    md5Expected = fd.read().strip()
    fd.close()
    #Calculate md5
    md5Calculated = md5()
    fd = open( os.path.join( cliParams.targetPath, tarName ), "r" )
    buf = fd.read( 4096 )
    while buf:
      md5Calculated.update( buf )
      buf = fd.read( 4096 )
    fd.close()
    #Check
    if md5Expected != md5Calculated.hexdigest():
      logERROR( "Oops... md5 for package %s failed!" % pkgVer )
      sys.exit( 1 )
    #Delete md5 file
    os.unlink( md5Path )
  #Extract
  #cwd = os.getcwd()
  #os.chdir(cliParams.targetPath)
  #tf = tarfile.open( tarPath, "r" )
  #for member in tf.getmembers():
  #  tf.extract( member )
  #os.chdir(cwd)
  tarCmd = "tar xzf '%s' -C '%s'" % ( tarPath, cliParams.targetPath )
  os.system( tarCmd )
  #Delete tar
  os.unlink( tarPath )
  return True

def fixBuildPaths():
  """
  At compilation time many scripts get the building directory inserted, 
  this needs to be changed to point to the current installation path: 
  cliParams.targetPath
"""

  # Locate build path (from header of pydoc)
  pydocPath = os.path.join( cliParams.targetPath, cliParams.platform, 'bin', 'pydoc' )
  try:
    fd = open( pydocPath )
    line = fd.readline()
    fd.close()
    buildPath = line[2:line.find( cliParams.platform ) - 1]
    replaceCmd = "grep -rIl '%s' %s | xargs sed -i'.org' 's:%s:%s:g'" % ( buildPath, cliParams.targetPath, buildPath, cliParams.targetPath )
    os.system( replaceCmd )

  except:
    pass


def runExternalsPostInstall():
  """
   If there are any postInstall in externals, run them
  """
  postInstallPath = os.path.join( cliParams.targetPath, cliParams.platform, "postInstall" )
  if not os.path.isdir( postInstallPath ):
    logDEBUG( "There's no %s directory. Skipping postInstall step" % postInstallPath )
    return
  postInstallSuffix = "-postInstall"
  for scriptName in os.listdir( postInstallPath ):
    suffixFindPos = scriptName.find( postInstallSuffix )
    if suffixFindPos == -1 or not suffixFindPos == len( scriptName ) - len( postInstallSuffix ):
      logDEBUG( "%s does not have the %s suffix. Skipping.." % ( scriptName, postInstallSuffix ) )
      continue
    scriptPath = os.path.join( postInstallPath, scriptName )
    os.chmod( scriptPath , executablePerms )
    logINFO( "Executing %s..." % scriptPath )
    if os.system( "'%s' > '%s.out' 2> '%s.err'" % ( scriptPath, scriptPath, scriptPath ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( scriptPath, scriptPath ) )
      sys.exit( 1 )

def checkPlatformAliasLink():
  """
  Make a link if there's an alias
  """
  if cliParams.platform in platformAlias:
    os.symlink( os.path.join( cliParams.targetPath, platformAlias[ cliParams.platform ] ),
                os.path.join( cliParams.targetPath, cliParams.platform ) )

####
# End of helper functions
####

cmdOpts = ( ( 'r:', 'release=', 'Release version to install' ),
            ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
            ( 't:', 'installType=', 'Installation type (client/server)' ),
            ( 'i:', 'pythonVersion=', 'Python version to compile (25/24)' ),
            ( 'p:', 'platform=', 'Platform to install' ),
            ( 'P:', 'installationPath=', 'Path where to install (default current working dir)' ),
            ( 'b', 'build', 'Force local compilation' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'B', 'buildIfNotAvailable', 'Build if not available' ),
            ( 'v', 'useVersionsDir', 'Use versions directory' ),
            ( 'd', 'debug', 'Show debug messages' ),
            ( 'h', 'help', 'Show this help' ),
          )

optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )

def usage():
  print "Usage %s <opts> <cfgFile>" % sys.argv[0]
  for cmdOpt in cmdOpts:
    print " %s %s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
  sys.exit( 1 )

#Load CFG  
downloadFileFromSVN( "DIRAC/trunk/DIRAC/Core/Utilities/CFG.py", cliParams.targetPath, False, [ '@gCFGSynchro' ] )
cfgPath = os.path.join( cliParams.targetPath , "CFG.py" )
cfgFD = open( cfgPath, "r" )
CFG = imp.load_module( "CFG", cfgFD, cfgPath, ( "", "r", imp.PY_SOURCE ) )
cfgFD.close()

optCfg = CFG.CFG()
for arg in args:
  if not arg[-4:] == ".cfg":
    continue
  cfg = CFG.CFG().loadFromFile( arg )
  if not cfg.isSection( 'LocalInstallation' ):
    continue
  if not optCfg:
    optCfg = cfg['LocalInstallation']
    continue
  optCfg = optCfg.mergeWith( cfg['LocalInstallation'] )

cliParams.release = optCfg.getOption( 'Release', cliParams.release )
cliParams.externalsType = optCfg.getOption( 'InstallType', cliParams.externalsType )
cliParams.pythonVersion = optCfg.getOption( 'PythonVersion', cliParams.pythonVersion )
cliParams.platform = optCfg.getOption( 'Platform', cliParams.platform )
cliParams.targetPath = optCfg.getOption( 'TargetPath', cliParams.targetPath )
cliParams.buildExternals = optCfg.getOption( 'BuildExternals', cliParams.buildExternals )
cliParams.lcgVer = optCfg.getOption( 'LcgVer', cliParams.lcgVer )
cliParams.downBaseURL = optCfg.getOption( 'BaseURL', cliParams.downBaseURL )
cliParams.useVersionsDir = optCfg.getOption( 'UseVersionsDir', cliParams.useVersionsDir )

for o, v in optList:
  if o in ( '-h', '--help' ):
    usage()
  elif o in ( '-r', '--release' ):
    cliParams.release = v
  elif o in ( '-e', '--extraPackages' ):
    for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
      pl = pkg.split( '@' )
      if pl[0] != 'Web':
        iPos = pl[0].find( "DIRAC" )
        if iPos == -1 or iPos != len( pl[0] ) - 5:
          pl[0] = "%sDIRAC" % pl[0]
      pkg = "@".join( pl )
      if pkg not in cliParams.packagesToInstall:
        cliParams.packagesToInstall.append( pkg )
  elif o in ( '-t', '--installType' ):
    cliParams.externalsType = v
  elif o in ( '-y', '--pythonVersion' ):
    cliParams.pythonVersion = v
  elif o in ( '-p', '--platform' ):
    cliParams.platform = v
  elif o in ( '-d', '--debug' ):
    cliParams.debug = True
  elif o in ( '-g', '--grid' ):
    cliParams.lcgVer = v
  elif o in ( '-P', '--installationPath' ):
    cliParams.targetPath = v
    try:
      os.makedirs( v )
    except:
      pass
  elif o in ( '-v', '--useVersionsDir' ):
    cliParams.useVersionsDir = True

  elif o in ( '-b', '--build' ):
    cliParams.buildExternals = True

if not cliParams.release:
  logERROR( ": Need to define a release version to install!" )
  usage()

if cliParams.useVersionsDir:
  # install under <installPath>/versions/<version>_<timestamp>
  cliParams.basePath = cliParams.targetPath
  cliParams.targetPath = os.path.join( cliParams.targetPath, 'versions', '%s_%s' % ( cliParams.release, int( time.time() ) ) )
  try:
    os.makedirs( cliParams.targetPath )
  except:
    pass

#Get the list of tarfiles
tarsURL = "%s/tars/tars.list" % cliParams.downBaseURL
logDEBUG( "Getting the tar list from %s" % tarsURL )
tarListPath = os.path.join( cliParams.targetPath, "tars.list" )
try:
  urlretrieveTimeout( tarsURL, tarListPath, 300 )
except Exception, e:
  logERROR( "Cannot download list of tars: %s" % ( str( e ) ) )
  sys.exit( 1 )
fd = open( tarListPath, "r" )
availableTars = [ line.strip() for line in fd.readlines() if line.strip() ]
fd.close()
os.unlink( tarListPath )

#Load releases
cfgURL = "%s/%s/%s" % ( cliParams.downBaseURL, "tars", "releases-%s.cfg" % cliParams.release )
cfgLocation = os.path.join( cliParams.targetPath, "releases.cfg" )
if not urlretrieveTimeout( cfgURL, cfgLocation, 300 ):
  logERROR( "Release %s doesn't seem to have been distributed" % cliParams.release )
  sys.exit( 1 )
mainCFG = CFG.CFG().loadFromFile( cfgLocation )

if 'Releases' not in mainCFG.listSections():
  logERROR( " There's no Releases section in releases.cfg" )
  sys.exit( 1 )

if cliParams.release not in mainCFG[ 'Releases' ].listSections():
  logERROR( " There's no release %s" % cliParams.release )
  sys.exit( 1 )

#Tar fest!

moduleDIRACRe = re.compile( "^.*DIRAC$" )

releaseCFG = mainCFG[ 'Releases' ][ cliParams.release ]
for package in cliParams.packagesToInstall:
  pl = package.split( '@' )
  packageVersion = False
  #Explicit version can be defined for packages using pkgName@version
  if len( pl ) > 1 :
    package = pl[0]
    packageVersion = "@".join( pl[1:] )
  else:
    #Try to get the defined package version
    if package not in releaseCFG.listOptions():
      logERROR( " Package %s is not defined for the release" % package )
      sys.exit( 1 )
    packageVersion = releaseCFG.getOption( package, "trunk" )
  packageTar = "%s-%s.tar.gz" % ( package, packageVersion )
  if packageTar not in availableTars:
    logERROR( "%s is not registered" % packageTar )
    sys.exit( 1 )
  logINFO( "Installing package %s version %s" % ( package, packageVersion ) )
  if not downloadAndExtractTarball( "%s-%s" % ( package, packageVersion ), cliParams.targetPath ):
    sys.exit( 1 )
  if moduleDIRACRe.match( package ):
    initFilePath = os.path.join( cliParams.targetPath, package, "__init__.py" )
    if not os.path.isfile( initFilePath ):
      fd = open( initFilePath, "w" )
      fd.write( "#Generated by dirac-install\n" )
      fd.close()

#Deploy scripts :)
os.system( os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-deploy-scripts.py" ) )

#Do we have a platform defined?
if not cliParams.platform:
  platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  cliParams.platform = Platform.getPlatformString()

logINFO( "Using platform: %s" % cliParams.platform )

#Externals stuff
extVersion = releaseCFG.getOption( 'Externals', "trunk" )
if cliParams.platform in platformAlias:
  effectivePlatform = platformAlias[ cliParams.platform ]
else:
  effectivePlatform = cliParams.platform
extDesc = "-".join( [ cliParams.externalsType, extVersion,
                          effectivePlatform, 'python%s' % cliParams.pythonVersion ] )

logDEBUG( "Externals version is %s" % extDesc )
extTar = "Externals-%s" % extDesc
extAvailable = "%s.tar.gz" % ( extTar ) in availableTars

buildCmd = os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-compile-externals.py" )
buildCmd = "%s -t '%s' -d '%s' -v '%s' -i '%s'" % ( buildCmd, cliParams.externalsType,
                                                    os.path.join( cliParams.targetPath, cliParams.platform ),
                                                    extVersion,
                                                    cliParams.pythonVersion )
if cliParams.buildExternals:
  if os.system( buildCmd ):
    logERROR( "Could not compile binaries" )
    sys.exit( 1 )
else:
  if extAvailable:
    if not downloadAndExtractTarball( extTar, cliParams.targetPath ):
      sys.exit( 1 )
    fixBuildPaths()
    runExternalsPostInstall()
    checkPlatformAliasLink()
  else:
    if cliParams.buildIfNotAvailable:
      if os.system( buildCmd ):
        logERROR( "Could not compile binaries" )
        sys.exit( 1 )
    else:
      logERROR( "%s.tar.gz is not registered" % extTar )
      sys.exit( 1 )

#LCG utils if required
if cliParams.lcgVer:
  tarBallName = "DIRAC-lcg-%s-%s-python%s" % ( cliParams.lcgVer, cliParams.platform, cliParams.pythonVersion )
  if not downloadAndExtractTarball( tarBallName, cliParams.targetPath, "lcgBundles", False ):
    logERROR( "Check that there is a release for your platform: %s" % tarBallName )

for file in ( "releases.cfg", "CFG.py", "CFG.pyc", "CFG.pyo" ):
  filePath = os.path.join( cliParams.targetPath, file )
  if os.path.isfile( filePath ):
    os.unlink( filePath )

if cliParams.useVersionsDir:
  oldPath = os.path.join( cliParams.basePath, 'old' )
  proPath = os.path.join( cliParams.basePath, 'pro' )
  fakeEtc = os.path.join( cliParams.targetPath, 'etc' )
  realEtc = os.path.join( cliParams.basePath, 'etc' )
  try:
    if os.path.exists( proPath ):
      if os.path.exists( oldPath ):
        os.unlink( oldPath )
      os.rename( proPath, oldPath )
    os.symlink( cliParams.targetPath, proPath )
    if not os.path.exists( realEtc ):
      os.makedirs( realEtc )
    os.symlink( realEtc, fakeEtc )
  except Exception, x:
    logERROR( x )
    sys.exit( 1 )

logINFO( "DIRAC release %s successfully installed" % cliParams.release )
sys.exit( 0 )
