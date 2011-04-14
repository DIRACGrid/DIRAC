#!/usr/bin/env python
# $HeadURL$
"""
Compile the externals
"""
__RCSID__ = "$Id$"

import sys, os, getopt, tarfile, urllib2, imp, signal, re, time, stat, types, copy

try:
  import zipfile
  zipEnabled = True
except:
  zipEnabled = False

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

try:
  from hashlib import md5
except:
  from md5 import md5

def S_OK( value = "" ):
  return { 'OK' : True, 'Value' : value }

def S_ERROR( msg = "" ):
  return { 'OK' : False, 'Message' : msg }

############
# Start of CFG
############


class Params:

  def __init__( self ):
    self.packagesToInstall = []
    self.project = 'DIRAC'
    self.release = False
    self.externalsType = 'client'
    self.pythonVersion = '26'
    self.platform = False
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.buildIfNotAvailable = False
    self.debug = False
    self.lcgVer = ''
    self.useVersionsDir = False

cliParams = Params()

###
# Release config manager
###

class ReleaseConfig:

  #Because python's INI parser is the utmost shittiest thing ever done
  class CFG:
    def __init__( self, cfgData = "" ):
      self.__data = {}
      self.__children = {}
      if cfgData:
        self.parse( cfgData )

    def parse( self, cfgData ):
      self.__parse( cfgData )
      return self

    def __parse( self, cfgData, cIndex = 0 ):
      childName = ""
      numLine = 0
      while cIndex < len( cfgData ):
        eol = cfgData.find( "\n", cIndex )
        if eol < cIndex:
          #End?
          return cIndex
        numLine += 1
        if eol == cIndex:
          cIndex += 1
          continue
        line = cfgData[ cIndex : eol ].strip()
        #Jump EOL
        cIndex = eol + 1
        if line[0] == "#":
          continue
        if line.find( "=" ) > -1:
          fields = line.split( "=" )
          self.__data[ fields[0].strip() ] = "=".join( fields[1:] ).strip()
          continue

        opFound = line.find( "{" )
        if opFound > -1:
          childName += line[ :opFound ].strip()
          if not childName:
            raise Exception( "No section name defined for opening in line %s" % numLine )
          childName = childName.strip()
          self.__children[ childName ] = ReleaseConfig.CFG()
          eoc = self.__children[ childName ].__parse( cfgData, cIndex )
          cIndex = eoc
          childName = ""
          continue

        if line == "}":
          return cIndex
        #Must be name for section
        childName += line.strip()
      return cIndex

    def isSection( self, obList ):
      return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 2

    def isOption( self, obList ):
      return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 1

    def __exists( self, obList ):
      if len( obList ) == 1:
        if obList[0] in self.__children:
          return  2
        elif obList[0] in self.__data:
          return 1
        else:
          return 0
      if obList[0] in self.__children:
        return self.__children[ obList[0] ].__exists( obList[1:] )
      return 0

    def get( self, opName ):
      return self.__get( [ op.strip() for op in opName.split( "/" ) if op.strip() ] )

    def __get( self, obList ):
      if len( obList ) == 1:
        if obList[0] in self.__data:
          return self.__data[ obList[0] ]
        raise ValueError( "Missing option %s" % obList[0] )
      if obList[0] in self.__children:
        return self.__children[ obList[0] ].__get( obList[1:] )
      raise ValueError( "Missing section %s" % obList[0] )

    def toString( self, tabs = 0 ):
      lines = [ "%s = %s" % ( opName, self.__data[ opName ] ) for opName in self.__data ]
      for secName in self.__children:
        lines.append( "%s" % secName )
        lines.append( "{" )
        lines.append( self.__children[ secName ].toString( tabs + 1 ) )
        lines.append( "}" )
      return "\n".join( [ "%s%s" % ( "  " * tabs, line ) for line in lines ] )

    #END OF CFG CLASS

  def __init__( self, projectName ):
    self.__configs = {}
    self.__globalDefaults = ReleaseConfig.CFG()
    self.__defaults = {}
    self.__depsLoaded = {}
    self.__projectsLoaded = []
    self.__debugCB = False
    self.__projectName = projectName
    self.__projectRelease = {
        'DIRAC' : "https://github.com/DIRACGrid/DIRAC/raw/master/releases.cfg"
        }
    self.__projectTarLocation = {
        'DIRAC' : "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/tars"
        }

  def setDebugCB( self, debFunc ):
    self.__debugCB = debFunc

  def __dbgMsg( self, msg ):
    if self.__debugCB:
      self.__debugCB( msg )

  def loadDefaults( self ):
    globalDefaultsLoc = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/globalDefaults.cfg"
    self.__dbgMsg( "Loading global defaults from: %s" % globalDefaultsLoc )
    try:
      defFile = urllib2.urlopen( globalDefaultsLoc )
      self.__globalDefaults.parse( defFile.read() )
      defFile.close()
    except Exception, excp:
      return S_ERROR( "Could not load global defaults: %s" % excp )
    self.__dbgMsg( "Loaded" )
    self.__defaults[ self.__projectName ] = ReleaseConfig.CFG()
    try:
      defaultsLocation = self.__globalDefaults.get( "%s/defaultsLocation" % self.__projectName )
      self.__dbgMsg( "Defaults for project %s are in %s" % ( self.__projectName, defaultsLocation ) )
      defFile = urllib2.urlopen( defaultsLocation )
      self.__defaults[ self.__projectName ].parse( defFile.read() )
      defFile.close()
      self.__dbgMsg( "Loaded" )
    except ValueError:
      self.__dbgMsg( "No defaults file defined for project %s" % self.__projectName )
      pass
    return S_OK()


  def getDefaultValue( self, opName ):
    try:
      return self.__defaults[ self.__projectName ].get( opName )
    except:
      pass
    try:
      return self.__globalDefaults.get( "%s/%s" % ( self.__projectName, opName ) )
    except:
      pass
    return None

  #HERE!

  def __loadCFGFromURL( self, urlcfg ):
    try:
      cfgFile = urllib2.urlopen( urlcfg )
    except:
      return S_ERROR( "Could not open %s" % urlcfg )
    try:
      cfg = ReleaseConfig.CFG( cfgFile.read() )
    except Exception, excp:
      return S_ERROR( "Could not parse %s: %s" % ( urlcfg, excp ) )
    cfgFile.close()
    return S_OK( cfg )

  def loadProjectForRelease( self, releasesLocation = False ):
    if self.__projectName in self.__configs:
      return S_OK()
    if releasesLocation:
      relFile = "file://%s" % os.path.realpath( releasesLocation )
    elif self.getDefaultValue( "releases" ):
      releasesLocation = self.getDefaultValue( "releases" )
    elif self.__projectName in self.__projectRelease:
      return S_ERROR( "Don't know how to find releases.cfg for project %s" % self.__projectName )
    else:
      releasesLocation = self.__projectRelease[ self.__projectName ]
    self.__dbgMsg( "Releases definition is %s" % releasesLocation )
    #Load it
    result = self.__loadCFGFromURL( releasesLocation )
    if not result[ 'OK' ]:
      return result
    self.__configs[ self.__projectName ] = result[ 'Value' ]
    self.__projectsLoaded.append( self.__projectName )
    self.__dbgMsg( "Loaded %s" % releasesLocation )
    return S_OK()

  def getTarsLocation( self, projectName = "" ):
    if not projectName:
      projectName = self.__projectName
    defLoc = self.getDefaultValue( "tarsPath" )
    if defLoc:
      return S_OK( defLoc )
    elif projectName in self.__projectTarLocation:
      return S_OK( self.__projectTarLocation[ projectName ] )
    return S_ERROR( "Don't know how to find the tarballs location for project %s" % projectName )


  def loadProjectForInstall( self, releaseVersion, projectName = "" ):
    if not projectName:
      projectName = self.__projectName
    #Check what's loaded
    if projectName not in self.__depsLoaded:
      self.__depsLoaded[ projectName ] = releaseVersion
    elif self.__depsLoaded[ projectName ] != releaseVersion:
      S_ERROR( "Oops. Project %s (%s) is required already in a different version (%s)!" % ( projectName,
                                                                                            releaseVersion,
                                                                                            self.__depsLoaded[ projectName ] ) )
    else:
      #Already loaded
      return S_OK()
    #Load
    self.__dbgMsg( "Loading release definition for project %s version %s" % ( projectName, releaseVersion ) )
    if projectName not in self.__configs:
      result = self.getTarsLocation( projectName )
      if not result[ 'OK' ]:
        return result
      relcfgLoc = "%s/releases-%s-%s.cfg" % ( result[ 'Value' ], projectName, releaseVersion )
      self.__dbgMsg( "Releases file is %s" % relcfgLoc )
      result = self.__loadCFGFromURL( relcfgLoc )
      if not result[ 'OK' ]:
        return result
      self.__configs[ projectName ] = result[ 'Value' ]
      self.__dbgMsg( "Loaded %s" % relcfgLoc )
      self.__projectsLoaded.append( projectName )
    deps = self.getReleaseDependencies( projectName, releaseVersion )
    for dProj in deps:
      self.__dbgMsg( "%s:%s requires on %s:%s to be installed" % ( projectName, releaseVersion, dProj, dVer ) )
      dVer = deps[ dProj ]
      result = self.loadProjectForInstall( dVer, dProj )
      if not result[ 'OK' ]:
        return result

    return S_OK()

  def __getOpt( self, projectName, option ):
    try:
      return self.__configs[ projectName ].get( option )
    except ValueError:
      self.__dbgMsg( "Missing option %s for project %s" % ( option, projectName ) )
      return False

  def getCFG( self, projectName ):
    return self.__configs[ projectName ]

  def getReleaseDependencies( self, projectName, releaseVersion ):
    if not self.__configs[ projectName ].isOption( "%s/depends" % projectName ):
      return {}
    deps = {}
    for field in self.__configs[ projectName ].get( "%s/depends" % projectName ).split( "," ):
      field = field.strip()
      if not field:
        continue
      pv = field.split( ":" )
      if len( pv ) == 1:
        deps[ pv[0].strip() ] = releaseVersion
      else:
        dep[ pv[0].strip() ] = ":".join( pv[1:] ).strip()
    return deps

  def getModulesForRelease( self, releaseVersion, projectName = False ):
    if not projectName:
      projectName = self.__projectName
    if not projectName in self.__configs:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % projectName )
    config = self.__configs[ projectName ]
    if not config.isSection( "releases/%s" % releaseVersion ):
      return S_ERROR( "Release %s is not defined for project %s" % ( releaseVersion, projectName ) )
    #Defined Modules explicitly in the release
    modules = self.__getOpt( projectName, "%s/modules" % releaseVersion )
    if modules:
      dMods = {}
      for entry in [ entry.split( ":" ) for entry in modules.split( "," ) if entry.strip() ]:
        if len( entry ) == 1:
          dMods[ entry[0].strip() ] = releaseVersion
        else:
          dMods[ entry[0].strip() ] = entry[1].strip()
      modules = dMods
    else:
      #Default modules with the same version as the release version
      modules = self.__getOpt( projectName, "defaultModules" )
      if modules:
        modules = dict( [ ( modName.strip() , releaseVersion ) for modName in modules.split( "," ) if modName.strip() ] )
      else:
        #Mod = projectName and same version
        modules = { projectName : releaseVersion }
    #Check projectName is in the modNames if not DIRAC
    if projectName != "DIRAC":
      for modName in modules:
        if modName.find( projectName ) != 0:
          S_ERROR( "Module %s does not start with the project name %s" ( modName, projectName ) )
    return S_OK( modules )

  def getModSource( self, modName ):
    if not self.__projectName in self.__configs:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % self.__projectName )
    modLocation = self.__getOpt( self.__projectName, "sources/%s" % modName )
    if not modLocation:
      return S_ERROR( "Source origin for module %s is not defined" % modName )
    modTpl = [ field.strip() for field in modLocation.split( "|" ) if field.strip() ]
    if len( modTpl ) == 1:
      return S_OK( ( False, modTpl[0] ) )
    return S_OK( ( modTpl[0], modTpl[1] ) )

  def getExtenalsVersion( self, releaseVersion = False ):
    if 'DIRAC' not in self.__configs:
      return False
    if not releaseVersion:
      if 'DIRAC' not in self.__depsLoaded:
        return False
      releaseVersion = self.__depsLoaded[ 'DIRAC' ]
    return self.__configs[ 'DIRAC' ].get( 'releases/%s/externals' % releaseVersion )

  def getModulesToInstall( self, extraModules = False ):
    if not extraModules:
      extraModules = []
    extraFound = []
    modsToInstall = {}
    for projectName in self.__projectsLoaded:
      if projectName not in self.__depsLoaded:
        continue
      result = self.getTarsLocation( projectName )
      if not result[ 'OK' ]:
        return result
      tarsPath = result[ 'Value' ]
      relVersion = self.__depsLoaded[ projectName ]
      self.__dbgMsg( "Discovering modules to install for project %s (%s)" % ( projectName, relVersion ) )
      result = self.getModulesForRelease( relVersion )
      if not result[ 'OK' ]:
        return result
      modVersions = result[ 'Value' ]
      modNames = []
      defaultMods = self.__configs[ projectName ].get( "defaultModules" )
      if defaultMods:
        modNames += [ mod.strip() for mod in defaultMods.split( "," ) if mod.strip() ]
      for extraMod in extraModules:
        if extraMod in modVersions:
          modNames.append( extraMod )
          extraFound.append( extraMod )
        if projectName != 'DIRAC':
          dExtraMod = "%sDIRAC" % extraMod
          if dExtraMod in modVersions:
            modNames.append( dExtraMod )
            extraFound.append( extraMod )
      modNameVer = [ "%s:%s" % ( modName, modVersions[ modName ] ) for modName in modNames ]
      self.__dbgMsg( "Modules to be installed for project %s are: %s" % ( projectName, ", ".join( modNameVer ) ) )
      for modName in modNames:
        modsToInstall[ modName ] = ( tarsPath, modVersions[ modName ] )

    for modName in extraModules:
      if modName not in extraFound:
        return S_ERROR( "No module %s defined. You sure it's defined for this release?" % modName )

    return S_OK( modsToInstall )


###
# End of ReleaseConfig
###


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

def logNOTICE( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [NOTICE]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

def urlretrieveTimeout( url, fileName, timeout = 0 ):
  """
   Retrieve remote url to local file, with timeout wrapper
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

def downloadAndExtractTarball( tarsURL, pkgMod, targetPath, checkHash = True ):
  tarName = "%s.tar.gz" % ( pkgMod )
  tarPath = os.path.join( cliParams.targetPath, tarName )
  try:
    if not urlretrieveTimeout( "%s/%s" % ( tarsURL, tarName ), tarPath, 300 ):
      logERROR( "Cannot download %s" % tarName )
      return False
  except Exception, e:
    logERROR( "Cannot download %s: %s" % ( tarName, str( e ) ) )
    sys.exit( 1 )
  if checkHash:
    md5Name = "%s.md5" % ( pkgMod )
    md5Path = os.path.join( cliParams.targetPath, md5Name )
    try:
      if not urlretrieveTimeout( "%s/%s" % ( tarsURL, md5Name ), md5Path, 300 ):
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
    logNOTICE( "Executing %s..." % scriptPath )
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
            ( 't:', 'project=', 'Project to install' ),
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

def usage():
  print "Usage %s <opts> <cfgFile>" % sys.argv[0]
  for cmdOpt in cmdOpts:
    print " %s %s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
  print
  print "Known options and default values from /defaults section of releases file"
  for options in [ ( 'release', cliParams.release ),
                   ( 'packagesToInstall', [] ),
                   ( 'externalsType', cliParams.externalsType ),
                   ( 'pythonVersion', cliParams.pythonVersion ),
                   ( 'lcgVer', cliParams.lcgVer ),
                   ( 'useVersionsDir', cliParams.useVersionsDir ),
                   ( 'buildExternals', cliParams.buildExternals ),
                   ( 'buildIfNotAvailable', cliParams.buildIfNotAvailable ),
                   ( 'debug', cliParams.debug ) ]:
    print " %s = %s" % options

  sys.exit( 1 )


def loadConfiguration():

  optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )

  # First check if the project name is defined
  for o, v in optList:
    if o in ( '-h', '--help' ):
      usage()
    elif o in ( '-t', '--project' ):
      cliParams.project = v
    elif o in ( "-d", "--debug" ):
      cliParams.debug = True

  releaseConfig = ReleaseConfig( cliParams.project )
  if cliParams.debug:
    releaseConfig.setDebugCB( logDEBUG )
  result = releaseConfig.loadDefaults()
  if not result[ 'OK' ]:
    logERROR( "Cold not load defaults" )


  for opName in ( 'packagesToInstall', 'release', 'externalsType', 'pythonVersion',
                  'buildExternals', 'buildIfNotAvailable', 'debug' , 'lcgVer', 'useVersionsDir' ):
    opVal = releaseConfig.getDefaultValue( "default/%s" % opName )
    if opVal == None:
      continue
    if type( getattr( cliParams, opName ) ) == types.StringType:
      cliParams.opName = opVal
    elif type( getattr( cliParams, opName ) ) == types.BooleanType:
      cliParams.opName = opVal.lower() in ( "y", "yes", "true", "1" )
    elif type( getattr( cliParams, opName ) ) == types.LisType:
      cliParams.opName = [ opV.strip() for opV in opVal.split( "," ) if opV ]

  #Now parse the ops
  for o, v in optList:
    if o in ( '-r', '--release' ):
      cliParams.release = v
    elif o in ( '-e', '--extraPackages' ):
      for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
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
    elif o in ( '-u', '--baseURL' ):
      cliParams.downBaseURL = v
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
    elif o in ( "-B", '--buildIfNotAvailable' ):
      cliParams.buildIfNotAvailable = True

  if not cliParams.release:
    logERROR( "Missing release to install" )
    usage()

  if cliParams.useVersionsDir:
    # install under <installPath>/versions/<version>_<timestamp>
    cliParams.basePath = cliParams.targetPath
    cliParams.targetPath = os.path.join( cliParams.targetPath, 'versions', '%s_%s' % ( cliParams.release, int( time.time() ) ) )
    try:
      os.makedirs( cliParams.targetPath )
    except:
      pass

  logNOTICE( "Destination path for installation is %s" % cliParams.targetPath )

  result = releaseConfig.loadProjectForInstall( cliParams.release )
  if not result[ 'OK' ]:
    return result

  return S_OK( releaseConfig )


def installExternals( externalsVersion ):
  if not cliParams.platform:
    platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
    platFD = open( platformPath, "r" )
    Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
    platFD.close()
    cliParams.platform = Platform.getPlatformString()

  logNOTICE( "Using platform: %s" % cliParams.platform )
  extName = "Externals-%s-%s-%s-python%s" % ( cliParams.externalsType, externalsVersion, cliParams.platform, cliParams.pythonVersion )
  logDEBUG( "Externals %s are to be installed" % extName )
  if not downloadAndExtractTarball( releaseConfig.getTarsLocation( 'DIRAC' )[ 'Value' ], extName, cliParams.targetPath ):
    return False
  logNOTICE( "Fixing externals paths..." )
  fixBuildPaths()
  logNOTICE( "Runnning externals post install..." )
  runExternalsPostInstall()
  checkPlatformAliasLink()
  #TODO: If externals NOT there, compile them if necessary
  return True



















































if __name__ == "__main__":
  logNOTICE( "Processing installation requirements" )
  result = loadConfiguration()
  if not result[ 'OK' ]:
    logERROR( result[ 'Value' ] )
    sys.exit( 1 )
  releaseConfig = result[ 'Value' ]
  logNOTICE( "Discovering modules to install" )
  result = releaseConfig.getModulesToInstall()
  if not result[ 'OK' ]:
    logERROR( result[ 'Value' ] )
    sys.exit( 1 )
  modsToInstall = result[ 'Value' ]
  logNOTICE( "Installing modules..." )
  for modName in modsToInstall:
    tarURL, modVersion = modsToInstall[ modName ]
    pkgMod = "%s-%s" % ( modName, modVersion )
    logNOTICE( "Installing %s" % pkgMod )
    if not downloadAndExtractTarball( tarURL, pkgMod, cliParams.targetPath ):
      sys.exit( 1 )
  logNOTICE( "Installing externals..." )
  externalsVersion = releaseConfig.getExtenalsVersion()
  if not externalsVersion:
    logERROR( "No externals defined" )
    sys.exit( 1 )
  if not installExternals( externalsVersion ):
    sys.exit( 1 )


sys.exit( 0 )




# First check if -V option is set to attempt retrieval of defaults.cfg

for o, v in optList:
  if o in ( '-h', '--help' ):
    usage()
  elif o in ( '-V', '--virtualOrganization' ):
    cliParams.vo = v

#Load CFG  
#downloadFileFromSVN( "DIRAC/trunk/DIRAC/Core/Utilities/CFG.py", cliParams.targetPath, False, [ '' ] )
#cfgPath = os.path.join( cliParams.targetPath , "CFG.py" )
#cfgFD = open( cfgPath, "r" )
#CFG = imp.load_module( "CFG", cfgFD, cfgPath, ( "", "r", imp.PY_SOURCE ) )
#cfgFD.close()

optCfg = CFG()

defCfgFile = "defaults.cfg"
defaultsURL = "%s/%s" % ( cliParams.downBaseURL, defCfgFile )
logNOTICE( "Getting defaults from %s" % defaultsURL )
try:
  urlretrieveTimeout( defaultsURL, defCfgFile, 30 )
  # when all defaults are move to use LocalInstallation Section the next 2 lines can be removed
  defCfg = CFG().loadFromFile( defCfgFile )
  optCfg = defCfg
  if defCfg.isSection( 'LocalInstallation' ):
    optCfg = optCfg.mergeWith( defCfg['LocalInstallation'] )
except Exception, e:
  logNOTICE( "Cannot download default release version: %s" % ( str( e ) ) )

if cliParams.vo:
  voCfgFile = '%s_defaults.cfg' % cliParams.vo
  voURL = "%s/%s" % ( cliParams.downBaseURL, voCfgFile )
  logNOTICE( "Getting defaults from %s" % voURL )
  try:
    urlretrieveTimeout( voURL, voCfgFile, 30 )
    voCfg = CFG().loadFromFile( voCfgFile )
    # when all defaults are move to use LocalInstallation Section the next 5 lines can be removed
    if not optCfg:
      optCfg = voCfg
    else:
      optCfg = optCfg.mergeWith( voCfg )
    if voCfg.isSection( 'LocalInstallation' ):
      optCfg = optCfg.mergeWith( voCfg['LocalInstallation'] )
  except Exception, e:
    logNOTICE( "Cannot download VO default release version: %s" % ( str( e ) ) )

for arg in args:
  if not arg[-4:] == ".cfg":
    continue
  cfg = CFG().loadFromFile( arg )
  if not cfg.isSection( 'LocalInstallation' ):
    continue
  if not optCfg:
    optCfg = cfg['LocalInstallation']
    continue
  optCfg = optCfg.mergeWith( cfg['LocalInstallation'] )

cliParams.release = optCfg.getOption( 'Release', cliParams.release )
cliParams.packagesToInstall.extend( optCfg.getOption( 'Extensions', [] ) )
cliParams.externalsType = optCfg.getOption( 'InstallType', cliParams.externalsType )
cliParams.pythonVersion = optCfg.getOption( 'PythonVersion', cliParams.pythonVersion )
cliParams.platform = optCfg.getOption( 'Platform', cliParams.platform )
cliParams.targetPath = optCfg.getOption( 'TargetPath', cliParams.targetPath )
cliParams.buildExternals = optCfg.getOption( 'BuildExternals', cliParams.buildExternals )
cliParams.lcgVer = optCfg.getOption( 'LcgVer', cliParams.lcgVer )
cliParams.downBaseURL = optCfg.getOption( 'BaseURL', cliParams.downBaseURL )
cliParams.useVersionsDir = optCfg.getOption( 'UseVersionsDir', cliParams.useVersionsDir )


for o, v in optList:
  if o in ( '-r', '--release' ):
    cliParams.release = v
  elif o in ( '-e', '--extraPackages' ):
    for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
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
  elif o in ( '-u', '--baseURL' ):
    cliParams.downBaseURL = v
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


# Make sure Extensions are not duplicated and have the full name
pkgList = cliParams.packagesToInstall
cliParams.packagesToInstall = []
for pkg in pkgList:
  pl = pkg.split( '@' )
  if pl[0] != 'Web':
    iPos = pl[0].find( "DIRAC" )
    if iPos == -1 or iPos != len( pl[0] ) - 5:
      pl[0] = "%sDIRAC" % pl[0]
  pkg = "@".join( pl )
  if pkg not in cliParams.packagesToInstall:
    cliParams.packagesToInstall.append( pkg )

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
mainCFG = CFG().loadFromFile( cfgLocation )

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
  logNOTICE( "Installing package %s version %s" % ( package, packageVersion ) )
  if not downloadAndExtractTarball( "%s-%s" % ( package, packageVersion ), cliParams.targetPath ):
    sys.exit( 1 )
  if moduleDIRACRe.match( package ):
    initFilePath = os.path.join( cliParams.targetPath, package, "__init__.py" )
    if not os.path.isfile( initFilePath ):
      fd = open( initFilePath, "w" )
      fd.write( "#Generated by dirac-install\n" )
      fd.close()
  postInstallScript = os.path.join( cliParams.targetPath, package, 'dirac-postInstall.py' )
  if os.path.isfile( postInstallScript ):
    os.chmod( postInstallScript , executablePerms )
    logNOTICE( "Executing %s..." % postInstallScript )
    if os.system( "python '%s' > '%s.out' 2> '%s.err'" % ( postInstallScript,
                                                           postInstallScript,
                                                           postInstallScript ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( postInstallScript,
                                                                       postInstallScript ) )
      sys.exit( 1 )

#Deploy scripts :)
os.system( os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-deploy-scripts.py" ) )

#Do we have a platform defined?
if not cliParams.platform:
  platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
  platFD = open( platformPath, "r" )
  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  cliParams.platform = Platform.getPlatformString()

logNOTICE( "Using platform: %s" % cliParams.platform )

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
buildCmd = "%s -t '%s' -D '%s' -v '%s' -i '%s'" % ( buildCmd, cliParams.externalsType,
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
  dirs = [ cliParams.targetPath, os.getcwd() ]
  if cliParams.useVersionsDir:
    dirs.append( cliParams.basePath )
  for dir in dirs:
    filePath = os.path.join( dir, file )
    if os.path.isfile( filePath ):
      os.unlink( filePath )


proPath = cliParams.targetPath
if cliParams.useVersionsDir:
  oldPath = os.path.join( cliParams.basePath, 'old' )
  proPath = os.path.join( cliParams.basePath, 'pro' )
  try:
    if os.path.exists( proPath ):
      if os.path.exists( oldPath ):
        os.unlink( oldPath )
      os.rename( proPath, oldPath )
    os.symlink( cliParams.targetPath, proPath )
    for dir in ['startup', 'runit', 'data', 'work', 'control', 'sbin', 'etc']:
      fake = os.path.join( cliParams.targetPath, dir )
      real = os.path.join( cliParams.basePath, dir )
      if not os.path.exists( real ):
        os.makedirs( real )
      os.symlink( real, fake )
  except Exception, x:
    logERROR( str( x ) )
    sys.exit( 1 )

# Now create bashrc at basePath
try:
  bashrcFile = os.path.join( cliParams.targetPath, 'bashrc' )
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join( cliParams.basePath, 'bashrc' )
  logNOTICE( 'Creating %s' % bashrcFile )
  if not os.path.exists( bashrcFile ):
    lines = [ '# DIRAC bashrc file, used by service and agent run scripts to set environment',
              'export PYTHONUNBUFFERED=yes',
              'export PYTHONOPTIMIZE=x' ]
    if 'HOME' in os.environ:
      lines.append( '[ -z "$HOME" ] && export HOME=%s' % os.environ['HOME'] )
    if 'X509_CERT_DIR' in os.environ:
      lines.append( 'export X509_CERT_DIR=%s' % os.environ['X509_CERT_DIR'] )
    lines.append( 'export X509_VOMS_DIR=%s' % os.path.join( proPath, 'etc', 'grid-security', 'vomsdir' ) )
    lines.extend( ['# Some DIRAC locations',
                   'export DIRAC=%s' % proPath,
                   'export DIRACBIN=%s' % os.path.join( proPath, cliParams.platform, 'bin' ),
                   'export DIRACSCRIPTS=%s' % os.path.join( proPath, 'scripts' ),
                   'export DIRACLIB=%s' % os.path.join( proPath, cliParams.platform, 'lib' ),
                   'export TERMINFO=%s' % os.path.join( proPath, cliParams.platform, 'share', 'terminfo' ),
                   'export RRD_DEFAULT_FONT=%s' % os.path.join( proPath, cliParams.platform, 'share', 'rrdtool', 'fonts', 'DejaVuSansMono-Roman.ttf' ) ] )

    lines.extend( ['# Clear the PYTHONPATH and the LD_LIBRARY_PATH',
                  'PYTHONPATH=""',
                  'LD_LIBRARY_PATH=""'] )

    lines.extend( ['( echo $PATH | grep -q $DIRACBIN ) || export PATH=$DIRACBIN:$PATH',
                   '( echo $PATH | grep -q $DIRACSCRIPTS ) || export PATH=$DIRACSCRIPTS:$PATH',
                   'export LD_LIBRARY_PATH=$DIRACLIB:$DIRACLIB/mysql',
                   'export PYTHONPATH=$DIRAC'] )
    lines.append( '' )
    f = open( bashrcFile, 'w' )
    f.write( '\n'.join( lines ) )
    f.close()
except Exception, x:
 logERROR( str( x ) )
 sys.exit( 1 )

logNOTICE( "DIRAC release %s successfully installed" % cliParams.release )
sys.exit( 0 )
