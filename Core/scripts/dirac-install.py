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
  import hashlib as md5
except:
  import md5

def S_OK( value = "" ):
  return { 'OK' : True, 'Value' : value }

def S_ERROR( msg = "" ):
  return { 'OK' : False, 'Message' : msg }

g_GlobalDefaultsLoc = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/globalDefaults.cfg"

############
# Start of CFG
############


class Params:

  def __init__( self ):
    self.extraPackages = []
    self.project = 'DIRAC'
    self.installation = 'DIRAC'
    self.release = ""
    self.externalsType = 'client'
    self.pythonVersion = '26'
    self.platform = ""
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.noAutoBuild = False
    self.debug = False
    self.externalsOnly = False
    self.lcgVer = ''
    self.useVersionsDir = False
    self.installSource = ""

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
      try:
        self.__parse( cfgData )
      except:
        import traceback
        traceback.print_exc()
        raise
      return self

    def getChild( self, path ):
      child = self
      if type( path ) in ( types.ListType, types.TupleType ):
        pathList = path
      else:
        pathList = [ sec.strip() for sec in path.split( "/" ) if sec.strip() ]
      for childName in pathList:
        if childName not in child.__children:
          return False
        child = child.__children[ childName ]
      return child

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
        if not line or line[0] == "#":
          continue
        if line.find( "+=" ) > -1:
          fields = line.split( "+=" )
          opName = fields[0].strip()
          if opName in self.__data:
            self.__data[ opName ] = "+=".join( fields[1:] ).strip()
          else:
            self.__data[ opName ].append( "+=".join( fields[1:] ).strip() )
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
    
    def getOptions( self, path = "" ):
      parentPath = [ sec.strip() for sec in path.split("/") if sec.strip() ][:-1]
      if parentPath:
        parent = getChild( parentPath )
      else:
        parent = self
      if not parent:
        return []
      return tuple( parent.__data )
    
    def delPath(self, path ):
      path = [ sec.strip() for sec in path.split("/") if sec.strip() ]
      if not path:
        return
      keyName = path[ -1 ]
      parentPath = path[:-1]
      if parentPath:
        parent = getChild( parentPath )
      else:
        parent = self
      if parent:
        parent.__data.pop( keyName )

    #END OF CFG CLASS

  def __init__( self, defaultObjectName ):
    self.__configs = {}
    self.__globalDefaults = ReleaseConfig.CFG()
    self.__localDefaults = ReleaseConfig.CFG()
    self.__defaults = {}
    self.__depsLoaded = {}
    self.__projectsLoaded = []
    self.__debugCB = False
    self.__defaultObject = defaultObjectName
    self.__projectRelease = {
        'DIRAC' : "http://svnweb.cern.ch/guest/dirac/DIRAC/trunk/DIRAC/releases.cfg",
        'LHCb' : "http://svnweb.cern.ch/guest/lbdirac/LHCbDIRAC/trunk/LHCbDIRAC/releases.cfg"
        }
    self.__projectTarLocation = {
        'DIRAC' : "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/installSource",
        'LHCb' : "http://lhcbproject.web.cern.ch/lhcbproject/dist/LHCbDirac_project"
        }

  def getDefaultObject( self ):
    return self.__defaultObject

  def setDefaultObject( self, objectName ):
    self.__defaultObject = objectName

  def setDebugCB( self, debFunc ):
    self.__debugCB = debFunc

  def __dbgMsg( self, msg ):
    if self.__debugCB:
      self.__debugCB( msg )

  def loadDefaults( self ):
    self.__dbgMsg( "Loading global defaults from: %s" % g_GlobalDefaultsLoc )
    result = self.__loadCFGFromURL( g_GlobalDefaultsLoc )
    if not result[ 'OK' ]:
      return result
    self.__globalDefaults = result[ 'Value' ]
    self.__dbgMsg( "Loaded global defaults" )
    return self.__loadObjectDefaults( self.__defaultObject )

  def __loadObjectDefaults( self, objectName ):
    if objectName in self.__defaults:
      return S_OK()
    self.__defaults[ objectName ] = ReleaseConfig.CFG()

    aliasTo = self.getDefaultValue( "Alias", objectName )
    #If not link to then load defaults
    if not aliasTo:
      self.__dbgMsg( "Loading defaults for %s" % objectName )
      try:
        defaultsLocation = self.__globalDefaults.get( "%s/DefaultsLocation" % objectName )
      except ValueError:
        defaultsLocation = False
        self.__dbgMsg( "No defaults file defined for %s" % objectName )

      if defaultsLocation:
        self.__dbgMsg( "Defaults for %s are in %s" % ( objectName, defaultsLocation ) )
        result = self.__loadCFGFromURL( defaultsLocation )
        if not result[ 'OK' ]:
          return result
        self.__defaults[ objectName ] = result[ 'Value' ]
        self.__dbgMsg( "Loaded defaults for %s" % objectName )
        #Update link to var
        aliasTo = self.getDefaultValue( "Alias", objectName )

    if aliasTo:
      if self.__defaultObject == objectName:
        self.__defaultObject = aliasTo
      return self.__loadObjectDefaults( aliasTo )

    #If a default is there, load the defaults
    defaultProject = self.getDefaultValue( "LocalInstallation/Project", objectName )
    if defaultProject:
      return self.__loadObjectDefaults( defaultProject )

    return S_OK()

  def loadLocalDefaults( self, fileName ):
    try:
      fd = open( fileName, "r" )
      self.__localDefaults.parse( fd.read() )
      fd.close()
    except Exception, excp :
      return S_ERROR( "Could not load %s: %s" % ( fileName, excp ) )
    return S_OK()
  
  def getDefaultCFG(self, objectName = False ):
    if not objectName:
      objectName = self.__defaultObject
    if objectName in self.__defaults:
      return self.__defaults[ objectName ]
    return None

  def getDefaultValue( self, opName, objectName = False ):
    try:
      return self.__localDefaults.get( opName )
    except:
      pass
    if not objectName:
      objectName = self.__defaultObject
    try:
      return self.__defaults[ objectName ].get( opName )
    except:
      pass
    try:
      return self.__globalDefaults.get( "%s/%s" % ( objectName, opName ) )
    except:
      pass
    return None

  def isProjectLoaded( self, objectName ):
    return objectName in self.__configs

  #HERE!

  def __loadCFGFromURL( self, urlcfg, checkHash = False ):
    try:
      cfgFile = urllib2.urlopen( urlcfg )
    except:
      return S_ERROR( "Could not open %s" % urlcfg )
    try:
      cfgData = cfgFile.read()
      cfg = ReleaseConfig.CFG( cfgData )
    except Exception, excp:
      return S_ERROR( "Could not parse %s: %s" % ( urlcfg, excp ) )
    cfgFile.close()
    if not checkHash:
      return S_OK( cfg )
    try:
      md5File = urllib2.urlopen( urlcfg[:-4] + ".md5" )
      md5Hex = md5File.read().strip()
      md5File.close()
      if md5Hex != md5.md5( cfgData ).hexdigest():
        return S_ERROR( "Hash check failed on %s" % urlcfg )
    except Exception, excp:
      return S_ERROR( "Hash check failed on %s: %s" % ( urlcfg, excp ) )
    return S_OK( cfg )

  def loadProjectForRelease( self, releasesLocation = False ):
    if self.__defaultObject in self.__configs:
      return S_OK()
    if releasesLocation:
      releasesLocation = "file://%s" % os.path.realpath( releasesLocation )
    elif self.getDefaultValue( "Releases" ):
      releasesLocation = self.getDefaultValue( "Releases" )
    elif self.__defaultObject not in self.__projectRelease:
      return S_ERROR( "Don't know how to find releases.cfg for %s" % self.__defaultObject )
    else:
      releasesLocation = self.__projectRelease[ self.__defaultObject ]
    self.__dbgMsg( "Releases definition is %s" % releasesLocation )
    #Load it
    result = self.__loadCFGFromURL( releasesLocation )
    if not result[ 'OK' ]:
      return result
    self.__configs[ self.__defaultObject ] = result[ 'Value' ]
    self.__projectsLoaded.append( self.__defaultObject )
    self.__dbgMsg( "Loaded %s" % releasesLocation )
    return S_OK()

  def getTarsLocation( self, objectName = "" ):
    if not objectName:
      objectName = self.__defaultObject
    defLoc = self.getDefaultValue( "LocalInstallation/BaseURL", objectName )
    if defLoc:
      return S_OK( defLoc )
    elif objectName in self.__projectTarLocation:
      return S_OK( self.__projectTarLocation[ objectName ] )
    return S_ERROR( "Don't know how to find the installation tarballs for %s" % objectName )


  def loadProjectForInstall( self, releaseVersion, objectName = "", sourceURL = False ):
    if not objectName:
      defProject = self.getDefaultValue( "LocalInstallation/Project" )
      if defProject:
        objectName = defProject
      else:
        objectName = self.__defaultObject

    #Check what's loaded
    if objectName not in self.__depsLoaded:
      self.__depsLoaded[ objectName ] = releaseVersion
    elif self.__depsLoaded[ objectName ] != releaseVersion:
      S_ERROR( "Oops. Project %s (%s) is required already in a different version (%s)!" % ( objectName,
                                                                                            releaseVersion,
                                                                                            self.__depsLoaded[ objectName ] ) )
    else:
      return S_OK()
    #Load defaults
    result = self.__loadObjectDefaults( objectName )
    if not result[ 'OK' ]:
      self.__dbgMsg( "Could not load defaults for %s" % objectName )
      return result
    #Load the release definitions
    self.__dbgMsg( "Loading release definition for %s version %s" % ( objectName, releaseVersion ) )
    if objectName not in self.__configs:
      if not sourceURL:
        result = self.getTarsLocation( objectName )
        if not result[ 'OK' ]:
          return result
        siu = result[ 'Value' ]
      else:
        siu = sourceURL
      relcfgLoc = "%s/release-%s-%s.cfg" % ( siu, objectName, releaseVersion )
      self.__dbgMsg( "Releases file is %s" % relcfgLoc )
      result = self.__loadCFGFromURL( relcfgLoc, checkHash = True )
      if not result[ 'OK' ]:
        return result
      self.__configs[ objectName ] = result[ 'Value' ]
      self.__dbgMsg( "Loaded %s" % relcfgLoc )
      self.__projectsLoaded.append( objectName )
    deps = self.getReleaseDependencies( releaseVersion, objectName )
    if deps:
      self.__dbgMsg( "Depends on %s" % ", ".join( [ "%s:%s" % ( k, deps[k] ) for k in deps ] ) )
    for dProj in deps:
      dVer = deps[ dProj ]
      self.__dbgMsg( "%s:%s requires on %s:%s to be installed" % ( objectName, releaseVersion, dProj, dVer ) )
      dVer = deps[ dProj ]
      result = self.loadProjectForInstall( dVer, dProj, sourceURL = sourceURL )
      if not result[ 'OK' ]:
        return result

    return S_OK()

  def __getOpt( self, objectName, option ):
    try:
      return self.__configs[ objectName ].get( option )
    except ValueError:
      self.__dbgMsg( "Missing option %s for %s" % ( option, objectName ) )
      return False

  def getCFG( self, objectName ):
    return self.__configs[ objectName ]

  def getReleaseDependencies( self, releaseVersion, objectName = False ):
    if not objectName:
      objectName = self.__defaultObject
    if not self.__configs[ objectName ].isOption( "Releases/%s/Depends" % releaseVersion ):
      return {}
    deps = {}
    for field in self.__configs[ objectName ].get( "Releases/%s/Depends" % releaseVersion ).split( "," ):
      field = field.strip()
      if not field:
        continue
      pv = field.split( ":" )
      if len( pv ) == 1:
        deps[ pv[0].strip() ] = releaseVersion
      else:
        deps[ pv[0].strip() ] = ":".join( pv[1:] ).strip()
    return deps

  def getModulesForRelease( self, releaseVersion, objectName = False ):
    if not objectName:
      objectName = self.__defaultObject
    if not objectName in self.__configs:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % objectName )
    config = self.__configs[ objectName ]
    if not config.isSection( "Releases/%s" % releaseVersion ):
      return S_ERROR( "Release %s is not defined for project %s" % ( releaseVersion, objectName ) )
    #Defined Modules explicitly in the release
    modules = self.__getOpt( objectName, "Releases/%s/Modules" % releaseVersion )
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
      modules = self.__getOpt( objectName, "DefaultModules" )
      if modules:
        modules = dict( [ ( modName.strip() , releaseVersion ) for modName in modules.split( "," ) if modName.strip() ] )
      else:
        #Mod = objectName and same version
        modules = { objectName : releaseVersion }
    #Check objectName is in the modNames if not DIRAC
    if objectName != "DIRAC":
      for modName in modules:
        if modName.find( objectName ) != 0:
          S_ERROR( "Module %s does not start with the name %s" ( modName, objectName ) )
    return S_OK( modules )

  def getModSource( self, modName ):
    if not self.__defaultObject in self.__configs:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % self.__defaultObject )
    modLocation = self.__getOpt( self.__defaultObject, "Sources/%s" % modName )
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
    try:
      return self.__configs[ 'DIRAC' ].get( 'Releases/%s/Externals' % releaseVersion )
    except ValueError:
      return False

  def getModulesToInstall( self, extraModules = False ):
    if not extraModules:
      extraModules = []
    extraFound = []
    modsToInstall = {}
    modsOrder = []
    for objectName in self.__projectsLoaded:
      if objectName not in self.__depsLoaded:
        continue
      result = self.getTarsLocation( objectName )
      if not result[ 'OK' ]:
        return result
      tarsPath = result[ 'Value' ]
      relVersion = self.__depsLoaded[ objectName ]
      self.__dbgMsg( "Discovering modules to install for %s (%s)" % ( objectName, relVersion ) )
      result = self.getModulesForRelease( relVersion, objectName )
      if not result[ 'OK' ]:
        return result
      modVersions = result[ 'Value' ]
      modNames = []
      defaultMods = self.__configs[ objectName ].get( "DefaultModules" )
      if defaultMods:
        modNames += [ mod.strip() for mod in defaultMods.split( "," ) if mod.strip() ]
      for extraMod in extraModules:
        if extraMod in modVersions:
          modNames.append( extraMod )
          extraFound.append( extraMod )
        if objectName != 'DIRAC':
          dExtraMod = "%sDIRAC" % extraMod
          if dExtraMod in modVersions:
            modNames.append( dExtraMod )
            extraFound.append( extraMod )
      modNameVer = [ "%s:%s" % ( modName, modVersions[ modName ] ) for modName in modNames ]
      self.__dbgMsg( "Modules to be installed for %s are: %s" % ( objectName, ", ".join( modNameVer ) ) )
      for modName in modNames:
        modsToInstall[ modName ] = ( tarsPath, modVersions[ modName ] )
        modsOrder.insert( 0, modName )

    for modName in extraModules:
      if modName not in extraFound:
        return S_ERROR( "No module %s defined. You sure it's defined for this release?" % modName )

    return S_OK( ( modsOrder, modsToInstall ) )


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

def downloadAndExtractTarball( tarsURL, pkgName, pkgVer, checkHash = True ):
  tarName = "%s-%s.tar.gz" % ( pkgName, pkgVer )
  tarPath = os.path.join( cliParams.targetPath, tarName )
  tarFileURL = "%s/%s" % ( tarsURL, tarName )
  logNOTICE( "Retrieving %s" % tarFileURL )
  try:
    if not urlretrieveTimeout( tarFileURL, tarPath, 300 ):
      logERROR( "Cannot download %s" % tarName )
      return False
  except Exception, e:
    logERROR( "Cannot download %s: %s" % ( tarName, str( e ) ) )
    sys.exit( 1 )
  if checkHash:
    md5Name = "%s-%s.md5" % ( pkgName, pkgVer )
    md5Path = os.path.join( cliParams.targetPath, md5Name )
    md5FileURL = "%s/%s" % ( tarsURL, md5Name )
    logNOTICE( "Retrieving %s" % md5FileURL )
    try:
      if not urlretrieveTimeout( md5FileURL, md5Path, 300 ):
        logERROR( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      logERROR( "Cannot download %s: %s" % ( md5Name, str( e ) ) )
      return False
    #Read md5  
    fd = open( os.path.join( cliParams.targetPath, md5Name ), "r" )
    md5Expected = fd.read().strip()
    fd.close()
    #Calculate md5
    md5Calculated = md5.md5()
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

  postInstallScript = os.path.join( cliParams.targetPath, pkgName, 'dirac-postInstall.py' )
  if os.path.isfile( postInstallScript ):
    os.chmod( postInstallScript , executablePerms )
    logNOTICE( "Executing %s..." % postInstallScript )
    if os.system( "python '%s' > '%s.out' 2> '%s.err'" % ( postInstallScript,
                                                           postInstallScript,
                                                           postInstallScript ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( postInstallScript,
                                                                       postInstallScript ) )
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
            ( 'l:', 'project=', 'Project to install' ),
            ( 'e:', 'extraPackages=', 'Extra packages to install (comma separated)' ),
            ( 't:', 'installType=', 'Installation type (client/server)' ),
            ( 'i:', 'pythonVersion=', 'Python version to compile (25/24)' ),
            ( 'p:', 'platform=', 'Platform to install' ),
            ( 'P:', 'installationPath=', 'Path where to install (default current working dir)' ),
            ( 'b', 'build', 'Force local compilation' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'B', 'noAutoBuild', 'Do not build if not available' ),
            ( 'v', 'useVersionsDir', 'Use versions directory' ),
            ( 'u:', 'baseURL=', "Use URL as the source for installation tarballs" ),
            ( 'd', 'debug', 'Show debug messages' ),
            ( 'V:', 'VO=', 'Virtual Organization (deprecated, use -l or --project)' ),
            ( 'X', 'externalsOnly', 'Only install external binaries' ),
            ( 'h', 'help', 'Show this help' ),
          )

def usage():
  print "Usage %s <opts> <cfgFile>" % sys.argv[0]
  for cmdOpt in cmdOpts:
    print " %s %s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
  print
  print "Known options and default values from /defaults section of releases file"
  for options in [ ( 'Release', cliParams.release ),
                   ( 'Project', cliParams.project ),
                   ( 'ModulesToInstall', [] ),
                   ( 'ExternalsType', cliParams.externalsType ),
                   ( 'PythonVersion', cliParams.pythonVersion ),
                   ( 'LcgVer', cliParams.lcgVer ),
                   ( 'UseVersionsDir', cliParams.useVersionsDir ),
                   ( 'BuildExternals', cliParams.buildExternals ),
                   ( 'NoAutoBuild', cliParams.noAutoBuild ),
                   ( 'Debug', cliParams.debug ) ]:
    print " %s = %s" % options

  sys.exit( 1 )


def loadConfiguration():

  optList, args = getopt.getopt( sys.argv[1:],
                               "".join( [ opt[0] for opt in cmdOpts ] ),
                               [ opt[1] for opt in cmdOpts ] )

  # First check if the name is defined
  for o, v in optList:
    if o in ( '-h', '--help' ):
      usage()
    elif o in ( '-V', '--installation' ):
      cliParams.installation = v
    elif o in ( "-d", "--debug" ):
      cliParams.debug = True

  releaseConfig = ReleaseConfig( cliParams.installation )
  if cliParams.debug:
    releaseConfig.setDebugCB( logDEBUG )

  for arg in args:
    if len( arg ) > 4 and arg.find( ".cfg" ) == len( arg ) - 4:
      result = releaseConfig.loadLocalDefaults( arg )
      if not result[ 'OK' ]:
        logERROR( result[ 'Message' ] )
      else:
        logNOTICE( "Loaded %s" % arg )

  result = releaseConfig.loadDefaults()
  if not result[ 'OK' ]:
    logERROR( "Could not load defaults" )


  for opName in ( 'release', 'externalsType', 'pythonVersion',
                  'buildExternals', 'noAutoBuild', 'debug' ,
                  'lcgVer', 'useVersionsDir', 'targetPath',
                  'project', 'release', 'extraPackages' ):
    opVal = releaseConfig.getDefaultValue( "LocalInstallation/%s" % ( opName[0].upper() + opName[1:] ) )
    if opVal == None:
      continue
    if type( getattr( cliParams, opName ) ) == types.StringType:
      setattr( cliParams, opName, opVal )
    elif type( getattr( cliParams, opName ) ) == types.BooleanType:
      setattr( cliParams, opName, opVal.lower() in ( "y", "yes", "true", "1" ) )
    elif type( getattr( cliParams, opName ) ) == types.ListType:
      setattr( cliParams, opName, [ opV.strip() for opV in opVal.split( "," ) if opV ] )

  #Now parse the ops
  for o, v in optList:
    if o in ( '-r', '--release' ):
      cliParams.release = v
    elif o in ( '-l', '--project' ):
      cliParams.project = v
    elif o in ( '-e', '--extraPackages' ):
      for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
        if pkg not in cliParams.extraPackages:
          cliParams.extraPackages.append( pkg )
    elif o in ( '-t', '--installType' ):
      cliParams.externalsType = v
    elif o in ( '-i', '--pythonVersion' ):
      cliParams.pythonVersion = v
    elif o in ( '-p', '--platform' ):
      cliParams.platform = v
    elif o in ( '-d', '--debug' ):
      cliParams.debug = True
    elif o in ( '-g', '--grid' ):
      cliParams.lcgVer = v
    elif o in ( '-u', '--baseURL' ):
      cliParams.installSource = v
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
    elif o in ( "-B", '--noAutoBuild' ):
      cliParams.noAutoBuild = True
    elif o in ( '-X', '--externalsOnly' ):
      cliParams.externalsOnly = True

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

  result = releaseConfig.loadProjectForInstall( cliParams.release, cliParams.project, sourceURL = cliParams.installSource )
  if not result[ 'OK' ]:
    return result

  if not releaseConfig.isProjectLoaded( "DIRAC" ):
    return S_ERROR( "DIRAC is not depended by this installation. Aborting" )

  return S_OK( releaseConfig )

def compileExternals( extVersion ):
  logNOTICE( "Compiling externals %s" % extVersion )
  buildCmd = os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-compile-externals.py" )
  buildCmd = "%s -t '%s' -D '%s' -v '%s' -i '%s'" % ( buildCmd, cliParams.externalsType,
                                                      os.path.join( cliParams.targetPath, cliParams.platform ),
                                                      extVersion,
                                                      cliParams.pythonVersion )
  if os.system( buildCmd ):
    logERROR( "Could not compile binaries" )
    return False
  return True

def installExternals( externalsVersion ):
  if not cliParams.platform:
    platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
    try:
      platFD = open( platformPath, "r" )
    except IOError:
      logERROR( "Cannot open Platform.py. Is DIRAC installed?" )
      return False

    Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
    platFD.close()
    cliParams.platform = Platform.getPlatformString()

  if cliParams.buildExternals:
    return compileExternals( externalsVersion )

  logDEBUG( "Using platform: %s" % cliParams.platform )
  extVer = "%s-%s-%s-python%s" % ( cliParams.externalsType, externalsVersion, cliParams.platform, cliParams.pythonVersion )
  logDEBUG( "Externals %s are to be installed" % extVer )
  if cliParams.installSource:
    tarsURL = cliParams.installSource
  else:
    tarsURL = releaseConfig.getTarsLocation( 'DIRAC' )[ 'Value' ]
  if not downloadAndExtractTarball( tarsURL, "Externals", extVer ):
    return ( not cliParams.noAutoBuild ) and compileExternals( externalsVersion )
  logNOTICE( "Fixing externals paths..." )
  fixBuildPaths()
  logNOTICE( "Runnning externals post install..." )
  runExternalsPostInstall()
  checkPlatformAliasLink()
  #lcg utils?
  #LCG utils if required
  if cliParams.lcgVer:
    verString = "%s-%s-python%s" % ( cliParams.lcgVer, cliParams.platform, cliParams.pythonVersion )
    #HACK: try to find a more elegant solution for the lcg bundles location
    if not downloadAndExtractTarball( tarsURL + "/../lcgBundles", "DIRAC-lcg", verString, False ):
      logERROR( "Check that there is a release for your platform: DIRAC-lcg-%s" % verString )
  return True

def createBashrc():

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
      return False

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
   return False

  return True

def writeDefaultConfiguration():
  defaultCFG = releaseConfig.getDefaultCFG()
  if not defaultCFG:
    return
  for opName in defaultCFG.getOptions():
    defaultCFG.delPath( opName )
  
  filePath = os.path.join( cliParams.targetPath, "defaults-%s.cfg" % cliParams.installation )
  try:
    fd = open( filePath, "wb" )
    fd.write( defaultCFG.toString() )
    fd.close()
  except Exception, excp:
    logERROR( "Could not write %s: %s" % ( filePath, excp ) )
  logNOTICE( "Defaults written to %s" % filePath )

if __name__ == "__main__":
  logNOTICE( "Processing installation requirements" )
  result = loadConfiguration()
  if not result[ 'OK' ]:
    logERROR( result[ 'Message' ] )
    sys.exit( 1 )
  releaseConfig = result[ 'Value' ]
  if not cliParams.externalsOnly:
    logNOTICE( "Discovering modules to install" )
    result = releaseConfig.getModulesToInstall( cliParams.extraPackages )
    if not result[ 'OK' ]:
      logERROR( result[ 'Message' ] )
      sys.exit( 1 )
    modsOrder, modsToInstall = result[ 'Value' ]
    logNOTICE( "Installing modules..." )
    for modName in modsOrder:
      tarsURL, modVersion = modsToInstall[ modName ]
      if cliParams.installSource:
        tarsURL = cliParams.installSource
      logNOTICE( "Installing %s:%s" % ( modName, modVersion ) )
      if not downloadAndExtractTarball( tarsURL, modName, modVersion ):
        sys.exit( 1 )
    logNOTICE( "Deloying scripts..." )
    ddeLocation = os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-deploy-scripts.py" )
    if os.path.isfile( ddeLocation ):
      os.system( ddeLocation )
    else:
      logDEBUG( "No dirac-deploy-scripts found. This doesn't look good" )
  else:
    logNOTICE( "Skipping installing DIRAC" )
  logNOTICE( "Installing %s externals..." % cliParams.externalsType )
  externalsVersion = releaseConfig.getExtenalsVersion()
  if not externalsVersion:
    logERROR( "No externals defined" )
    sys.exit( 1 )
  if not installExternals( externalsVersion ):
    sys.exit( 1 )
  if not createBashrc():
    sys.exit( 1 )
  writeDefaultConfiguration()
  logNOTICE( "%s properly installed" % releaseConfig.getDefaultObject() )
  sys.exit( 0 )
