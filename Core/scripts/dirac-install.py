#!/usr/bin/env python
"""
The main DIRAC installer script
"""
import sys
import os
import getopt
import urllib2
import imp
import signal
import time
import stat
import types
import shutil
import ssl
import hashlib

__RCSID__ = "$Id$"

executablePerms = stat.S_IWUSR | stat.S_IRUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH

def S_OK( value = "" ):
  return { 'OK' : True, 'Value' : value }

def S_ERROR( msg = "" ):
  return { 'OK' : False, 'Message' : msg }

############
# Start of CFG
############


class Params( object ):

  def __init__( self ):
    self.extensions = []
    self.project = 'DIRAC'
    self.installation = 'DIRAC'
    self.release = ""
    self.externalsType = 'client'
    self.pythonVersion = '27'
    self.platform = ""
    self.basePath = os.getcwd()
    self.targetPath = os.getcwd()
    self.buildExternals = False
    self.noAutoBuild = False
    self.debug = False
    self.externalsOnly = False
    self.lcgVer = ''
    self.useVersionsDir = False
    self.installSource = ""
    self.globalDefaults = False
    self.timeout = 300

cliParams = Params()

###
# Release config manager
###

class ReleaseConfig( object ):

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
            self.__data[ opName ] += ', %s' % '+='.join( fields[1:] ).strip()
          else:
            self.__data[ opName ] = '+='.join( fields[1:] ).strip()
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

    def createSection( self, name, cfg = False ):
      if type( name ) in ( types.ListType, types.TupleType ):
        pathList = name
      else:
        pathList = [ sec.strip() for sec in name.split( "/" ) if sec.strip() ]
      parent = self
      for lev in pathList[:-1]:
        if lev not in parent.__children:
          parent.__children[ lev ] = ReleaseConfig.CFG()
        parent = parent.__children[ lev ]
      secName = pathList[-1]
      if secName not in parent.__children:
        if not cfg:
          cfg = ReleaseConfig.CFG()
        parent.__children[ secName ] = cfg
      return parent.__children[ secName ]

    def isSection( self, obList ):
      return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 2

    def sections( self ):
      return [ k for k in self.__children ]

    def isOption( self, obList ):
      return self.__exists( [ ob.strip() for ob in obList.split( "/" ) if ob.strip() ] ) == 1

    def options( self ):
      return [ k for k in self.__data ]

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

    def get( self, opName, defaultValue = None ):
      try:
        value = self.__get( [ op.strip() for op in opName.split( "/" ) if op.strip() ] )
      except KeyError:
        if defaultValue != None:
          return defaultValue
        raise
      if defaultValue == None:
        return value
      defType = type( defaultValue )
      if defType == types.BooleanType:
        return value.lower() in ( "1", "true", "yes" )
      try:
        return defType( value )
      except ValueError:
        return defaultValue


    def __get( self, obList ):
      if len( obList ) == 1:
        if obList[0] in self.__data:
          return self.__data[ obList[0] ]
        raise KeyError( "Missing option %s" % obList[0] )
      if obList[0] in self.__children:
        return self.__children[ obList[0] ].__get( obList[1:] )
      raise KeyError( "Missing section %s" % obList[0] )

    def toString( self, tabs = 0 ):
      lines = [ "%s%s = %s" % ( "  " * tabs, opName, self.__data[ opName ] ) for opName in self.__data ]
      for secName in self.__children:
        lines.append( "%s%s" % ( "  " * tabs, secName ) )
        lines.append( "%s{" % ( "  " * tabs ) )
        lines.append( self.__children[ secName ].toString( tabs + 1 ) )
        lines.append( "%s}" % ( "  " * tabs ) )
      return "\n".join( lines )

    def getOptions( self, path = "" ):
      parentPath = [ sec.strip() for sec in path.split( "/" ) if sec.strip() ][:-1]
      if parentPath:
        parent = self.getChild( parentPath )
      else:
        parent = self
      if not parent:
        return []
      return tuple( parent.__data )

    def delPath( self, path ):
      path = [ sec.strip() for sec in path.split( "/" ) if sec.strip() ]
      if not path:
        return
      keyName = path[ -1 ]
      parentPath = path[:-1]
      if parentPath:
        parent = self.getChild( parentPath )
      else:
        parent = self
      if parent:
        parent.__data.pop( keyName )

    def update( self, path, cfg ):
      parent = self.getChild( path )
      if not parent:
        self.createSection( path, cfg )
        return
      parent.__apply( cfg )

    def __apply( self, cfg ):
      for k in cfg.sections():
        if k in self.__children:
          self.__children[ k ].__apply( cfg.getChild( k ) )
        else:
          self.__children[ k ] = cfg.getChild( k )
      for k in cfg.options():
        self.__data[ k ] = cfg.get( k )

############################################################################
# END OF CFG CLASS
############################################################################

  def __init__( self, instName = 'DIRAC', projectName = 'DIRAC', globalDefaultsURL = False ):

    if globalDefaultsURL:
      self.__globalDefaultsURL = globalDefaultsURL
    else:
      self.__globalDefaultsURL = "http://lhcbproject.web.cern.ch/lhcbproject/dist/DIRAC3/globalDefaults.cfg"
    self.__globalDefaults = ReleaseConfig.CFG()
    self.__loadedCfgs = []
    self.__prjDepends = {}
    self.__prjRelCFG = {}
    self.__projectsLoadedBy = {}
    self.__cfgCache = {}

    self.__debugCB = False
    self.__instName = instName
    self.__projectName = projectName

  def getInstallation( self ):
    return self.__instName

  def getProject( self ):
    return self.__projectName

  def setInstallation( self, instName ):
    self.__instName = instName

  def setProject( self, projectName ):
    self.__projectName = projectName

  def setDebugCB( self, debFunc ):
    self.__debugCB = debFunc

  def __dbgMsg( self, msg ):
    if self.__debugCB:
      self.__debugCB( msg )

  def __loadCFGFromURL( self, urlcfg, checkHash = False ):

    # This can be a local file
    if os.path.exists( urlcfg ):
      with open( urlcfg, 'r' ) as relFile:
        cfgData = relFile.read()
    else:
      if urlcfg in self.__cfgCache:
        return S_OK( self.__cfgCache[ urlcfg ] )
      try:
        cfgData = urlretrieveTimeout( urlcfg, timeout = cliParams.timeout )
        if not cfgData:
          return S_ERROR( "Could not get data from %s" % urlcfg )
      except:
        return S_ERROR( "Could not open %s" % urlcfg )
    try:
      #cfgData = cfgFile.read()
      cfg = ReleaseConfig.CFG( cfgData )
    except Exception, excp:
      return S_ERROR( "Could not parse %s: %s" % ( urlcfg, excp ) )
    #cfgFile.close()
    if not checkHash:
      self.__cfgCache[ urlcfg ] = cfg
      return S_OK( cfg )
    try:
      md5path = urlcfg[:-4] + ".md5"
      if os.path.exists( md5path ):
        md5File = open( md5path, 'r' )
        md5Data = md5File.read()
        md5File.close()
      else:
        md5Data = urlretrieveTimeout( md5path, timeout = 60 )
      md5Hex = md5Data.strip()
      #md5File.close()
      if md5Hex != hashlib.md5( cfgData ).hexdigest():
        return S_ERROR( "Hash check failed on %s" % urlcfg )
    except Exception, excp:
      return S_ERROR( "Hash check failed on %s: %s" % ( urlcfg, excp ) )
    self.__cfgCache[ urlcfg ] = cfg
    return S_OK( cfg )

  def loadInstallationDefaults( self ):
    result = self.__loadGlobalDefaults()
    if not result[ 'OK' ]:
      return result
    return self.__loadObjectDefaults( "Installations", self.__instName )

  def loadProjectDefaults( self ):
    result = self.__loadGlobalDefaults()
    if not result[ 'OK' ]:
      return result
    return self.__loadObjectDefaults( "Projects", self.__projectName )

  def __loadGlobalDefaults( self ):
    self.__dbgMsg( "Loading global defaults from: %s" % self.__globalDefaultsURL )
    result = self.__loadCFGFromURL( self.__globalDefaultsURL )
    if not result[ 'OK' ]:
      return result
    self.__globalDefaults = result[ 'Value' ]
    for k in ( "Installations", "Projects" ):
      if not self.__globalDefaults.isSection( k ):
        self.__globalDefaults.createSection( k )
    self.__dbgMsg( "Loaded global defaults" )
    return S_OK()

  def __loadObjectDefaults( self, rootPath, objectName ):
    basePath = "%s/%s" % ( rootPath, objectName )
    if basePath in self.__loadedCfgs:
      return S_OK()

    #Check if it's a direct alias
    try:
      aliasTo = self.__globalDefaults.get( basePath )
    except KeyError:
      aliasTo = False

    if aliasTo:
      self.__dbgMsg( "%s is an alias to %s" % ( objectName, aliasTo ) )
      result = self.__loadObjectDefaults( rootPath, aliasTo )
      if not result[ 'OK' ]:
        return result
      cfg = result[ 'Value' ]
      self.__globalDefaults.update( basePath, cfg )
      return S_OK()

    #Load the defaults
    if self.__globalDefaults.get( "%s/SkipDefaults" % basePath, False ):
      defaultsLocation = ""
    else:
      defaultsLocation = self.__globalDefaults.get( "%s/DefaultsLocation" % basePath, "" )

    if not defaultsLocation:
      self.__dbgMsg( "No defaults file defined for %s %s" % ( rootPath.lower()[:-1], objectName ) )
    else:
      self.__dbgMsg( "Defaults for %s are in %s" % ( basePath, defaultsLocation ) )
      result = self.__loadCFGFromURL( defaultsLocation )
      if not result[ 'OK' ]:
        return result
      cfg = result[ 'Value' ]
      self.__globalDefaults.update( basePath, cfg )

    #Check if the defaults have a sub alias
    try:
      aliasTo = self.__globalDefaults.get( "%s/Alias" % basePath )
    except KeyError:
      aliasTo = False

    if aliasTo:
      self.__dbgMsg( "%s is an alias to %s" % ( objectName, aliasTo ) )
      result = self.__loadObjectDefaults( rootPath, aliasTo )
      if not result[ 'OK' ]:
        return result
      cfg = result[ 'Value' ]
      self.__globalDefaults.update( basePath, cfg )

    self.__loadedCfgs.append( basePath )
    return S_OK( self.__globalDefaults.getChild( basePath ) )


  def loadInstallationLocalDefaults( self, fileName ):
    try:
      fd = open( fileName, "r" )
      # TODO: Merge with installation CFG
      cfg = ReleaseConfig.CFG().parse( fd.read() )
      fd.close()
    except Exception, excp :
      return S_ERROR( "Could not load %s: %s" % ( fileName, excp ) )
    self.__globalDefaults.update( "Installations/%s" % self.getInstallation(), cfg )
    return S_OK()

  def getInstallationCFG( self, instName = False ):
    if not instName:
      instName = self.__instName
    return self.__globalDefaults.getChild( "Installations/%s" % instName )

  def getInstallationConfig( self, opName, instName = False ):
    if not instName:
      instName = self.__instName
    return self.__globalDefaults.get( "Installations/%s/%s" % ( instName, opName ) )

  def isProjectLoaded( self, project ):
    return project in self.__prjRelCFG

  def getTarsLocation( self, project ):
    defLoc = self.__globalDefaults.get( "Projects/%s/BaseURL" % project, "" )
    if defLoc:
      return S_OK( defLoc )
    return S_ERROR( "Don't know how to find the installation tarballs for project %s" % project )

  def getUploadCommand( self, project = False ):
    if not project:
      project = self.__projectName
    defLoc = self.__globalDefaults.get( "Projects/%s/UploadCommand" % project, "" )
    if defLoc:
      return S_OK( defLoc )
    return S_ERROR( "No UploadCommand for %s" % project )

  def __loadReleaseConfig( self, project, release, releaseMode, sourceURL = False, relLocation = False ):
    if project not in self.__prjRelCFG:
      self.__prjRelCFG[ project ] = {}
    if release in self.__prjRelCFG[ project ]:
      self.__dbgMsg( "Release config for %s:%s has already been loaded" % ( project, release ) )
      return S_OK()

    if relLocation:
      relcfgLoc = relLocation
    else:
      if releaseMode:
        try:
          relcfgLoc = self.__globalDefaults.get( "Projects/%s/Releases" % project )
        except KeyError:
          return S_ERROR( "Missing Releases file for project %s" % project )
      else:
        if not sourceURL:
          result = self.getTarsLocation( project )
          if not result[ 'OK' ]:
            return result
          siu = result[ 'Value' ]
        else:
          siu = sourceURL
        relcfgLoc = "%s/release-%s-%s.cfg" % ( siu, project, release )
    self.__dbgMsg( "Releases file is %s" % relcfgLoc )
    result = self.__loadCFGFromURL( relcfgLoc, checkHash = not releaseMode )
    if not result[ 'OK' ]:
      return result
    self.__prjRelCFG[ project ][ release ] = result[ 'Value' ]
    self.__dbgMsg( "Loaded releases file %s" % relcfgLoc )

    return S_OK( self.__prjRelCFG[ project ][ release ] )

  def getReleaseCFG( self, project, release ):
    return self.__prjRelCFG[ project ][ release ]

  def dumpReleasesToPath( self, path ):
    for project in self.__prjRelCFG:
      prjRels = self.__prjRelCFG[ project ]
      for release in prjRels:
        self.__dbgMsg( "Dumping releases file for %s:%s" % ( project, release ) )
        fd = open( os.path.join( cliParams.targetPath, "releases-%s-%s.cfg" % ( project, release ) ), "w" )
        fd.write( prjRels[ release ].toString() )
        fd.close()

  def __checkCircularDependencies( self, key, routePath = False ):
    if not routePath:
      routePath = []
    if key not in self.__projectsLoadedBy:
      return S_OK()
    routePath.insert( 0, key )
    for lKey in self.__projectsLoadedBy[ key ]:
      if lKey in routePath:
        routePath.insert( 0, lKey )
        route = "->".join( [ "%s:%s" % sKey for sKey in routePath ] )
        return S_ERROR( "Circular dependency found for %s: %s" % ( "%s:%s" % lKey, route ) )
      result = self.__checkCircularDependencies( lKey, routePath )
      if not result[ 'OK' ]:
        return result
    routePath.pop( 0 )
    return S_OK()


  def loadProjectRelease( self, releases, project = False, sourceURL = False, releaseMode = False, relLocation = False ):
    if not project:
      project = self.__projectName

    if type( releases ) not in ( types.ListType, types.TupleType ):
      releases = [ releases ]

    #Load defaults
    result = self.__loadObjectDefaults( "Projects", project )
    if not result[ 'OK' ]:
      self.__dbgMsg( "Could not load defaults for project %s" % project )
      return result

    if project not in self.__prjDepends:
      self.__prjDepends[ project ] = {}

    for release in releases:
      self.__dbgMsg( "Processing dependencies for %s:%s" % ( project, release ) )
      result = self.__loadReleaseConfig( project, release, releaseMode, sourceURL, relLocation )
      if not result[ 'OK' ]:
        return result
      relCFG = result[ 'Value' ]


      #Calculate dependencies and avoid circular deps
      self.__prjDepends[ project ][ release ] = [ ( project, release ) ]
      relDeps = self.__prjDepends[ project ][ release ]

      if not relCFG.getChild( "Releases/%s" % ( release ) ): # pylint: disable=no-member
        return S_ERROR( "Release %s is not defined for project %s in the release file" % ( release, project ) )

      initialDeps = self.getReleaseDependencies( project, release )
      if initialDeps:
        self.__dbgMsg( "%s %s depends on %s" % ( project, release, ", ".join( [ "%s:%s" % ( k, initialDeps[k] ) for k in initialDeps ] ) ) )
      relDeps.extend( [ ( p, initialDeps[p] ) for p in initialDeps ] )
      for depProject in initialDeps:
        depVersion = initialDeps[ depProject ]

        #Check if already processed
        dKey = ( depProject, depVersion )
        if dKey not in self.__projectsLoadedBy:
          self.__projectsLoadedBy[ dKey ] = []
        self.__projectsLoadedBy[ dKey ].append( ( project, release ) )
        result = self.__checkCircularDependencies( dKey )
        if not result[ 'OK' ]:
          return result
        #if it has already been processed just return OK
        if len( self.__projectsLoadedBy[ dKey ] ) > 1:
          return S_OK()

        #Load dependencies and calculate incompatibilities
        result = self.loadProjectRelease( depVersion, project = depProject )
        if not result[ 'OK' ]:
          return result
        subDep = self.__prjDepends[ depProject ][ depVersion ]
        #Merge dependencies
        for sKey in subDep:
          if sKey not in relDeps:
            relDeps.append( sKey )
            continue
          prj, vrs = sKey
          for pKey in relDeps:
            if pKey[0] == prj and pKey[1] != vrs:
              errMsg = "%s is required with two different versions ( %s and %s ) starting with %s:%s" % ( prj,
                                                                                                          pKey[1], vrs,
                                                                                                          project, release )
              return S_ERROR( errMsg )
          #Same version already required
      if project in relDeps and relDeps[ project ] != release:
        errMsg = "%s:%s requires itself with a different version through dependencies ( %s )" % ( project, release,
                                                                                                  relDeps[ project ] )
        return S_ERROR( errMsg )

    return S_OK()

  def getReleaseOption( self, project, release, option ):
    try:
      return self.__prjRelCFG[ project ][ release ].get( option )
    except KeyError:
      self.__dbgMsg( "Missing option %s for %s:%s" % ( option, project, release ) )
      return False

  def getReleaseDependencies( self, project, release ):
    try:
      data = self.__prjRelCFG[ project ][ release ].get( "Releases/%s/Depends" % release )
    except KeyError:
      return {}
    data = [ field for field in data.split( "," ) if field.strip() ]
    deps = {}
    for field in data:
      field = field.strip()
      if not field:
        continue
      pv = field.split( ":" )
      if len( pv ) == 1:
        deps[ pv[0].strip() ] = release
      else:
        deps[ pv[0].strip() ] = ":".join( pv[1:] ).strip()
    return deps

  def getModulesForRelease( self, release, project = False ):
    if not project:
      project = self.__projectName
    if not project in self.__prjRelCFG:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % project )
    if not release in self.__prjRelCFG[ project ]:
      return S_ERROR( "Version %s has not been loaded for project %s" % ( release, project ) )
    config = self.__prjRelCFG[ project ][ release ]
    if not config.isSection( "Releases/%s" % release ):
      return S_ERROR( "Release %s is not defined for project %s" % ( release, project ) )
    #Defined Modules explicitly in the release
    modules = self.getReleaseOption( project, release, "Releases/%s/Modules" % release )
    if modules:
      dMods = {}
      for entry in [ entry.split( ":" ) for entry in modules.split( "," ) if entry.strip() ]: # pylint: disable=no-member
        if len( entry ) == 1:
          dMods[ entry[0].strip() ] = release
        else:
          dMods[ entry[0].strip() ] = entry[1].strip()
      modules = dMods
    else:
      #Default modules with the same version as the release version
      modules = self.getReleaseOption( project, release, "DefaultModules" )
      if modules:
        modules = dict( ( modName.strip() , release ) for modName in modules.split( "," ) if modName.strip() ) # pylint: disable=no-member
      else:
        #Mod = project and same version
        modules = { project : release }
    #Check project is in the modNames if not DIRAC
    if project != "DIRAC":
      for modName in modules:
        if modName.find( project ) != 0:
          return S_ERROR( "Module %s does not start with the name %s" % ( modName, project ) )
    return S_OK( modules )

  def getModSource( self, release, modName ):
    if not self.__projectName in self.__prjRelCFG:
      return S_ERROR( "Project %s has not been loaded. I'm a MEGA BUG! Please report me!" % self.__projectName )
    modLocation = self.getReleaseOption( self.__projectName, release, "Sources/%s" % modName )
    if not modLocation:
      return S_ERROR( "Source origin for module %s is not defined" % modName )
    modTpl = [ field.strip() for field in modLocation.split( "|" ) if field.strip() ] # pylint: disable=no-member
    if len( modTpl ) == 1:
      return S_OK( ( False, modTpl[0] ) )
    return S_OK( ( modTpl[0], modTpl[1] ) )

  def getExtenalsVersion( self, release = False ):
    if 'DIRAC' not in self.__prjRelCFG:
      return False
    if not release:
      release = list( self.__prjRelCFG[ 'DIRAC' ] )
      release = max( release )
    try:
      return self.__prjRelCFG[ 'DIRAC' ][ release ].get( 'Releases/%s/Externals' % release )
    except KeyError:
      return False

  def getLCGVersion( self, lcgVersion = "" ):
    if lcgVersion:
      return lcgVersion
    for objName in self.__projectsLoadedBy:
      try:
        return self.__prjRelCFG[ self.__projectName ][ cliParams.release ].get( "Releases/%s/LcgVer" % cliParams.release, lcgVersion )
      except KeyError:
        pass
    return lcgVersion

  def getModulesToInstall( self, release, extensions = False ):
    if not extensions:
      extensions = []
    extraFound = []
    modsToInstall = {}
    modsOrder = []
    if self.__projectName not in self.__prjDepends:
      return S_ERROR( "Project %s has not been loaded" % self.__projectName )
    if release not in self.__prjDepends[ self.__projectName ]:
      return S_ERROR( "Version %s has not been loaded for project %s" % ( release, self.__projectName ) )
    #Get a list of projects with their releases
    projects = list( self.__prjDepends[ self.__projectName ][ release ] )
    for project, relVersion in projects:
      try:
        requiredModules = self.__prjRelCFG[ project ][ relVersion ].get( "RequiredExtraModules" )
        requiredModules = [ modName.strip() for modName in requiredModules.split( "/" ) if modName.strip() ]
      except KeyError:
        requiredModules = []
      for modName in requiredModules:
        if modName not in extensions:
          extensions.append( modName )
      result = self.getTarsLocation( project )
      if not result[ 'OK' ]:
        return result
      tarsPath = result[ 'Value' ]
      self.__dbgMsg( "Discovering modules to install for %s (%s)" % ( project, relVersion ) )
      result = self.getModulesForRelease( relVersion, project )
      if not result[ 'OK' ]:
        return result
      modVersions = result[ 'Value' ]
      try:
        defaultMods = self.__prjRelCFG[ project ][ relVersion ].get( "DefaultModules" )
        modNames = [ mod.strip() for mod in defaultMods.split( "," ) if mod.strip() ]
      except KeyError:
        modNames = []
      for extension in extensions:
        # Check if the version of the extension module is specified in the command line
        extraVersion = None
        if ":" in extension:
          extension, extraVersion = extension.split( ":" )
          modVersions[extension] = extraVersion
        if extension in modVersions:
          modNames.append( extension )
          extraFound.append( extension )
        if 'DIRAC' not in extension:
          dextension = "%sDIRAC" % extension
          if dextension in modVersions:
            modNames.append( dextension )
            extraFound.append( extension )
      modNameVer = [ "%s:%s" % ( modName, modVersions[ modName ] ) for modName in modNames ]
      self.__dbgMsg( "Modules to be installed for %s are: %s" % ( project, ", ".join( modNameVer ) ) )
      for modName in modNames:
        modsToInstall[ modName ] = ( tarsPath, modVersions[ modName ] )
        modsOrder.insert( 0, modName )

    for modName in extensions:
      if modName.split(":")[0] not in extraFound:
        return S_ERROR( "No module %s defined. You sure it's defined for this release?" % modName )

    return S_OK( ( modsOrder, modsToInstall ) )


#################################################################################
# End of ReleaseConfig
#################################################################################


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

def logWARN( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [WARN] %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def logNOTICE( msg ):
  for line in msg.split( "\n" ):
    print "%s UTC dirac-install [NOTICE]  %s" % ( time.strftime( '%Y-%m-%d %H:%M:%S', time.gmtime() ), line )
  sys.stdout.flush()

def alarmTimeoutHandler( *args ):
  raise Exception( 'Timeout' )

def urlretrieveTimeout( url, fileName = '', timeout = 0 ):
  """
   Retrieve remote url to local file, with timeout wrapper
  """
  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
  logDEBUG( 'Retrieving remote file "%s"' % url )

  urlData = ''
  if timeout:
    signal.signal( signal.SIGALRM, alarmTimeoutHandler )
    # set timeout alarm
    signal.alarm( timeout + 5 )
  try:
    # if "http_proxy" in os.environ and os.environ['http_proxy']:
    #   proxyIP = os.environ['http_proxy']
    #   proxy = urllib2.ProxyHandler( {'http': proxyIP} )
    #   opener = urllib2.build_opener( proxy )
    #   #opener = urllib2.build_opener()
    #  urllib2.install_opener( opener )

    # Try to use insecure context explicitly, needed for python >= 2.7.9
    try:
      context = ssl._create_unverified_context()
      remoteFD = urllib2.urlopen( url, context = context ) # pylint: disable=unexpected-keyword-arg
       # the keyword 'context' is present from 2.7.9+
    except AttributeError:
      remoteFD = urllib2.urlopen( url )
    expectedBytes = 0
    # Sometimes repositories do not return Content-Length parameter
    try:
      expectedBytes = long( remoteFD.info()[ 'Content-Length' ] )
    except Exception, x:
      logWARN( 'Content-Length parameter not returned, skipping expectedBytes check' )

    if fileName:
      localFD = open( fileName, "wb" )
    receivedBytes = 0L
    data = remoteFD.read( 16384 )
    count = 1
    progressBar = False
    while data:
      receivedBytes += len( data )
      if fileName:
        localFD.write( data )
      else:
        urlData += data
      data = remoteFD.read( 16384 )
      if count % 20 == 0 and sys.stdout.isatty():
        print '\033[1D' + ".",
        sys.stdout.flush()
        progressBar = True
      count += 1
    if progressBar and sys.stdout.isatty():
      # return cursor to the beginning of the line
      print '\033[1K',
      print '\033[1A'
    if fileName:
      localFD.close()
    remoteFD.close()
    if receivedBytes != expectedBytes and expectedBytes > 0:
      logERROR( "File should be %s bytes but received %s" % ( expectedBytes, receivedBytes ) )
      return False
  except urllib2.HTTPError, x:
    if x.code == 404:
      logERROR( "%s does not exist" % url )
      if timeout:
        signal.alarm( 0 )
      return False
  except urllib2.URLError:
    logERROR( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
  except Exception, x:
    if x == 'Timeout':
      logERROR( 'Timeout after %s seconds on transfer request for "%s"' % ( str( timeout ), url ) )
    if timeout:
      signal.alarm( 0 )
    raise x

  if timeout:
    signal.alarm( 0 )

  if fileName:
    return True
  else:
    return urlData

def downloadAndExtractTarball( tarsURL, pkgName, pkgVer, checkHash = True, cache = False ):
  tarName = "%s-%s.tar.gz" % ( pkgName, pkgVer )
  tarPath = os.path.join( cliParams.targetPath, tarName )
  tarFileURL = "%s/%s" % ( tarsURL, tarName )
  tarFileCVMFS = "/cvmfs/dirac.egi.eu/installSource/%s" % tarName
  cacheDir = os.path.join( cliParams.basePath, ".installCache" )
  tarCachePath = os.path.join( cacheDir, tarName )
  if cache and os.path.isfile( tarCachePath ):
    logNOTICE( "Using cached copy of %s" % tarName )
    shutil.copy( tarCachePath, tarPath )
  elif os.path.exists( tarFileCVMFS ):
    logNOTICE( "Using CVMFS copy of %s" % tarName )
    tarPath = tarFileCVMFS
    checkHash = False
    cache = False
  else:
    logNOTICE( "Retrieving %s" % tarFileURL )
    try:
      if not urlretrieveTimeout( tarFileURL, tarPath, cliParams.timeout ):
        logERROR( "Cannot download %s" % tarName )
        return False
    except Exception, e:
      logERROR( "Cannot download %s: %s" % ( tarName, str( e ) ) )
      sys.exit( 1 )
  if checkHash:
    md5Name = "%s-%s.md5" % ( pkgName, pkgVer )
    md5Path = os.path.join( cliParams.targetPath, md5Name )
    md5FileURL = "%s/%s" % ( tarsURL, md5Name )
    md5CachePath = os.path.join( cacheDir, md5Name )
    if cache and os.path.isfile( md5CachePath ):
      logNOTICE( "Using cached copy of %s" % md5Name )
      shutil.copy( md5CachePath, md5Path )
    else:
      logNOTICE( "Retrieving %s" % md5FileURL )
      try:
        if not urlretrieveTimeout( md5FileURL, md5Path, 60 ):
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
    md5Calculated = hashlib.md5()
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
    if cache:
      if not os.path.isdir( cacheDir ):
        os.makedirs( cacheDir )
      os.rename( md5Path, md5CachePath )
    else:
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
  if cache:
    if not os.path.isdir( cacheDir ):
      os.makedirs( cacheDir )
    os.rename( tarPath, tarCachePath )
  else:
    if tarPath != tarFileCVMFS:
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
  binaryPath = os.path.join( cliParams.targetPath, cliParams.platform )
  pydocPath = os.path.join( binaryPath, 'bin', 'pydoc' )
  try:
    fd = open( pydocPath )
    line = fd.readline()
    fd.close()
    buildPath = line[2:line.find( cliParams.platform ) - 1]
    replaceCmd = "grep -rIl '%s' %s | xargs sed -i'.org' 's:%s:%s:g'" % ( buildPath,
                                                                          binaryPath,
                                                                          buildPath,
                                                                          cliParams.targetPath )
    os.system( replaceCmd )

  except:
    pass

def fixPythonShebang():
  """
  Some scripts (like the gfal2 scripts) come with a shebang pointing to the system python.
  We replace it with the environment one
 """

  binaryPath = os.path.join( cliParams.targetPath, cliParams.platform )
  try:
    replaceCmd = "grep -rIl '#!/usr/bin/python' %s/bin | xargs sed -i'.org' 's:#!/usr/bin/python:#!/usr/bin/env python:g'" %  binaryPath
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
    if not scriptName.endswith( postInstallSuffix ):
      logDEBUG( "%s does not have the %s suffix. Skipping.." % ( scriptName, postInstallSuffix ) )
      continue
    scriptPath = os.path.join( postInstallPath, scriptName )
    os.chmod( scriptPath , executablePerms )
    logNOTICE( "Executing %s..." % scriptPath )
    if os.system( "'%s' > '%s.out' 2> '%s.err'" % ( scriptPath, scriptPath, scriptPath ) ):
      logERROR( "Post installation script %s failed. Check %s.err" % ( scriptPath, scriptPath ) )
      sys.exit( 1 )

def fixMySQLScript():
  """
   Update the mysql.server script (if installed) to point to the proper datadir
  """
  scriptPath = os.path.join( cliParams.targetPath, 'scripts', 'dirac-fix-mysql-script' )
  bashrcFile = os.path.join( cliParams.targetPath, 'bashrc' )
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join( cliParams.basePath, 'bashrc' )
  command = 'source %s; %s > /dev/null' % (bashrcFile,scriptPath)
  if os.path.exists( scriptPath ):
    logNOTICE( "Executing %s..." % command )
    os.system( 'bash -c "%s"' % command )

def checkPlatformAliasLink():
  """
  Make a link if there's an alias
  """
  if cliParams.platform in platformAlias:
    os.symlink( os.path.join( cliParams.targetPath, platformAlias[ cliParams.platform ] ),
                os.path.join( cliParams.targetPath, cliParams.platform ) )

def installExternalRequirements( extType ):
  """ Install the extension requirements if any
  """
  reqScript = os.path.join( cliParams.targetPath, "scripts", 'dirac-externals-requirements' )
  bashrcFile = os.path.join( cliParams.targetPath, 'bashrc' )
  if cliParams.useVersionsDir:
    bashrcFile = os.path.join( cliParams.basePath, 'bashrc' )
  if os.path.isfile( reqScript ):
    os.chmod( reqScript , executablePerms )
    logNOTICE( "Executing %s..." % reqScript )
    command = "python '%s' -t '%s' > '%s.out' 2> '%s.err'" % ( reqScript, extType,
                                                               reqScript, reqScript )
    if os.system( 'bash -c "source %s; %s"' % (bashrcFile,command) ):
      logERROR( "Requirements installation script %s failed. Check %s.err" % ( reqScript,
                                                                               reqScript ) )
  return True

####
# End of helper functions
####

cmdOpts = ( ( 'r:', 'release=', 'Release version to install' ),
            ( 'l:', 'project=', 'Project to install' ),
            ( 'e:', 'extensions=', 'Extensions to install (comma separated)' ),
            ( 't:', 'installType=', 'Installation type (client/server)' ),
            ( 'i:', 'pythonVersion=', 'Python version to compile (27/26)' ),
            ( 'p:', 'platform=', 'Platform to install' ),
            ( 'P:', 'installationPath=', 'Path where to install (default current working dir)' ),
            ( 'b', 'build', 'Force local compilation' ),
            ( 'g:', 'grid=', 'lcg tools package version' ),
            ( 'B', 'noAutoBuild', 'Do not build if not available' ),
            ( 'v', 'useVersionsDir', 'Use versions directory' ),
            ( 'u:', 'baseURL=', "Use URL as the source for installation tarballs" ),
            ( 'd', 'debug', 'Show debug messages' ),
            ( 'V:', 'installation=', 'Installation from which to extract parameter values' ),
            ( 'X', 'externalsOnly', 'Only install external binaries' ),
            ( 'M:', 'defaultsURL=', 'Where to retrieve the global defaults from' ),
            ( 'h', 'help', 'Show this help' ),
            ( 'T:', 'Timeout=', 'Timeout for downloads (default = %s)' )
          )

def usage():
  print "\nUsage:\n\n  %s <opts> <cfgFile>" % os.path.basename( sys.argv[0] )
  print "\nOptions:"
  for cmdOpt in cmdOpts:
    print "\n  %s %s : %s" % ( cmdOpt[0].ljust( 3 ), cmdOpt[1].ljust( 20 ), cmdOpt[2] )
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
                   ( 'Debug', cliParams.debug ),
                   ( 'Timeout', cliParams.timeout ) ]:
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
    elif o in ( "-M", "--defaultsURL" ):
      cliParams.globalDefaults = v

  releaseConfig = ReleaseConfig( instName = cliParams.installation, globalDefaultsURL = cliParams.globalDefaults )
  if cliParams.debug:
    releaseConfig.setDebugCB( logDEBUG )

  result = releaseConfig.loadInstallationDefaults()
  if not result[ 'OK' ]:
    logERROR( "Could not load defaults: %s" % result[ 'Message' ] )

  for arg in args:
    if len( arg ) > 4 and arg.find( ".cfg" ) == len( arg ) - 4:
      result = releaseConfig.loadInstallationLocalDefaults( arg )
      if not result[ 'OK' ]:
        logERROR( result[ 'Message' ] )
      else:
        logNOTICE( "Loaded %s" % arg )

  for opName in ( 'release', 'externalsType', 'installType', 'pythonVersion',
                  'buildExternals', 'noAutoBuild', 'debug', 'globalDefaults',
                  'lcgVer', 'useVersionsDir', 'targetPath',
                  'project', 'release', 'extraModules', 'extensions', 'timeout' ):
    try:
      opVal = releaseConfig.getInstallationConfig( "LocalInstallation/%s" % ( opName[0].upper() + opName[1:] ) )
    except KeyError:
      continue

    if opName == 'extraModules':
      logWARN( "extraModules is deprecated please use extensions instead!" )
      opName = 'extensions'

    if opName == 'installType':
      opName = 'externalsType'
    if isinstance( getattr( cliParams, opName ), basestring ):
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
    elif o in ( '-e', '--extensions' ):
      for pkg in [ p.strip() for p in v.split( "," ) if p.strip() ]:
        if pkg not in cliParams.extensions:
          cliParams.extensions.append( pkg )
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
    elif o in ( '-T', '--Timeout' ):
      try:
        cliParams.timeout = max( cliParams.timeout, int( v ) )
        cliParams.timeout = min( cliParams.timeout, 3600 )
      except ValueError:
        pass


  if not cliParams.release:
    logERROR( "Missing release to install" )
    usage()

  cliParams.basePath = cliParams.targetPath
  if cliParams.useVersionsDir:
    # install under <installPath>/versions/<version>_<timestamp>
    cliParams.targetPath = os.path.join( cliParams.targetPath, 'versions', '%s_%s' % ( cliParams.release, int( time.time() ) ) )
    try:
      os.makedirs( cliParams.targetPath )
    except:
      pass

  logNOTICE( "Destination path for installation is %s" % cliParams.targetPath )
  releaseConfig.setProject( cliParams.project )

  result = releaseConfig.loadProjectRelease( cliParams.release,
                                             project = cliParams.project,
                                             sourceURL = cliParams.installSource )
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

def getPlatform():
  platformPath = os.path.join( cliParams.targetPath, "DIRAC", "Core", "Utilities", "Platform.py" )
  try:
    platFD = open( platformPath, "r" )
  except IOError:
    logERROR( "Cannot open Platform.py. Is DIRAC installed?" )
    return ''

  Platform = imp.load_module( "Platform", platFD, platformPath, ( "", "r", imp.PY_SOURCE ) )
  platFD.close()
  return Platform.getPlatformString()

def installExternals( releaseConfig ):
  externalsVersion = releaseConfig.getExtenalsVersion()
  if not externalsVersion:
    logERROR( "No externals defined" )
    return False

  if not cliParams.platform:
    cliParams.platform = getPlatform()
  if not cliParams.platform:
    return False

  if cliParams.installSource:
    tarsURL = cliParams.installSource
  else:
    tarsURL = releaseConfig.getTarsLocation( 'DIRAC' )[ 'Value' ]

  if cliParams.buildExternals:
    compileExternals( externalsVersion )
  else:
    logDEBUG( "Using platform: %s" % cliParams.platform )
    extVer = "%s-%s-%s-python%s" % ( cliParams.externalsType, externalsVersion, cliParams.platform, cliParams.pythonVersion )
    logDEBUG( "Externals %s are to be installed" % extVer )
    if not downloadAndExtractTarball( tarsURL, "Externals", extVer, cache = True ):
      return ( not cliParams.noAutoBuild ) and compileExternals( externalsVersion )
    logNOTICE( "Fixing externals paths..." )
    fixBuildPaths()
  logNOTICE( "Running externals post install..." )
  checkPlatformAliasLink()
  #lcg utils?
  #LCG utils if required
  lcgVer = releaseConfig.getLCGVersion( cliParams.lcgVer )
  if lcgVer:
    verString = "%s-%s-python%s" % ( lcgVer, cliParams.platform, cliParams.pythonVersion )
    #HACK: try to find a more elegant solution for the lcg bundles location
    if not downloadAndExtractTarball( tarsURL + "/../lcgBundles", "DIRAC-lcg", verString, False, cache = True ):
      logERROR( "Check that there is a release for your platform: DIRAC-lcg-%s" % verString )
  logNOTICE( "Fixing Python Shebang..." )
  fixPythonShebang()
  return True

def createPermanentDirLinks():
  """ Create links to permanent directories from within the version directory
  """
  if cliParams.useVersionsDir:
    try:
      for dir in ['startup', 'runit', 'data', 'work', 'control', 'sbin', 'etc', 'webRoot']:
        fake = os.path.join( cliParams.targetPath, dir )
        real = os.path.join( cliParams.basePath, dir )
        if not os.path.exists( real ):
          os.makedirs( real )
        if os.path.exists( fake ):
          # Try to reproduce the directory structure to avoid lacking directories
          fakeDirs = os.listdir( fake )
          for fd in fakeDirs:
            if os.path.isdir( os.path.join( fake, fd ) ):
              if not os.path.exists( os.path.join( real, fd ) ):
                os.makedirs( os.path.join( real, fd ) )
          os.rename( fake, fake + '.bak' )
        os.symlink( real, fake )
    except Exception, x:
      logERROR( str( x ) )
      return False

  return True

def createOldProLinks():
  """ Create links to permanent directories from within the version directory
  """
  proPath = cliParams.targetPath
  if cliParams.useVersionsDir:
    oldPath = os.path.join( cliParams.basePath, 'old' )
    proPath = os.path.join( cliParams.basePath, 'pro' )
    try:
      if os.path.exists( proPath ) or os.path.islink( proPath ):
        if os.path.exists( oldPath ) or os.path.islink( oldPath ):
          os.unlink( oldPath )
        os.rename( proPath, oldPath )
      os.symlink( cliParams.targetPath, proPath )
    except Exception, x:
      logERROR( str( x ) )
      return False

  return True

def createBashrc():
  """ Create DIRAC environment setting script for the bash shell
  """

  proPath = cliParams.targetPath
  # Now create bashrc at basePath
  try:
    bashrcFile = os.path.join( cliParams.targetPath, 'bashrc' )
    if cliParams.useVersionsDir:
      bashrcFile = os.path.join( cliParams.basePath, 'bashrc' )
      proPath = os.path.join( cliParams.basePath, 'pro' )
    logNOTICE( 'Creating %s' % bashrcFile )
    if not os.path.exists( bashrcFile ):
      lines = [ '# DIRAC bashrc file, used by service and agent run scripts to set environment',
                'export PYTHONUNBUFFERED=yes',
                'export PYTHONOPTIMIZE=x' ]
      if 'HOME' in os.environ:
        lines.append( '[ -z "$HOME" ] && export HOME=%s' % os.environ['HOME'] )

      # Determining where the CAs are...
      if 'X509_CERT_DIR' in os.environ:
        certDir = os.environ['X509_CERT_DIR']
      else:
        if os.path.isdir( '/etc/grid-security/certificates' ):
          certDir = '/etc/grid-security/certificates' # Assuming that, if present, it is not empty, and has correct CAs
        else:
          certDir = '%s/etc/grid-security/certificates' % proPath # But this will have to be created at some point (dirac-configure)
      lines.extend( ['# CAs path for SSL verification',
                     'export X509_CERT_DIR=%s' % certDir,
                     'export SSL_CERT_DIR=%s' % certDir,
                     'export REQUESTS_CA_BUNDLE=%s' % certDir] )

      lines.append( 'export X509_VOMS_DIR=%s' % os.path.join( proPath, 'etc', 'grid-security', 'vomsdir' ) )
      lines.extend( ['# Some DIRAC locations',
                     '[ -z "$DIRAC" ] && export DIRAC=%s' % proPath,
                     'export DIRACBIN=%s' % os.path.join( "$DIRAC", cliParams.platform, 'bin' ),
                     'export DIRACSCRIPTS=%s' % os.path.join( "$DIRAC", 'scripts' ),
                     'export DIRACLIB=%s' % os.path.join( "$DIRAC", cliParams.platform, 'lib' ),
                     'export TERMINFO=%s' % __getTerminfoLocations( os.path.join( "$DIRAC", cliParams.platform, 'share', 'terminfo' ) ),
                     'export RRD_DEFAULT_FONT=%s' % os.path.join( "$DIRAC", cliParams.platform, 'share', 'rrdtool', 'fonts', 'DejaVuSansMono-Roman.ttf' ) ] )

      lines.extend( ['# Prepend the PYTHONPATH, the LD_LIBRARY_PATH, and the DYLD_LIBRARY_PATH'] )

      lines.extend( ['( echo $PATH | grep -q $DIRACBIN ) || export PATH=$DIRACBIN:$PATH',
                     '( echo $PATH | grep -q $DIRACSCRIPTS ) || export PATH=$DIRACSCRIPTS:$PATH',
                     '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB ) || export LD_LIBRARY_PATH=$DIRACLIB:$LD_LIBRARY_PATH',
                     '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || export LD_LIBRARY_PATH=$DIRACLIB/mysql:$LD_LIBRARY_PATH',
                     '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB ) || export DYLD_LIBRARY_PATH=$DIRACLIB:$DYLD_LIBRARY_PATH',
                     '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || export DYLD_LIBRARY_PATH=$DIRACLIB/mysql:$DYLD_LIBRARY_PATH',
                     '( echo $PYTHONPATH | grep -q $DIRAC ) || export PYTHONPATH=$DIRAC:$PYTHONPATH'] )
      lines.extend( ['# new OpenSSL version require OPENSSL_CONF to point to some accessible location',
                     'export OPENSSL_CONF=/tmp'] )

      # gfal2 requires some environment variables to be set
      lines.extend( ['# Gfal2 configuration and plugins',
                     'export GFAL_CONFIG_DIR=%s' % os.path.join( "$DIRAC", cliParams.platform, 'etc/gfal2.d'),
                     'export  GFAL_PLUGIN_DIR=%s' %os.path.join( "$DIRACLIB", 'gfal2-plugins')] )
      # add DIRACPLAT environment variable for client installations
      if cliParams.externalsType == 'client':
        lines.extend( ['# DIRAC platform',
                       '[ -z "$DIRACPLAT" ] && export DIRACPLAT=`$DIRAC/scripts/dirac-platform`'] )
      # Add the lines required for globus-* tools to use IPv6
      lines.extend( ['# IPv6 support',
                     'export GLOBUS_IO_IPV6=TRUE',
                     'export GLOBUS_FTP_CLIENT_IPV6=TRUE'] )
      # Add the lines required for ARC CE support
      lines.extend( ['# ARC Computing Element',
                     'export ARC_PLUGIN_PATH=$DIRACLIB/arc'] )
      lines.append( '' )
      f = open( bashrcFile, 'w' )
      f.write( '\n'.join( lines ) )
      f.close()
  except Exception, x:
    logERROR( str( x ) )
    return False

  return True

def createCshrc():
  """ Create DIRAC environment setting script for the (t)csh shell
  """

  proPath = cliParams.targetPath
  # Now create cshrc at basePath
  try:
    cshrcFile = os.path.join( cliParams.targetPath, 'cshrc' )
    if cliParams.useVersionsDir:
      cshrcFile = os.path.join( cliParams.basePath, 'cshrc' )
      proPath = os.path.join( cliParams.basePath, 'pro' )
    logNOTICE( 'Creating %s' % cshrcFile )
    if not os.path.exists( cshrcFile ):
      lines = [ '# DIRAC cshrc file, used by clients to set up the environment',
                'setenv PYTHONUNBUFFERED yes',
                'setenv PYTHONOPTIMIZE x' ]

      # Determining where the CAs are...
      if 'X509_CERT_DIR' in os.environ:
        certDir = os.environ['X509_CERT_DIR']
      else:
        if os.path.isdir( '/etc/grid-security/certificates' ):
          certDir = '/etc/grid-security/certificates' # Assuming that, if present, it is not empty, and has correct CAs
        else:
          certDir = '%s/etc/grid-security/certificates' % proPath # But this will have to be created at some point (dirac-configure)
      lines.extend( ['# CAs path for SSL verification',
                     'setenv X509_CERT_DIR %s' %certDir,
                     'setenv SSL_CERT_DIR %s' % certDir,
                     'setenv REQUESTS_CA_BUNDLE %s' % certDir] )

      lines.append( 'setenv X509_VOMS_DIR %s' % os.path.join( proPath, 'etc', 'grid-security', 'vomsdir' ) )
      lines.extend( ['# Some DIRAC locations',
                     '( test $?DIRAC -eq 1 ) || setenv DIRAC %s' % proPath,
                     'setenv DIRACBIN %s' % os.path.join( "$DIRAC", cliParams.platform, 'bin' ),
                     'setenv DIRACSCRIPTS %s' % os.path.join( "$DIRAC", 'scripts' ),
                     'setenv DIRACLIB %s' % os.path.join( "$DIRAC", cliParams.platform, 'lib' ),
                     'setenv TERMINFO %s' % __getTerminfoLocations( os.path.join( "$DIRAC", cliParams.platform, 'share', 'terminfo' ) ) ] )

      lines.extend( ['# Prepend the PYTHONPATH, the LD_LIBRARY_PATH, and the DYLD_LIBRARY_PATH'] )

      lines.extend( ['( test $?PATH -eq 1 ) || setenv PATH ""',
                     '( test $?LD_LIBRARY_PATH -eq 1 ) || setenv LD_LIBRARY_PATH ""',
                     '( test $?DY_LD_LIBRARY_PATH -eq 1 ) || setenv DYLD_LIBRARY_PATH ""',
                     '( test $?PYTHONPATH -eq 1 ) || setenv PYTHONPATH ""',
                     '( echo $PATH | grep -q $DIRACBIN ) || setenv PATH ${DIRACBIN}:$PATH',
                     '( echo $PATH | grep -q $DIRACSCRIPTS ) || setenv PATH ${DIRACSCRIPTS}:$PATH',
                     '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB ) || setenv LD_LIBRARY_PATH ${DIRACLIB}:$LD_LIBRARY_PATH',
                     '( echo $LD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || setenv LD_LIBRARY_PATH ${DIRACLIB}/mysql:$LD_LIBRARY_PATH',
                     '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB ) || setenv DYLD_LIBRARY_PATH ${DIRACLIB}:$DYLD_LIBRARY_PATH',
                     '( echo $DYLD_LIBRARY_PATH | grep -q $DIRACLIB/mysql ) || setenv DYLD_LIBRARY_PATH ${DIRACLIB}/mysql:$DYLD_LIBRARY_PATH',
                     '( echo $PYTHONPATH | grep -q $DIRAC ) || setenv PYTHONPATH ${DIRAC}:$PYTHONPATH'] )
      lines.extend( ['# new OpenSSL version require OPENSSL_CONF to point to some accessible location',
                     'setenv OPENSSL_CONF /tmp'] )
      lines.extend( ['# IPv6 support',
                     'setenv GLOBUS_IO_IPV6 TRUE',
                     'setenv GLOBUS_FTP_CLIENT_IPV6 TRUE'] )
      # gfal2 requires some environment variables to be set
      lines.extend( ['# Gfal2 configuration and plugins',
                     'setenv GFAL_CONFIG_DIR %s' % os.path.join( "$DIRAC", cliParams.platform, 'etc/gfal2.d'),
                     'setenv  GFAL_PLUGIN_DIR %s' %os.path.join( "$DIRACLIB", 'gfal2-plugins')] )
     # add DIRACPLAT environment variable for client installations
      if cliParams.externalsType == 'client':
        lines.extend( ['# DIRAC platform',
                       'test $?DIRACPLAT -eq 1 || setenv DIRACPLAT `$DIRAC/scripts/dirac-platform`'] )
      # Add the lines required for ARC CE support
      lines.extend( ['# ARC Computing Element',
                     'setenv ARC_PLUGIN_PATH $DIRACLIB/arc'] )
      lines.append( '' )
      f = open( cshrcFile, 'w' )
      f.write( '\n'.join( lines ) )
      f.close()
  except Exception, x:
    logERROR( str( x ) )
    return False

  return True

def writeDefaultConfiguration():
  instCFG = releaseConfig.getInstallationCFG()
  if not instCFG:
    return
  for opName in instCFG.getOptions():
    instCFG.delPath( opName )

  # filePath = os.path.join( cliParams.targetPath, "defaults-%s.cfg" % cliParams.installation )
  # Keep the default configuration file in the working directory
  filePath = "defaults-%s.cfg" % cliParams.installation
  try:
    fd = open( filePath, "wb" )
    fd.write( instCFG.toString() )
    fd.close()
  except Exception, excp:
    logERROR( "Could not write %s: %s" % ( filePath, excp ) )
  logNOTICE( "Defaults written to %s" % filePath )

def __getTerminfoLocations( defaultLocation=None ):
  """returns the terminfo locations as a colon separated string"""

  terminfoLocations = []
  if defaultLocation:
    terminfoLocations = [ defaultLocation ]

  for termpath in [ '/usr/share/terminfo', '/etc/terminfo' ]:
    if os.path.exists( termpath ):
      terminfoLocations.append( termpath )

  return ":".join( terminfoLocations )

if __name__ == "__main__":
  logNOTICE( "Processing installation requirements" )
  result = loadConfiguration()
  if not result[ 'OK' ]:
    logERROR( result[ 'Message' ] )
    sys.exit( 1 )
  releaseConfig = result[ 'Value' ]
  if not createPermanentDirLinks():
    sys.exit( 1 )
  if not cliParams.externalsOnly:
    logNOTICE( "Discovering modules to install" )
    result = releaseConfig.getModulesToInstall( cliParams.release, cliParams.extensions )
    if not result[ 'OK' ]:
      logERROR( result[ 'Message' ] )
      sys.exit( 1 )
    modsOrder, modsToInstall = result[ 'Value' ]
    if cliParams.debug:
      logNOTICE( "Writing down the releases files" )
      releaseConfig.dumpReleasesToPath( cliParams.targetPath )
    logNOTICE( "Installing modules..." )
    for modName in modsOrder:
      tarsURL, modVersion = modsToInstall[ modName ]
      if cliParams.installSource:
        tarsURL = cliParams.installSource
      logNOTICE( "Installing %s:%s" % ( modName, modVersion ) )
      if not downloadAndExtractTarball( tarsURL, modName, modVersion ):
        sys.exit( 1 )
    logNOTICE( "Deploying scripts..." )
    ddeLocation = os.path.join( cliParams.targetPath, "DIRAC", "Core", "scripts", "dirac-deploy-scripts.py" )
    if os.path.isfile( ddeLocation ):
      cmd = ddeLocation
      # In MacOS /usr/bin/env does not find python in the $PATH, passing binary path
      # as an argument to the dirac-deploy-scripts
      if not cliParams.platform:
        cliParams.platform = getPlatform()
      if "Darwin" in cliParams.platform:
        binaryPath = os.path.join( cliParams.targetPath, cliParams.platform )
        logNOTICE( "For MacOS (Darwin) use explicit binary path %s" % binaryPath )
        cmd += ' %s' % binaryPath
      os.system( cmd )
    else:
      logDEBUG( "No dirac-deploy-scripts found. This doesn't look good" )
  else:
    logNOTICE( "Skipping installing DIRAC" )
  logNOTICE( "Installing %s externals..." % cliParams.externalsType )
  if not installExternals( releaseConfig ):
    sys.exit( 1 )
  if not createOldProLinks():
    sys.exit( 1 )
  if not createBashrc():
    sys.exit( 1 )
  if not createCshrc():
    sys.exit( 1 )
  runExternalsPostInstall()
  writeDefaultConfiguration()
  if cliParams.externalsType == "server":
    fixMySQLScript()
  installExternalRequirements( cliParams.externalsType )
  logNOTICE( "%s properly installed" % cliParams.installation )
  sys.exit( 0 )
