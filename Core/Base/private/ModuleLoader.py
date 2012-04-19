
import types
import os
import imp
from DIRAC.Core.Utilities import List
from DIRAC import gConfig, S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers import getInstalledExtensions

class ModuleLoader( object ):

  def __init__( self, importLocation, sectionFinder, superClass, csSuffix = False ):
    self.__modules = {}
    self.__loadedModules = {}
    self.__superClass = superClass
    #Function to find the
    self.__sectionFinder = sectionFinder
    #Import from where? <Ext>.<System>System.<importLocation>.<module>
    self.__importLocation = importLocation
    #Where to look in the CS for the module? /Systems/<System>/<Instance>/<csSuffix>
    if not csSuffix:
      csSuffix = "%ss" % importLocation
    self.__csSuffix = csSuffix

  def getModules( self ):
    return dict( self.__modules )

  def loadModules( self, modulesList, hideExceptions = False ):
    """
      Load all modules required in moduleList
    """
    for modName in modulesList:
      gLogger.verbose( "Checking %s" % modName )
      #if it's a executor modName name just load it and be done with it
      if modName.find( "/" ) > -1:
        result = self.loadModule( modName, hideExceptions = hideExceptions )
        if not result[ 'OK' ]:
          return result
        continue
      #Check if it's a system name
      #Look in the CS
      system = modName
      #Can this be generated with sectionFinder?
      csPath = "%s/Executors" % PathFinder.getSystemSection ( system, ( system, ) )
      gLogger.verbose( "Exploring %s to discover modules" % csPath )
      result = gConfig.getSections( csPath )
      if result[ 'OK' ]:
        #Add all modules in the CS :P
        for modName in result[ 'Value' ]:
          result = self.loadModule( "%s/%s" % ( system, modName ), hideExceptions = hideExceptions )
          if not result[ 'OK' ]:
            return result
      #Look what is installed
      parentModule = False
      for rootModule in getInstalledExtensions():
        if system.find( "System" ) != len( system ) - 6:
          parentImport = "%s.%sSystem.%s" % ( rootModule, system, self.__csSuffix )
        else:
          parentImport = "%s.%s.%s" % ( rootModule, system, self.__csSuffix )
        #HERE!
        result = self.__recurseImport( parentImport )
        if not result[ 'OK' ]:
          return result
        parentModule = result[ 'Value' ]
        if parentModule:
          break
      if not parentModule:
        continue
      parentPath = parentModule.__path__[0]
      gLogger.notice( "Found modules path at %s" % parentImport )
      for entry in os.listdir( parentPath ):
        if entry[-3:] != ".py" or entry == "__init__.py":
          continue
        if not os.path.isfile( os.path.join( parentPath, entry ) ):
          continue
        modName = "%s/%s" % ( system, entry[:-3] )
        gLogger.verbose( "Trying to import %s" % modName )
        result = self.loadModule( modName,
                                  hideExceptions = hideExceptions,
                                  parentModule = parentModule )

    return S_OK()

  def loadModule( self, modName, hideExceptions = False, parentModule = False ):
    """
      Load module name.
      name must take the form [DIRAC System Name]/[DIRAC module]
    """
    if modName in self.__modules:
     return S_OK()
    modList = modName.split( "/" )
    if len( modList ) != 2:
      return S_ERROR( "Can't load %s: Invalid module name" % ( modName ) )
    csSection = self.__sectionFinder( modName )
    loadGroup = gConfig.getValue( "%s/Load" % csSection, [] )
    #Check if it's a load group
    if loadGroup:
      gLogger.info( "Found load group %s. Will load %s" % ( modName, ", ".join( loadGroup ) ) )
      for loadModName in loadGroup:
        if loadModName.find( "/" ) == -1:
          loadModName = "%s/%s" % ( modList[0], loadModName )
        result = self.loadModule( loadModName, hideExceptions = hideExceptions, parentModule = False )
        if not result[ 'OK' ]:
          return result
      return S_OK()
    #Normal load
    loadName = gConfig.getValue( "%s/Module" % csSection, "" )
    if not loadName:
      loadName = modName
      gLogger.info( "Loading %s" % ( modName ) )
    else:
      if loadName.find( "/" ) == -1:
        loadName = "%s/%s" % ( modList[0], loadName )
      gLogger.info( "Loading %s (%s)" % ( modName, loadName ) )
    #If already loaded, skip
    loadList = loadName.split( "/" )
    if len( loadList ) != 2:
      return S_ERROR( "Can't load %s: Invalid executor name" % ( loadName ) )
    system, module = loadList
    if loadName not in self.__loadedModules:
      if parentModule:
        #If we've got a parent module, load from there.
        result = self.__recurseImport( module, parentModule, hideExceptions = hideExceptions )
      else:
        #HERE: :P
        #Check to see if the module exists in any of the root modules
        rootModulesToLook = getInstalledExtensions()
        for rootModule in rootModulesToLook:
          importString = '%s.%sSystem.%s.%s' % ( rootModule, system, self.__importLocation, module )
          gLogger.verbose( "Trying to load %s" % importString )
          result = self.__recurseImport( importString, hideExceptions = hideExceptions )
          #Error while loading
          if not result[ 'OK' ]:
            return result
          #Something has been found! break :)
          if result[ 'Value' ]:
            gLogger.verbose( "Found %s" % importString )
            break
      #Nothing found
      if not result[ 'Value' ]:
        return S_ERROR( "Could not find %s" % loadName )
      modObj = result[ 'Value' ]
      try:
        #Try to get the class from the module
        modClass = getattr( modObj, module )
      except AttributeError:
        location = ""
        if '__file__' in dir( modObj ):
          location = modObj.__file__
        else:
          locateion = modObj.__path__
        gLogger.exception( "%s module does not have a %s class!" % ( location, module ) )
        return S_ERROR( "Cannot load %s" % module )
      #Check if it's subclass
      if not issubclass( modClass, self.__superClass ):
        return S_ERROR( "%s has to inherit from %s" % ( loadName, self.__superClass.__name__ ) )
      self.__loadedModules[ loadName ] = { 'classObj' : modClass, 'moduleObj' : modObj }
      #End of loading of 'loadName' module

    #A-OK :)
    self.__modules[ modName ] = self.__loadedModules[ loadName ].copy()
    #keep the name of the real code module
    self.__modules[ modName ][ 'loadName' ] = loadName
    gLogger.notice( "Loaded module %s" % modName )

    return S_OK()

  def __recurseImport( self, modName, parentModule = False, hideExceptions = False ):
    if type( modName ) in types.StringTypes:
      modName = List.fromChar( modName, "." )
    try:
      if parentModule:
        impData = imp.find_module( modName[0], parentModule.__path__ )
      else:
        impData = imp.find_module( modName[0] )
      impModule = imp.load_module( modName[0], *impData )
      if impData[0]:
        impData[0].close()
    except ImportError, excp:
      if str( excp ).find( "No module named" ) == 0:
        return S_OK()
      errMsg = "Can't load %s" % ".".join( modName )
      if not hideExceptions:
        gLogger.exception( errMsg )
      return S_ERROR( errMsg )
    if len( modName ) == 1:
      return S_OK( impModule )
    return self.__recurseImport( modName[1:], impModule )



