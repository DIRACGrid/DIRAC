""" An utility to load modules and objects in DIRAC and extensions, being sure that the extensions are considered
"""

__RCSID__ = "$Id$"

import re
import types
import imp
import pkgutil
import collections
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import List, DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals

class ObjectLoader( object ):
  """ Class for loading objects. Example:

      from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
      ol = ObjectLoader()
      ol.loadObject('TransformationSystem.Client.TransformationClient')
  """
  __metaclass__ = DIRACSingleton.DIRACSingleton

  def __init__( self, baseModules = False ):
    """ init
    """
    if not baseModules:
      baseModules = [ 'DIRAC' ]
    self.__rootModules = baseModules
    self.__objs = {}
    self.__generateRootModules( baseModules )

  def __rootImport( self, modName, hideExceptions = False ):
    """ Auto search which root module has to be used
    """
    for rootModule in self.__rootModules:
      impName = modName
      if rootModule:
        impName = "%s.%s" % ( rootModule, impName )
      gLogger.debug( "Trying to load %s" % impName )
      result = self.__recurseImport( impName, hideExceptions = hideExceptions )
      #Error. Something cannot be imported. Return error
      if not result[ 'OK' ]:
        return result
      #Huge success!
      if result[ 'Value' ]:
        return S_OK( ( impName, result[ 'Value' ] ) )
      #Nothing found, continue
    #Return nothing found
    return S_OK()


  def __recurseImport( self, modName, parentModule = None, hideExceptions = False, fullName = False ):
    """ Internal function to load modules
    """
    if type( modName ) in types.StringTypes:
      modName = List.fromChar( modName, "." )
    if not fullName:
      fullName = ".".join( modName )
    if fullName in self.__objs:
      return S_OK( self.__objs[ fullName ] )
    try:
      if parentModule:
        impData = imp.find_module( modName[0], parentModule.__path__ )
      else:
        impData = imp.find_module( modName[0] )
      impModule = imp.load_module( modName[0], *impData )
      if impData[0]:
        impData[0].close()
    except ImportError, excp:
      if str( excp ).find( "No module named %s" % modName[0] ) == 0:
        return S_OK( None )
      errMsg = "Can't load %s in %s" % ( ".".join( modName ), parentModule.__path__[0] )
      if not hideExceptions:
        gLogger.exception( errMsg )
      return S_ERROR( errMsg )
    if len( modName ) == 1:
      self.__objs[ fullName ] = impModule
      return S_OK( impModule )
    return self.__recurseImport( modName[1:], impModule,
                                 hideExceptions = hideExceptions, fullName = fullName )

  def __generateRootModules( self, baseModules ):
    """ Iterate over all the possible root modules
    """
    self.__rootModules = baseModules
    for rootModule in reversed( CSGlobals.getCSExtensions() ):
      if rootModule[-5:] != "DIRAC" and rootModule not in self.__rootModules:
        self.__rootModules.append( "%sDIRAC" % rootModule )
    self.__rootModules.append( "" )

    # Reversing the order because we want first to look in the extension(s)
    self.__rootModules.reverse()


  def loadModule( self, importString ):
    """ Load a module from an import string
    """
    result = self.__rootImport( importString )
    if not result[ 'OK' ]:
      return result
    if not result[ 'Value' ]:
      return S_ERROR( "No module %s found" % importString )
    return S_OK( result[ 'Value' ][1] )

  def loadObject( self, importString, objName = False ):
    """ Load an object from inside a module
    """
    result = self.loadModule( importString )
    if not result[ 'OK' ]:
      return result
    modObj = result[ 'Value' ]

    if not objName:
      objName = List.fromChar( importString, "." )[-1]

    try:
      return S_OK( getattr( modObj, objName ) )
    except AttributeError:
      return S_ERROR( "%s does not contain a %s object" % ( importString, objName ) )

  def getObjects( self, modulePath, reFilter = None, parentClass = None, recurse = False ):
    """ Search for modules under a certain path

        modulePath is the import string needed to access the parent module.
        Root modules will be included automatically (like DIRAC). For instance "ConfigurationSystem.Service"

        reFilter is a regular expression to filter what to load. For instance ".*Handler"
        parentClass is a class object from which the loaded modules have to import from. For instance RequestHandler
    """

    if 'OrderedDict' in dir( collections ):
      modules = collections.OrderedDict()
    else:
      modules = {}

    if type( reFilter ) in types.StringTypes:
      reFilter = re.compile( reFilter )


    for rootModule in self.__rootModules:
      if rootModule:
        impPath = "%s.%s" % ( rootModule, modulePath )
      else:
        impPath = modulePath
      gLogger.debug( "Trying to load %s" % impPath )

      result = self.__recurseImport( impPath )
      if not result[ 'OK' ]:
        return result
      if not result[ 'Value' ]:
        continue

      parentModule = result[ 'Value' ]
      fsPath = parentModule.__path__[0]
      gLogger.verbose( "Loaded module %s at %s" % ( impPath, fsPath ) )

      for _modLoader, modName, isPkg in pkgutil.walk_packages( parentModule.__path__ ):
        if reFilter and not reFilter.match( modName ):
          continue
        if isPkg:
          if recurse:
            result = self.getObjects( "%s.%s" % ( modulePath, modName ), reFilter = reFilter,
                                      parentClass = parentClass, recurse = recurse )
            if not result[ 'OK' ]:
              return result
            modules.update( result[ 'Value' ] )
          continue
        modKeyName = "%s.%s" % ( modulePath, modName )
        if modKeyName in modules:
          continue
        fullName = "%s.%s" % ( impPath, modName )
        result = self.__recurseImport( modName, parentModule = parentModule, fullName = fullName )
        if not result[ 'OK' ]:
          return result
        if not result[ 'Value' ]:
          continue
        modObj = result[ 'Value' ]

        try:
          modClass = getattr( modObj, modName )
        except AttributeError:
          gLogger.warn( "%s does not contain a %s object" % ( fullName, modName ) )
          continue

        if parentClass and not issubclass( modClass, parentClass ):
          continue

        #Huge success!
        modules[ modKeyName ] = modClass

    return S_OK( modules )
