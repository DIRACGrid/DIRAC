import re
import os
import DIRAC
from DIRAC import gLogger
from DIRAC.Core.Utilities import List, DIRACSingleton
from DIRAC.ConfigurationSystem.Client.Helpers import CSGlobals
from DIRAC.AccountingSystem.Client.Types.BaseAccountingType import BaseAccountingType

class TypeLoader( object ):
  __metaclass__ = DIRACSingleton.DIRACSingleton

  def __init__( self ):
    self.__loaded = {}
    self.__path = "AccountingSystem/Client/Types"
    self.__parentCls = BaseAccountingType
    self.__reFilter = re.compile( ".*[a-z1-9]\.py$" )

  def getTypes( self ):
    if not self.__loaded:
      self.__loaded = self.__loadObjects()
    return self.__loaded

  def __loadObjects( self ):
    pathList = List.fromChar( self.__path, "/" )

    parentModuleList = [ "%sDIRAC" % ext for ext in CSGlobals.getCSExtensions() ] + [ 'DIRAC' ]
    objectsToLoad = {}
    #Find which object files match
    for parentModule in parentModuleList:
      objDir = os.path.join( DIRAC.rootPath, parentModule, *pathList )
      if not os.path.isdir( objDir ):
        continue
      for objFile in os.listdir( objDir ):
        if self.__reFilter.match( objFile ):
          pythonClassName = objFile[:-3]
          if pythonClassName not in objectsToLoad:
            gLogger.info( "Adding to load queue %s/%s/%s" % ( parentModule, self.__path, pythonClassName ) )
            objectsToLoad[ pythonClassName ] = parentModule

    #Load them!
    loadedObjects = {}

    for pythonClassName in objectsToLoad:
      parentModule = objectsToLoad[ pythonClassName ]
      try:
        #Where parentModule can be DIRAC, pathList is something like [ "AccountingSystem", "Client", "Types" ]
        #And the python class name is.. well, the python class name
        objPythonPath = "%s.%s.%s" % ( parentModule, ".".join( pathList ), pythonClassName )
        objModule = __import__( objPythonPath,
                                 globals(),
                                 locals(), pythonClassName )
        objClass = getattr( objModule, pythonClassName )
      except Exception, e:
        gLogger.error( "Can't load type %s/%s: %s" % ( parentModule, pythonClassName, str( e ) ) )
        continue
      if self.__parentCls == objClass:
        continue
      if self.__parentCls and not issubclass( objClass, self.__parentCls ):
        gLogger.warn( "%s is not a subclass of %s. Skipping" % ( objClass, self.__parentCls ) )
        continue
      gLogger.info( "Loaded %s" % objPythonPath )
      loadedObjects[ pythonClassName ] = objClass

    return loadedObjects
