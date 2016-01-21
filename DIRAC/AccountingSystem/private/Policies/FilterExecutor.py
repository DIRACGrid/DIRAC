
import types
from DIRAC import S_OK, S_ERROR, gLogger

class FilterExecutor:

  ALLKW = "all"

  def __init__( self ):
    self.__filters = {}
    self.__globalFilters = []

  def applyFilters( self, iD, credDict, condDict, groupingList ):
    filters2Apply = list( self.__globalFilters )
    if iD in self.__filters:
      filters2Apply.extend( self.__filters[ iD ] )
    for myFilter in filters2Apply:
      try:
        gLogger.info( "Applying filter %s for %s" % ( myFilter.__name__, iD ) )
        retVal = myFilter( credDict, condDict, groupingList )
        if not retVal[ 'OK' ]:
          gLogger.info( "Filter %s for %s failed: %s" % ( myFilter.__name__, iD, retVal[ 'Message' ] ) )
          return retVal
      except:
        gLogger.exception( "Exception while applying filter", "%s for %s" % ( myFilter.__name__, iD ) )
        return S_ERROR( "Exception while applying filters" )
    return S_OK()

  def addFilter( self, iD, myFilter ):
    if iD not in self.__filters:
      self.__filters[ iD ] = []
    if type( myFilter ) in ( types.ListType, types.TupleType ):
      self.__filters[ iD ].extend( myFilter )
    else:
      self.__filters[ iD ].append( myFilter )

  def addGlobalFilter( self, myFilter ):
    if type( myFilter ) in ( types.ListType, types.TupleType ):
      self.__globalFilters.extend( myFilter )
    else:
      self.__globalFilters.append( myFilter )
