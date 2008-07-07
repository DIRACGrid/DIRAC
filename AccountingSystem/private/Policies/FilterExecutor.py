
import types
from DIRAC import S_OK, S_ERROR, gLogger

class FilterExecutor:

  ALLKW = "all"

  def __init__(self):
    self.__filters = {}
    self.__globalFilters = []

  def applyFilters( self, id, credDict, condDict, groupingList ):
    filters2Apply = list( self.__globalFilters )
    if id in self.__filters:
      filters2Apply.extend( self.__filters[ id ] )
    for filter in filters2Apply:
      try:
        gLogger.info( "Applying filter %s for %s" % ( filter.__name__, id ) )
        retVal = filter( credDict, condDict, groupingList )
        if not retVal[ 'OK' ]:
          gLogger.info( "Filter %s for %s failed: %s" % ( filter.__name__, id, retVal[ 'Message' ] ) )
          return retVal
      except Exception, e:
        gLogger.exception( "Exception while applying filter", "%s for %s" % ( filter.__name__, id ), e )
        return S_ERROR( "Exception while applying filters" )
    return S_OK()

  def addFilter( self, id, filter ):
    if id not in self.__filters:
      self.__filters[ id ] = []
    if type( filter ) in ( types.ListType, types.TupleType ):
      self.__filters[ id ].extend( filter )
    else:
      self.__filters[ id ].append( filter )

  def addGlobalFilter( self, filter ):
    if type( filter ) in ( types.ListType, types.TupleType ):
      self.__globalFilters.extend( filter )
    else:
      self.__globalFilters.append( filter )