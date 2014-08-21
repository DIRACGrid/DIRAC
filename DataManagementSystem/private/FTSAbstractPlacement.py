from DIRAC import S_ERROR, S_OK, gLogger

from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus

class FTSRoute(object):
  """
    This class represents the route of a transfer: source, dest and which server
  """
  
  def __init__( self, sourceSE, targetSE, ftsServer ):
    """
      :param sourceSE : source se
      :param targetSE : destination SE
      :param ftsServer : fts server to use
    """

    self.sourceSE = sourceSE
    self.targetSE = targetSE
    self.ftsServer = ftsServer



class FTSAbstractPlacement( object ):
  """
  This class manages all the FTS strategies, routes and what not
  """
  
  def __init__( self, csPath = None, ftsHistoryViews = None ):
    """
       Nothing special done here
       :param csPath : path of the CS
       :param ftsHistoryViews : history view of the db (useful for FTS2)
    """
    self.csPath = csPath
    self.ftsHistoryViews = ftsHistoryViews

    self.rssStatus = ResourceStatus()
    
    self.log = gLogger.getSubLogger( 'FTSAbstractPlacement', True )


  def getReplicationTree( self, sourceSEs, targetSEs, size, strategy = None ):
    """ For multiple source to multiple destination, find the optimal replication
        strategy.

       :param sourceSEs : list of source SE
       :param targetSEs : list of destination SE
       :param size : size of the File
       :param strategy : which strategy to use

       :returns S_OK(dict) < route name :  { dict with key Ancestor, SourceSE, TargetSEtargetSE, Strategy } >
    """

    return S_ERROR( 'IMPLEMENT ME' )
  
  def refresh( self, ftsHistoryViews = None ):
    """
    Refresh, whatever that means... recalculate all what you need,
    fetches the latest conf and what not.
    """
    return S_OK()

  
  def findRoute( self, sourceSE, targetSE ):
    """ Find the appropriate route from point A to B
      :param sourceSE : source SE
      :param targetSE : destination SE

      :returns S_OK(FTSRoute)

    """
    return S_ERROR( 'IMPLEMENT ME' )

  def isRouteValid( self, route ):
    """ Check whether a given route is valid
       (whatever that means here)
       :param route : FTSRoute

       :returns S_OK or S_ERROR(reason)
    """

    return S_ERROR( 'IMPLEMENT ME' )

  def startTransferOnRoute( self, route ):
    """Declare that one starts a transfer on a given route.
       Accounting purpose only

       :param route : FTSRoute that is used

    """
    return S_OK()

  def finishTransferOnRoute( self, route ):
    """Declare that one finishes a transfer on a given route.
       Accounting purpose only

       :param route : FTSRoute that is used

    """
    return S_OK()


