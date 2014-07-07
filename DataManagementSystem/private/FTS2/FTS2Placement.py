from DIRAC import S_ERROR, S_OK

from DIRAC.DataManagementSystem.private.FTSAbstractPlacement import FTSAbstractPlacement, FTSRoute
from DIRAC.DataManagementSystem.private.FTS2.FTS2Strategy import FTS2Strategy
# from DIRAC.DataManagementSystem.private.FTS2Graph import FTS2Graph


class FTS2Placement(FTSAbstractPlacement):

  """
  This class manages all the FTS strategies, routes and what not
  """

  def __init__( self, csPath = None, ftsHistoryViews = None ):
    super( FTS2Placement, self ).__init__( csPath = csPath, ftsHistoryViews = ftsHistoryViews )
#     self.fts2Graph = FTS2Graph( "FTSGraph", ftsHistoryViews = ftsHistoryViews )
    self.fts2Strategy = FTS2Strategy( csPath = csPath, ftsHistoryViews = ftsHistoryViews )


  def getReplicationTree( self, sourceSEs, targetSEs, size, strategy = None ):
    """ For multiple source to multiple destination, find the optimal replication
        strategy.

       :param sourceSEs : list of source SE
       :param targetSEs : list of destination SE
       :param size : size of the File
       :param strategy : which strategy to use

       :returns S_OK(dict) < route name :  { dict with key Ancestor, SourceSE, TargetSEtargetSE, Strategy } >
    """

    return self.fts2Strategy.replicationTree( sourceSEs = sourceSEs,
                                              targetSEs = targetSEs,
                                              size = size,
                                              strategy = strategy )


  def refresh( self, ftsHistoryViews ):
    """
    Refresh, whatever that means... recalculate all what you need,
    fetches the latest conf and what not.
    """
    super( FTS2Placement, self ).refresh( ftsHistoryViews = ftsHistoryViews )
    self.fts2Strategy.resetGraph( ftsHistoryViews )
    return self.fts2Strategy.updateRWAccess()



  def findRoute( self, sourceSE, targetSE ):
    """ Find the appropriate route from point A to B
      :param sourceSE : source SE
      :param targetSE : destination SE

      :returns S_OK(FTSRoute)

    """

    edge = self.fts2Strategy.ftsGraph.findRoute( sourceSE, targetSE )

    if not edge['OK']:
      return edge

    edge = edge['Value']

    # The FTS2 server to use is the one from the destination
    route = FTSRoute( sourceSE, targetSE, edge.toNode.FTSServer )

    return S_OK( route )

  def isRouteValid( self, route ):
    """ Check whether a given route is valid
       (whatever that means here)
       :param route : FTSRoute

       :returns S_OK or S_ERROR(reason)
    """

    edge = self.fts2Strategy.ftsGraph.findRoute( route.sourceSE, route.targetSE )

    if not edge['OK']:
      return edge

    edge = edge['Value']

    sourceRead = edge.fromNode.SEs[route.sourceSE]["read"]
    if not sourceRead:
      return S_ERROR( "SourceSE %s is banned for reading right now" % route.sourceSE )

    targetWrite = edge.toNode.SEs[route.targetSE]["write"]
    if not targetWrite:
      return S_ERROR( "TargetSE %s is banned for writing right now" % route.targetSE )

    if edge.ActiveJobs > edge.toNode.MaxActiveJobs:
      return S_ERROR( "unable to submit new FTS job, max active jobs reached" )

    return S_OK()

  def startTransferOnRoute( self, route ):
    """Declare that one starts a transfer on a given route.
       Accounting purpose only

       :param route : FTSRoute that is used

    """
    edge = self.fts2Strategy.ftsGraph.findRoute( route.sourceSE, route.targetSE )

    if edge['OK']:
      edge['Value'].ActiveJobs += 1

    return S_OK()

  def finishTransferOnRoute( self, route ):
    """Declare that one finishes a transfer on a given route.
       Accounting purpose only

       :param route : FTSRoute that is used

    """
    edge = self.fts2Strategy.ftsGraph.findRoute( route.sourceSE, route.targetSE )

    if edge['OK']:
      edge['Value'].ActiveJobs -= 1

    return S_OK()
