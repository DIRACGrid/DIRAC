from DIRAC import S_ERROR, S_OK, gLogger

from DIRAC.DataManagementSystem.private.FTSAbstractPlacement import FTSAbstractPlacement, FTSRoute
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getFTS3Servers

import random

class FTS3Placement( FTSAbstractPlacement ):

  """
  This class manages all the FTS strategies, routes and what not
  """


  __serverPolicy = "Random"
  __nextServerID = 0
  __serverList = None
  __maxAttempts = 0


  def __init__( self, csPath = None, ftsHistoryViews = None ):
    self.log = gLogger.getSubLogger( "FTS3Placement" )
    super( FTS3Placement, self ).__init__( csPath = csPath, ftsHistoryViews = ftsHistoryViews )
    srvList = getFTS3Servers()
    if not srvList['OK']:
      self.log.error( srvList['Message'] )

    self.__serverList = srvList.get( 'Value', [] )
    self.maxAttempts = len( self.__serverList )



  def getReplicationTree( self, sourceSEs, targetSEs, size, strategy = None ):
    """ For multiple source to multiple destination, find the optimal replication
        strategy.

       :param sourceSEs : list of source SE
       :param targetSEs : list of destination SE
       :param size : size of the File
       :param strategy : which strategy to use

       :returns S_OK(dict) < route name :  { dict with key Ancestor, SourceSE, TargetSEtargetSE, Strategy } >

       For the time being, we are waiting for FTS3 to provide advisory mechanisms. So we just use
       simple techniques
    """

    # We will use a single random source
    sourceSE = random.choice( sourceSEs )

    tree = {}
    for targetSE in targetSEs:
      tree["%s#%s" % ( sourceSE, targetSE )] = { "Ancestor" : False, "SourceSE" : sourceSE,
                           "TargetSE" : targetSE, "Strategy" : "FTS3Simple" }

    return S_OK( tree )



  def refresh( self, ftsHistoryViews ):
    """
    Refresh, whatever that means... recalculate all what you need,
    fetches the latest conf and what not.
    """
    return super( FTS3Placement, self ).refresh( ftsHistoryViews = ftsHistoryViews )



  def __failoverServerPolicy(self, attempt = 0):
    if attempt >= len( self.__serverList ):
      raise Exception( "FTS3Placement.__failoverServerPolicy: attempt to reach non existing server index" )

    return self.__serverList[attempt]

  def __sequenceServerPolicy( self ):
    fts3server = self.__serverList[self.__nextServerID]
    self.__nextServerID = ( self.__nextServerID + 1 ) % len( self.__serverList )
    return fts3server

  def __randomServerPolicy(self):
    return random.choice( self.__serverList )


  def chooseFTS3Server( self, sourceSE, targetSE ):
    """
      Choose the appropriate FTS3 server depending on the policy
    """

    fts3Server = None
    attempt = 0

    while not fts3Server and attempt < self.maxAttempts:
      attempt += 1
      if self.__serverPolicy == 'Random':
        fts3Server = self.__randomServerPolicy()
      elif self.__serverPolicy == 'Sequence':
        fts3Server = self.__sequenceServerPolicy()
      elif self.__serverPolicy == 'Failover':
        fts3Server = self.__failoverServerPolicy( attempt = attempt )


    if fts3Server:
      return S_OK( fts3Server )

    return S_ERROR ( "Could not find an FTS3 server (max attempt reached)" )

  def findRoute( self, sourceSE, targetSE ):
    """ Find the appropriate route from point A to B
      :param sourceSE : source SE
      :param targetSE : destination SE

      :returns S_OK(FTSRoute)

    """

    fts3server = self.chooseFTS3Server( sourceSE, targetSE )

    if not fts3server['OK']:
      return fts3server

    fts3server = fts3server['Value']

    route = FTSRoute( sourceSE, targetSE, fts3server )

    return S_OK( route )

  def isRouteValid( self, route ):
    """ In FTS3, all routes are valid a priori
       :param route : FTSRoute

       :returns S_OK or S_ERROR(reason)
    """

    return S_OK()

#   def startTransferOnRoute( self, route ):
#     """Declare that one starts a transfer on a given route.
#        Accounting purpose only
#
#        :param route : FTSRoute that is used
#
#     """
#     edge = self.fts2Strategy.ftsGraph.findRoute( route.sourceSE, route.targetSE )
#
#     if edge['OK']:
#       edge['Value'].ActiveJobs += 1
#
#     return S_OK()
#
#   def finishTransferOnRoute( self, route ):
#     """Declare that one finishes a transfer on a given route.
#        Accounting purpose only
#
#        :param route : FTSRoute that is used
#
#     """
#     edge = self.fts2Strategy.ftsGraph.findRoute( route.sourceSE, route.targetSE )
#
#     if edge['OK']:
#       edge['Value'].ActiveJobs -= 1
#
#     return S_OK()
