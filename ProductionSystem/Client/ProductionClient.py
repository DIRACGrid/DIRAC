""" Class that contains client access to the production DB handler. """

__RCSID__ = "$Id$"

from DIRAC                                                         import S_OK, gLogger
from DIRAC.Core.Base.Client                                        import Client
from DIRAC.ConfigurationSystem.Client.Helpers.Operations           import Operations

class ProductionClient( Client ):


  """ Exposes the functionality available in the DIRAC/ProductionHandler
  """
  def __init__( self, **kwargs ):
    """ Simple constructor
    """

    Client.__init__( self, **kwargs )
    self.setServer( 'Production/ProductionManager' )

  def setServer( self, url ):
    self.serverURL = url

  def setName ( self, prodName ):
    """
          set the name of the production
    """
    pass

  def addTransformation( self, transformation ):
    """
          add a transformation to the production
    """
    pass

  def addProduction( self, prodName, timeout = 1800 ):
    """ add a new production
    """
    rpcClient = self._getRPC( timeout = timeout )
    return rpcClient.addProduction( prodName )

  def setProductionStatus( self, prodID, status ):
    """ Sets the production status
    """
    rpcClient = self._getRPC()
    return rpcClient.setProductionStatus( prodID, status )

  def getProductions( self, condDict = None, older = None, newer = None, timeStamp = None,
                          orderAttribute = None, limit = 100 ):
    """ gets all the productions in the system, incrementally. "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC()

    productions = []
    if condDict is None:
      condDict = {}
    if timeStamp is None:
      timeStamp = 'CreationDate'
    # getting transformations - incrementally
    offsetToApply = 0
    while True:
      res = rpcClient.getProductions( condDict, older, newer, timeStamp, orderAttribute, limit,
                                          offsetToApply )
      if not res['OK']:
        return res
      else:
        gLogger.verbose( "Result for limit %d, offset %d: %d" % ( limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          productions = productions + res['Value']
          offsetToApply += limit
        if len( res['Value'] ) < limit:
          break
    return S_OK( productions )

  def getProduction( self, prodName ):
    """ gets a specific production.
    """
    rpcClient = self._getRPC()
    return rpcClient.getProduction( prodName )

  def getProductionTransformations( self, prodName, condDict = None, older = None, newer = None, timeStamp = None,
                              orderAttribute = None, limit = 10000 ):
    """ gets all the production transformations for a production, incrementally.
        "limit" here is just used to determine the offset.
    """
    rpcClient = self._getRPC()
    productionTransformations = []

    if condDict is None:
      condDict = {}
    if timeStamp is None:
      timeStamp = 'CreationTime'
    # getting productionTransformations - incrementally
    offsetToApply = 0
    while True:
      res = rpcClient.getProductionTransformations( prodName, condDict, older, newer, timeStamp, orderAttribute, limit,
                                                    offsetToApply )
      if not res['OK']:
        return res
      else:
        gLogger.verbose( "Result for limit %d, offset %d: %d" % ( limit, offsetToApply, len( res['Value'] ) ) )
        if res['Value']:
          productionTransformations = productionTransformations + res['Value']
          offsetToApply += limit
        if len( res['Value'] ) < limit:
          break
    return S_OK( productionTransformations )






