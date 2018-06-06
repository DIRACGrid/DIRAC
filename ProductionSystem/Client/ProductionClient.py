""" Class that contains client access to the production DB handler. """

__RCSID__ = "$Id$"

from DIRAC                                                import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client                                        import Client

class ProductionClient( Client ):


  """ Exposes the functionality available in the DIRAC/ProductionHandler
  """
  def __init__( self, **kwargs ):
    """ Simple constructor
    """

    Client.__init__( self, **kwargs )
    self.setServer( 'Production/ProductionManager' )

    self.prodDescription =  {}
    self.stepCounter = 1
    ##### Default values for transformation step parameters
    self.stepDescription = 'description'
    self.stepLongDescription = 'longDescription'
    self.stepType = 'MCSimulation'
    self.stepPlugin = 'Standard'
    self.stepAgentType = 'Manual'
    self.stepFileMask = ''
    #########################################
    self.stepInputquery = {}
    self.stepOutputquery = {}
    self.stepGroupSize = 1
    self.stepBody = 'body'

  def setServer( self, url ):
    self.serverURL = url

  ### Methods working on the client to prepare the production description
  def getDescription( self ):
    """ get the production description
    """
    return self.prodDescription

  def setDescription( self, prodDescription ):
    """ set the production description
    """
    self.prodDescription = prodDescription

  def addStep( self, prodStep ):
    """ add a step to the production description
    """
    ### Mandatory fields ###################
    stepName = 'Step' + str(self.stepCounter)
    self.stepCounter+=1
    prodStep['name'] = stepName

    if 'description' not in prodStep:
      prodStep['description'] = self.stepDescription
    if 'longDescription' not in prodStep:
      prodStep['longDescription'] = self.stepLongDescription
    if 'type' not in prodStep:
      prodStep['type'] = self.stepType
    if 'plugin' not in prodStep:
      prodStep['plugin'] = self.stepPlugin
    if 'agentType' not in prodStep:
      prodStep['agentType'] = self.stepAgentType
    if 'fileMask' not in prodStep:
      prodStep['fileMask'] = self.stepFileMask
    ### Optional fields ###################
    if 'inputquery' not in prodStep:
      prodStep['inputquery'] = self.stepInputquery
    if 'outputquery' not in prodStep:
      prodStep['outputquery'] = self.stepOutputquery
    if 'groupsize' not in prodStep:
      prodStep['groupsize'] = self.stepGroupSize
    if 'body' not in prodStep:
      prodStep['body'] = self.stepBody

    self.prodDescription[stepName] = prodStep

  ### Methods to contact the ProductionManager Service

  ### Obsolete: to be replaced by createProduction
  def addProduction( self, prodName, timeout = 1800 ):
    """ add a new production
    """
    rpcClient = self._getRPC( timeout = timeout )
    return rpcClient.addProduction( prodName )

  def createProduction( self, prodName, prodDescription, timeout = 1800 ):
    """ create a new production starting from its description
    """
    rpcClient = self._getRPC( timeout = timeout )
    return rpcClient.createProduction( prodName, prodDescription )

  def startProduction( self, prodID ):
    """ Instantiate the transformations of the production and start the production
    """
    rpcClient = self._getRPC()
    return rpcClient.startProduction( prodID )

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






