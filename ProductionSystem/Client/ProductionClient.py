""" Class that contains client access to the production DB handler. """

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client


class ProductionClient(Client):

  """ Exposes the functionality available in the DIRAC/ProductionHandler
  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """

    Client.__init__(self, **kwargs)
    self.setServer('Production/ProductionManager')
    self.prodDescription = {}
    self.stepCounter = 1

  def setServer(self, url):
    self.serverURL = url

  # Methods working on the client to prepare the production description

  def getDescription(self):
    """ Get the production description
    """
    return self.prodDescription

  def setDescription(self, prodDescription):
    """ Set the production description

        prodDescription is a dictionary with production description
    """
    self.prodDescription = prodDescription

  def addStep(self, prodStep):
    """ Add a step to the production description, by updating the description dictionary

        prodStep is a ProductionSystem.Client.ProductionStep object
    """

    stepName = 'Step' + str(self.stepCounter) + '_' + prodStep.Name
    self.stepCounter += 1
    prodStep.Name = stepName

    res = prodStep.getAsDict()
    if not res['OK']:
      return S_ERROR('Failed to add step to production:', res['Message'])
    prodStepDict = res['Value']
    prodStepDict['name'] = stepName

    self.prodDescription[prodStep.Name] = prodStepDict
    return S_OK()

  # Methods to contact the ProductionManager Service

  def addProduction(self, prodName, prodDescription, timeout=1800):
    """ Create a new production starting from its description
    """
    rpcClient = self._getRPC(timeout=timeout)
    return rpcClient.addProduction(prodName, prodDescription)

  def startProduction(self, prodID):
    """ Instantiate the transformations of the production and start the production
    """
    rpcClient = self._getRPC()
    return rpcClient.startProduction(prodID)

  def setProductionStatus(self, prodID, status):
    """ Sets the production status
    """
    rpcClient = self._getRPC()
    return rpcClient.setProductionStatus(prodID, status)

  def getProductions(self, condDict=None, older=None, newer=None, timeStamp=None,
                     orderAttribute=None, limit=100):
    """ Gets all the productions in the system, incrementally. "limit" here is just used to determine the offset.
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
      res = rpcClient.getProductions(condDict, older, newer, timeStamp, orderAttribute, limit,
                                     offsetToApply)
      if not res['OK']:
        return res
      else:
        gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res['Value'])))
        if res['Value']:
          productions = productions + res['Value']
          offsetToApply += limit
        if len(res['Value']) < limit:
          break
    return S_OK(productions)

  def getProduction(self, prodName):
    """ gets a specific production.
    """
    rpcClient = self._getRPC()
    return rpcClient.getProduction(prodName)

  def getProductionTransformations(self, prodName, condDict=None, older=None, newer=None, timeStamp=None,
                                   orderAttribute=None, limit=10000):
    """ Gets all the production transformations for a production, incrementally.
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
      res = rpcClient.getProductionTransformations(prodName, condDict, older, newer, timeStamp, orderAttribute, limit,
                                                   offsetToApply)
      if not res['OK']:
        return res
      else:
        gLogger.verbose("Result for limit %d, offset %d: %d" % (limit, offsetToApply, len(res['Value'])))
        if res['Value']:
          productionTransformations = productionTransformations + res['Value']
          offsetToApply += limit
        if len(res['Value']) < limit:
          break
    return S_OK(productionTransformations)
