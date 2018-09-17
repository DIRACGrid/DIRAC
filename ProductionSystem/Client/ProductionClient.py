""" Class that contains client access to the production DB handler. """

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.Client import Client
from DIRAC.ProductionSystem.Utilities.StateMachine import ProductionsStateMachine


class ProductionClient(Client):

  """ Exposes the functionality available in the ProductionSystem/ProductionManagerHandler
  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """

    Client.__init__(self, **kwargs)
    self.setServer('Production/ProductionManager')

  # Method applying the Production System State machine

  def _applyProductionStatusStateMachine(self, prodID, status, force):
    """ Performs a state machine check for productions when asked to change the status
    """
    res = self.getProductionParameters(prodID, 'Status')
    originalStatus = res['Value']
    proposedStatus = status
    if force:
      return proposedStatus
    else:
      stateChange = ProductionsStateMachine(originalStatus).setState(proposedStatus)
      if not stateChange['OK']:
        return originalStatus
      else:
        return stateChange['Value']

  # Methods contacting the ProductionManager Service

  def setProductionStatus(self, prodID, status):
    """ Sets the production status
    """
    rpcClient = self._getRPC()
    # Apply the production state machine
    newStatus = self._applyProductionStatusStateMachine(prodID, status, force=False)
    if newStatus != status:
      return S_ERROR('Cannot change status')
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
