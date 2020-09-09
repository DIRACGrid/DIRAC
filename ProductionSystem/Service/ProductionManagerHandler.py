""" DISET request handler base class for the ProductionDB.
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.ProductionSystem.DB.ProductionDB import ProductionDB

prodTypes = [basestring, int]
transTypes = [basestring, int, list]


class ProductionManagerHandlerBase(RequestHandler):

  def _parseRes(self, res):
    if not res['OK']:
      gLogger.error('ProductionManager failure', res['Message'])
    return res

  def setDatabase(self, oDatabase):
    global database
    database = oDatabase

  ####################################################################
  #
  # These are the methods to manipulate the Productions table
  #

  types_addProduction = [basestring, basestring]

  def export_addProduction(self, prodName, prodDescription):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    authorGroup = credDict.get('group')
    res = database.addProduction(prodName, prodDescription, authorDN, authorGroup)
    if res['OK']:
      gLogger.info("Added production %d" % res['Value'])
    return self._parseRes(res)

  types_deleteProduction = [prodTypes]

  def export_deleteProduction(self, prodName):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    res = database.deleteProduction(prodName, author=authorDN)
    return self._parseRes(res)

  types_getProductions = []

  def export_getProductions(self, condDict=None, older=None, newer=None, timeStamp='CreationDate',
                            orderAttribute=None, limit=None, offset=None):
    if not condDict:
      condDict = {}
    res = database.getProductions(condDict=condDict,
                                  older=older,
                                  newer=newer,
                                  timeStamp=timeStamp,
                                  orderAttribute=orderAttribute,
                                  limit=limit,
                                  offset=offset)
    return self._parseRes(res)

  types_getProduction = [prodTypes]

  def export_getProduction(self, prodName):
    res = database.getProduction(prodName)
    return self._parseRes(res)

  types_getProductionParameters = [prodTypes, [basestring, list, tuple]]

  def export_getProductionParameters(self, prodName, parameters):
    res = database.getProductionParameters(prodName, parameters)
    return self._parseRes(res)

  types_setProductionStatus = [prodTypes, basestring]

  def export_setProductionStatus(self, prodName, status):
    res = database.setProductionStatus(prodName, status)
    return self._parseRes(res)

  types_startProduction = [prodTypes]

  def export_startProduction(self, prodName):
    res = database.startProduction(prodName)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the ProductionTransformations table
  #

  types_addTransformationsToProduction = [prodTypes, transTypes, transTypes]

  def export_addTransformationsToProduction(self, prodName, transIDs, parentTransIDs):
    res = database.addTransformationsToProduction(prodName, transIDs, parentTransIDs=parentTransIDs)
    return self._parseRes(res)

  types_getProductionTransformations = []

  def export_getProductionTransformations(
          self,
          prodName,
          condDict=None,
          older=None,
          newer=None,
          timeStamp='CreationTime',
          orderAttribute=None,
          limit=None,
          offset=None):

    if not condDict:
      condDict = {}
    res = database.getProductionTransformations(
        prodName,
        condDict=condDict,
        older=older,
        newer=newer,
        timeStamp=timeStamp,
        orderAttribute=orderAttribute,
        limit=limit,
        offset=offset)

    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods to manipulate the ProductionSteps table
  #

  types_addProductionStep = [dict]

  def export_addProductionStep(self, prodStep):
    stepName = prodStep['name']
    stepDescription = prodStep['description']
    stepLongDescription = prodStep['longDescription']
    stepBody = prodStep['body']
    stepType = prodStep['stepType']
    stepPlugin = prodStep['plugin']
    stepAgentType = prodStep['agentType']
    stepGroupSize = prodStep['groupsize']
    stepInputQuery = prodStep['inputquery']
    stepOutputQuery = prodStep['outputquery']
    res = database.addProductionStep(stepName, stepDescription, stepLongDescription, stepBody, stepType, stepPlugin,
                                     stepAgentType, stepGroupSize, stepInputQuery, stepOutputQuery)
    if res['OK']:
      gLogger.info("Added production step %d" % res['Value'])
    return self._parseRes(res)

  types_getProductionStep = [int]

  def export_getProductionStep(self, stepID):
    res = database.getProductionStep(stepID)
    return self._parseRes(res)

  ####################################################################
  #
  # These are the methods for production logging manipulation
  #

  ####################################################################
  #
  # These are the methods used for web monitoring
  #

  ###########################################################################


database = False


def initializeProductionManagerHandler(serviceInfo):
  global database
  database = ProductionDB('ProductionDB', 'Production/ProductionDB')
  return S_OK()


class ProductionManagerHandler(ProductionManagerHandlerBase):

  def __init__(self, *args, **kargs):
    self.setDatabase(database)
    ProductionManagerHandlerBase.__init__(self, *args, **kargs)
