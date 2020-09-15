""" DISET request handler base class for the ProductionDB.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

import six

from DIRAC import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.Core.Utilities.DEncode import ignoreEncodeWarning
from DIRAC.ProductionSystem.DB.ProductionDB import ProductionDB

prodTypes = [six.string_types, int]
transTypes = [six.string_types, int, list]


class ProductionManagerHandlerBase(RequestHandler):

  def setDatabase(self, oDatabase):
    global database
    database = oDatabase

  ####################################################################
  #
  # These are the methods to manipulate the Productions table
  #

  types_addProduction = [six.string_types, six.string_types]

  def export_addProduction(self, prodName, prodDescription):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    authorGroup = credDict.get('group')
    res = database.addProduction(prodName, prodDescription, authorDN, authorGroup)
    if res['OK']:
      gLogger.info("Added production %d" % res['Value'])
    return res

  types_deleteProduction = [prodTypes]

  def export_deleteProduction(self, prodName):
    credDict = self.getRemoteCredentials()
    authorDN = credDict.get('DN', credDict.get('CN'))
    return database.deleteProduction(prodName, author=authorDN)

  types_getProductions = []

  @staticmethod
  def export_getProductions(condDict=None, older=None, newer=None, timeStamp='CreationDate',
                            orderAttribute=None, limit=None, offset=None):
    if not condDict:
      condDict = {}
    return database.getProductions(condDict=condDict,
                                   older=older,
                                   newer=newer,
                                   timeStamp=timeStamp,
                                   orderAttribute=orderAttribute,
                                   limit=limit,
                                   offset=offset)

  types_getProduction = [prodTypes]

  @staticmethod
  def export_getProduction(prodName):
    return database.getProduction(prodName)

  types_getProductionParameters = [prodTypes, [six.string_types, list, tuple]]

  @staticmethod
  def export_getProductionParameters(prodName, parameters):
    return database.getProductionParameters(prodName, parameters)

  types_setProductionStatus = [prodTypes, six.string_types]

  @staticmethod
  def export_setProductionStatus(prodName, status):
    return database.setProductionStatus(prodName, status)

  types_startProduction = [prodTypes]

  @staticmethod
  @ignoreEncodeWarning
  def export_startProduction(prodName):
    return database.startProduction(prodName)

  ####################################################################
  #
  # These are the methods to manipulate the ProductionTransformations table
  #

  types_addTransformationsToProduction = [prodTypes, transTypes, transTypes]

  @staticmethod
  def export_addTransformationsToProduction(prodName, transIDs, parentTransIDs):
    return database.addTransformationsToProduction(prodName, transIDs, parentTransIDs=parentTransIDs)

  types_getProductionTransformations = []

  @staticmethod
  def export_getProductionTransformations(prodName,
                                          condDict=None,
                                          older=None,
                                          newer=None,
                                          timeStamp='CreationTime',
                                          orderAttribute=None,
                                          limit=None,
                                          offset=None):

    if not condDict:
      condDict = {}
    return database.getProductionTransformations(prodName,
                                                 condDict=condDict,
                                                 older=older,
                                                 newer=newer,
                                                 timeStamp=timeStamp,
                                                 orderAttribute=orderAttribute,
                                                 limit=limit,
                                                 offset=offset)

  ####################################################################
  #
  # These are the methods to manipulate the ProductionSteps table
  #

  types_addProductionStep = [dict]

  @staticmethod
  def export_addProductionStep(prodStep):
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
    return res

  types_getProductionStep = [int]

  @staticmethod
  def export_getProductionStep(stepID):
    return database.getProductionStep(stepID)


database = False


def initializeProductionManagerHandler(serviceInfo):
  global database
  database = ProductionDB('ProductionDB', 'Production/ProductionDB')
  return S_OK()


class ProductionManagerHandler(ProductionManagerHandlerBase):

  def __init__(self, *args, **kargs):
    self.setDatabase(database)
    ProductionManagerHandlerBase.__init__(self, *args, **kargs)
