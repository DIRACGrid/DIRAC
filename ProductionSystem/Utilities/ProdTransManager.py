"""
  This module manages is a client for the TS and manages the transformations associated to the productions

"""

__RCSID__ = "$Id $"

# from DIRAC
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient


class ProdTransManager(object):

  def __init__(self):
    self.transClient = TransformationClient()
    self.prodClient = ProductionClient()

  def deleteTransformations(self, transIDs):

    gLogger.notice("Deleting transformations %s from the TS" % transIDs)

    for transID in transIDs:
      res = self.transClient.deleteTransformation(transID)
      if not res['OK']:
        return res

    return S_OK()

  def deleteProductionTransformations(self, prodID):

    res = self.prodClient.getProductionTransformations(prodID)

    if res['OK']:
      transList = res['Value']

    gLogger.notice("Deleting production transformations %s from the TS" % transList)

    for trans in transList:
      transID = trans['TransformationID']
      res = self.transClient.deleteTransformation(transID)
      if not res['OK']:
        gLogger.error(res['Message'])

    return S_OK()

  def addTransformationStep(self, prodStep, prodID):
    # Here I should check that all needed parameters of prodStep are defined!!
    gLogger.notice("Add step %s to production %s" % (prodStep['name'], prodID))

    description = prodStep['description']
    longDescription = prodStep['longDescription']
    type = prodStep['type']
    plugin = prodStep['plugin']
    agentType = prodStep['agentType']
    fileMask = prodStep['fileMask']
    groupsize = prodStep['groupsize']
    body = prodStep['body']
    inputquery = prodStep['inputquery']
    outputquery = prodStep['outputquery']

    name = '%08d' % prodID + '_Step_' + prodStep['name']

    res = self.transClient.addTransformation(
        name,
        description,
        longDescription,
        type,
        plugin,
        agentType,
        fileMask,
        groupSize=groupsize,
        body=body,
        inputMetaQuery=inputquery,
        outputMetaQuery=outputquery)

    if not res['OK']:
      return S_ERROR(res['Message'])

    return S_OK(res['Value'])

  def executeActionOnTransformations(self, prodID, action, transID=None):
    """ Wrapper to start/stop/clean the transformations of a production"""
    res = self.prodClient.getProductionTransformations(prodID)
    if res['OK']:
      transList = res['Value']

    # Check if there is any action to do
    if not action:
      return S_OK()

    method = getattr(self.transClient, action)

    if transID:
      transList = [{'TransformationID': transID}]

    gLogger.notice("Executing action %s to %s" % (action, transList))

    for trans in transList:
      transID = trans['TransformationID']
      res = method(transID)
      if not res['OK']:
        return res

    return S_OK()
