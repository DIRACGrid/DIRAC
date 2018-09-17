"""
  This module manages is a client for the TS and manages the transformations associated to the productions
"""

__RCSID__ = "$Id $"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient


class ProdTransManager(object):

  def __init__(self):
    self.transClient = TransformationClient()
    self.prodClient = ProductionClient()

  def deleteTransformations(self, transIDs):
    """ Delete given transformations from the TS

    :param transIDs: a list of Transformation IDs
    """
    gLogger.notice("Deleting transformations %s from the TS" % transIDs)

    for transID in transIDs:
      res = self.transClient.deleteTransformation(transID)
      if not res['OK']:
        return res

    return S_OK()

  def deleteProductionTransformations(self, prodID):
    """ Delete the production transformations from the TS

    :param prodID: the ProductionID
    """
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
    """ Add the transformation step to the TS

    :param object prodStep: an object of type ~:mod:`~DIRAC.ProductionSystem.Client.ProductionStep`
    :param prodID: the ProductionID
    """
    gLogger.notice("Add step %s to production %s" % (prodStep['name'], prodID))

    description = prodStep['description']
    longDescription = prodStep['longDescription']
    stepType = prodStep['stepType']
    plugin = prodStep['plugin']
    agentType = prodStep['agentType']
    fileMask = prodStep['fileMask']
    groupsize = prodStep['groupsize']
    body = prodStep['body']
    inputquery = prodStep['inputquery']
    outputquery = prodStep['outputquery']
    name = '%08d' % prodID + '_' + prodStep['name']

    res = self.transClient.addTransformation(
        name,
        description,
        longDescription,
        stepType,
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

  def executeActionOnTransformations(self, prodID, action):
    """ Wrapper to start/stop/clean the transformations of a production

    :param prodID: the ProductionID
    :param action: it can be start/stop/clean
    """

    # Check if there is any action to do
    if not action:
      return S_OK()

    # Get the transformations of the production
    res = self.prodClient.getProductionTransformations(prodID)
    if not res['OK']:
      return res

    transList = res['Value']
    method = getattr(self.transClient, action)
    gLogger.notice("Executing action %s to %s" % (action, transList))

    # Execute the action on each transformation
    for trans in transList:
      transID = trans['TransformationID']
      res = method(transID)
      if not res['OK']:
        return res

    return S_OK()
