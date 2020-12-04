"""
  This module manages is a client for the TS and manages the transformations associated to the productions
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id $"

import json

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.ProductionSystem.Client.ProductionClient import ProductionClient


class ProdTransManager(object):

  def __init__(self):
    self.transClient = TransformationClient()
    self.prodClient = ProductionClient()

  def deleteTransformations(self, transIDs):
    """ Delete given transformations from the TS

    :param list transIDs: a list of Transformation IDs
    """
    gLogger.notice("Deleting transformations %s from the TS" % transIDs)

    for transID in transIDs:
      res = self.transClient.deleteTransformation(transID)
      if not res['OK']:
        return res

    return S_OK()

  def deleteProductionTransformations(self, prodID):
    """ Delete the production transformations from the TS

    :param int prodID: the ProductionID
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

  def addTransformationStep(self, stepID, prodID):
    """ Add the transformation step to the TS

    :param int stepID: the production step ID
    :param int prodID: the production ID
    :return:
    """

    res = self.prodClient.getProductionStep(stepID)
    if not res['OK']:
      return res
    prodStep = res['Value']

    gLogger.notice("Add step %s to production %s" % (prodStep[0], prodID))

    stepDesc = prodStep[2]
    stepLongDesc = prodStep[3]
    stepBody = prodStep[4]
    stepType = prodStep[5]
    stepPlugin = prodStep[6]
    stepAgentType = prodStep[7]
    stepGroupsize = prodStep[8]
    stepInputquery = json.loads(prodStep[9])
    stepOutputquery = json.loads(prodStep[10])

    stepName = '%08d' % prodID + '_' + prodStep[1]

    res = self.transClient.addTransformation(
        stepName,
        stepDesc,
        stepLongDesc,
        stepType,
        stepPlugin,
        stepAgentType,
        '',
        groupSize=stepGroupsize,
        body=stepBody,
        inputMetaQuery=stepInputquery,
        outputMetaQuery=stepOutputquery)

    if not res['OK']:
      return S_ERROR(res['Message'])

    return S_OK(res['Value'])

  def executeActionOnTransformations(self, prodID, action):
    """ Wrapper to start/stop/clean the transformations of a production

    :param int prodID: the production ID
    :param str action: it can be start/stop/clean
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
