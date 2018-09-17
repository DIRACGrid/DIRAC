""" A generic client for creating productions
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.API import API


class Production(API):

  """ Contains methods to build a production on the client, before it's actually added to the Production System
  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """
    super(Production, self).__init__()

    self.prodDescription = {}
    self.stepCounter = 1

  # Methods running on the client to prepare the production description

  def addStep(self, prodStep):
    """ Add a step to the production description, by updating the description dictionary

    :param object prodStep: an object of type ~:mod:`~DIRAC.ProductionSystem.Client.ProductionStep`
    """

    stepName = 'Step' + str(self.stepCounter) + '_' + prodStep.Name
    self.stepCounter += 1
    prodStep.Name = stepName

    res = prodStep.getAsDict()
    if not res['OK']:
      return res
    prodStepDict = res['Value']
    prodStepDict['name'] = stepName

    self.prodDescription[prodStep.Name] = prodStepDict
    return S_OK()
