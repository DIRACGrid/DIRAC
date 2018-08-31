""" Class defining a production step. """

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR


class ProductionStep(object):

  """ Define the Production Step object
  """

  def __init__(self, **kwargs):
    """ Simple constructor
    """
    # Default values for transformation step parameters
    self.Name = ''
    self.ParentStep = -1
    self.Description = 'description'
    self.LongDescription = 'longDescription'
    self.Type = 'MCSimulation'
    self.Plugin = 'Standard'
    self.AgentType = 'Manual'
    self.FileMask = ''
    #########################################
    self.Inputquery = {}
    self.Outputquery = {}
    self.GroupSize = 1
    self.Body = 'body'

  def getAsDict(self):

    prodStepDict = {}

    prodStepDict['name'] = self.Name
    if isinstance(self.ParentStep, list):
      prodStepDict['parentStep'] = []
      for parentStep in self.ParentStep:
        if not parentStep.Name:
          return S_ERROR('Parent Step does not exist')
        prodStepDict['parentStep'].append(parentStep.Name)
    if isinstance(self.ParentStep, ProductionStep):
      if not self.ParentStep.Name:
        return S_ERROR('Parent Step does not exist')
      prodStepDict['parentStep'] = [self.ParentStep.Name]

    prodStepDict['description'] = self.Description
    prodStepDict['longDescription'] = self.LongDescription
    prodStepDict['type'] = self.Type
    prodStepDict['plugin'] = self.Plugin
    prodStepDict['agentType'] = self.AgentType
    prodStepDict['fileMask'] = self.FileMask
    # Optional fields
    prodStepDict['inputquery'] = self.Inputquery
    prodStepDict['outputquery'] = self.Outputquery
    prodStepDict['groupsize'] = self.GroupSize
    prodStepDict['body'] = self.Body

    return S_OK(prodStepDict)
