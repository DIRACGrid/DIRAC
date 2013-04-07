''' Workflow Utility module contains a number of functions useful for the client operations
'''

from DIRAC.Core.Workflow.Workflow import Workflow
from DIRAC.Core.Workflow.Step import StepDefinition

def createSingleModuleWorkflow(module,name):
  ''' Creates a workflow based on a single module definition
  '''

  moduleType = module.getType()
  moduleName = name

  workflow = Workflow()
  step = StepDefinition(moduleType+'_step')
  step.addModule(module)
  moduleInstance = step.createModuleInstance(moduleType,moduleName)

  step.addParameter(moduleInstance.parameters.getInput())
  workflow.addParameter(moduleInstance.parameters.getInput())

  workflow.addStep(step)
  stepInstance = workflow.createStepInstance(moduleType+'_step',moduleName+'_step')

  # Propagate the module input parameters to the workflow level
  moduleInstance.linkParameterUp(moduleInstance.parameters.getInput())
  stepInstance.linkParameterUp(moduleInstance.parameters.getInput())

  workflow.setName(name)
  workflow.setDescription('Single module workflow from '+moduleType+' type module')
  workflow.setDescrShort(moduleType+' workflow')
  return workflow
