""" Collection of utilities function
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import importlib
import os
import time

from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Step import StepDefinition

#############################################################################


def getStepDefinition(stepName, modulesNameList=None, importLine="", parametersList=None):
  """ Given a name, a list of modules name, and a list of parameters, returns a step definition.
      Remember that Step definition = Parameters + Module Instances
  """
  if modulesNameList is None:
    modulesNameList = []

  if parametersList is None:
    parametersList = []

  stepDef = StepDefinition(stepName)

  for moduleName in modulesNameList:
    module = None
    if importLine:
      try:
        module = importlib.import_module(importLine + "." + moduleName)
      except ImportError:
        pass
    # In case the importLine is not set, this is looking for a DIRAC extension, if any
    if module is None:
      module = ObjectLoader().loadModule("Workflow.Modules." + moduleName)["Value"]

    # create the module definition
    moduleDef = ModuleDefinition(moduleName)
    moduleDef.setDescription(module.__doc__)
    moduleDef.setBody("\nfrom %s import %s\n" % (module.__name__, moduleName))

    # add the module to the step, and instance it
    stepDef.addModule(moduleDef)
    stepDef.createModuleInstance(module_type=moduleName, name=moduleName)

  # add parameters to the module definition
  for pName, pType, pValue, pDesc in parametersList:
    p = Parameter(pName, pValue, pType, "", "", True, False, pDesc)
    stepDef.addParameter(Parameter(parameter=p))

  return stepDef

#############################################################################


def addStepToWorkflow(workflow, stepDefinition, name):
  """ Add a stepDefinition to a workflow, instantiating it, and giving it a name
  """

  workflow.addStep(stepDefinition)
  return workflow.createStepInstance(stepDefinition.getType(), name)

#############################################################################


def getStepCPUTimes(step_commons):
  """ CPU times of a step
  """
  exectime = 0
  if 'StartTime' in step_commons:
    exectime = time.time() - step_commons['StartTime']

  cputime = 0
  if 'StartStats' in step_commons:
    # os.times() returns a 5-tuple (utime, stime, cutime, cstime, elapsed_time)
    cputimeNow = sum(os.times()[:4])
    cputimeBefore = sum(step_commons['StartStats'][:4])
    cputime = cputimeNow - cputimeBefore

  return exectime, cputime

#############################################################################
