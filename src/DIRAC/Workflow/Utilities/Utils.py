""" Collection of utilities function
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time

from DIRAC.ConfigurationSystem.Client.Helpers.CSGlobals import getVO, getCSExtensions

from DIRAC.Core.Workflow.Module import ModuleDefinition
from DIRAC.Core.Workflow.Parameter import Parameter
from DIRAC.Core.Workflow.Step import StepDefinition

#############################################################################


def getStepDefinition(stepName, modulesNameList=None, importLine="""""", parametersList=None):
  """ Given a name, a list of modules name, and a list of parameters, returns a step definition.
      Remember that Step definition = Parameters + Module Instances
  """

  if modulesNameList is None:
    modulesNameList = []

  if parametersList is None:
    parametersList = []

  # In case the importLine is not set, this is looking for a DIRAC extension, if any.
  # The extension is supposed to be called ExtDIRAC.
  if not importLine:
    importLine = "DIRAC.Workflow.Modules"
    for ext in getCSExtensions():
      if ext.lower() == getVO():
        importLine = ext + "DIRAC.Workflow.Modules"
        break

  stepDef = StepDefinition(stepName)

  for moduleName in modulesNameList:

    # create the module definition
    moduleDef = ModuleDefinition(moduleName)
    try:
      # Look in the importLine given, or the DIRAC if the given location can't be imported
      moduleDef.setDescription(getattr(__import__("%s.%s" % (importLine, moduleName),
                                                  globals(), locals(), ['__doc__']),
                                       "__doc__"))
      moduleDef.setBody("""\nfrom %s.%s import %s\n""" % (importLine, moduleName, moduleName))
    except ImportError:
      alternativeImportLine = "DIRAC.Workflow.Modules"
      moduleDef.setDescription(getattr(__import__("%s.%s" % (alternativeImportLine, moduleName),
                                                  globals(), locals(), ['__doc__']),
                                       "__doc__"))
      moduleDef.setBody("""\nfrom %s.%s import %s\n""" % (alternativeImportLine, moduleName, moduleName))

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
    # 5-tuple: utime, stime, cutime, cstime, elapsed_time
    stats = os.times()
    cputimeNow = stats[0] + stats[1] + stats[2] + stats[3]
    cputimeBefore = step_commons['StartStats'][0] + step_commons['StartStats'][1] \
        + step_commons['StartStats'][2] + step_commons['StartStats'][3]
    cputime = cputimeNow - cputimeBefore

  return exectime, cputime

#############################################################################
