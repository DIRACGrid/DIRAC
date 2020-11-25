""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

__RCSID__ = "$Id$"

from tempfile import mkdtemp
import shutil

from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getQueue
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getGroupOption
from DIRAC.FrameworkSystem.Client.ProxyManagerClient import gProxyManager
from DIRAC.Resources.Computing.ComputingElementFactory import ComputingElementFactory
from DIRAC.WorkloadManagementSystem.Client.ServerUtils import pilotAgentsDB


# List of files to be inserted/retrieved into/from pilot Output Sandbox
# first will be defined as StdOut in JDL and the second as StdErr
outputSandboxFiles = ['StdOut', 'StdErr']

COMMAND_TIMEOUT = 60
###########################################################################


def getGridEnv():

  gridEnv = ''
  setup = gConfig.getValue('/DIRAC/Setup', '')
  if setup:
    instance = gConfig.getValue('/DIRAC/Setups/%s/WorkloadManagement' % setup, '')
    if instance:
      gridEnv = gConfig.getValue('/Systems/WorkloadManagement/%s/GridEnv' % instance, '')

  return gridEnv


@executeWithUserProxy
def getPilotLoggingInfo(grid, pilotRef):
  """
   Get LoggingInfo of a GRID job
  """
  if grid == 'CREAM':
    # pilotRef may integrate the pilot stamp
    # it has to be removed before being passed in parameter
    pilotRef = pilotRef.split(':::')[0]
    cmd = ['glite-ce-job-status', '-L', '2', '%s' % pilotRef]
  elif grid == 'HTCondorCE':
    # need to import here, otherwise import errors happen
    from DIRAC.Resources.Computing.HTCondorCEComputingElement import getCondorLogFile
    resLog = getCondorLogFile(pilotRef)
    if not resLog['OK']:
      return resLog
    logFile = resLog['Value']
    cmd = ['cat', " ".join(logFile)]
  else:
    return S_ERROR('Pilot logging not available for %s CEs' % grid)

  gridEnv = getGridEnv()
  ret = executeGridCommand('', cmd, gridEnv)
  if not ret['OK']:
    return ret

  status, output, error = ret['Value']
  if status:
    return S_ERROR(error)

  return S_OK(output)


def getGridJobOutput(pilotReference):
  """ Get the pilot job standard output and standard error files for the Grid job reference

      :param str pilotReference: a grid (job) pilot reference
  """

  result = pilotAgentsDB.getPilotInfo(pilotReference)
  if not result['OK']:
    gLogger.error("Failed to get info for pilot", result['Message'])
    return S_ERROR('Failed to get info for pilot')
  if not result['Value']:
    gLogger.warn("The pilot info is empty", pilotReference)
    return S_ERROR('Pilot info is empty')

  pilotDict = result['Value'][pilotReference]
  owner = pilotDict['OwnerDN']
  group = pilotDict['OwnerGroup']

  # FIXME: What if the OutputSandBox is not StdOut and StdErr, what do we do with other files?
  result = pilotAgentsDB.getPilotOutput(pilotReference)
  if result['OK']:
    stdout = result['Value']['StdOut']
    error = result['Value']['StdErr']
    if stdout or error:
      resultDict = {}
      resultDict['StdOut'] = stdout
      resultDict['StdErr'] = error
      resultDict['OwnerDN'] = owner
      resultDict['OwnerGroup'] = group
      resultDict['FileList'] = []
      return S_OK(resultDict)
    else:
      gLogger.warn('Empty pilot output found',
                   'for %s' % pilotReference)

  # Instantiate the appropriate CE
  ceFactory = ComputingElementFactory()
  result = getQueue(pilotDict['GridSite'], pilotDict['DestinationSite'], pilotDict['Queue'])
  if not result['OK']:
    return result
  queueDict = result['Value']
  gridEnv = getGridEnv()
  queueDict['GridEnv'] = gridEnv
  queueDict['WorkingDirectory'] = mkdtemp()
  result = ceFactory.getCE(pilotDict['GridType'], pilotDict['DestinationSite'], queueDict)
  if not result['OK']:
    shutil.rmtree(queueDict['WorkingDirectory'])
    return result
  ce = result['Value']
  groupVOMS = getGroupOption(group, 'VOMSRole', group)
  result = gProxyManager.getPilotProxyFromVOMSGroup(owner, groupVOMS)
  if not result['OK']:
    gLogger.error('Could not get proxy:',
                  'User "%s" Group "%s" : %s' % (owner, groupVOMS, result['Message']))
    return S_ERROR("Failed to get the pilot's owner proxy")
  proxy = result['Value']
  ce.setProxy(proxy)
  pilotStamp = pilotDict['PilotStamp']
  pRef = pilotReference
  if pilotStamp:
    pRef = pRef + ':::' + pilotStamp
  result = ce.getJobOutput(pRef)
  if not result['OK']:
    shutil.rmtree(queueDict['WorkingDirectory'])
    return result
  stdout, error = result['Value']
  if stdout:
    result = pilotAgentsDB.storePilotOutput(pilotReference, stdout, error)
    if not result['OK']:
      gLogger.error('Failed to store pilot output:', result['Message'])

  resultDict = {}
  resultDict['StdOut'] = stdout
  resultDict['StdErr'] = error
  resultDict['OwnerDN'] = owner
  resultDict['OwnerGroup'] = group
  resultDict['FileList'] = []
  shutil.rmtree(queueDict['WorkingDirectory'])
  return S_OK(resultDict)


def killPilotsInQueues(pilotRefDict):
  """ kill pilots queue by queue

      :params dict pilotRefDict: a dict of pilots in queues
  """

  ceFactory = ComputingElementFactory()
  failed = []
  for key, pilotDict in pilotRefDict.items():

    owner, group, site, ce, queue = key.split('@@@')
    result = getQueue(site, ce, queue)
    if not result['OK']:
      return result
    queueDict = result['Value']
    gridType = pilotDict['GridType']
    result = ceFactory.getCE(gridType, ce, queueDict)
    if not result['OK']:
      return result
    ce = result['Value']

    # FIXME: quite hacky. Should be either removed, or based on some flag
    if gridType in ["CREAM", "ARC", "Globus", "HTCondorCE"]:
      group = getGroupOption(group, 'VOMSRole', group)
      ret = gProxyManager.getPilotProxyFromVOMSGroup(owner, group)
      if not ret['OK']:
        gLogger.error('Could not get proxy:',
                      'User "%s" Group "%s" : %s' % (owner, group, ret['Message']))
        return S_ERROR("Failed to get the pilot's owner proxy")
      proxy = ret['Value']
      ce.setProxy(proxy)

    pilotList = pilotDict['PilotList']
    result = ce.killJob(pilotList)
    if not result['OK']:
      failed.extend(pilotList)

  return failed
