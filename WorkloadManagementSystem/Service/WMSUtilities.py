""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy


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
