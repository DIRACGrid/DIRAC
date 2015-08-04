""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""

__RCSID__ = "$Id$"

from tempfile import mkdtemp
import shutil, os
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Resources.Computing.ComputingElementFactory     import ComputingElementFactory
import arc

from DIRAC import S_OK, S_ERROR, gConfig

# List of files to be inserted/retrieved into/from pilot Output Sandbox
# first will be defined as StdOut in JDL and the second as StdErr
outputSandboxFiles = [ 'StdOut', 'StdErr', 'std.out', 'std.err' ]

COMMAND_TIMEOUT = 60
###########################################################################

def getGridEnv():

  gridEnv = ''
  setup = gConfig.getValue( '/DIRAC/Setup', '' )
  if setup:
    instance = gConfig.getValue( '/DIRAC/Setups/%s/WorkloadManagement' % setup, '' )
    if instance:
      gridEnv = gConfig.getValue( '/Systems/WorkloadManagement/%s/GridEnv' % instance, '' )

  return gridEnv

def getWMSPilotOutput( proxy, grid, pilotRef ):
  """
   Get Output of a GRID job
  """
  tmp_dir = mkdtemp()
  if grid == 'LCG':
    cmd = [ 'edg-job-get-output' ]
  elif grid == 'gLite':
    cmd = [ 'glite-wms-job-output' ]
  else:
    return S_ERROR( 'Unknown GRID %s' % grid )

  cmd.extend( ['--noint', '--dir', tmp_dir, pilotRef] )

  gridEnv = getGridEnv()

  ret = executeGridCommand( proxy, cmd, gridEnv )
  if not ret['OK']:
    shutil.rmtree( tmp_dir )
    return ret

  status, output, error = ret['Value']

  for errorString in [ 'already retrieved',
                       'Output not yet Ready',
                       'not yet ready',
                       'the status is ABORTED',
                       'No output files' ]:
    if errorString in error:
      shutil.rmtree( tmp_dir )
      return S_ERROR( error )
    if errorString in output:
      shutil.rmtree( tmp_dir )
      return S_ERROR( output )

  if status:
    shutil.rmtree( tmp_dir )
    return S_ERROR( error )

  # Get the list of files
  tmp_dir = os.path.join( tmp_dir, os.listdir( tmp_dir )[0] )

  result = S_OK()
  result['FileList'] = outputSandboxFiles

  for filename in outputSandboxFiles:
    tmpname = os.path.join( tmp_dir, filename )
    if os.path.exists( tmpname ):
      myfile = file( tmpname, 'r' )
      f = myfile.read()
      myfile.close()
    else:
      f = ''
    # HACK: removed after the current scheme has been in production for at least 1 week
    if filename == 'std.out' and f:
      filename = 'StdOut'
    if filename == 'std.err' and f:
      filename = 'StdErr'
    result[filename] = f

  shutil.rmtree( tmp_dir )
  return result

###########################################################################
def getPilotLoggingInfo( proxy, grid, pilotRef ):
  """
   Get LoggingInfo of a GRID job
  """
  if grid == 'gLite':
    cmd = [ 'glite-wms-job-logging-info', '-v', '3', '--noint', pilotRef ]
  elif grid == 'CREAM':
    cmd = [ 'glite-ce-job-status', '-L', '2', '%s' % pilotRef ]
  else:
    return S_ERROR( 'Pilot logging not available for %s CEs' % grid )

  gridEnv = getGridEnv()
  ret = executeGridCommand( proxy, cmd, gridEnv )
  if not ret['OK']:
    return ret

  status, output, error = ret['Value']
  if status:
    return S_ERROR( error )

  return S_OK( output )

