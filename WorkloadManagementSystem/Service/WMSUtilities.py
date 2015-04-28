########################################################################
# $HeadURL$
########################################################################

""" A set of utilities used in the WMS services
    Requires the Nordugrid ARC plugins. In particular : nordugrid-arc-python
"""

__RCSID__ = "$Id$"

from tempfile import mkdtemp
import shutil, os
from DIRAC.Core.Utilities.Grid import executeGridCommand
from DIRAC.Resources.Computing.ComputingElementFactory     import ComputingElementFactory
from DIRAC.Core.Utilities.SiteCEMapping import getCESiteMapping

from DIRAC import S_OK, S_ERROR, gConfig

import sys
sys.path.append('/usr/lib64/python2.6/site-packages')

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

def getPilotOutput( proxy, grid, pilotRef, pilotStamp = '' ):

  if grid in ['LCG', 'gLite']:
    return getWMSPilotOutput( proxy, grid, pilotRef )
  elif grid == "CREAM":
    return getCREAMPilotOutput( proxy, pilotRef, pilotStamp )
  elif grid == "ARC":
    return getARCPilotOutput( proxy, pilotRef )
  else:
    return S_ERROR( 'Non-valid grid type %s' % grid )

def getCREAMPilotOutput( proxy, pilotRef, pilotStamp ):
  """
  """
  gridEnv = getGridEnv()
  tmpdir = mkdtemp()
  result = ComputingElementFactory().getCE( ceName = 'CREAMSite', ceType = 'CREAM',
                                       ceParametersDict = {'GridEnv':gridEnv,
                                                           'Queue':'Qeuue',
                                                           'OutputURL':"gsiftp://localhost",
                                                           'WorkingDirectory':tmpdir} )

  if not result['OK']:
    shutil.rmtree( tmpdir )
    return result
  ce = result['Value']
  ce.setProxy( proxy )
  fullPilotRef = ":::".join( [pilotRef, pilotStamp] )
  result = ce.getJobOutput( fullPilotRef )
  shutil.rmtree( tmpdir )
  if not result['OK']:
    return S_ERROR( 'Failed to get pilot output: %s' % result['Message'] )
  output, error = result['Value']
  fileList = outputSandboxFiles
  result = S_OK()
  result['FileList'] = fileList
  result['StdOut'] = output
  result['StdErr'] = error
  return result

def getARCPilotOutput( proxy, pilotRef ):
  try :
    import arc
  except :
    return S_ERROR( "ARC api not available. Sorry." )
  tmp_dir = mkdtemp()
  myce = pilotRef.split(":")[1].strip("/")
  gridEnv = getGridEnv()
  usercfg = arc.UserConfig()
  usercfg.CredentialString(proxy)
  job = arc.Job()
  job.jobID = pilotRef
  job.JobStatusURL = arc.URL(os.path.join("ldap://", myce, ":2135/Mds-Vo-Name=local,o=grid??sub?(nordugrid-job-globalid=", job.JobID, ")"))
  job.JobManagementURL = arc.URL(os.path.join("gsiftp://", myce, ":2811/jobs/"))
  job.JobManagementInterfaceName = "org.nordugrid.gridftpjob"
  job.Retrieve(usercfg, arc.URL(tmp_dir), False) 

  if 'Results stored at:' in output :
    tmp_dir = os.path.join( tmp_dir, os.listdir( tmp_dir )[0] )
    result = S_OK()
    result['FileList'] = os.listdir( tmp_dir )
    for filename in result['FileList']:
      tmpname = os.path.join( tmp_dir, filename )
      if os.path.exists( tmpname ):
        myfile = file( tmpname, 'r' )
        f = myfile.read()
        myfile.close()
      else :
        f = ' '
      if ".out" in filename:
        filename = 'StdOut'
      if ".err" in filename:
        filename = 'StdErr'
      result[filename] = f
    shutil.rmtree( tmp_dir )
    return result
  if 'Warning: Job not found in job list' in output:
    shutil.rmtree( tmp_dir )
    message = "Pilot not yet visible in ARC dB"
    return S_ERROR( message )
  return S_ERROR("Sorry - requested pilot output not yet available")

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
  if grid == 'LCG':
    cmd = [ 'edg-job-get-logging-info', '-v', '2', '--noint', pilotRef ]
  elif grid == 'gLite':
    cmd = [ 'glite-wms-job-logging-info', '-v', '3', '--noint', pilotRef ]
  elif grid == 'CREAM':
    cmd = [ 'glite-ce-job-status', '-L', '2', '%s' % pilotRef ]
  else:
    return S_ERROR( 'Unknnown GRID %s' % grid )

  gridEnv = getGridEnv()
  ret = executeGridCommand( proxy, cmd, gridEnv )
  if not ret['OK']:
    return ret

  status, output, error = ret['Value']
  if status:
    return S_ERROR( error )

  return S_OK( output )

