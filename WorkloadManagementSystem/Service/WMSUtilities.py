########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/WMSUtilities.py,v 1.6 2008/07/11 07:07:43 rgracian Exp $
########################################################################

""" A set of utilities used in the WMS services
"""

__RCSID__ = "$Id: WMSUtilities.py,v 1.6 2008/07/11 07:07:43 rgracian Exp $"

from tempfile import mkdtemp
import shutil, os
from DIRAC.Core.Utilities.Subprocess import systemCall
from DIRAC.FrameworkSystem.Client.ProxyManagerClient       import gProxyManager

from DIRAC import S_OK, S_ERROR

COMMAND_TIMEOUT = 60
###########################################################################
def getPilotOutput( proxy, grid, pilotRef ):
  """
   Get Output of a GRID job
  """
  tmp_dir = mkdtemp()
  if grid == 'LCG':
    cmd = [ 'edg-job-get-output' ]
  elif grid == 'gLite':
    cmd = [ 'glite-wms-job-output' ]
  else:
    return S_ERROR( 'Unknnown GRID %s' % grid  )

  cmd.extend( ['--dir', tmpdir, pilotRef] )

  ret = _gridCommand( proxy, cmd )
  if not ret['OK']:
    shutil.rmtree(tmp_dir)
    return ret

  status,output,error = ret['Value']
  if error.find('already retrieved') != -1:
    shutil.rmtree(tmp_dir)
    return S_ERROR('Pilot job output already retrieved')    
    
  # Get the list of files
  fileList = os.listdir(tmp_dir)
  result = S_OK()
  result['FileList'] = fileList
  
  if os.path.exists(tmp_dir+'/std.out'):
    f = file(tmp_dir+'/std.out','r').read()
  else:
    f = ''  
  result['StdOut'] = f
  if os.path.exists(tmp_dir+'/std.err'):
    f = file(tmp_dir+'/std.err','r').read()
  else:
    f = ''    
  result['StdError'] = f
  
  shutil.rmtree(tmp_dir)
  return result


def getLCGPilotOutput(jRef):
  """ Get output of an LCG job
  """

  tmp_dir = mkdtemp()
  cmd = "edg-job-get-output --dir %s %s" % (tmp_dir,jRef)  
  result = shellCall(COMMAND_TIMEOUT,cmd)
  
  if not result['OK']:
    shutil.rmtree(tmp_dir)
    return result
    
  status,output,error = result['Value']
  if error.find('already retrieved') != -1:
    shutil.rmtree(tmp_dir)
    return S_ERROR('Pilot job output already retrieved')    
    
  if error.find('Output not yet Ready') != -1 :  
    shutil.rmtree(tmp_dir)
    return S_ERROR(error)  

  if output.find('not yet ready') != -1 :  
    shutil.rmtree(tmp_dir)
    return S_ERROR(output)  

  # Get the list of files
  # FIXME: the name of standard Error and Output are set on the JDL
  fileList = os.listdir(tmp_dir)
  result = S_OK()
  result['FileList'] = fileList
  
  if os.path.exists(tmp_dir+'/std.out'):
    f = file(tmp_dir+'/std.out','r').read()
  else:
    f = ''  
  result['StdOut'] = f
  if os.path.exists(tmp_dir+'/std.err'):
    f = file(tmp_dir+'/std.err','r').read()
  else:
    f = ''    
  result['StdError'] = f
  
  shutil.rmtree(tmp_dir)
  return result

def _gridCommand(self, proxy, cmd):
  """
   Execute cmd tuple
  """
  gridEnv = dict(os.environ)

  ret = gProxyManager.dumpProxyToFile( proxy )
  if not ret['OK']:
    self.log.error( 'Failed to dump Proxy to file' )
    return ret
  gridEnv[ 'X509_USER_PROXY' ] = ret['Value']
  self.log.verbose( 'Executing', ' '.join(cmd) )
  return systemCall( COMMAND_TIMEOUT, cmd, env = gridEnv )
  
  