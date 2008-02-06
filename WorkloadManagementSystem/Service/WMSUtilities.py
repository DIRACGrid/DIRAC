########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/WMSUtilities.py,v 1.4 2008/02/06 18:19:05 atsareg Exp $
########################################################################

""" A set of utilities used in the WMS services
"""

__RCSID__ = "$Id: WMSUtilities.py,v 1.4 2008/02/06 18:19:05 atsareg Exp $"

from tempfile import mkdtemp
import shutil, os
from DIRAC.Core.Utilities.Subprocess import shellCall, systemCall
from DIRAC import S_OK, S_ERROR

COMMAND_TIMEOUT = 20
###########################################################################
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

###########################################################################
def getgLitePilotOutput(jRef):
  """ Get output of a gLite job
  """

  tmp_dir = mkdtemp()
  cmd = 'glite-wms-job-output --noint --dir %s %s' % (tmp_dir,jRef)  
  result = shellCall(COMMAND_TIMEOUT,cmd)
  
  print cmd
  print result
  
  if not result['OK']:
    shutil.rmtree(tmp_dir)
    return S_ERROR('Failed to call the glite-wms-job-output')
    
  status,output,error = result['Value']
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
