########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/WorkloadManagementSystem/Service/WMSUtilities.py,v 1.1 2008/02/04 21:52:33 atsareg Exp $
########################################################################

""" A set of utilities used in the WMS services
"""

__RCSID__ = "$Id: WMSUtilities.py,v 1.1 2008/02/04 21:52:33 atsareg Exp $"

from tempfile import mkdtemp
import shutil, os
from DIRAC.Core.Utilities.Subprocess import shellCall

COMMAND_TIMEOUT = 20

def getLCGPilotOutput(jRef):
  """ Get output of an LCG job
  """

  tmp_dir = mkdtemp()
  cmd = "edg-job-get-output --dir %s %s" % (tmp_dir,jRef)
  result = shellCall(COMMAND_TIMEOUT,cmd)
  if not result['OK']:
    return result
  # Get the list of files
  fileList = os.listdir(tmp_dir)
  result = S_OK()
  result['FileList'] = fileList
  f = file(tmp_dir+'/std.out','r').read()
  result['StdOut'] = f
  f = file(tmp_dir+'/std.err','r').read()
  result['StdOut'] = f
  shutil.rmtree(tmp_dir)
  return result

def getgLitePilotOutput():
  """ Get output of a gLite job
  """

  tmp_dir = mkdtemp()
  cmd = "glite-wms-job-output --dir %s %s" % (tmp_dir,jRef)
  result = shellCall(COMMAND_TIMEOUT,cmd)
  if not result['OK']:
    return result
  # Get the list of files
  fileList = os.listdir(tmp_dir)
  result = S_OK()
  result['FileList'] = fileList
  f = file(tmp_dir+'/std.out','r').read()
  result['StdOut'] = f
  f = file(tmp_dir+'/std.err','r').read()
  result['StdOut'] = f
  shutil.rmtree(tmp_dir)
  return result