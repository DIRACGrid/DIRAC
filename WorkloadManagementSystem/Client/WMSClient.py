########################################################################
# $Id: WMSClient.py,v 1.9 2008/07/04 08:28:50 rgracian Exp $
########################################################################

""" DIRAC Workload Management System Client class encapsulates all the
    methods necessary to communicate with the Workload Management System
"""

from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.GridCert import getGridProxy
from DIRAC.Core.Utilities.ClassAd.ClassAdLight import ClassAd
from DIRAC import S_OK, S_ERROR
from DIRAC.WorkloadManagementSystem.Client.SandboxClient import SandboxClient

import os

class WMSClient:

  def __init__(self):
    """ WMS Client constructor
    """
    self.sandbox = None

###############################################################################
  def __checkInputSandbox(self, classAdJob):
    """Checks the validity of the job Input Sandbox.
       The function returns the list of Input Sandbox files.
       The total volume of the input sandbox is evaluated
    """

    if (classAdJob.lookupAttribute("InputSandbox")):
      inputSandbox = classAdJob.get_expression("InputSandbox")
      inputSandbox = inputSandbox.replace('","',"\n")
      inputSandbox = inputSandbox.replace('{',"")
      inputSandbox = inputSandbox.replace('}',"")
      inputSandbox = inputSandbox.replace('"',"")
      inputSandbox = inputSandbox.replace(',',"")
      inputSandbox = inputSandbox.split()
      ok = 1
      #print inputSandbox
      # Check the Input Sandbox files

      totalSize = 0
      for file in inputSandbox:
        if file.find('lfn:') != -1 or file.find('LFN:'):
          if not os.path.exists(file):
            badfile = file
            print "inputSandbox file/directory "+file+" not found"
            ok = 0
          else:
            if os.path.isdir(file):
              comm = 'du -b -s '+file
              status,out = commands.getstatusoutput(comm)
              try:
                dirSize = int(out.split()[0])
              except Exception,x:
                print "Input Sandbox directory name",file,"is not valid !"
                print str(x)
                badfile = file
                ok = 0
              totalSize = totalSize + dirSize
            else:
              totalSize = int(os.stat(file)[6]) + totalSize

      #print "Total size of the inputSandbox: "+str(totalSize)
      if not ok:
        result = S_ERROR('Input Sandbox is not valid')
        result['BadFile'] = file
        result['TotalSize'] = totalSize
        return result

      result = S_OK()
      result['InputSandbox'] = inputSandbox
      result['TotalSize'] = totalSize
      return result
    else:
      #print "No input sandbox defined for this job."
      result = S_OK()
      result['TotalSize'] = 0
      result['InputSandbox'] = None
      return result

  def submitJob(self,jdl):
    """ Submit one job specified by its JDL to WMS
    """
    self.sandbox = SandboxClient()
    if os.path.exists(jdl):
      fic = open (jdl, "r")
      jdlString = fic.read()
      fic.close()
    else:
      # If file JDL does not exist, assume that the JDL is
      # passed as a string
      jdlString = jdl

    # Check the validity of the input JDL
    classAdJob = ClassAd('['+jdlString+']')
    if not classAdJob.isOK():
      return S_ERROR('Invalid job JDL')

    # Check the size and the contents of the input sandbox
    result = self.__checkInputSandbox(classAdJob)
    if not result['OK']:
      return result

    inputs = result['InputSandbox']
    insize = result['TotalSize']

    # Submit the job now and get the new job ID
    proxyfile = getGridProxy()
    proxy = open(proxyfile,'r').read()
    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    result = jobManager.submitJob(jdlString, proxy)

    if not result['OK']:
      return result
    else:
      jobID = result['Value']

    #print "Sandbox uploading"

    # Upload input sandbox if any
    if insize > 0:
      result = self.sandbox.sendFiles(jobID,inputs)
      #print result
      if result['OK']:
        result = self.sandbox.setSandboxReady(jobID)
        if not result['OK']:
          return S_ERROR('Failed to set the Input Sandbox flag to ready')
      else:
        return S_ERROR('Failed to upload the Input Sandbox')
    else:
      result = self.sandbox.setSandboxReady(jobID)
      if not result['OK']:
        return S_ERROR('Failed to set the Input Sandbox flag to ready')

    return S_OK(jobID)

  def killJob(self,jobID):
    """ Kill running job.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    return jobManager.killJob(jobID)

  def deleteJob(self,jobID):
    """ Delete job(s) from the WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    return jobManager.deleteJob(jobID)

  def rescheduleJob(self,jobID):
    """ Reschedule job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    return jobManager.rescheduleJob(jobID)

  def resetJob(self,jobID):
    """ Reset job(s) in WMS Job database.
        jobID can be an integer representing a single DIRAC job ID or a list of IDs
    """
    jobManager = RPCClient('WorkloadManagement/JobManager',useCertificates=False)
    return jobManager.resetJob(jobID)
