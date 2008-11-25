from DIRAC.Core.DISET.TransferClient import TransferClient
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.Core.Utilities.File import getSize, getGlobbedTotalSize
from DIRAC import gLogger, S_OK, S_ERROR
import re,os, string

class SandboxClient:

  def __init__(self, sandbox_type='Input'):
    """ Standard constructor
    """

    self.sandbox_type = sandbox_type

########################################################################
  # FIXME: all over the place jobID is considered either int or string
  # this module does no check
  def sendFiles(self,jobID,fileList,sizeLimit=0):
    """ Send files in the fileList to a Sandbox service for the given jobID.
        This is the preferable method to upload sandboxes. fileList can contain
        both files and directories
    """
    print "sendFiles: sizeLimit =", sizeLimit
    error_files = []
    files_to_send = []
    for file in fileList:

      if re.search('^lfn:',file) or re.search('^LFN:',file):
        pass
      else:
        if os.path.exists(file):
          files_to_send.append(file)
        else:
          error_files.append(file)

    if error_files:
      return S_ERROR('Failed to locate files: \n'+string.join(error_files,','))
    
    if sizeLimit > 0:
      # Evaluate the compressed size of the sandbox
      if getGlobbedTotalSize( files_to_send ) > sizeLimit:

        tname = 'Sandbox_'+str(jobID)+'.tar.gz'
        import tarfile
        tarFile = tarfile.open( tname, 'w:gz' )
        for file in files_to_send:
          tarFile.add( file )
        tarFile.close()

        result = S_ERROR('Size over the limit')
        result['SandboxFileName'] = tname
        return result

    sendName = str(jobID)+"::Job__Sandbox__"
    sandbox = TransferClient('WorkloadManagement/%sSandbox' % self.sandbox_type)
    result = sandbox.sendBulk(files_to_send,sendName)
    return result

########################################################################
  def sendFile(self,jobID,fname):
    """ Send a file specified by fname to Sandbox service for job with jobID
    """

    if os.path.exists(fname):
      if os.path.isdir(fname):
        dname = os.path.dirname(fname)
        bname = os.path.basename(fname)
        bzname = bname+'.tar.gz'
        if dname:
          comm = 'tar czf '+bzname+' -C '+dname+' '+bname
        else:
          comm = 'tar czf '+bzname+' '+bname
        result = shellCall(0,comm)
        if not result['OK']:
          return S_ERROR('Failed to send directory '+fname)

        sendName = `jobID`+"::"+bzname
        sandbox = TransferClient('WorkloadManagement/%sSandbox' % self.sandbox_type)
        result = sandbox.sendFile(sendName,bzname)

        if not result['OK']:
          gLogger.error('Failed to send directory '+bzname+' to Sandbox service for job '+`jobID`)
          os.remove(bzname)
          return result
        os.remove(bzname)
      else:  # This is a file
        bname = os.path.basename(fname)
        sendName = `jobID`+"::"+bname
        sandbox = TransferClient('WorkloadManagement/%sSandbox' % self.sandbox_type)
        result = sandbox.sendFile(bname, sendName)
        print "0000",result
        if not result['OK']:
          gLogger.error('Failed to send file '+bname+' to Sandbox service for job '+`jobID`)
          return result

      # We are done OK
      return S_OK()

    else:
      gLogger.error("Can't find file "+ fname)
      return S_ERROR("Can't find file "+ fname)

########################################################################
  def getSandbox(self,jobID,output_dir=''):
    """  Get the job complete sandbox
    """

    # Get the list of files in the sandbox
    sandbox_status = RPCClient('WorkloadManagement/%sSandbox' % self.sandbox_type,timeout=120)
    result = sandbox_status.getFileNames(jobID)
    if not result['OK']:
      return S_ERROR('Failed to get the list of file names')

    fileList = result['Value']

    cwd = os.getcwd()
    if output_dir:
      os.chdir(os.path.realpath(output_dir))

    error_files = []
    for f in fileList:
      sname = `jobID`+"::"+f
      sandbox = TransferClient('WorkloadManagement/%sSandbox' % self.sandbox_type)
      result = sandbox.receiveFile(f,sname)
      if not result['OK']:
        error_files.append(f)
      else:
        if f.find('__Sandbox__.tar') != -1 or f.find('__Sandbox__.tgz') != -1 :
          if f.find('.bz') != -1:
            os.system('tar xjf '+f)
          elif f.find('.gz') != -1 or f.find('.tgz') != -1:
            os.system('tar xzf '+f)
          else:
            os.system('tar xf '+f)

          os.remove(f)

    if output_dir:
      os.chdir(cwd)

    if error_files:
      result = S_ERROR('Failed to download all the files')
      result['FailedFiles'] = error_files
    else:
      result = S_OK(fileList)
      # Set job retrieved flag
      jobState = RPCClient('WorkloadManagement/JobStateUpdate',timeout=120)
      jobState.setJobFlag(jobID,'RetrievedFlag')

    return result

  def setSandboxReady(self,jobID):
    """ Set sandbox status to ready for the given job
    """
    sandbox_status = RPCClient('WorkloadManagement/%sSandbox' % self.sandbox_type,timeout=120)
    return sandbox_status.setSandboxReady(jobID)
