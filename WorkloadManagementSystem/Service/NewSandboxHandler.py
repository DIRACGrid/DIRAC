########################################################################
# $Id$
########################################################################

""" SandboxHandler is the implementation of the Sandbox service
    in the DISET framework

"""

__RCSID__ = "$Id$"

from types import *
import os
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB
from DIRAC.DataManagementSystem.Service.StorageElementHandler import initializeStorageElementHandler, StorageElementHandler
from DIRAC.ConfigurationSystem.Client.PathFinder import getDatabaseSection

# This is a global instance of the JobDB class
jobDB = False

def initializeNewSandboxHandler(serviceInfo):

  global jobDB
  jobDB = JobDB()
  dmsServiceInfo = dict(serviceInfo)
  # initializeStorageElementHandler will use the Sandbox configuration Section
  # to retrieve its working parameters ( BasePath, UseTokens, MaxStorageSize )
  print serviceInfo
  dmsServiceInfo['serviceSectionPath'] = dmsServiceInfo['serviceSectionPath']
  return initializeStorageElementHandler(serviceInfo)

class NewSandboxHandler(StorageElementHandler):

  def __parsefileId(self, fileId):
    """ Parse the file ID string to extract job metadata, e.g. jobID
    """

    ind = fileId.find('::')
    if ind == -1:
      return S_ERROR("Can not get the SandboxID")

    try:
      sandboxID = int(fileId[:ind])
      fname = fileId[ind+2:]
    except Exception, x:
      return S_ERROR("Can not get the SandboxID")

    acceptedNames = ['InputSandbox','OutputSandbox']
    if fname.split('.')[0] not in acceptedNames:
      return S_ERROR( 'Can only upload (%s) : %s' % ( fname, ', '.join(acceptedNames) )  )

    sandboxIDString = '%012d' % sandboxID

    storePath = os.path.join( fname.split('.')[0], 
                              sandboxIDString[-12:-9],
                              sandboxIDString[-9:-6],
                              sandboxIDString[-6:-3],'%s_%s' % ( sandboxIDString, fname ) )

    return S_OK(storePath)

  def transfer_bulkFromClient( self, fileId, token, fileSize, fileHelper ):
    """ Receive files packed into a tar archive by the fileHelper logic.
        token is used for access rights confirmation.
    """
    print 'transfer_bulkFromClient', fileId, token, fileSize, fileHelper

    result = self.__parsefileId(fileId)
    if not result['OK']:
      return result

    return StorageElementHandler.transfer_fromClient( self, result['Value'], token, fileSize, fileHelper )

  def transfer_bulkToClient( self, fileId, token, fileHelper ):
    """ Send directories and files specified in the fileId.
        The fileId string can be a single directory name or a list of
        colon (:) separated file/directory names.
        token is used for access rights confirmation.
    """
    base_path = self.getCSOption( "BasePath", '' )
    print 'transfer_bulkToClient', fileId, token, fileHelper

    result = self.__parsefileId(fileId)
    if not result['OK']:
      return result
    
    print result['Value']

    file_path = os.path.join(base_path,result['Value'])
    result = fileHelper.getFileDescriptor(file_path,'r')
    if not result['OK']:
      result = fileHelper.sendEOF()
      # check if the file does not really exist
      if not os.path.exists(file_path):
        return S_ERROR('File %s does not exist' % os.path.basename(file_path))
      else:
        return S_ERROR('Failed to get file descriptor')

    fileDescriptor = result['Value']
    result = fileHelper.FDToNetwork(fileDescriptor)
    if not result['OK']:
      print result
      return S_ERROR('Failed to get file '+fileID)
    else:
      return result

  def export_setSandboxReady(self,jobID):
    """  Set the sandbox ready for the job with jobID
    """

    return jobDB.setSandboxReady(jobID,sandbox_type)

  types_removeSandbox = [ IntType ]
  def export_removeSandbox(self,jobID):
    """ Remove sandbox for the given job
    """

    return sandboxDB.removeSandbox(jobID,fname,sandbox_type)

  types_getSandboxStats = []
  def export_getSandboxStats(self):
    """ Get sandbox statistics
    """
    result = sandboxDB.getSandboxStats(sandbox_type)
    return result
