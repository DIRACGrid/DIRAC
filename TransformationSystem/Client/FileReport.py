# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/ProductionManagementSystem/Client/FileReport.py $

"""
  FileReport class encapsulates methods to report file status in the
  production environment in failover safe way
"""

__RCSID__ = "$Id: FileReport.py 18161 2009-11-11 12:07:09Z acasajus $"

import copy
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC import S_OK, S_ERROR
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest


class FileReport:

  def __init__(self):
    self.statusDict = {}
    self.production = None

  def setFileStatus(self,production,lfn,status,sendFlag=False):
    """ Set file status in the contesxt of the given transformation
    """

    if not self.production:
      self.production = production

    result = S_OK()
    if sendFlag:
      sendList = []
      if self.statusDict:
        for lfn_s,status_s in self.statusDict.items():
          sendList.append((lfn_s,status_s))
      sendList.append((lfn,status))
      productionSvc = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      result = productionSvc.setFileStatusForTransformation(production,sendList)
      if result['OK']:
        return result

    # Add the file status info to the internal cache for later retry
    self.statusDict[lfn] = status
    return result

  def setCommonStatus(self,status):
    """ Set common status for all files in the internal cache
    """

    for lfn in self.statusDict.keys():
      self.statusDict[lfn] = status

    return S_OK()

  def getFiles(self):
    """ Get the statuses of the files already accumulated in the FileReport object
    """

    return copy.deepcopy(self.statusDict)

  def commit(self):
    """ Commit pending file status update records
    """

    # create intermediate status dictionary
    sDict = {}
    for lfn,status in self.statusDict.items():
      if not sDict.has_key(status):
        sDict[status] = []
      sDict[status].append(lfn)

    statusList = []
    for status,lfns in sDict.items():
      statusList.append((status,lfns))

    if self.statusDict:
      productionSvc = RPCClient('ProductionManagement/ProductionManager',timeout=120)
      result = productionSvc.setFileStatusForTransformation(self.production,statusList)
    else:
      return S_OK()

    if result['OK']:
      self.statusDict = {}
      return S_OK()

    return result


  def generateRequest(self):
    """ Commit the accumulated records and generate request eventually
    """

    result = self.commit()

    request = None
    if not result['OK']:
      # Generate Request
      request = RequestContainer()
      if result.has_key('rpcStub'):
        request.setDISETRequest(result['rpcStub'])

    return S_OK(request)