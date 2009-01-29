"""
    DIRAC ReplicationPlacementDB class is a front-end to the transformation metadata and associated files.
"""

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, gMonitor, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB
from DIRAC.Core.Utilities.List import stringListToString,intListToString

class ReplicationPlacementDB(TransformationDB):

  def __init__( self, maxQueueSize=4 ):
    """ Constructor
    """
    TransformationDB.__init__(self,'ReplicationPlacementDB', 'DataManagement/ReplicationPlacementDB', maxQueueSize)

  def setFileStatusForTransformation(self,transName,status,lfns):
    """ Set file status for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """
    transID = self.getTransformationID(transName)
    fileIDs = self.__getFileIDsForLfns(lfns).keys()
    if not fileIDs:
      return S_ERROR('TransformationDB.setFileStatusForTransformation: No files found.')
    else:
      req = "UPDATE T_%s SET Status='%s' WHERE FileID IN (%s);" % (transID,status,intListToString(fileIDs))
      return self._update(req)

  def __getFileIDsForLfns(self,lfns):
    """ Get file IDs for the given list of lfns
    """
    fids = {}
    req = "SELECT LFN,FileID FROM DataFiles WHERE LFN in (%s);" % stringListToString(lfns)
    res = self._query(req)
    if not res['OK']:
      return res
    for lfn,fileID in res['Value']:
      fids[fileID] = lfn
    return fids
