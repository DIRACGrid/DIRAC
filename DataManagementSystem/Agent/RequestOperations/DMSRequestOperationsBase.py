""" :mod: DMSRequestOperationsBase
    ====================

    Just a collector of common functions
"""

__RCSID__ = "$Id $"

from DIRAC import S_OK

from DIRAC.RequestManagementSystem.Client.Operation             import Operation
from DIRAC.RequestManagementSystem.Client.File                  import File

from DIRAC.DataManagementSystem.Client.ReplicaManager           import ReplicaManager


class DMSRequestOperationsBase:

  def __init__( self ):
    """ c'tor
    """
    self.rm = ReplicaManager()

  def addRegisterReplica( self, opFile, targetSE ):
    """ add RegisterReplica operation for file

    :param File opFile: operation file
    :param str targetSE: target SE
    """
    # # add RegisterReplica operation
    registerOperation = Operation()
    registerOperation.Type = "RegisterFile"
    registerOperation.TargetSE = targetSE

    registerFile = File()
    registerFile.LFN = opFile.LFN
    registerFile.PFN = opFile.PFN
    registerFile.GUID = opFile.GUID
    registerFile.Checksum = opFile.Checksum
    registerFile.ChecksumType = opFile.ChecksumType
    registerFile.Size = opFile.Size

    registerOperation.addFile( registerFile )
    self.request.insertAfter( registerOperation, self.operation )
    return S_OK()
