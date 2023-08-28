""" FileReport module defines the FileReport class, to report file status to the transformation DB
"""
import copy

from DIRAC import S_OK
from DIRAC.Core.Utilities import DEncode
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient
from DIRAC.RequestManagementSystem.Client.Operation import Operation


class FileReport:
    """A stateful object for reporting to TransformationDB"""

    def __init__(self, server="Transformation/TransformationManager"):
        """c'tor

        self.transClient is a TransformationClient object
        """
        self.transClient = TransformationClient()
        self.transClient.setServer(server)
        self.statusDict = {}
        self.transformation = None
        self.force = False

    def setFileStatus(self, transformation, lfn, status, sendFlag=False):
        """Set file status in the context of the given transformation"""
        if not self.transformation:
            self.transformation = transformation
        if isinstance(lfn, (list, dict, tuple)):
            self.statusDict.update(dict.fromkeys(lfn, status))
        else:
            self.statusDict[lfn] = status
        if sendFlag:
            return self.commit()
        return S_OK()

    def setCommonStatus(self, status):
        """Set common status for all files in the internal cache"""
        for lfn in self.statusDict:
            self.statusDict[lfn] = status
        return S_OK()

    def getFiles(self):
        """Get the statuses of the files already accumulated in the FileReport object"""
        return copy.deepcopy(self.statusDict)

    def commit(self):
        """Commit pending file status update records"""
        if not self.statusDict:
            return S_OK({})

        result = self.transClient.setFileStatusForTransformation(self.transformation, self.statusDict, force=self.force)
        if result["OK"]:
            self.statusDict = {}
        return result

    def generateForwardDISET(self):
        """Commit the accumulated records and generate request eventually"""
        result = self.commit()
        commitOp = None
        if not result["OK"]:
            # Generate Request
            commitOp = Operation()
            commitOp.Type = "SetFileStatus"
            commitOp.Arguments = DEncode.encode(
                {"transformation": self.transformation, "statusDict": self.statusDict, "force": self.force}
            )

        return S_OK(commitOp)
