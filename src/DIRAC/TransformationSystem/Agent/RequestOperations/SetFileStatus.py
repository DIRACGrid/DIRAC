""" Operation handler to set the status for transformation files

TODO: Is there any good reason why this is just not using a forwardDISET instead?
"""
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase
from DIRAC.TransformationSystem.Client.TransformationClient import TransformationClient


class SetFileStatus(OperationHandlerBase):
    """
    .. class:: SetFileStatus

    SetFileStatus operation handler
    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param self: self reference
        :param Operation operation: Operation instance
        :param str csPath: CS path for this handler
        """
        OperationHandlerBase.__init__(self, operation, csPath)

    def __call__(self):
        """It expects to find the arguments for tc.setFileStatusForTransformation in operation.Arguments"""
        try:
            setFileStatusDict = DEncode.decode(self.operation.Arguments)[0]
            self.log.debug(f"decoded filStatusDict={str(setFileStatusDict)}")
        except ValueError as error:
            self.log.exception(error)
            self.operation.Error = str(error)
            self.operation.Status = "Failed"
            return S_ERROR(str(error))

        tc = TransformationClient()
        setStatus = tc.setFileStatusForTransformation(
            setFileStatusDict["transformation"], setFileStatusDict["statusDict"], setFileStatusDict["force"]
        )

        if not setStatus["OK"]:
            errorStr = f"failed to change status: {setStatus['Message']}"
            self.operation.Error = errorStr
            self.log.warn(errorStr)
            return S_ERROR(self.operation.Error)

        else:
            self.operation.Status = "Done"
            return S_OK()
