import importlib
from collections.abc import Sequence

# imports
from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Base.Client import executeRPCStub as disetExecuteRPCStub
from DIRAC.Core.Utilities import DEncode
from DIRAC.Core.Security.DiracX import executeRPCStub as diracxExecuteRPCStub
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

########################################################################


class ForwardDISET(OperationHandlerBase):
    """
    .. class:: ForwardDISET

    functor forwarding DISET operations

    There are fundamental differences in behavior between the forward diset
    for DIPS service and the one for DiracX:
    * dips call will be done with the server certificates and use the delegated DN field
    * diracx call will be done with the credentials setup by request tasks
    * dips call are just RPC call, they do not execute the logic of the client (that is anyway not relied upon for now)
    * diracx calls will effectively call the client entirely.

    """

    def __init__(self, operation=None, csPath=None):
        """c'tor

        :param Operation operation: an Operation instance
        :param str csPath: CS path for this handler
        """
        # # call base class c'tor
        OperationHandlerBase.__init__(self, operation, csPath)

    # We can ignore the warnings here because we are just
    # replaying something that ought to be sanitized already
    @DEncode.ignoreEncodeWarning
    def __call__(self):
        """execute RPC stub"""
        # # decode arguments
        try:
            stub, length = DEncode.decode(self.operation.Arguments)
            self.log.debug(f"decoded len={length} val={stub}")
        except ValueError as error:
            self.log.exception(error)
            self.operation.Error = str(error)
            self.operation.Status = "Failed"
            return S_ERROR(str(error))

        # This is the DISET rpcStub
        if isinstance(stub, Sequence):
            # Ensure the forwarded request is done on behalf of the request owner
            res = getDNForUsername(self.request.Owner)
            if not res["OK"]:
                return res
            stub[0][1]["delegatedDN"] = res["Value"][0]
            stub[0][1]["delegatedGroup"] = self.request.OwnerGroup

            # ForwardDiset is supposed to be used with a host certificate
            useServerCertificate = gConfig.useServerCertificate()
            gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "true")
            forward = disetExecuteRPCStub(stub)
            if not useServerCertificate:
                gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "false")
        # DiracX stub
        elif isinstance(stub, dict):
            forward = diracxExecuteRPCStub(stub)
        else:
            raise TypeError("Unknwon type of stub")

        if not forward["OK"]:
            self.log.error("unable to execute operation", f"'{self.operation.Type}' : {forward['Message']}")
            self.operation.Error = forward["Message"]
            return forward
        self.log.info("DISET forwarding done")
        self.operation.Status = "Done"
        return S_OK()
