""" :mod: ForwardDISET

    ==================

    .. module: ForwardDISET

    :synopsis: DISET forwarding operation handler

    .. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

    DISET forwarding operation handler
"""

# imports
from DIRAC import S_ERROR, S_OK, gConfig
from DIRAC.ConfigurationSystem.Client.ConfigurationData import gConfigurationData
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername
from DIRAC.Core.Base.Client import executeRPCStub
from DIRAC.Core.Utilities import DEncode
from DIRAC.RequestManagementSystem.private.OperationHandlerBase import OperationHandlerBase

########################################################################


class ForwardDISET(OperationHandlerBase):
    """
    .. class:: ForwardDISET

    functor forwarding DISET operations
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
            decode, length = DEncode.decode(self.operation.Arguments)
            self.log.debug(f"decoded len={length} val={decode}")
        except ValueError as error:
            self.log.exception(error)
            self.operation.Error = str(error)
            self.operation.Status = "Failed"
            return S_ERROR(str(error))

        # Ensure the forwarded request is done on behalf of the request owner
        res = getDNForUsername(self.request.Owner)
        if not res["OK"]:
            return res
        decode[0][1]["delegatedDN"] = res["Value"][0]
        decode[0][1]["delegatedGroup"] = self.request.OwnerGroup

        # ForwardDiset is supposed to be used with a host certificate
        useServerCertificate = gConfig.useServerCertificate()
        gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "true")
        forward = executeRPCStub(decode)
        if not useServerCertificate:
            gConfigurationData.setOptionInCFG("/DIRAC/Security/UseServerCertificate", "false")

        if not forward["OK"]:
            self.log.error("unable to execute operation", f"'{self.operation.Type}' : {forward['Message']}")
            self.operation.Error = forward["Message"]
            return forward
        self.log.info("DISET forwarding done")
        self.operation.Status = "Done"
        return S_OK()
