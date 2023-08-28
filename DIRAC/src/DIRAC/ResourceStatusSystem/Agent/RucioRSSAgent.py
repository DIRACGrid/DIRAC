""" :mod: RucioRSSAgent

    Agent that synchronizes Rucio and Dirac


The following options can be set for the RucioRSSAgent.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN RucioRSSAgent
  :end-before: ##END
  :dedent: 2
  :caption: RucioRSSAgent options
"""

# # imports
from traceback import format_exc

from DIRAC import S_OK, S_ERROR
from DIRAC import gLogger
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security import Locations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs
from DIRAC.ResourceStatusSystem.Client.ResourceStatus import ResourceStatus
from DIRAC.ConfigurationSystem.Agent.RucioSynchronizerAgent import configHelper

from rucio.client import Client


class RucioRSSAgent(AgentModule):
    """
    .. class:: RucioRSSAgent

    Agent that passes Dirac SE status to Rucio.
    """

    def initialize(self):
        """agent's initialisation

        :param self: self reference
        """
        self.log.info("Starting RucioRSSAgent")
        # CA location
        self.caCertPath = Locations.getCAsLocation()
        # configured VOs
        res = getVOs()
        if res["OK"]:
            voList = getVOs().get("Value", [])
        else:
            return S_ERROR(res["Message"])
        # configHelper test
        if isinstance(voList, str):
            voList = [voList]

        self.clientConfig = configHelper(voList)
        self.log.debug(" VO-specific Rucio Client config parameters: ", self.clientConfig)
        return S_OK()

    def execute(self):
        """
        Perform RSS->RSE synchronisation for all eligible VOs.

        :return: S_OK or S_ERROR
        """

        # for each VO: accumulate failed execution results with their messages
        gen = ((key, self.executeForVO(key)) for key in self.clientConfig)
        voRes = {key: result["Message"] for key, result in gen if not result["OK"]}

        if not voRes:
            return S_OK()

        message = "RSS -> RSE synchronisation for at least one VO among eligible VOs was NOT successful."
        self.log.info(message)
        self.log.debug(voRes)
        return S_ERROR(message)

    def executeForVO(self, vo):
        """
        Perform the synchronisation for one VO.

        :param vo: VO name
        :return: S_OK or S_ERROR
        """

        rSS = ResourceStatus()

        try:
            try:
                self.log.info("Login to Rucio as privileged user with host cert/key")
                certKeyTuple = Locations.getHostCertificateAndKeyLocation()
                if not certKeyTuple:
                    self.log.error("Hostcert/key location not set")
                    return S_ERROR("Hostcert/key location not set")
                hostcert, hostkey = certKeyTuple

                self.log.info("Logging in with a host cert/key pair:")
                self.log.debug("account: ", self.clientConfig[vo]["privilegedAccount"])
                self.log.debug("rucio host: ", self.clientConfig[vo]["rucioHost"])
                self.log.debug("auth  host: ", self.clientConfig[vo]["authHost"])
                self.log.debug("CA cert path: ", self.caCertPath)
                self.log.debug("Cert location: ", hostcert)
                self.log.debug("Key location: ", hostkey)
                self.log.debug("VO: ", vo)

                client = Client(
                    account=self.clientConfig[vo]["privilegedAccount"],
                    rucio_host=self.clientConfig[vo]["rucioHost"],
                    auth_host=self.clientConfig[vo]["authHost"],
                    ca_cert=self.caCertPath,
                    auth_type="x509",
                    creds={"client_cert": hostcert, "client_key": hostkey},
                    timeout=600,
                    user_agent="rucio-clients",
                    vo=vo,
                )
            except Exception as err:
                self.log.info("Login to Rucio as privileged user with host cert/key failed. Try username/password")
                client = Client(account="root", auth_type="userpass")
        except Exception as exc:
            # login exception, skip this VO
            self.log.exception("Login for VO failed. VO skipped ", f"VO={vo}", lException=exc)
            return S_ERROR(str(format_exc()))

        self.log.info(" Rucio login successful - continue with the RSS synchronisation")
        # return S_OK()
        try:
            for rse in client.list_rses():
                thisSe = rse["rse"]
                self.log.info(f"Checking Dirac SE status for {thisSe}")
                resStatus = rSS.getElementStatus(thisSe, "StorageElement", vO=vo)
                dictSe = client.get_rse(thisSe)
                if resStatus["OK"]:
                    self.log.debug("SE status ", resStatus["Value"])
                    seAccessValue = resStatus["Value"][thisSe]
                    availabilityRead = True if seAccessValue["ReadAccess"] in ["Active", "Degraded"] else False
                    availabilityWrite = True if seAccessValue["WriteAccess"] in ["Active", "Degraded"] else False
                    availabilityDelete = True if seAccessValue["RemoveAccess"] in ["Active", "Degraded"] else False
                    isUpdated = False
                    if dictSe["availability_read"] != availabilityRead:
                        self.log.info(
                            "Set availability_read for RSE", f"RSE: {thisSe}, availability: {availabilityRead}"
                        )
                        client.update_rse(thisSe, {"availability_read": availabilityRead})
                        isUpdated = True
                    if dictSe["availability_write"] != availabilityWrite:
                        self.log.info(
                            "Set availability_write for RSE", f"RSE: {thisSe}, availability: {availabilityWrite}"
                        )
                        client.update_rse(thisSe, {"availability_write": availabilityWrite})
                        isUpdated = True
                    if dictSe["availability_delete"] != availabilityDelete:
                        self.log.info(
                            "Set availability_delete for RSE",
                            f"RSE: {thisSe}, availability: {availabilityDelete}",
                        )
                        client.update_rse(thisSe, {"availability_delete": availabilityDelete})
                        isUpdated = True
        except Exception as err:
            return S_ERROR(str(err))
        return S_OK()
