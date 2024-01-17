""" :mod: PilotLoggingAgent

    PilotLoggingAgent sends Pilot log files to an SE.

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN PilotLoggingAgent
  :end-before: ##END
  :dedent: 2
  :caption: PilotLoggingAgent options
"""

# # imports
import os
import tempfile
import time

from DIRAC import S_ERROR, S_OK
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername, getVOMSAttributeForGroup, getVOs
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Security.ProxyInfo import getProxyInfo
from DIRAC.Core.Utilities.Proxy import executeWithoutServerCertificate, getProxy
from DIRAC.DataManagementSystem.Client.DataManager import DataManager
from DIRAC.WorkloadManagementSystem.Client.TornadoPilotLoggingClient import TornadoPilotLoggingClient


class PilotLoggingAgent(AgentModule):
    """
    .. class:: PilotLoggingAgent

    The agent sends completed pilot log files to permanent storage for analysis.
    """

    def __init__(self, *args, **kwargs):
        """c'tor"""
        super().__init__(*args, **kwargs)
        self.clearPilotsDelay = 30  # in days
        self.proxyTimeleftLimit = 600  # in seconds

    def initialize(self):
        """
        agent's initialisation. Use this agent's CS information to:
        Determine VOs with remote logging enabled,
        Determine what Defaults/Shifter shifter proxy to use., download the proxies.

        :param self: self reference
        """
        # pilot logs lifetime in days
        self.clearPilotsDelay = self.am_getOption("ClearPilotsDelay", self.clearPilotsDelay)
        # proxy timeleft limit before we get a new one.
        self.proxyTimeleftLimit = self.am_getOption("ProxyTimeleftLimit", self.proxyTimeleftLimit)
        # configured VOs
        res = getVOs()
        if not res["OK"]:
            return res
        self.voList = res.get("Value", [])

        if isinstance(self.voList, str):
            self.voList = [self.voList]
        # download shifter proxies for enabled VOs:
        self.proxyDict = {}

        for vo in self.voList:
            opsHelper = Operations(vo=vo)
            # is remote pilot logging enabled for the VO ?
            pilotLogging = opsHelper.getValue("/Pilot/RemoteLogging", False)
            if pilotLogging:
                res = opsHelper.getOptionsDict("Shifter/DataManager")
                if not res["OK"]:
                    self.log.error(f"No shifter defined for VO: {vo} - skipping ...")
                    continue

                proxyUser = res["Value"].get("User")
                proxyGroup = res["Value"].get("Group")
                if proxyGroup is None or proxyUser is None:
                    self.log.error(
                        f"No proxy user or group defined for pilot: VO: {vo}, User: {proxyUser}, Group: {proxyGroup}"
                    )
                    continue

                self.log.info(f"Proxy used for pilot logging: VO: {vo}, User: {proxyUser}, Group: {proxyGroup}")
                # download a proxy and save a file name, userDN and proxyGroup for future use:
                result = getDNForUsername(proxyUser)
                if not result["OK"]:
                    self.log.error(f"Could not obtain a DN of user {proxyUser} for VO {vo}, skipped")
                    continue
                userDNs = result["Value"]  # the same user may have more than one DN

                with tempfile.NamedTemporaryFile(prefix="gridpp" + "__", delete=False) as ntf:
                    result = self._downloadProxy(vo, userDNs, proxyGroup, ntf.name)

                if not result["OK"]:
                    # no proxy, we have no other option than to skip the VO
                    continue
                self.proxyDict[vo] = {"proxy": result["Value"], "DN": userDNs, "group": proxyGroup}

        return S_OK()

    def execute(self):
        """
        Execute one agent cycle. Upload log files to the SE and register them in the DFC.
        Consider only VOs we have proxies for.

        :param self: self reference
        """
        voRes = {}
        self.log.verbose(f"VOs configured for remote logging: {list(self.proxyDict.keys())}")
        originalUserProxy = os.environ.get("X509_USER_PROXY")
        for vo, elem in self.proxyDict.items():
            if self._isProxyExpired(elem["proxy"], self.proxyTimeleftLimit):
                result = self._downloadProxy(vo, elem["DN"], elem["group"], elem["proxy"])
                if not result["OK"]:
                    voRes[vo] = result["Message"]
                    continue
            os.environ["X509_USER_PROXY"] = elem["proxy"]
            res = self.executeForVO(vo)
            if not res["OK"]:
                voRes[vo] = res["Message"]
        # restore the original proxy:
        if originalUserProxy:
            os.environ["X509_USER_PROXY"] = originalUserProxy
        else:
            os.environ.pop("X509_USER_PROXY", None)

        if voRes:
            for key, value in voRes.items():
                self.log.error(f"Error for {key} vo; message: {value}")
            voRes.update(S_ERROR("Agent cycle for some VO finished with errors"))
            return voRes

        return S_OK()

    @executeWithoutServerCertificate
    def executeForVO(self, vo):
        """
        Execute one agent cycle for a VO. It obtains VO-specific configuration pilot options from the CS:
        UploadPath - the path where the VO wants to upload pilot logs. It has to start with a VO name (/vo/path).
        UploadSE - Storage element where the logs will be kept.

        :param str vo: vo enabled for remote pilot logging (and a successfully downloaded proxy for the VO)
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        self.log.info(f"Pilot files upload cycle started for VO: {vo}")
        opsHelper = Operations(vo=vo)
        res = opsHelper.getOptionsDict("Pilot")
        if not res["OK"]:
            return S_ERROR(f"No pilot section for {vo} vo")
        pilotOptions = res["Value"]
        uploadSE = pilotOptions.get("UploadSE")
        if uploadSE is None:
            return S_ERROR("Upload SE not defined")
        self.log.info(f"Pilot upload SE: {uploadSE}")

        uploadPath = pilotOptions.get("UploadPath")
        if uploadPath is None:
            return S_ERROR(f"Upload path on SE {uploadSE} not defined")
        self.log.info(f"Pilot upload path: {uploadPath}")

        client = TornadoPilotLoggingClient(useCertificates=True)
        resDict = client.getMetadata()

        if not resDict["OK"]:
            return resDict

        # vo-specific source log path:
        pilotLogPath = os.path.join(resDict["Value"]["LogPath"], vo)
        # check for new files and upload them
        if not os.path.exists(pilotLogPath):
            # not a disaster, the VO is enabled, but no logfiles were ever stored.
            return S_OK()
        # delete old pilot log files for the vo VO
        self.clearOldPilotLogs(pilotLogPath)

        self.log.info(f"Pilot log files location = {pilotLogPath} for VO: {vo}")

        # get finalised (.log) files from Tornado and upload them to the selected SE

        files = [
            f for f in os.listdir(pilotLogPath) if os.path.isfile(os.path.join(pilotLogPath, f)) and f.endswith("log")
        ]

        if not files:
            self.log.info("No files to upload for this cycle")
        for elem in files:
            lfn = os.path.join(uploadPath, elem)
            name = os.path.join(pilotLogPath, elem)
            res = DataManager().putAndRegister(lfn=lfn, fileName=name, diracSE=uploadSE, overwrite=True)
            if not res["OK"]:
                self.log.error("Could not upload", f"to {uploadSE}: {res['Message']}")
            else:
                self.log.verbose("File uploaded: ", f"LFN = {res['Value']}")
                try:
                    os.remove(name)
                except Exception as excp:
                    self.log.exception("Cannot remove a local file after uploading", lException=excp)
        return S_OK()

    def clearOldPilotLogs(self, pilotLogPath):
        """
        Delete old pilot log files unconditionally. Assumes that pilotLogPath exists.

        :param str pilotLogPath: log files directory
        :return: None
        :rtype: None
        """

        files = os.listdir(pilotLogPath)
        seconds = int(self.clearPilotsDelay) * 86400
        currentTime = time.time()

        for file in files:
            fullpath = os.path.join(pilotLogPath, file)
            modifTime = os.stat(fullpath).st_mtime
            if modifTime < currentTime - seconds:
                self.log.debug(f" Deleting old log : {fullpath}")
                try:
                    os.remove(fullpath)
                except Exception as excp:
                    self.log.exception(f"Cannot remove an old log file after {fullpath}", lException=excp)

    def _downloadProxy(self, vo, userDNs, proxyGroup, filename):
        """
        Fetch a new proxy and store it in a file filename.

        :param str vo: VO to get a proxy for
        :param list userDNs: user DN list
        :param str proxyGroup: user group
        :param str filename: file name to store a proxy
        :return: Dirac S_OK or S_ERROR object
        :rtype: dict
        """
        vomsAttr = getVOMSAttributeForGroup(proxyGroup)
        result = getProxy(userDNs, proxyGroup, vomsAttr=vomsAttr, proxyFilePath=filename)
        if not result["OK"]:
            self.log.error(f"Could not download a proxy for DN {userDNs}, group {proxyGroup} for VO {vo}, skipped")
            return S_ERROR(f"Could not download a proxy, {vo} skipped")
        return result

    def _isProxyExpired(self, proxyfile, limit):
        """
        Check proxy timeleft. If less than a limit, return True.

        :param str proxyfile:
        :param int limit: timeleft threshold below which a proxy is considered expired.
        :return: True or False
        :rtype: bool
        """
        result = getProxyInfo(proxyfile)
        if not result["OK"]:
            self.log.error(f"Could not get proxy info {result['Message']}")
            return True
        timeleft = result["Value"]["secondsLeft"]
        self.log.debug(f"Proxy {proxyfile} time left: {timeleft}")
        if timeleft < limit:
            self.log.info(f"proxy {proxyfile} expired/is about to expire. Will fetch a new one")
            return True
        return False
