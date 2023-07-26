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
import time
from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getVOs
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Core.Utilities.Proxy import executeWithUserProxy
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
        self.clearPilotsDelay = 30

    def initialize(self):
        """
        agent's initialisation. Use this agent's CS information to:
        Determine what Defaults/Shifter shifter proxy to use.,
        get the target SE name from the CS.
        Obtain log file location from Tornado.

        :param self: self reference
        """
        # pilot logs lifetime in days
        self.clearPilotsDelay = self.am_getOption("ClearPilotsDelay", self.clearPilotsDelay)
        # configured VOs and setup
        res = getVOs()
        if not res["OK"]:
            return res
        self.voList = res.get("Value", [])

        if isinstance(self.voList, str):
            self.voList = [self.voList]

        return S_OK()

    def execute(self):
        """
        Execute one agent cycle. Upload log files to the SE and register them in the DFC.
        Use a shifter proxy dynamically loaded for every VO

        :param self: self reference
        """
        voRes = {}
        for vo in self.voList:
            self.opsHelper = Operations(vo=vo)
            # is remote pilot logging enabled for the VO ?
            pilotLogging = self.opsHelper.getValue("/Pilot/RemoteLogging", False)
            if pilotLogging:
                res = self.opsHelper.getOptionsDict("Shifter/DataManager")
                if not res["OK"]:
                    voRes[vo] = "No shifter defined - skipped"
                    self.log.error(f"No shifter defined for VO: {vo} - skipping ...")
                    continue

                proxyUser = res["Value"].get("User")
                proxyGroup = res["Value"].get("Group")
                if proxyGroup is None or proxyUser is None:
                    self.log.error(
                        f"No proxy user or group defined for pilot: VO: {vo}, User: {proxyUser}, Group: {proxyGroup}"
                    )
                    voRes[vo] = "No proxy user or group defined - skipped"
                    continue

                self.log.info(f"Proxy used for pilot logging: VO: {vo}, User: {proxyUser}, Group: {proxyGroup}")
                res = self.executeForVO(  # pylint: disable=unexpected-keyword-arg
                    vo, proxyUserName=proxyUser, proxyUserGroup=proxyGroup
                )
                if not res["OK"]:
                    voRes[vo] = res["Message"]
        if voRes:
            for key, value in voRes.items():
                self.log.error(f"Error for {key} vo; message: {value}")
            voRes.update(S_ERROR("Agent cycle for some VO finished with errors"))
            return voRes
        return S_OK()

    @executeWithUserProxy
    def executeForVO(self, vo):
        """
        Execute one agent cycle for a VO. It obtains VO-specific configuration pilot options from the CS:
        UploadPath - the path where the VO wants to upload pilot logs. It has to start with a VO name (/vo/path).
        UploadSE - Storage element where the logs will be kept.

        :param str vo: vo enabled for remote pilot logging
        :return: S_OK or S_ERROR
        :rtype: dict
        """

        self.log.info(f"Pilot files upload cycle started for VO: {vo}")
        res = self.opsHelper.getOptionsDict("Pilot")
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
