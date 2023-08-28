########################################################################
# File: CleanReqDBAgent.py
# Author: Krzysztof.Ciba@NOSPAMgmail.com
# Date: 2013/05/17 08:31:26
########################################################################
"""Cleaning the RequestDB from obsolete records and kicking assigned requests

.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN CleanReqDBAgent
  :end-before: ##END
  :dedent: 2
  :caption: CleanReqDBAgent options

.. moduleauthor:: Krzysztof.Ciba@NOSPAMgmail.com

"""
# #
# @file CleanReqDBAgent.py
# @author Krzysztof.Ciba@NOSPAMgmail.com
# @date 2013/05/17 08:32:08
# @brief Definition of CleanReqDBAgent class.

# # imports
import datetime

# # from DIRAC
from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.RequestManagementSystem.Client.ReqClient import ReqClient

AGENT_NAME = "RequestManagement/CleanReqDBAgent"

########################################################################


class CleanReqDBAgent(AgentModule):
    """
    .. class:: CleanReqDBAgent

    """

    # # DEL GRACE PERIOD in DAYS
    DEL_GRACE_DAYS = 60
    # number of days before a scheduled request is set to cancelled
    # default: 0, i.e. do not cancel
    CANCEL_GRACE_DAYS = 0
    # # DEL LIMIT
    DEL_LIMIT = 100
    # # KICK PERIOD in HOURS
    KICK_GRACE_HOURS = 1
    # # KICK LIMIT
    KICK_LIMIT = 10000
    # # remove failed requests flag
    DEL_FAILED = False

    # # request client
    __requestClient = None

    def requestClient(self):
        """request client getter"""
        if not self.__requestClient:
            self.__requestClient = ReqClient()
        return self.__requestClient

    def initialize(self):
        """initialization"""
        self.DEL_GRACE_DAYS = self.am_getOption("DeleteGraceDays", self.DEL_GRACE_DAYS)
        self.log.info(f"Delete grace period = {self.DEL_GRACE_DAYS} days")
        self.DEL_LIMIT = self.am_getOption("DeleteLimit", self.DEL_LIMIT)
        self.log.info(f"Delete limit = {self.DEL_LIMIT} request/cycle")
        self.DEL_FAILED = self.am_getOption("DeleteFailed", self.DEL_FAILED)
        self.log.info("Delete failed requests: %s" % {True: "yes", False: "no"}[self.DEL_FAILED])
        self.cancelGraceDays = self.am_getOption("CancelGraceDays", self.CANCEL_GRACE_DAYS)
        self.log.info(f"Cancel grace period = {self.cancelGraceDays} days")
        self.KICK_GRACE_HOURS = self.am_getOption("KickGraceHours", self.KICK_GRACE_HOURS)
        self.log.info(f"Kick assigned requests period = {self.KICK_GRACE_HOURS} hours")
        self.KICK_LIMIT = self.am_getOption("KickLimit", self.KICK_LIMIT)
        self.log.info(f"Kick limit = {self.KICK_LIMIT} request/cycle")

        if self.cancelGraceDays >= self.DEL_GRACE_DAYS:
            self.cancelGraceDays = self.DEL_GRACE_DAYS - 1
            self.log.warn("Cancelled jobs grace period > delete period, capping to %u days" % self.cancelGraceDays)

        return S_OK()

    def execute(self):
        """execution in one cycle"""

        now = datetime.datetime.utcnow()
        kickTime = now - datetime.timedelta(hours=self.KICK_GRACE_HOURS)
        rmTime = now - datetime.timedelta(days=self.DEL_GRACE_DAYS)

        # # kick
        statusList = ["Assigned"]
        requestIDsList = self.requestClient().getRequestIDsList(statusList, self.KICK_LIMIT)
        if not requestIDsList["OK"]:
            self.log.error(f"execute: {requestIDsList['Message']}")
            return requestIDsList

        requestIDsList = requestIDsList["Value"]
        kicked = 0
        for requestID, status, lastUpdate in requestIDsList:
            reqStatus = self.requestClient().getRequestStatus(requestID)
            if not reqStatus["OK"]:
                self.log.error(("execute: unable to get request status", reqStatus["Message"]))
                continue
            status = reqStatus["Value"]
            if lastUpdate < kickTime and status == "Assigned":
                getRequest = self.requestClient().peekRequest(requestID)
                if not getRequest["OK"]:
                    self.log.error(f"execute: unable to read request '{requestID}': {getRequest['Message']}")
                    continue
                getRequest = getRequest["Value"]
                if getRequest and getRequest.LastUpdate < kickTime:
                    self.log.info(
                        "execute: kick assigned request (%s/'%s') in status %s"
                        % (requestID, getRequest.RequestName, getRequest.Status)
                    )
                    putRequest = self.requestClient().putRequest(getRequest)
                    if not putRequest["OK"]:
                        self.log.error(
                            "execute: unable to put request (%s/'%s'): %s"
                            % (requestID, getRequest.RequestName, putRequest["Message"])
                        )
                        continue
                    else:
                        self.log.verbose("Kicked request %d" % putRequest["Value"])
                    kicked += 1

        # # delete
        statusList = ["Done", "Failed", "Canceled"] if self.DEL_FAILED else ["Done"]
        requestIDsList = self.requestClient().getRequestIDsList(statusList, self.DEL_LIMIT)
        if not requestIDsList["OK"]:
            self.log.error(f"execute: {requestIDsList['Message']}")
            return requestIDsList

        requestIDsList = requestIDsList["Value"]
        deleted = 0
        for requestID, status, lastUpdate in requestIDsList:
            if lastUpdate < rmTime:
                self.log.info(f"execute: deleting request '{requestID}' with status {status}")
                delRequest = self.requestClient().deleteRequest(requestID)
                if not delRequest["OK"]:
                    self.log.error("execute: unable to delete request", f"'{requestID}': {delRequest['Message']}")
                    continue
                deleted += 1

        # optional: Set Scheduled requests to Cancelled if older than threshold
        if self.cancelGraceDays > 0:
            cancelTime = datetime.datetime.utcnow() - datetime.timedelta(days=self.cancelGraceDays)
            result = self.requestClient().getRequestIDsList(["Scheduled"], self.DEL_LIMIT)
            if not result["OK"]:
                self.log.error("Failed to get list of Scheduled requests:", result["Message"])
                return result
            requestIDsList = result["Value"]
            cancelled = 0
            for requestID, status, lastUpdate in requestIDsList:
                if lastUpdate < cancelTime:
                    self.log.info("Cancelling overdue request", str(requestID))
                    cancelReq = self.requestClient().cancelRequest(requestID)
                    if not cancelReq["OK"]:
                        self.log.error("Unable to cancel request", f"'{requestID}': {cancelReq['Message']}")
                        continue
                    cancelled += 1

        self.log.info("execute: kicked assigned requests", str(kicked))
        self.log.info("execute: deleted finished requests", str(deleted))
        if self.cancelGraceDays > 0:
            self.log.info("execute: cancelled overdue requests", str(cancelled))

        return S_OK()
