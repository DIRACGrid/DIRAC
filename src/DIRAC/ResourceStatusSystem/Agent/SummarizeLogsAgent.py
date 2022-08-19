""" SummarizeLogsAgent module

  This agents scans all the log tables (SiteLog, ResourceLog and NodeLog) on the
  ResourceStatusDB and summarizes them. The results are stored on the History
  tables (SiteHistory, ResourceHistory and NodeHistory) and the Log tables
  cleared.

  In order to summarize the logs, all entries with no changes on the Status or
  TokenOwner column for a given (Name, StatusType) tuple are discarded.

  The agent also adds a little prevention to avoid messing the summaries if the
  agent is restarted / killed abruptly. Please, please, please, DO NOT DO IT !


.. literalinclude:: ../ConfigTemplate.cfg
  :start-after: ##BEGIN SummarizeLogsAgent
  :end-before: ##END
  :dedent: 2
  :caption: SummarizeLogsAgent options
"""
from datetime import datetime, timedelta

from DIRAC import S_OK
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient

AGENT_NAME = "ResourceStatus/SummarizeLogsAgent"


class SummarizeLogsAgent(AgentModule):
    """SummarizeLogsAgent as extension of AgentModule."""

    def __init__(self, *args, **kwargs):
        """Constructor."""

        AgentModule.__init__(self, *args, **kwargs)

        self.rsClient = None
        self.months = 36

    def initialize(self):
        """Standard initialize.

        :return: S_OK

        """

        self.rsClient = ResourceStatusClient()
        self.months = self.am_getOption("Months", self.months)
        return S_OK()

    def execute(self):
        """execute (main method)

        The execute method runs over the three families of tables (Site, Resource and
        Node) performing identical operations. First, selects all logs for a given
        family (and keeps track of which one is the last row ID). It summarizes the
        logs and finally, deletes the logs from the database.

        At last, this agent removes older entries from history tables

        :return: S_OK
        """

        # loop over the tables
        for element in ("Site", "Resource", "Node"):

            self.log.info("Summarizing %s" % element)

            # get all logs to be summarized
            selectLogElements = self._summarizeLogs(element)
            if not selectLogElements["OK"]:
                self.log.error(selectLogElements["Message"])
                continue

            lastID, logElements = selectLogElements["Value"]

            # logElements is a dictionary of key-value pairs as follows:
            # (name, statusType) : list(logs)
            for key, logs in logElements.items():

                sumResult = self._registerLogs(element, key, logs)
                if not sumResult["OK"]:
                    self.log.error(sumResult["Message"])
                    continue

            if lastID is not None:
                self.log.info(f"Deleting {element}Log till ID {lastID}")
                deleteResult = self.rsClient.deleteStatusElement(element, "Log", meta={"older": ["ID", lastID]})
                if not deleteResult["OK"]:
                    self.log.error(deleteResult["Message"])
                    continue

        if self.months:
            self._removeOldHistoryEntries(element, self.months)

        return S_OK()

    def _summarizeLogs(self, element):
        """given an element, selects all logs in table <element>Log.

        :param str element: name of the table family (either Site, Resource or Node)
        :return: S_OK(lastID, listOfLogs) / S_ERROR
        """

        selectResults = self.rsClient.selectStatusElement(element, "Log")

        if not selectResults["OK"]:
            return selectResults

        selectedItems = {}
        latestID = None

        if not selectResults["Value"]:
            return S_OK((latestID, selectedItems))

        selectColumns = selectResults["Columns"]
        selectResults = selectResults["Value"]

        if selectResults:
            latestID = dict(zip(selectColumns, selectResults[-1]))["ID"]

        for selectResult in selectResults:

            elementDict = dict(zip(selectColumns, selectResult))

            key = (elementDict["Name"], elementDict["StatusType"])

            if key not in selectedItems:
                selectedItems[key] = [elementDict]
            else:
                lastStatus = selectedItems[key][-1]["Status"]
                lastToken = selectedItems[key][-1]["TokenOwner"]

                # If there are no changes on the Status or the TokenOwner with respect
                # the previous one, discards the log.
                if lastStatus != elementDict["Status"] or lastToken != elementDict["TokenOwner"]:
                    selectedItems[key].append(elementDict)

        return S_OK((latestID, selectedItems))

    def _registerLogs(self, element, key, logs):
        """Given an element, a key - which is a tuple (<name>, <statusType>)
        and a list of dictionaries, this method inserts them on the <element>History
        table. Before inserting them, checks whether the first one is or is not on
        the <element>History table. If it is, it is not inserted.


        :param str element: name of the table family (either Site, Resource or Node)
        :param tuple key: tuple with the name of the element and the statusType
        :param list logs: list of dictionaries containing the logs
        :return: S_OK(lastID, listOfLogs) / S_ERROR

         :return: S_OK / S_ERROR
        """

        if not logs:
            return S_OK()

        # Undo key
        name, statusType = key

        selectedRes = self.rsClient.selectStatusElement(
            element,
            "History",
            name,
            statusType,
            meta={"columns": ["Status", "TokenOwner"], "limit": 1, "order": ["DateEffective", "desc"]},
        )

        if not selectedRes["OK"]:
            return selectedRes
        selectedRes = selectedRes["Value"]
        if not selectedRes:
            for selectedItemDict in logs:
                res = self.__logToHistoryTable(element, selectedItemDict)
                if not res["OK"]:
                    return res
                return S_OK()

        # We want from the <element>History table the last Status, and TokenOwner
        lastStatus, lastToken = None, None
        if selectedRes:
            try:
                lastStatus = selectedRes[0][0]
                lastToken = selectedRes[0][1]
            except IndexError:
                pass

        # If the first of the selected items has a different status than the latest
        # on the history, we keep it, otherwise we remove it.
        if logs[0]["Status"] == lastStatus and logs[0]["TokenOwner"] == lastToken:
            logs.pop(0)

        if logs:
            self.log.info(f"{name} ({statusType}):")
            self.log.debug(logs)

        for selectedItemDict in logs:

            res = self.__logToHistoryTable(element, selectedItemDict)
            if not res["OK"]:
                return res

        return S_OK()

    def __logToHistoryTable(self, element, elementDict):
        """Given an element and a dictionary with all the arguments, this method
        inserts a new entry on the <element>History table

        :param str element: name of the table family (either Site, Resource or Node)
        :param dict elementDict: dictionary returned from the DB to be inserted on the History table

        :return: S_OK / S_ERROR
        """

        name = elementDict.get("Name")
        statusType = elementDict.get("StatusType")
        # vo = elementDict.get('VO')  # FIXME: not sure about it
        status = elementDict.get("Status")
        elementType = elementDict.get("ElementType")
        reason = elementDict.get("Reason")
        dateEffective = elementDict.get("DateEffective")
        lastCheckTime = elementDict.get("LastCheckTime")
        tokenOwner = elementDict.get("TokenOwner")
        tokenExpiration = elementDict.get("TokenExpiration")

        self.log.info(f"  {status} {dateEffective} {tokenOwner} {reason}")

        return self.rsClient.insertStatusElement(
            element=element,
            tableType="History",
            name=name,
            statusType=statusType,
            status=status,
            elementType=elementType,
            reason=reason,
            dateEffective=dateEffective,
            lastCheckTime=lastCheckTime,
            tokenOwner=tokenOwner,
            tokenExpiration=tokenExpiration,
        )

    def _removeOldHistoryEntries(self, element, months):
        """Delete entries older than period

        :param str element: name of the table family (either Site, Resource or Node)
        :param int months: number of months

        :return: S_OK / S_ERROR
        """
        toRemove = datetime.utcnow().replace(microsecond=0) - timedelta(days=30 * months)
        self.log.info("Removing history entries", "older than %s" % toRemove)

        deleteResult = self.rsClient.deleteStatusElement(
            element, "History", meta={"older": ["DateEffective", toRemove]}
        )
        if not deleteResult["OK"]:
            self.log.error(deleteResult["Message"])
