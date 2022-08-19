""" ResourceManagementClient

  Client to interact with the ResourceManagement service and from it with the DB.
"""
from DIRAC.Core.Base.Client import Client, createClient


def prepareDict(columnNames, columnValues):
    """
    Convert 2 same size lists into a key->value dict. All Nonetype values are removed.

    :param list columnNames: list containing column names, which are the keys in the returned dict
    :param list columnValues: list of the corresponding values

    :return: dict
    """

    paramsDict = {}

    # make each key name uppercase to match database column names (case sensitive)
    for key, value in zip(columnNames, columnValues):
        if value is not None:
            paramsDict[key] = value

    return paramsDict


@createClient("ResourceStatus/ResourceManagement")
class ResourceManagementClient(Client):
    """
    The :class:`ResourceManagementClient` class exposes the :mod:`DIRAC.ResourceManagement`
    API. All functions you need are on this client.

    You can use this client on this way

     >>> from DIRAC.ResourceManagementSystem.Client.ResourceManagementClient import ResourceManagementClient
     >>> rsClient = ResourceManagementClient()
    """

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.setServer("ResourceStatus/ResourceManagement")

    # AccountingCache Methods ....................................................

    def selectAccountingCache(
        self, name=None, plotType=None, plotName=None, result=None, dateEffective=None, lastCheckTime=None, meta=None
    ):
        """
        Gets from PolicyResult all rows that match the parameters given.

        :param name: name of an individual of the grid topology
        :type name: string, list
        :param plotType: the plotType name (e.g. 'Pilot')
        :type plotType: string, list
        :param plotName: the plot name
        :type plotName: string, list
        :param result: command result
        :type result: string, list
        :param dateEffective: time-stamp from which the result is effective
        :type dateEffective:  datetime, list
        :param lastCheckTime: time-stamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
             For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Name", "PlotType", "PlotName", "Result", "DateEffective", "LastCheckTime", "Meta"]
        columnValues = [name, plotType, plotName, result, dateEffective, lastCheckTime, meta]

        return self._getRPC().select("AccountingCache", prepareDict(columnNames, columnValues))

    def addOrModifyAccountingCache(
        self, name=None, plotType=None, plotName=None, result=None, dateEffective=None, lastCheckTime=None
    ):
        """
        Adds or updates-if-duplicated to AccountingCache. Using `name`, `plotType`
        and `plotName` to query the database, decides whether to insert or update the
        table.

        :param str name: name of an individual of the grid topology
        :param str plotType: name (e.g. 'Pilot')
        :param str plotName: the plot name
        :param str result: command result
        :param datetime dateEffective: timestamp from which the result is effective
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Name", "PlotType", "PlotName", "Result", "DateEffective", "LastCheckTime"]
        columnValues = [name, plotType, plotName, result, dateEffective, lastCheckTime]

        return self._getRPC().addOrModify("AccountingCache", prepareDict(columnNames, columnValues))

    def deleteAccountingCache(
        self, name=None, plotType=None, plotName=None, result=None, dateEffective=None, lastCheckTime=None
    ):
        """
        Deletes from AccountingCache all rows that match the parameters given.

        :param str name: name of an individual of the grid topology
        :param str plotType: the plotType name (e.g. 'Pilot')
        :param str plotName: the plot name
        :param str result: command result
        :param datetime dateEffective: timestamp from which the result is effective
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Name", "PlotType", "PlotName", "Result", "DateEffective", "LastCheckTime"]
        columnValues = [name, plotType, plotName, result, dateEffective, lastCheckTime]

        return self._getRPC().delete("AccountingCache", prepareDict(columnNames, columnValues))

    # GGUSTicketsCache Methods ...................................................

    def selectGGUSTicketsCache(
        self, gocSite=None, link=None, openTickets=None, tickets=None, lastCheckTime=None, meta=None
    ):
        """
        Gets from GGUSTicketsCache all rows that match the parameters given.

        :param str gocSite:
        :param str link: url to the details
        :param int openTickets:
        :param str tickets:
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
           For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """

        columnNames = ["GocSite", "Link", "OpenTickets", "Tickets", "LastCheckTime", "Meta"]
        columnValues = [gocSite, link, openTickets, tickets, lastCheckTime, meta]

        return self._getRPC().select("GGUSTicketsCache", prepareDict(columnNames, columnValues))

    def deleteGGUSTicketsCache(self, gocSite=None, link=None, openTickets=None, tickets=None, lastCheckTime=None):
        """
        Deletes from GGUSTicketsCache all rows that match the parameters given.

        :param str gocSite:
        :param str link: url to the details
        :param int openTickets:
        :param str tickets:
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["GocSite", "Link", "OpenTickets", "Tickets", "LastCheckTime"]
        columnValues = [gocSite, link, openTickets, tickets, lastCheckTime]

        return self._getRPC().delete("GGUSTicketsCache", prepareDict(columnNames, columnValues))

    def addOrModifyGGUSTicketsCache(self, gocSite=None, link=None, openTickets=None, tickets=None, lastCheckTime=None):
        """
        Adds or updates-if-duplicated to GGUSTicketsCache all rows that match the parameters given.

        :param str gocSite:
        :param str link: url to the details
        :param int openTickets:
        :param str tickets:
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["GocSite", "Link", "OpenTickets", "Tickets", "LastCheckTime"]
        columnValues = [gocSite, link, openTickets, tickets, lastCheckTime]

        return self._getRPC().addOrModify("GGUSTicketsCache", prepareDict(columnNames, columnValues))

    # DowntimeCache Methods ......................................................

    def selectDowntimeCache(
        self,
        downtimeID=None,
        element=None,
        name=None,
        startDate=None,
        endDate=None,
        severity=None,
        description=None,
        link=None,
        dateEffective=None,
        lastCheckTime=None,
        gOCDBServiceType=None,
        meta=None,
    ):
        """
        Gets from DowntimeCache all rows that match the parameters given.

        :param downtimeID: unique id for the downtime
        :type downtimeID: string, list
        :param element: valid element in the topology (Site, Resource, Node)
        :type element: string, list
        :param name: name of the element(s) where the downtime applies
        :type name: string, list
        :param startDate: starting time for the downtime
        :type startDate: datetime, list
        :param endDate: ending time for the downtime
        :type endDate: datetime, list
        :param severity: severity assigned by the gocdb
        :type severity: string, list
        :param description: brief description of the downtime
        :type description: string, list
        :param link: url to the details
        :type link: string, list
        :param dateEffective: time when the entry was created in this database
        :type dateEffective: datetime, list
        :param lastCheckTime: timestamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :param str gOCDBServiceType: service type assigned by gocdb
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
            For example: meta={'columns': ['Name']} will return only the 'Name' column.

        :return: S_OK() || S_ERROR()
        """
        columnNames = [
            "DowntimeID",
            "Element",
            "Name",
            "StartDate",
            "EndDate",
            "Severity",
            "Description",
            "Link",
            "DateEffective",
            "LastCheckTime",
            "GOCDBServiceType",
            "Meta",
        ]
        columnValues = [
            downtimeID,
            element,
            name,
            startDate,
            endDate,
            severity,
            description,
            link,
            dateEffective,
            lastCheckTime,
            gOCDBServiceType,
            meta,
        ]

        return self._getRPC().select("DowntimeCache", prepareDict(columnNames, columnValues))

    def deleteDowntimeCache(
        self,
        downtimeID=None,
        element=None,
        name=None,
        startDate=None,
        endDate=None,
        severity=None,
        description=None,
        link=None,
        dateEffective=None,
        lastCheckTime=None,
        gOCDBServiceType=None,
    ):
        """
        Deletes from DowntimeCache all rows that match the parameters given.

        :param downtimeID: unique id for the downtime
        :type downtimeID: string, list
        :param element: valid element in the topology ( Site, Resource, Node )
        :type element: string, list
        :param name: name of the element where the downtime applies
        :type name: string, list
        :param startDate: starting time for the downtime
        :type startDate: datetime, list
        :param endDate: ending time for the downtime
        :type endDate: datetime, list
        :param severity: severity assigned by the gocdb
        :type severity: string, list
        :param description: brief description of the downtime
        :type description: string, list
        :param link: url to the details
        :type link: string, list
        :param dateEffective: time when the entry was created in this database
        :type dateEffective: datetime, list
        :param lastCheckTime: time-stamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :param str gOCDBServiceType: service type assigned by gocdb
        :return: S_OK() || S_ERROR()
        """
        columnNames = [
            "DowntimeID",
            "Element",
            "Name",
            "StartDate",
            "EndDate",
            "Severity",
            "Description",
            "Link",
            "DateEffective",
            "LastCheckTime",
            "GOCDBServiceType",
        ]
        columnValues = [
            downtimeID,
            element,
            name,
            startDate,
            endDate,
            severity,
            description,
            link,
            dateEffective,
            lastCheckTime,
            gOCDBServiceType,
        ]

        return self._getRPC().delete("DowntimeCache", prepareDict(columnNames, columnValues))

    def addOrModifyDowntimeCache(
        self,
        downtimeID=None,
        element=None,
        name=None,
        startDate=None,
        endDate=None,
        severity=None,
        description=None,
        link=None,
        dateEffective=None,
        lastCheckTime=None,
        gOCDBServiceType=None,
    ):
        """
        Adds or updates-if-duplicated to DowntimeCache. Using `downtimeID` to query
        the database, decides whether to insert or update the table.

        :param str downtimeID: unique id for the downtime
        :param str element: valid element in the topology ( Site, Resource, Node )
        :param str name: name of the element where the downtime applies
        :param datetime startDate: starting time for the downtime
        :param datetime endDate: ending time for the downtime
        :param str severity: severity assigned by the gocdb
        :param str description: brief description of the downtime
        :param str link: url to the details
        :param datetime dateEffective: time when the entry was created in this database
        :param datetime lastCheckTime: timestamp setting last time the result was checked
        :param str gOCDBServiceType: service type assigned by gocdb
        :return: S_OK() || S_ERROR()
        """
        columnNames = [
            "DowntimeID",
            "Element",
            "Name",
            "StartDate",
            "EndDate",
            "Severity",
            "Description",
            "Link",
            "DateEffective",
            "LastCheckTime",
            "GOCDBServiceType",
        ]
        columnValues = [
            downtimeID,
            element,
            name,
            startDate,
            endDate,
            severity,
            description,
            link,
            dateEffective,
            lastCheckTime,
            gOCDBServiceType,
        ]

        return self._getRPC().addOrModify("DowntimeCache", prepareDict(columnNames, columnValues))

    # JobCache Methods ...........................................................

    def selectJobCache(self, site=None, maskStatus=None, efficiency=None, status=None, lastCheckTime=None, meta=None):
        """
        Gets from JobCache all rows that match the parameters given.

        :param site: name of the site element
        :type site: string, list
        :param maskStatus: maskStatus for the site
        :type maskStatus: string, list
        :param efficiency: job efficiency ( successful / total )
        :type efficiency: float, list
        :param status: status for the site computed
        :type status: string, list
        :param lastCheckTime: timestamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
           For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "MaskStatus", "Efficiency", "Status", "LastCheckTime", "Meta"]
        columnValues = [site, maskStatus, efficiency, status, lastCheckTime, meta]

        return self._getRPC().select("JobCache", prepareDict(columnNames, columnValues))

    def deleteJobCache(self, site=None, maskStatus=None, efficiency=None, status=None, lastCheckTime=None):
        """
        Deletes from JobCache all rows that match the parameters given.

        :param site: name of the site element
        :type site: string, list
        :param maskStatus: maskStatus for the site
        :type maskStatus: string, list
        :param efficiency: job efficiency ( successful / total )
        :type efficiency: float, list
        :param status: status for the site computed
        :type status: string, list
        :param lastCheckTime: timestamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "MaskStatus", "Efficiency", "Status", "LastCheckTime"]
        columnValues = [site, maskStatus, efficiency, status, lastCheckTime]

        return self._getRPC().delete("JobCache", prepareDict(columnNames, columnValues))

    def addOrModifyJobCache(self, site=None, maskStatus=None, efficiency=None, status=None, lastCheckTime=None):
        """
        Adds or updates-if-duplicated to JobCache. Using `site` to query
        the database, decides whether to insert or update the table.

        :param site: name of the site element
        :type site: string, list
        :param maskStatus: maskStatus for the site
        :type maskStatus: string, list
        :param efficiency: job efficiency ( successful / total )
        :type efficiency: float, list
        :param status: status for the site computed
        :type status: string, list
        :param lastCheckTime: time-stamp setting last time the result was checked
        :type lastCheckTime: datetime, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "MaskStatus", "Efficiency", "Status", "LastCheckTime"]
        columnValues = [site, maskStatus, efficiency, status, lastCheckTime]

        return self._getRPC().addOrModify("JobCache", prepareDict(columnNames, columnValues))

    # TransferCache Methods ......................................................

    def selectTransferCache(
        self, sourceName=None, destinationName=None, metric=None, value=None, lastCheckTime=None, meta=None
    ):
        """
        Gets from TransferCache all rows that match the parameters given.

        :param elementName: name of the element
        :type elementName: string, list
        :param direction: the element taken as Source or Destination of the transfer
        :type direction: string, list
        :param metric: measured quality of failed transfers
        :type metric: string, list
        :param value: percentage
        :type value: float, list
        :param lastCheckTime: time-stamp setting last time the result was checked
        :type lastCheckTime: float, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
          For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["SourceName", "DestinationName", "Metric", "Value", "LastCheckTime", "Meta"]
        columnValues = [sourceName, destinationName, metric, value, lastCheckTime, meta]

        return self._getRPC().select("TransferCache", prepareDict(columnNames, columnValues))

    def deleteTransferCache(self, sourceName=None, destinationName=None, metric=None, value=None, lastCheckTime=None):
        """
         Deletes from TransferCache all rows that match the parameters given.

        :param elementName: name of the element
        :type elementName: string, list
        :param direction: the element taken as Source or Destination of the transfer
        :type direction: string, list
        :param metric: measured quality of failed transfers
        :type metric: string, list
        :param value: percentage
        :type value: float, list
        :param lastCheckTime: time-stamp setting last time the result was checked
        :type lastCheckTime: float, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["SourceName", "DestinationName", "Metric", "Value", "LastCheckTime"]
        columnValues = [sourceName, destinationName, metric, value, lastCheckTime]

        return self._getRPC().delete("TransferCache", prepareDict(columnNames, columnValues))

    def addOrModifyTransferCache(
        self, sourceName=None, destinationName=None, metric=None, value=None, lastCheckTime=None
    ):
        """
         Adds or updates-if-duplicated to TransferCache. Using `elementName`, `direction`
         and `metric` to query the database, decides whether to insert or update the table.

        :param str elementName: name of the element
        :param str direction: the element taken as Source or Destination of the transfer
        :param str metric: measured quality of failed transfers
        :param float value: percentage
        :param datetime lastCheckTime: time-stamp setting last time the result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["SourceName", "DestinationName", "Metric", "Value", "LastCheckTime"]
        columnValues = [sourceName, destinationName, metric, value, lastCheckTime]

        return self._getRPC().addOrModify("TransferCache", prepareDict(columnNames, columnValues))

    # PilotCache Methods .........................................................

    def selectPilotCache(
        self,
        site=None,
        cE=None,
        pilotsPerJob=None,
        pilotJobEff=None,
        status=None,
        lastCheckTime=None,
        meta=None,
        vO=None,
    ):
        """
        Gets from TransferCache all rows that match the parameters given.

        :param site: name of the site
        :type site: string, list
        :param cE: name of the CE of 'Multiple' if all site CEs are considered
        :type cE: string, list
        :param pilotsPerJob: measure calculated
        :type pilotsPerJob: float, list
        :param pilotJobEff: percentage
        :type pilotJobEff: float, list
        :param status: status of the CE / Site
        :type status: float, list
        :param lastCheckTime: measure calculated
        :type lastCheckTime: datetime, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
           For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "CE", "PilotsPerJob", "PilotJobEff", "Status", "LastCheckTime", "Meta", "VO"]
        columnValues = [site, cE, pilotsPerJob, pilotJobEff, status, lastCheckTime, meta, vO]

        return self._getRPC().select("PilotCache", prepareDict(columnNames, columnValues))

    def deletePilotCache(
        self, site=None, cE=None, pilotsPerJob=None, pilotJobEff=None, status=None, lastCheckTime=None, vO=None
    ):
        """
        Deletes from TransferCache all rows that match the parameters given.

        :param site: name of the site
        :type site: string, list
        :param cE: name of the CE of 'Multiple' if all site CEs are considered
        :type cE: string, list
        :param pilotsPerJob: measure calculated
        :type pilotsPerJob: float, list
        :param pilotJobEff: percentage
        :type pilotJobEff: float, list
        :param status: status of the CE / Site
        :type status: float, list
        :param lastCheckTime: measure calculated
        :type lastCheckTime: datetime, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "CE", "PilotsPerJob", "PilotJobEff", "Status", "LastCheckTime", "VO"]
        columnValues = [site, cE, pilotsPerJob, pilotJobEff, status, lastCheckTime, vO]

        return self._getRPC().delete("PilotCache", prepareDict(columnNames, columnValues))

    def addOrModifyPilotCache(
        self, site=None, cE=None, pilotsPerJob=None, pilotJobEff=None, status=None, lastCheckTime=None, vO=None
    ):
        """
        Adds or updates-if-duplicated to PilotCache. Using `site` and `cE`
        to query the database, decides whether to insert or update the table.

        :param str site: name of the site
        :param str cE: name of the CE of 'Multiple' if all site CEs are considered
        :param float pilotsPerJob: measure calculated
        :param flaot pilotJobEff: percentage
        :param str status: status of the CE / Site
        :param datetime lastCheckTime: measure calculated
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Site", "CE", "PilotsPerJob", "PilotJobEff", "Status", "LastCheckTime", "VO"]
        columnValues = [site, cE, pilotsPerJob, pilotJobEff, status, lastCheckTime, vO]

        return self._getRPC().addOrModify("PilotCache", prepareDict(columnNames, columnValues))

    # PolicyResult Methods .......................................................

    def selectPolicyResult(
        self,
        element=None,
        name=None,
        policyName=None,
        statusType=None,
        status=None,
        reason=None,
        lastCheckTime=None,
        meta=None,
        vO=None,
    ):
        """
        Gets from PolicyResult all rows that match the parameters given.

        :param granularity: it has to be a valid element ( ValidElement ), any of the defaults:
           'Site' | 'Service' | 'Resource' | 'StorageElement'
        :type granularity: string, list
        :param name: name of the element
        :type name: string, list
        :param policyName: name of the policy
        :type policyName: string, list
        :param statusType: it has to be a valid status type for the given granularity
        :type statusType: string, list
        :param status: it has to be a valid status, any of the defaults:
            'Active' | 'Degraded' | 'Probing' | 'Banned'
        :type status: string, list
        :param reason: decision that triggered the assigned status
        :type reason: string, list
        :param lastCheckTime: time-stamp setting last time the policy result was checked
        :type lastCheckTime: datetime, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
            For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Element", "Name", "PolicyName", "StatusType", "Status", "Reason", "LastCheckTime", "Meta", "VO"]
        columnValues = [element, name, policyName, statusType, status, reason, lastCheckTime, meta, vO]

        return self._getRPC().select("PolicyResult", prepareDict(columnNames, columnValues))

    def deletePolicyResult(
        self,
        element=None,
        name=None,
        policyName=None,
        statusType=None,
        status=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        vO=None,
    ):
        """
        Deletes from PolicyResult all rows that match the parameters given.

        :param granularity: it has to be a valid element ( ValidElement ), any of the defaults:
           'Site' | 'Service' | 'Resource' | 'StorageElement'
        :type granularity: string, list
        :param name: name of the element
        :type name: string, list
        :param policyName: name of the policy
        :type policyName: string, list
        :param statusType: it has to be a valid status type for the given granularity
        :type statusType: string, list
        :param status: it has to be a valid status, any of the defaults: 'Active' | 'Degraded' | 'Probing' | 'Banned'
        :type status: string, list
        :param reason: decision that triggered the assigned status
        :type reason: string, list
        :param datetime dateEffective: time-stamp from which the policy result is effective
        :param lastCheckTime: time-stamp setting last time the policy result was checked
        :type lastCheckTime: datetime, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = [
            "Element",
            "Name",
            "PolicyName",
            "StatusType",
            "Status",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "VO",
        ]
        columnValues = [element, name, policyName, statusType, status, reason, dateEffective, lastCheckTime, vO]

        return self._getRPC().delete("PolicyResult", prepareDict(columnNames, columnValues))

    def addOrModifyPolicyResult(
        self,
        element=None,
        name=None,
        policyName=None,
        statusType=None,
        status=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        vO=None,
    ):
        """
        Adds or updates-if-duplicated to PolicyResult. Using `name`, `policyName` and
        `statusType` to query the database, decides whether to insert or update the table.

        :param str element: it has to be a valid element ( ValidElement ), any of the defaults:
           'Site' | 'Service' | 'Resource' | 'StorageElement'
        :param str name: name of the element
        :param str policyName: name of the policy
        :param str statusType: it has to be a valid status type for the given element
        :param str status: it has to be a valid status, any of the defaults:
          'Active' | 'Degraded' | 'Probing' | 'Banned'
        :param str reason: decision that triggered the assigned status
        :param datetime dateEffective: time-stamp from which the policy result is effective
        :param datetime lastCheckTime: time-stamp setting last time the policy result was checked
        :return: S_OK() || S_ERROR()
        """
        columnNames = [
            "Element",
            "Name",
            "PolicyName",
            "StatusType",
            "Status",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "VO",
        ]
        columnValues = [element, name, policyName, statusType, status, reason, dateEffective, lastCheckTime, vO]

        return self._getRPC().addOrModify("PolicyResult", prepareDict(columnNames, columnValues))

    # SpaceTokenOccupancyCache Methods ...........................................

    def selectSpaceTokenOccupancyCache(
        self, endpoint=None, token=None, total=None, guaranteed=None, free=None, lastCheckTime=None, meta=None
    ):
        """
        Gets from SpaceTokenOccupancyCache all rows that match the parameters given.

        :param endpoint: endpoint
        :type endpoint: string, list
        :param token: name of the token
        :type token: string, list
        :param total: total terabytes
        :type total: integer, list
        :param guaranteed: guaranteed terabytes
        :type guaranteed: integer, list
        :param free: free terabytes
        :type free: integer, list
        :param lastCheckTime: time-stamp from which the result is effective
        :type lastCheckTime: datetime, list
        :param dict meta: metadata for the mysql query. Currently it is being used only for column selection.
            For example: meta={'columns': ['Name']} will return only the 'Name' column.
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Endpoint", "Token", "Total", "Guaranteed", "Free", "LastCheckTime", "Meta"]
        columnValues = [endpoint, token, total, guaranteed, free, lastCheckTime, meta]

        return self._getRPC().select("SpaceTokenOccupancyCache", prepareDict(columnNames, columnValues))

    def deleteSpaceTokenOccupancyCache(
        self, endpoint=None, token=None, total=None, guaranteed=None, free=None, lastCheckTime=None
    ):
        """
        Deletes from SpaceTokenOccupancyCache all rows that match the parameters given.

        :param endpoint: endpoint
        :type endpoint: string, list
        :param token: name of the token
        :type token: string, list
        :param total: total terabytes
        :type total: integer, list
        :param guaranteed: guaranteed terabytes
        :type guaranteed: integer, list
        :param free: free terabytes
        :type free: integer, list
        :param lastCheckTime: time-stamp from which the result is effective
        :type lastCheckTime: datetime, list
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Endpoint", "Token", "Total", "Guaranteed", "Free", "LastCheckTime"]
        columnValues = [endpoint, token, total, guaranteed, free, lastCheckTime]

        return self._getRPC().delete("SpaceTokenOccupancyCache", prepareDict(columnNames, columnValues))

    def addOrModifySpaceTokenOccupancyCache(
        self, endpoint=None, token=None, total=None, guaranteed=None, free=None, lastCheckTime=None
    ):
        """
        Adds or updates-if-duplicated to SpaceTokenOccupancyCache. Using `site` and `token`
        to query the database, decides whether to insert or update the table.

        :param endpoint: endpoint
        :type endpoint: string, list
        :param str token: name of the token
        :param int total: total terabytes
        :param int guaranteed: guaranteed terabytes
        :param int free: free terabytes
        :param datetime lastCheckTime: time-stamp from which the result is effective
        :return: S_OK() || S_ERROR()
        """
        columnNames = ["Endpoint", "Token", "Total", "Guaranteed", "Free", "LastCheckTime"]
        columnValues = [endpoint, token, total, guaranteed, free, lastCheckTime]

        return self._getRPC().addOrModify("SpaceTokenOccupancyCache", prepareDict(columnNames, columnValues))


# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
