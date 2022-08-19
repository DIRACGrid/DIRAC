""" ResourceStatusClient

  Client to interact with the ResourceStatus service and from it with the DB.
"""

# pylint: disable=unused-argument

from DIRAC import S_OK
from DIRAC.Core.Base.Client import Client, createClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import prepareDict


@createClient("ResourceStatus/ResourceStatus")
class ResourceStatusClient(Client):
    """
    The :class:`ResourceStatusClient` class exposes the :mod:`DIRAC.ResourceStatus`
    API. All functions you need are on this client.

    You can use this client on this way

     >>> from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
     >>> rsClient = ResourceStatusClient()
    """

    def __init__(self, **kwargs):

        super().__init__(**kwargs)
        self.setServer("ResourceStatus/ResourceStatus")

    def insert(self, tableName, record):
        """
        Insert a dictionary `record` as a row in table `tableName`

        :param str tableName: the name of the table
        :param dict record: dictionary of record to insert in the table

        :return: S_OK() || S_ERROR()
        """

        return self._getRPC().insert(tableName, record)

    def select(self, tableName, params=None):
        """
        Select rows from the table `tableName`

        :param str tableName: the name of the table
        :param dict record: dictionary of the selection parameters

        :return: S_OK() || S_ERROR()
        """

        if params is None:
            params = {}
        return self._getRPC().select(tableName, params)

    def delete(self, tableName, params=None):
        """
        Delect rows from the table `tableName`

        :param str tableName: the name of the table
        :param dict record: dictionary of the deletion parameters

        :Returns:
          S_OK() || S_ERROR()
        """

        if params is None:
            params = {}
        return self._getRPC().delete(tableName, params)

    ################################################################################
    # Element status methods - enjoy !

    def insertStatusElement(
        self,
        element,
        tableType,
        name,
        statusType,
        status,
        elementType,
        reason,
        dateEffective,
        lastCheckTime,
        tokenOwner,
        tokenExpiration=None,
        vO="all",
    ):
        """
    Inserts on <element><tableType> a new row with the arguments given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            vO,
        ]

        return self._getRPC().insert(element + tableType, prepareDict(columnNames, columnValues))

    def selectStatusElement(
        self,
        element,
        tableType,
        name=None,
        statusType=None,
        status=None,
        elementType=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        tokenOwner=None,
        tokenExpiration=None,
        meta=None,
        vO="all",
    ):
        """
    Gets from <element><tableType> all rows that match the parameters given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `[, string, list]`
        name of the individual of class element
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership
      **meta** - `dict`
        metadata for the mysql query. Currently it is being used only for column selection.
        For example: meta = { 'columns' : [ 'Name' ] } will return only the 'Name' column.

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "Meta",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            meta,
            vO,
        ]

        return self._getRPC().select(element + tableType, prepareDict(columnNames, columnValues))

    def deleteStatusElement(
        self,
        element,
        tableType,
        name=None,
        statusType=None,
        status=None,
        elementType=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        tokenOwner=None,
        tokenExpiration=None,
        meta=None,
        vO="all",
    ):
        """
    Deletes from <element><tableType> all rows that match the parameters given.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `[, string, list]`
        name of the individual of class element
      **statusType** - `[, string, list]`
        it has to be a valid status type for the element class
      **status** - `[, string, list]`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `[, string, list]`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `[, string, list]`
        decision that triggered the assigned status
      **dateEffective** - `[, datetime, list]`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `[, datetime, list]`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `[, string, list]`
        token assigned to the site & status type
      **tokenExpiration** - `[, datetime, list]`
        time-stamp setting validity of token ownership
      **meta** - `dict`
        metadata for the mysql query

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "Meta",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            meta,
            vO,
        ]

        return self._getRPC().delete(element + tableType, prepareDict(columnNames, columnValues))

    def addOrModifyStatusElement(
        self,
        element,
        tableType,
        name=None,
        statusType=None,
        status=None,
        elementType=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        tokenOwner=None,
        tokenExpiration=None,
        vO="all",
    ):
        """
    Adds or updates-if-duplicated from <element><tableType> and also adds a log
    if flag is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            vO,
        ]

        return self._getRPC().addOrModify(element + tableType, prepareDict(columnNames, columnValues))

    def modifyStatusElement(
        self,
        element,
        tableType,
        name=None,
        statusType=None,
        status=None,
        elementType=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        tokenOwner=None,
        tokenExpiration=None,
        vO="all",
    ):
        """
    Updates from <element><tableType> and also adds a log if flag is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            vO,
        ]

        return self._getRPC().addOrModify(element + tableType, prepareDict(columnNames, columnValues))

    def addIfNotThereStatusElement(
        self,
        element,
        tableType,
        name=None,
        statusType=None,
        status=None,
        elementType=None,
        reason=None,
        dateEffective=None,
        lastCheckTime=None,
        tokenOwner=None,
        tokenExpiration=None,
        vO="all",
    ):
        """
    Adds if-not-duplicated from <element><tableType> and also adds a log if flag
    is active.

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElement ), any of the defaults: `Site` \
        | `Resource` | `Node`
      **tableType** - `string`
        it has to be a valid tableType [ 'Status', 'Log', 'History' ]
      **name** - `string`
        name of the individual of class element
      **statusType** - `string`
        it has to be a valid status type for the element class
      **status** - `string`
        it has to be a valid status, any of the defaults: `Active` | `Degraded` | \
        `Probing` | `Banned`
      **elementType** - `string`
        column to distinguish between the different elements in the same element
        table.
      **reason** - `string`
        decision that triggered the assigned status
      **dateEffective** - `datetime`
        time-stamp from which the status & status type are effective
      **lastCheckTime** - `datetime`
        time-stamp setting last time the status & status were checked
      **tokenOwner** - `string`
        token assigned to the site & status type
      **tokenExpiration** - `datetime`
        time-stamp setting validity of token ownership

    :return: S_OK() || S_ERROR()
    """
        columnNames = [
            "Name",
            "StatusType",
            "Status",
            "ElementType",
            "Reason",
            "DateEffective",
            "LastCheckTime",
            "TokenOwner",
            "TokenExpiration",
            "VO",
        ]
        columnValues = [
            name,
            statusType,
            status,
            elementType,
            reason,
            dateEffective,
            lastCheckTime,
            tokenOwner,
            tokenExpiration,
            vO,
        ]

        return self._getRPC().addIfNotThere(element + tableType, prepareDict(columnNames, columnValues))

    ##############################################################################
    # Protected methods - Use carefully !!

    def notify(self, request, params):
        """Send notification for a given request with its params to the diracAdmin"""
        address = Operations().getValue("ResourceStatus/Notification/DebugGroup/Users")
        msg = "Matching parameters: " + str(params)
        sbj = "[NOTIFICATION] DIRAC ResourceStatusDB: " + request + " entry"
        NotificationClient().sendMail(address, sbj, msg, address)

    def _extermineStatusElement(self, element, name, keepLogs=True):
        """
    Deletes from <element>Status,
                 <element>History
                 <element>Log
     all rows with `elementName`. It removes all the entries, logs, etc..
    Use with common sense !

    :Parameters:
      **element** - `string`
        it has to be a valid element ( ValidElements ), any of the defaults: \
          `Site` | `Resource` | `Node`
      **name** - `[, string, list]`
        name of the individual of class element
      **keepLogs** - `bool`
        if active, logs are kept in the database

    :return: S_OK() || S_ERROR()
    """
        return self.__extermineStatusElement(element, name, keepLogs)

    def __extermineStatusElement(self, element, name, keepLogs):
        """
        This method iterates over the three ( or four ) table types - depending
        on the value of keepLogs - deleting all matches of `name`.
        """

        tableTypes = ["Status", "History"]
        if keepLogs is False:
            tableTypes.append("Log")

        for table in tableTypes:

            deleteQuery = self.deleteStatusElement(element, table, name=name)
            if not deleteQuery["OK"]:
                return deleteQuery

        return S_OK()
