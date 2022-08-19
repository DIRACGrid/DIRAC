""" ResourceStatusDB:
    This module provides definition of the DB tables, and methods to access them.

    Written using sqlalchemy declarative_base


    For extending the ResourceStatusDB tables:

    1) In the extended module, call:

    from DIRAC.ResourceStatusSystem.DB.ResourceStatusDB import rmsBase, TABLESLIST, TABLESLISTWITHID
    TABLESLIST = TABLESLIST + [list of new table names]
    TABLESLISTWITHID = TABLESLISTWITHID + [list of new table names]

    2) provide a declarative_base definition of the tables (new or extended) in the extension module

"""
import datetime
from sqlalchemy.orm import class_mapper
from sqlalchemy.orm.query import Query
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, DateTime, exc, BigInteger

from DIRAC import S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.SQLAlchemyDB import SQLAlchemyDB
from DIRAC.ResourceStatusSystem.Utilities import Utils


TABLESLIST = ["SiteStatus", "ResourceStatus", "NodeStatus"]

TABLESLISTWITHID = [
    "ResourceStatusCache",
    "SiteLog",
    "SiteHistory",
    "ResourceLog",
    "ResourceHistory",
    "NodeLog",
    "NodeHistory",
]


# Defining the tables

rssBase = declarative_base()


class ResourceStatusCache(rssBase):
    """
    Table for EmailAction
    """

    __tablename__ = "ResourceStatusCache"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}

    id = Column("ID", BigInteger, nullable=False, autoincrement=True, primary_key=True)
    sitename = Column("SiteName", String(64), nullable=False)
    name = Column("ResourceName", String(64), nullable=False)
    status = Column("Status", String(8), nullable=False, server_default="")
    previousstatus = Column("PreviousStatus", String(8), nullable=False, server_default="")
    statustype = Column("StatusType", String(128), nullable=False, server_default="all")
    time = Column("Time", DateTime, nullable=False, server_default="9999-12-31 23:59:59")

    def fromDict(self, dictionary):
        """
        Fill the fields of the AccountingCache object from a dictionary

        :param dictionary: Dictionary to fill a single line
        :type arguments: dict
        """

        self.id = dictionary.get("ID", self.id)
        self.name = dictionary.get("ResourceName", self.name)
        self.sitename = dictionary.get("SiteName", self.sitename)
        self.status = dictionary.get("Status", self.status)
        self.previousstatus = dictionary.get("PreviousStatus", self.previousstatus)
        self.statustype = dictionary.get("StatusType", self.statustype)
        self.time = dictionary.get("Time", datetime.datetime.utcnow())

    def toList(self):
        """
        Simply returns a list of column values
        """

        return [self.id, self.sitename, self.name, self.status, self.previousstatus, self.statustype, self.time]


class ElementStatusBase:
    """
    Prototype for tables.
    """

    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}

    name = Column("Name", String(64), nullable=False, primary_key=True)
    statustype = Column("StatusType", String(128), nullable=False, server_default="all", primary_key=True)
    vo = Column("VO", String(64), nullable=False, primary_key=True, server_default="all")
    status = Column("Status", String(8), nullable=False, server_default="")
    reason = Column("Reason", String(512), nullable=False, server_default="Unspecified")
    dateeffective = Column("DateEffective", DateTime, nullable=False)
    tokenexpiration = Column("TokenExpiration", DateTime, nullable=False, server_default="9999-12-31 23:59:59")
    elementtype = Column("ElementType", String(32), nullable=False, server_default="")
    lastchecktime = Column("LastCheckTime", DateTime, nullable=False, server_default="1000-01-01 00:00:00")
    tokenowner = Column("TokenOwner", String(16), nullable=False, server_default="rs_svc")

    columnsOrder = [
        "Name",
        "StatusType",
        "Status",
        "Reason",
        "DateEffective",
        "TokenExpiration",
        "ElementType",
        "LastCheckTime",
        "TokenOwner",
        "VO",
    ]

    def fromDict(self, dictionary):
        """
        Fill the fields of the AccountingCache object from a dictionary

        :param dictionary: Dictionary to fill a single line
        :type arguments: dict
        """

        utcnow = (
            self.lastchecktime.replace(microsecond=0)
            if self.lastchecktime
            else datetime.datetime.utcnow().replace(microsecond=0)
        )

        self.name = dictionary.get("Name", self.name)
        self.statustype = dictionary.get("StatusType", self.statustype)
        self.vo = dictionary.get("VO", self.vo)
        self.status = dictionary.get("Status", self.status)
        self.reason = dictionary.get("Reason", self.reason)
        self.dateeffective = dictionary.get("DateEffective", self.dateeffective)
        self.tokenexpiration = dictionary.get("TokenExpiration", self.tokenexpiration)
        self.elementtype = dictionary.get("ElementType", self.elementtype)
        self.lastchecktime = dictionary.get("LastCheckTime", utcnow)
        self.tokenowner = dictionary.get("TokenOwner", self.tokenowner)

        if self.dateeffective:
            self.dateeffective = self.dateeffective.replace(microsecond=0)
        if self.tokenexpiration:
            self.tokenexpiration = self.tokenexpiration.replace(microsecond=0)

    def toList(self):
        """Simply returns a list of column values"""
        return [
            self.name,
            self.statustype,
            self.status,
            self.reason,
            self.dateeffective,
            self.tokenexpiration,
            self.elementtype,
            self.lastchecktime,
            self.tokenowner,
            self.vo,
        ]


class ElementStatusBaseWithID(ElementStatusBase):
    """Prototype for tables

    This is almost the same as ElementStatusBase, with the following differences:
    - there's an autoincrement ID column which is also the primary key
    - the name and statusType components are not part of the primary key
    """

    id = Column("ID", BigInteger, nullable=False, autoincrement=True, primary_key=True)
    name = Column("Name", String(64), nullable=False)
    statustype = Column("StatusType", String(128), nullable=False, server_default="all")
    vo = Column("VO", String(64), nullable=False, server_default="all")
    status = Column("Status", String(8), nullable=False, server_default="")
    reason = Column("Reason", String(512), nullable=False, server_default="Unspecified")
    dateeffective = Column("DateEffective", DateTime, nullable=False)
    tokenexpiration = Column("TokenExpiration", DateTime, nullable=False, server_default="9999-12-31 23:59:59")
    elementtype = Column("ElementType", String(32), nullable=False, server_default="")
    lastchecktime = Column("LastCheckTime", DateTime, nullable=False, server_default="1000-01-01 00:00:00")
    tokenowner = Column("TokenOwner", String(16), nullable=False, server_default="rs_svc")

    columnsOrder = [
        "ID",
        "Name",
        "StatusType",
        "Status",
        "Reason",
        "DateEffective",
        "TokenExpiration",
        "ElementType",
        "LastCheckTime",
        "TokenOwner",
        "VO",
    ]

    def fromDict(self, dictionary):
        """
        Fill the fields of the AccountingCache object from a dictionary

        :param dictionary: Dictionary to fill a single line
        :type arguments: dict
        """

        self.id = dictionary.get("ID", self.id)
        super().fromDict(dictionary)

    def toList(self):
        """Simply returns a list of column values"""
        return [
            self.id,
            self.name,
            self.statustype,
            self.status,
            self.reason,
            self.dateeffective,
            self.tokenexpiration,
            self.elementtype,
            self.lastchecktime,
            self.tokenowner,
            self.vo,
        ]


# tables with schema defined in ElementStatusBase


class SiteStatus(ElementStatusBase, rssBase):
    """SiteStatus table"""

    __tablename__ = "SiteStatus"


class ResourceStatus(ElementStatusBase, rssBase):
    """ResourceStatusDB table"""

    __tablename__ = "ResourceStatus"


class NodeStatus(ElementStatusBase, rssBase):
    """NodeStatus table"""

    __tablename__ = "NodeStatus"


# tables with schema defined in ElementStatusBaseWithID


class SiteLog(ElementStatusBaseWithID, rssBase):
    """SiteLog table"""

    __tablename__ = "SiteLog"


class SiteHistory(ElementStatusBaseWithID, rssBase):
    """SiteHistory table"""

    __tablename__ = "SiteHistory"


class ResourceLog(ElementStatusBaseWithID, rssBase):
    """ResourceLog table"""

    __tablename__ = "ResourceLog"


class ResourceHistory(ElementStatusBaseWithID, rssBase):
    """ResourceHistory table"""

    __tablename__ = "ResourceHistory"


class NodeLog(ElementStatusBaseWithID, rssBase):
    """NodeLog table"""

    __tablename__ = "NodeLog"


class NodeHistory(ElementStatusBaseWithID, rssBase):
    """NodeHistory table"""

    __tablename__ = "NodeHistory"


# Interaction with the DB


class ResourceStatusDB(SQLAlchemyDB):
    """
    Class that defines the interactions with the tables of the ResourceStatusDB.
    """

    def __init__(self, parentLogger=None):
        """c'tor

        :param self: self reference
        """

        super().__init__(parentLogger=parentLogger)

        # These are the list of tables that will be created.
        # They can be extended in an extension module
        self.tablesList = getattr(Utils.voimport("DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"), "TABLESLIST")
        self.tablesListWithID = getattr(
            Utils.voimport("DIRAC.ResourceStatusSystem.DB.ResourceStatusDB"), "TABLESLISTWITHID"
        )

        self.extensions = gConfig.getValue("DIRAC/Extensions", [])
        self._initializeConnection("ResourceStatus/ResourceStatusDB")

        # Create required tables
        self._createTablesIfNotThere(self.tablesList)
        self._createTablesIfNotThere(self.tablesListWithID)

    def insert(self, table, params):
        """
        Inserts params in the DB.

        :param table: table where to insert
        :type table: str
        :param params: Dictionary to fill a single line
        :type params: dict

        :return: S_OK() || S_ERROR()
        """

        if not params.get("DateEffective"):
            params["DateEffective"] = datetime.datetime.utcnow().replace(microsecond=0)

        return super().insert(table, params)

    def addOrModify(self, table, params):
        """
        Using the PrimaryKeys of the table, it looks for the record in the database.
        If it is there, it is updated, if not, it is inserted as a new entry.

        :param table: table where to add or modify
        :type table: str
        :param params: dictionary of what to add or modify
        :type params: dict

        :return: S_OK() || S_ERROR()
        """

        session = self.sessionMaker_o()
        found = False
        for ext in self.extensions:
            try:
                table_c = getattr(__import__(ext + __name__, globals(), locals(), [table]), table)
                found = True
                break
            except (ImportError, AttributeError):
                continue
        # If not found in extensions, import it from DIRAC base (this same module).
        if not found:
            table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)
        primaryKeys = [key.name for key in class_mapper(table_c).primary_key]

        try:
            select = Query(table_c, session=session)
            for columnName, columnValue in params.items():
                if not columnValue or columnName not in primaryKeys:
                    continue
                column_a = getattr(table_c, columnName.lower())
                if isinstance(columnValue, (list, tuple)):
                    select = select.filter(column_a.in_(list(columnValue)))
                elif isinstance(columnValue, str):
                    select = select.filter(column_a == columnValue)
                else:
                    self.log.error("type(columnValue) == %s" % type(columnValue))

            res = select.first()  # the selection is done via primaryKeys only
            if not res:  # if not there, let's insert it (and exit)
                return self.insert(table, params)

            # From now on, we assume we need to modify

            # Treating case of time value updates
            if not params.get("LastCheckTime"):
                params["LastCheckTime"] = None
            if not params.get("DateEffective"):
                params["DateEffective"] = None

            # Should we change DateEffective?
            changeDE = False
            if params.get("Status"):
                if params.get("Status") != res.status:  # we update dateEffective iff we change the status
                    changeDE = True

            for columnName, columnValue in params.items():
                if columnName == "LastCheckTime" and not columnValue:  # we always update lastCheckTime
                    columnValue = datetime.datetime.utcnow().replace(microsecond=0)
                if changeDE and columnName == "DateEffective" and not columnValue:
                    columnValue = datetime.datetime.utcnow().replace(microsecond=0)
                if columnValue:
                    if isinstance(columnValue, datetime.datetime):
                        columnValue = columnValue.replace(microsecond=0)
                    setattr(res, columnName.lower(), columnValue)
            session.commit()

            # and since we modified, we now insert a new line in the log table
            return self.insert(table.replace("Status", "") + "Log", params)
            # The line inserted will maybe become a History line thanks to the SummarizeLogsAgent

        except exc.SQLAlchemyError as e:
            session.rollback()
            self.log.exception("addOrModify: unexpected exception", lException=e)
            return S_ERROR("addOrModify: unexpected exception %s" % e)
        finally:
            session.close()

    def addIfNotThere(self, table, params):
        """
        Using the PrimaryKeys of the table, it looks for the record in the database.
        If it is not there, it is inserted as a new entry.

        :param table: table where to add or modify
        :type table: str
        :param params: dictionary of what to add or modify
        :type params: dict

        :return: S_OK() || S_ERROR()
        """

        session = self.sessionMaker_o()
        table_c = getattr(__import__(__name__, globals(), locals(), [table]), table)
        primaryKeys = [key.name for key in class_mapper(table_c).primary_key]

        try:
            select = Query(table_c, session=session)
            for columnName, columnValue in params.items():
                if not columnValue or columnName not in primaryKeys:
                    continue
                column_a = getattr(table_c, columnName.lower())
                if isinstance(columnValue, (list, tuple)):
                    select = select.filter(column_a.in_(list(columnValue)))
                elif isinstance(columnValue, str):
                    select = select.filter(column_a == columnValue)
                else:
                    self.log.error("type(columnValue) == %s" % type(columnValue))

            res = select.first()  # the selection is done via primaryKeys only
            if not res:  # if not there, let's insert it
                return self.insert(table, params)

            session.commit()
            return S_OK()

        except exc.SQLAlchemyError as e:
            session.rollback()
            self.log.exception("addIfNotThere: unexpected exception", lException=e)
            return S_ERROR("addIfNotThere: unexpected exception %s" % e)
        finally:
            session.close()
