""" SQLAlchemyDB:
    This module provides the BaseRSSDB class for providing standard DB interactions.

    Uses sqlalchemy
"""

import datetime
from urllib import parse as urlparse
from sqlalchemy import create_engine, desc, exc
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.query import Query

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DIRACDB import DIRACDB
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.Core.Utilities.ObjectLoader import ObjectLoader


class SQLAlchemyDB(DIRACDB):
    """
    Base class that defines some of the basic DB interactions.
    """

    def __init__(self, *args, **kwargs):
        """c'tor

        :param self: self reference
        """
        self.fullname = self.__class__.__name__
        super().__init__(*args, **kwargs)

        self.extensions = gConfig.getValue("DIRAC/Extensions", [])
        self.tablesList = []

        self.objectLoader = ObjectLoader()

    def _initializeConnection(self, dbPath):
        """
        Collect from the CS all the info needed to connect to the DB.
        """

        result = getDBParameters(dbPath)
        if not result["OK"]:
            raise Exception(f"Cannot get database parameters: {result['Message']}")

        dbParameters = result["Value"]
        db_param_redacted = dbParameters.copy()
        db_param_redacted["Password"] = "**REDACTED**"
        self.log.debug(f"db parameters: {db_param_redacted}")
        self.host = dbParameters["Host"]
        self.port = dbParameters["Port"]
        self.user = dbParameters["User"]
        self.password = urlparse.quote_plus(dbParameters["Password"])
        self.dbName = dbParameters["DBName"]

        self.engine = create_engine(
            f"mysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbName}",
            pool_recycle=3600,
            echo_pool=True,
            echo=self.log.getLevel() == "DEBUG",
        )
        self.sessionMaker_o = sessionmaker(bind=self.engine)
        self.inspector = Inspector.from_engine(self.engine)

    def _createTablesIfNotThere(self, tablesList):
        """
        Adds each table in tablesList to the DB if not already present
        """
        tablesInDB = self.inspector.get_table_names()

        for table in tablesList:
            if table not in tablesInDB:
                result = self.objectLoader.loadObject(self.__class__.__module__, table)
                if not result["OK"]:
                    return result
                result["Value"].__table__.create(self.engine)
            else:
                gLogger.debug(f"Table {table} already exists")

    def insert(self, table, params):
        """
        Inserts params in the DB.

        :param str table: table where to insert
        :param dict params: Dictionary to fill a single line

        :return: S_OK() || S_ERROR()
        """

        # expire_on_commit is set to False so that we can still use the object after we close the session
        session = self.sessionMaker_o(expire_on_commit=False)  # FIXME: should we use this flag elsewhere?

        result = self.objectLoader.loadObject(self.__class__.__module__, table)
        if not result["OK"]:
            return result
        tableRow_o = result["Value"]()
        tableRow_o.fromDict(params)

        try:
            session.add(tableRow_o)
            session.commit()
            return S_OK()
        except exc.IntegrityError as err:
            self.log.warn(f"insert: trying to insert a duplicate key? {err}")
            session.rollback()
        except exc.SQLAlchemyError as e:
            session.rollback()
            self.log.exception("insert: unexpected exception", lException=e)
            return S_ERROR(f"insert: unexpected exception {e}")
        finally:
            session.close()

        return S_OK()

    def select(self, table, params):
        """
        Uses params to build conditional SQL statement ( WHERE ... ).

        :Parameters:
          **params** - `dict`
            arguments for the mysql query ( must match table columns ! ).

        :return: S_OK() || S_ERROR()
        """

        session = self.sessionMaker_o()

        result = self.objectLoader.loadObject(self.__class__.__module__, table)
        if not result["OK"]:
            return result
        table_c = result["Value"]

        # handling query conditions found in 'Meta'
        columnNames = [column.lower() for column in params.get("Meta", {}).get("columns", [])]
        older = params.get("Meta", {}).get("older", None)
        newer = params.get("Meta", {}).get("newer", None)
        order = params.get("Meta", {}).get("order", None)
        limit = params.get("Meta", {}).get("limit", None)
        params.pop("Meta", None)

        try:
            # setting up the select query
            if not columnNames:  # query on the whole table
                wholeTable = True
                try:
                    columnNames = table_c.columnsOrder  # see ResourceStatusDB for example
                except AttributeError:
                    columns = table_c.__table__.columns  # retrieve the column names
                    columnNames = [str(column).split(".")[1] for column in columns]
                select = Query(table_c, session=session)
            else:  # query only the selected columns
                wholeTable = False
                columns = [getattr(table_c, column) for column in columnNames]
                select = Query(columns, session=session)

            # query conditions
            for columnName, columnValue in params.items():
                if not columnValue:
                    continue
                column_a = getattr(table_c, columnName.lower())
                if isinstance(columnValue, (list, tuple)):
                    select = select.filter(column_a.in_(list(columnValue)))
                elif isinstance(columnValue, (str, datetime.datetime, bool)):
                    select = select.filter(column_a == columnValue)
                else:
                    self.log.error(f"type(columnValue) == {type(columnValue)}")
            if older:
                column_a = getattr(table_c, older[0].lower())
                select = select.filter(column_a < older[1])
            if newer:
                column_a = getattr(table_c, newer[0].lower())
                select = select.filter(column_a > newer[1])
            if order:
                order = [order] if isinstance(order, str) else list(order)
                column_a = getattr(table_c, order[0].lower())
                if len(order) == 2 and order[1].lower() == "desc":
                    select = select.order_by(desc(column_a))
                else:
                    select = select.order_by(column_a)
            if limit:
                select = select.limit(int(limit))

            # querying
            selectionRes = select.all()

            # handling the results
            if wholeTable:
                selectionResToList = [res.toList() for res in selectionRes]
            else:
                selectionResToList = [[getattr(res, col) for col in columnNames] for res in selectionRes]

            finalResult = S_OK(selectionResToList)

            finalResult["Columns"] = columnNames
            return finalResult

        except exc.SQLAlchemyError as e:
            session.rollback()
            self.log.exception("select: unexpected exception", lException=e)
            return S_ERROR(f"select: unexpected exception {e}")
        finally:
            session.close()

    def delete(self, table, params):
        """
        :param table: table from where to delete
        :type table: str
        :param params: dictionary of which line(s) to delete
        :type params: dict

        :return: S_OK() || S_ERROR()
        """
        session = self.sessionMaker_o()

        result = self.objectLoader.loadObject(self.__class__.__module__, table)
        if not result["OK"]:
            return result
        table_c = result["Value"]

        # handling query conditions found in 'Meta'
        older = params.get("Meta", {}).get("older", None)
        newer = params.get("Meta", {}).get("newer", None)
        order = params.get("Meta", {}).get("order", None)
        limit = params.get("Meta", {}).get("limit", None)
        params.pop("Meta", None)

        try:
            deleteQuery = Query(table_c, session=session)
            for columnName, columnValue in params.items():
                if not columnValue:
                    continue
                column_a = getattr(table_c, columnName.lower())
                if isinstance(columnValue, (list, tuple)):
                    deleteQuery = deleteQuery.filter(column_a.in_(list(columnValue)))
                elif isinstance(columnValue, (str, datetime.datetime, bool)):
                    deleteQuery = deleteQuery.filter(column_a == columnValue)
                else:
                    self.log.error(f"type(columnValue) == {type(columnValue)}")
            if older:
                column_a = getattr(table_c, older[0].lower())
                deleteQuery = deleteQuery.filter(column_a < older[1])
            if newer:
                column_a = getattr(table_c, newer[0].lower())
                deleteQuery = deleteQuery.filter(column_a > newer[1])
            if order:
                order = [order] if isinstance(order, str) else list(order)
                column_a = getattr(table_c, order[0].lower())
                if len(order) == 2 and order[1].lower() == "desc":
                    deleteQuery = deleteQuery.order_by(desc(column_a))
                else:
                    deleteQuery = deleteQuery.order_by(column_a)
            if limit:
                deleteQuery = deleteQuery.limit(int(limit))

            res = deleteQuery.delete(synchronize_session=False)  # FIXME: unsure about it
            session.commit()
            return S_OK(res)

        except exc.SQLAlchemyError as e:
            session.rollback()
            self.log.exception("delete: unexpected exception", lException=e)
            return S_ERROR(f"delete: unexpected exception {e}")
        finally:
            session.close()
