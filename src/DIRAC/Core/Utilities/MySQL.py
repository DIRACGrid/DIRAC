""" DIRAC Basic MySQL Class
    It provides access to the basic MySQL methods in a multithread-safe mode
    keeping used connections in a python Queue for further reuse.

    These are the coded methods:


    __init__( host, user, passwd, name, [maxConnsInQueue=10] )

    Initializes the Queue and tries to connect to the DB server,
    using the _connect method.
    "maxConnsInQueue" defines the size of the Queue of open connections
    that are kept for reuse. It also defined the maximum number of open
    connections available from the object.
    maxConnsInQueue = 0 means unlimited and it is not supported.


    _except( methodName, exception, errorMessage )

    Helper method for exceptions: the "methodName" and the "errorMessage"
    are printed with ERROR level, then the "exception" is printed (with
    full description if it is a MySQL Exception) and S_ERROR is returned
    with the errorMessage and the exception.


    _connect()

    Attempts connection to DB and sets the _connected flag to True upon success.
    Returns S_OK or S_ERROR.


    _query( cmd, [conn=conn] )

    Executes SQL command "cmd".
    Gets a connection from the Queue (or open a new one if none is available),
    the used connection is  back into the Queue.
    If a connection to the the DB is passed as second argument this connection
    is used and is not  in the Queue.
    Returns S_OK with fetchall() out in Value or S_ERROR upon failure.


    _update( cmd, [conn=conn] )

    Executes SQL command "cmd" and issue a commit
    Gets a connection from the Queue (or open a new one if none is available),
    the used connection is  back into the Queue.
    If a connection to the the DB is passed as second argument this connection
    is used and is not  in the Queue
    Returns S_OK with number of updated registers in Value or S_ERROR upon failure.


    _createTables( tableDict )

    Create a new Table in the DB


    _getConnection()

    Gets a connection from the Queue (or open a new one if none is available)
    Returns S_OK with connection in Value or S_ERROR
    the calling method is responsible for closing this connection once it is no
    longer needed.




    Some high level methods have been added to avoid the need to write SQL
    statement in most common cases. They should be used instead of low level
    _insert, _update methods when ever possible.

    buildCondition( self, condDict = None, older = None, newer = None,
                      timeStamp = None, orderAttribute = None, limit = False,
                      greater = None, smaller = None ):

      Build SQL condition statement from provided condDict and other extra check on
      a specified time stamp.
      The conditions dictionary specifies for each attribute one or a List of possible
      values
      greater and smaller are dictionaries in which the keys are the names of the fields,
      that are requested to be >= or < than the corresponding value.
      For compatibility with current usage it uses Exceptions to exit in case of
      invalid arguments


    insertFields( self, tableName, inFields = None, inValues = None, conn = None, inDict = None ):

      Insert a new row in "tableName" assigning the values "inValues" to the
      fields "inFields".
      Alternatively inDict can be used
      String type values will be appropriately escaped.


    updateFields( self, tableName, updateFields = None, updateValues = None,
                  condDict = None,
                  limit = False, conn = None,
                  updateDict = None,
                  older = None, newer = None,
                  timeStamp = None, orderAttribute = None ):

      Update "updateFields" from "tableName" with "updateValues".
      updateDict alternative way to provide the updateFields and updateValues
      N records can match the condition
      return S_OK( number of updated rows )
      if limit is not False, the given limit is set
      String type values will be appropriately escaped.


    deleteEntries( self, tableName,
                   condDict = None,
                   limit = False, conn = None,
                   older = None, newer = None,
                   timeStamp = None, orderAttribute = None ):

      Delete rows from "tableName" with
      N records can match the condition
      if limit is not False, the given limit is set
      String type values will be appropriately escaped, they can be single values or lists of values.


    getFields( self, tableName, outFields = None,
               condDict = None,
               limit = False, conn = None,
               older = None, newer = None,
               timeStamp = None, orderAttribute = None ):

      Select "outFields" from "tableName" with condDict
      N records can match the condition
      return S_OK( tuple(Field,Value) )
      if limit is not False, the given limit is set
      String type values will be appropriately escaped, they can be single values or lists of values.

      for compatibility with other methods condDict keyed argument is added


    getCounters( self, table, attrList, condDict = None, older = None,
                 newer = None, timeStamp = None, connection = False ):

      Count the number of records on each distinct combination of AttrList, selected
      with condition defined by condDict and time stamps


    getDistinctAttributeValues( self, table, attribute, condDict = None, older = None,
                                newer = None, timeStamp = None, connection = False ):

      Get distinct values of a table attribute under specified conditions


"""
import collections
import functools
import json
import os
import time
import threading
import MySQLdb

from DIRAC import gLogger
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Utilities.TimeUtilities import fromString
from DIRAC.Core.Utilities import DErrno

gInstancesCount = 0
MAXCONNECTRETRY = 10
RETRY_SLEEP_DURATION = 5


def _checkFields(inFields, inValues):
    """
    Helper to check match between inFields and inValues lengths
    """

    if inFields is None and inValues is None:
        return S_OK()

    try:
        assert len(inFields) == len(inValues)
    except AssertionError:
        return S_ERROR(DErrno.EMYSQL, "Mismatch between inFields and inValues.")

    return S_OK()


def _quotedList(fieldList=None):
    """
    Quote a list of MySQL Field Names with "`"
    Return a comma separated list of quoted Field Names

    To be use for Table and Field Names
    """
    if fieldList is None:
        return None
    quotedFields = []
    try:
        for field in fieldList:
            quotedFields.append(f"`{field.replace('`', '')}`")
    except Exception:
        return None
    if not quotedFields:
        return None

    return ", ".join(quotedFields)


def captureOptimizerTraces(meth):
    """If enabled, this will dump the optimizer trace for each query performed.
    Obviously, it has a performance cost...

    In order to enable the tracing, the environment variable ``DIRAC_MYSQL_OPTIMIZER_TRACES_PATH``
    should be set and point to an existing directory where the files will be stored.

    It makes sense to enable it when preparing the migration to a newer major version
    of mysql: you run your integration tests (or whever scenario you prepared) with the old version,
    then the same tests with the new version, and compare the output files.

    The file produced are called "optimizer_trace_<timestamp>_<hash>.json"
    The hash is here to minimize the risk of concurence for the same file.
    The timestamp is to maintain the execution order. For easier comparison between two executions,
    you can rename the files with a sequence number.

    .. code-block:: bash

      cd ${DIRAC_MYSQL_OPTIMIZER_TRACES_PATH}
      c=0; for i in $(ls); do newFn=$(echo $i | sed -E "s/_trace_[0-9]+.[0-9]+_(.*)/_trace_${c}_\1/g"); mv $i $newFn; c=$(( c + 1 )); done

    This tool is useful then to compare the files https://github.com/crusaderky/recursive_diff

    Note that this method is far from pretty:

    * error handling is not really done. Of course, I could add a lot of safety and try/catch and what not,
      but if you are using this, it means you reaaaally want to profile something. And in that case, you want things
      to go smoothly. And if they don't, you want to see it, and you want it to crash.
    * it mangles a bit with the connection pool to be able to capture the traces

    All the docs related to the optimizer tracing is available here https://dev.mysql.com/doc/internals/en/optimizer-tracing.html

    The generated file contains one of the following:

    * ``{"EmptyTrace": arguments}``: some method like "show tables" do not generate a trace
    * A list of dictionaries, one per trace for the specific call:

        * ``{ "Query": <query executed>, "Trace" : <optimizer analysis>}`` if all is fine
        * ``{"Error": <the error>}`` in case something goes wrong. See the lower in the code
            for the description of errors


    """

    optimizerTracingFolder = os.environ.get("DIRAC_MYSQL_OPTIMIZER_TRACES_PATH")

    @functools.wraps(meth)
    def innerMethod(self, *args, **kwargs):
        # First, get a connection to the DB, and enable the tracing
        connection = self._MySQL__connectionPool.get(self._MySQL__dbName)["Value"]
        connection.cursor().execute('SET optimizer_trace="enabled=on";')

        # We also set some options that worked for my use case.
        # you may need to tune these parameters if you have huge traces
        # or more recursive calls.
        # I doubt it though....

        connection.cursor().execute(
            "SET optimizer_trace_offset=0, optimizer_trace_limit=20, optimizer_trace_max_mem_size=131072;"
        )

        # Because we can only trace on a per session base, give the same connection object
        # to the actual method
        kwargs["conn"] = connection

        # Execute the method
        res = meth(self, *args, **kwargs)

        # Turn of the tracing
        connection.cursor().execute('SET optimizer_trace="enabled=off";')

        # Get all the traces
        cursor = connection.cursor()
        if cursor.execute("SELECT * FROM INFORMATION_SCHEMA.OPTIMIZER_TRACE;"):
            queryTraces = cursor.fetchall()
        else:
            queryTraces = ()

        # Generate a filename stored in DIRAC_MYSQL_OPTIMIZER_TRACES_PATH
        methHash = hash(f"{args},{kwargs}")
        # optimizer_trace_<timestamp>_<hash>.json
        jsonFn = os.path.join(optimizerTracingFolder, f"optimizer_trace_{time.time()}_{methHash}.json")

        with open(jsonFn, "w") as f:
            # Some calls do not generate a trace, like "show tables"
            if not queryTraces:
                json.dump({"EmptyTrace": args}, f)

            jsonTraces = []

            for trace_query, trace_analysis, trace_missingBytes, trace_privilegeError in queryTraces:
                # if trace_privilegeError is True, it's a permission error
                # https://dev.mysql.com/doc/internals/en/privilege-checking.html
                # It may particularly happen with stored procedures.
                # Although it is not really a good practice, for profiling purposes,
                # I would go with the no brainer solution: GRANT ALL ON <yourDB>.* TO 'Dirac'@'%';
                if trace_privilegeError:
                    # f.write(f"ERROR: {args}")
                    jsonTraces.append({"Error": f"PrivilegeError: {args}"})
                    continue

                # The memory is not large enough to store all the traces, so it is truncated
                # https://dev.mysql.com/doc/internals/en/tracing-memory-usage.html
                if trace_missingBytes:
                    jsonTraces.append({"Error": f"MissingBytes {trace_missingBytes}"})
                    continue

                jsonTraces.append(
                    {"Query": trace_query, "Trace": json.loads(trace_analysis) if trace_analysis else None}
                )

            json.dump(jsonTraces, f, indent=True)

        return res

    if optimizerTracingFolder:
        return innerMethod
    else:
        return meth


class ConnectionPool:
    """
    Management of connections per thread
    """

    def __init__(self, host, user, passwd, port=3306, graceTime=600):
        self.__host = host
        self.__user = user
        self.__passwd = passwd
        self.__port = port
        self.__graceTime = graceTime
        self.__spares = collections.deque()
        self.__maxSpares = 10
        self.__lastClean = 0
        self.__assigned = {}

    @property
    def __thid(self):
        return threading.current_thread()

    def __newConn(self):
        conn = MySQLdb.connect(host=self.__host, port=self.__port, user=self.__user, passwd=self.__passwd)

        self.__execute(conn, "SET AUTOCOMMIT=1")
        return conn

    def __execute(self, conn, cmd):
        cursor = conn.cursor()
        res = cursor.execute(cmd)
        conn.commit()
        cursor.close()
        return res

    def get(self, dbName, retries=10):
        retries = max(0, min(MAXCONNECTRETRY, retries))
        self.clean()
        return self.__getWithRetry(dbName, retries, retries)

    def __getWithRetry(self, dbName, totalRetries, retriesLeft):
        sleepTime = RETRY_SLEEP_DURATION * (totalRetries - retriesLeft)
        if sleepTime > 0:
            time.sleep(sleepTime)
        try:
            conn, lastName, thid = self.__innerGet()
        except MySQLdb.MySQLError as excp:
            if retriesLeft > 0:
                return self.__getWithRetry(dbName, totalRetries, retriesLeft - 1)
            return S_ERROR(DErrno.EMYSQL, f"Could not connect: {excp}")

        if not self.__ping(conn):
            try:
                self.__assigned.pop(thid)
            except KeyError:
                pass
            if retriesLeft > 0:
                return self.__getWithRetry(dbName, totalRetries, retriesLeft)
            return S_ERROR(DErrno.EMYSQL, "Could not connect")

        if lastName != dbName:
            try:
                conn.select_db(dbName)
            except MySQLdb.MySQLError as excp:
                if retriesLeft > 0:
                    return self.__getWithRetry(dbName, totalRetries, retriesLeft - 1)
                return S_ERROR(DErrno.EMYSQL, f"Could not select db {dbName}: {excp}")
            try:
                self.__assigned[thid][1] = dbName
            except KeyError:
                if retriesLeft > 0:
                    return self.__getWithRetry(dbName, totalRetries, retriesLeft - 1)
                return S_ERROR(DErrno.EMYSQL, "Could not connect")
        return S_OK(conn)

    def __ping(self, conn):
        try:
            conn.ping(True)
            return True
        except Exception:
            return False

    def __innerGet(self):
        thid = self.__thid
        now = time.time()
        if thid in self.__assigned:
            data = self.__assigned[thid]
            conn = data[0]
            data[2] = now
            return data[0], data[1], thid
        # Not cached
        try:
            conn, dbName = self.__spares.pop()
        except IndexError:
            conn = self.__newConn()
            dbName = ""

        self.__assigned[thid] = [conn, dbName, now]
        return conn, dbName, thid

    def __pop(self, thid):
        try:
            data = self.__assigned.pop(thid)
            if len(self.__spares) < self.__maxSpares:
                self.__spares.append((data[0], data[1]))
            else:
                try:
                    data[0].close()
                except MySQLdb.ProgrammingError as exc:
                    gLogger.warn(f"ProgrammingError exception while closing MySQL connection: {exc}")
                except Exception as exc:
                    gLogger.warn(f"Exception while closing MySQL connection: {exc}")
        except KeyError:
            pass

    def clean(self, now=False):
        if not now:
            now = time.time()
        self.__lastClean = now
        for thid in list(self.__assigned):
            if not thid.is_alive():
                self.__pop(thid)
            try:
                data = self.__assigned[thid]
            except KeyError:
                continue
            if now - data[2] > self.__graceTime:
                self.__pop(thid)

    def transactionStart(self, dbName):
        result = self.get(dbName)
        if not result["OK"]:
            return result
        conn = result["Value"]
        try:
            return S_OK(self.__execute(conn, "START TRANSACTION WITH CONSISTENT SNAPSHOT"))
        except MySQLdb.MySQLError as excp:
            return S_ERROR(DErrno.EMYSQL, f"Could not begin transaction: {excp}")

    def transactionCommit(self, dbName):
        result = self.get(dbName)
        if not result["OK"]:
            return result
        conn = result["Value"]
        try:
            result = self.__execute(conn, "COMMIT")
            return S_OK(result)
        except MySQLdb.MySQLError as excp:
            return S_ERROR(DErrno.EMYSQL, f"Could not commit transaction: {excp}")

    def transactionRollback(self, dbName):
        result = self.get(dbName)
        if not result["OK"]:
            return result
        conn = result["Value"]
        try:
            result = self.__execute(conn, "ROLLBACK")
            return S_OK(result)
        except MySQLdb.MySQLError as excp:
            return S_ERROR(DErrno.EMYSQL, f"Could not rollback transaction: {excp}")


class MySQL:
    """
    Basic multithreaded DIRAC MySQL Client Class
    """

    __initialized = False

    __connectionPools = {}

    def __init__(self, hostName="localhost", userName="dirac", passwd="dirac", dbName="", port=3306, debug=False):
        """
        set MySQL connection parameters and try to connect

        :param debug: unused
        """
        global gInstancesCount
        gInstancesCount += 1

        self._connected = False

        if "log" not in dir(self):
            self.log = gLogger.getSubLogger(self.__class__.__name__)
        self.logger = self.log

        # let the derived class decide what to do with if is not 1
        self._threadsafe = MySQLdb.threadsafety
        # self.log.debug('thread_safe = %s' % self._threadsafe)

        self.__hostName = str(hostName)
        self.__userName = str(userName)
        self.__passwd = str(passwd)
        self.__dbName = str(dbName)
        self.__port = port
        cKey = (self.__hostName, self.__userName, self.__passwd, self.__port)
        if cKey not in MySQL.__connectionPools:
            MySQL.__connectionPools[cKey] = ConnectionPool(*cKey)
        self.__connectionPool = MySQL.__connectionPools[cKey]

        self.__initialized = True
        result = self._connect()
        if not result["OK"]:
            gLogger.error("Cannot connect to the DB", f" {result['Message']}")

    def __del__(self):
        global gInstancesCount
        try:
            gInstancesCount -= 1
        except Exception:
            pass

    def _except(self, methodName, x, err, cmd="", print=True):
        """
        print MySQL error or exception
        return S_ERROR with Exception
        """

        try:
            raise x
        except MySQLdb.Error as e:
            if print:
                self.log.error(f"{methodName} ({self._safeCmd(cmd)}): {err}", "%d: %s" % (e.args[0], e.args[1]))
            return S_ERROR(DErrno.EMYSQL, "%s: ( %d: %s )" % (err, e.args[0], e.args[1]))
        except Exception as e:
            if print:
                self.log.error(f"{methodName} ({self._safeCmd(cmd)}): {err}", repr(e))
            return S_ERROR(DErrno.EMYSQL, f"{err}: ({repr(e)})")

    def __isDateTime(self, dateString):
        if dateString == "UTC_TIMESTAMP()":
            return True
        try:
            dtime = dateString.replace('"', "").replace("'", "")
            dtime = fromString(dtime)
            if dtime is None:
                return False
            return True
        except Exception:
            return False

    def __escapeString(self, myString, connection=None):
        """
        To be used for escaping any MySQL string before passing it to the DB
        this should prevent passing non-MySQL accepted characters to the DB
        It also includes quotation marks " around the given string
        """
        if connection is None:
            retDict = self._getConnection()
            if not retDict["OK"]:
                return retDict
            connection = retDict["Value"]

        if isinstance(myString, bytes):
            myString = myString.decode()
        try:
            myString = str(myString)
        except ValueError:
            return S_ERROR(DErrno.EMYSQL, "Cannot escape value!")

        timeUnits = ["MICROSECOND", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"]

        try:
            # Check datetime functions first
            if myString.strip() == "UTC_TIMESTAMP()":
                return S_OK(myString)

            for func in ["TIMESTAMPDIFF", "TIMESTAMPADD"]:
                if myString.strip().startswith(f"{func}(") and myString.strip().endswith(")"):
                    args = myString.strip()[:-1].replace(f"{func}(", "").strip().split(",")
                    arg1, arg2, arg3 = (x.strip() for x in args)
                    if arg1 in timeUnits:
                        if self.__isDateTime(arg2) or arg2.isalnum():
                            if self.__isDateTime(arg3) or arg3.isalnum():
                                return S_OK(myString)
                    # self.log.debug('__escape_string: Could not escape string', '"%s"' % myString)
                    return S_ERROR(DErrno.EMYSQL, "__escape_string: Could not escape string")

            escape_string = connection.escape_string(myString.encode()).decode()
            # self.log.debug('__escape_string: returns', '"%s"' % escape_string)
            return S_OK(f'"{escape_string}"')
        except Exception as x:
            return self._except("__escape_string", x, "Could not escape string", myString)

    def __checkTable(self, tableName, force=False):
        """Check if a table exists by issuing 'SHOW TABLES'

        :param str tableName: table name in check
        :param bool force: force or not the re-creation (would drop the previous one)
        :returns: S_OK/S_ERROR
        """

        table = _quotedList([tableName])
        if not table:
            return S_ERROR(DErrno.EMYSQL, "Invalid tableName argument")

        cmd = "SHOW TABLES"
        retDict = self._query(cmd)
        if not retDict["OK"]:
            return retDict
        if (tableName,) in retDict["Value"]:
            if not force:
                # the requested exist and table creation is not force, return with error
                return S_ERROR(DErrno.EMYSQL, "The requested table already exist")
            else:
                cmd = f"DROP TABLE {table}"
                retDict = self._update(cmd)
                if not retDict["OK"]:
                    return retDict

        return S_OK()

    def _escapeString(self, myString, conn=None):
        """
        Wrapper around the internal method __escapeString
        """
        # self.log.debug('_escapeString:', '"%s"' % str(myString))

        return self.__escapeString(myString)

    def _escapeValues(self, inValues=None):
        """
        Escapes all strings in the list of values provided
        """
        # self.log.debug('_escapeValues:', inValues)
        retDict = self._getConnection()
        if not retDict["OK"]:
            return retDict
        connection = retDict["Value"]

        inEscapeValues = []

        if not inValues:
            return S_OK(inEscapeValues)

        for value in inValues:
            if isinstance(value, str):
                retDict = self.__escapeString(value, connection=connection)
                if not retDict["OK"]:
                    return retDict
                inEscapeValues.append(retDict["Value"])
            elif isinstance(value, (tuple, list)):
                tupleValues = []
                for val in value:
                    retDict = self.__escapeString(val, connection=connection)
                    if not retDict["OK"]:
                        return retDict
                    tupleValues.append(retDict["Value"])
                inEscapeValues.append("(" + ", ".join(tupleValues) + ")")
            elif isinstance(value, bool):
                inEscapeValues = [str(value)]
            else:
                if isinstance(value, bytes):
                    value = value.decode()
                retDict = self.__escapeString(str(value), connection=connection)
                if not retDict["OK"]:
                    return retDict
                inEscapeValues.append(retDict["Value"])
        return S_OK(inEscapeValues)

    def _safeCmd(self, command):
        """Just replaces password, if visible, with *********"""
        return command.replace(self.__passwd, "**********")

    def _connect(self):
        """
        open connection to MySQL DB and put Connection into Queue
        set connected flag to True and return S_OK
        return S_ERROR upon failure
        """
        if not self.__initialized:
            error = "DB not properly initialized"
            gLogger.error(error)
            return S_ERROR(DErrno.EMYSQL, error)

        # self.log.debug('_connect:', self._connected)
        if self._connected:
            return S_OK()

        # Test the connection to the DB
        retDict = self._getConnection()
        if not retDict["OK"]:
            return retDict
        self._connected = True
        return S_OK()

    @captureOptimizerTraces
    def _query(self, cmd, *, conn=None, debug=True):
        """
        execute MySQL query command

        :param debug:  print or not the errors

        return S_OK structure with fetchall result as tuple
        it returns an empty tuple if no matching rows are found
        return S_ERROR upon error
        """

        self.log.debug(f"_query: {self._safeCmd(cmd)}")

        if conn:
            connection = conn
        else:
            retDict = self._getConnection()
            if not retDict["OK"]:
                return retDict
            connection = retDict["Value"]

        try:
            cursor = connection.cursor()
            if cursor.execute(cmd):
                res = cursor.fetchall()
            else:
                res = ()

            # Log the result limiting it to just 10 records
            # if len(res) <= 10:
            #  self.log.debug('_query: returns', res)
            # else:
            #  self.log.debug('_query: Total %d records returned' % len(res))
            #  self.log.debug('_query: %s ...' % str(res[:10]))

            retDict = S_OK(res)
        except Exception as x:
            # self.log.debug('_query: %s' % self._safeCmd(cmd))
            retDict = self._except("_query", x, "Execution failed.", cmd, debug)

        try:
            cursor.close()
        except Exception:
            pass

        return retDict

    @captureOptimizerTraces
    def _update(self, cmd, *, conn=None, debug=True):
        """execute MySQL update command

        :param debug: print or not the errors

        return S_OK with number of updated registers upon success
        return S_ERROR upon error
        """

        self.log.debug(f"_update: {self._safeCmd(cmd)}")
        if conn:
            connection = conn
        else:
            retDict = self._getConnection()
            if not retDict["OK"]:
                return retDict
            connection = retDict["Value"]

        try:
            cursor = connection.cursor()
            res = cursor.execute(cmd)
            retDict = S_OK(res)
            if cursor.lastrowid:
                retDict["lastRowId"] = cursor.lastrowid
        except Exception as x:
            retDict = self._except("_update", x, "Execution failed.", cmd, debug)

        try:
            cursor.close()
        except Exception:
            pass

        return retDict

    def _transaction(self, cmdList, conn=None):
        """dummy transaction support

        :param self: self reference
        :param list cmdList: list of queries to be executed within the transaction
        :param MySQLDB.Connection conn: connection

        :return: S_OK( [ ( cmd1, ret1 ), ... ] ) or S_ERROR
        """
        if not isinstance(cmdList, list):
            return S_ERROR(DErrno.EMYSQL, f"_transaction: wrong type ({type(cmdList)}) for cmdList")

        # # get connection
        connection = conn
        if not connection:
            retDict = self._getConnection()
            if not retDict["OK"]:
                return retDict
            connection = retDict["Value"]

        # # list with cmds and their results
        cmdRet = []
        try:
            cursor = connection.cursor()
            for cmd in cmdList:
                cmdRet.append((cmd, cursor.execute(cmd)))
            connection.commit()
        except Exception as error:
            self.logger.exception(error)
            # # rollback, put back connection to the pool
            connection.rollback()
            return S_ERROR(DErrno.EMYSQL, error)
        # # close cursor, put back connection to the pool
        cursor.close()
        return S_OK(cmdRet)

    def _createViews(self, viewsDict, force=False):
        """create view based on query

        :param dict viewDict: { 'ViewName': "Fields" : { "`a`": `tblA.a`, "`sumB`" : "SUM(`tblB.b`)" }
                                            "SelectFrom" : "tblA join tblB on tblA.id = tblB.id",
                                            "Clauses" : [ "`tblA.a` > 10", "`tblB.Status` = 'foo'" ] ## WILL USE AND CLAUSE
                                            "GroupBy": [ "`a`" ],
                                            "OrderBy": [ "`b` DESC" ] }
        """
        if force:
            # gLogger.debug(viewsDict)

            for viewName, viewDict in viewsDict.items():
                viewQuery = [f"CREATE OR REPLACE VIEW `{self.__dbName}`.`{viewName}` AS"]

                columns = ",".join([f"{colDef} AS {colName}" for colName, colDef in viewDict.get("Fields", {}).items()])
                tables = viewDict.get("SelectFrom", "")
                if columns and tables:
                    viewQuery.append(f"SELECT {columns} FROM {tables}")

                where = " AND ".join(viewDict.get("Clauses", []))
                if where:
                    viewQuery.append(f"WHERE {where}")

                groupBy = ",".join(viewDict.get("GroupBy", []))
                if groupBy:
                    viewQuery.append(f"GROUP BY {groupBy}")

                orderBy = ",".join(viewDict.get("OrderBy", []))
                if orderBy:
                    viewQuery.append(f"ORDER BY {orderBy}")

                viewQuery.append(";")
                viewQuery = " ".join(viewQuery)
                # self.log.debug("`%s` VIEW QUERY IS: %s" % (viewName, viewQuery))
                createView = self._query(viewQuery)
                if not createView["OK"]:
                    self.log.error("Can not create view", createView["Message"])
                    return createView
        return S_OK()

    def _createTables(self, tableDict, force=False):
        """
        tableDict:
          tableName: { 'Fields' : { 'Field': 'Description' },
                       'ForeignKeys': {'Field': 'Table.key' },
                       'PrimaryKey': 'Id',
                       'Indexes': { 'Index': [] },
                       'UniqueIndexes': { 'Index': [] },
                       'Engine': 'InnoDB' }
          only 'Fields' is a mandatory key.

        Creates a new Table for each key in tableDict, "tableName" in the DB with
        the provided description.
        It allows to create:
          - flat tables if no "ForeignKeys" key defined.
          - tables with foreign keys to auxiliary tables holding the values
          of some of the fields
        Arguments:
          tableDict: dictionary of dictionary with description of tables to be created.
          Only "Fields" is a mandatory key in the table description.
            "Fields": Dictionary with Field names and description of the fields
            "ForeignKeys": Dictionary with Field names and name of auxiliary tables.
              The auxiliary tables must be defined in tableDict.
            "PrimaryKey": Name of PRIMARY KEY for the table (if exist).
            "Indexes": Dictionary with definition of indexes, the value for each
              index is the list of fields to be indexed.
            "UniqueIndexes": Dictionary with definition of indexes, the value for each
              index is the list of fields to be indexed. This indexes will declared
              unique.
            "Engine": use the given DB engine, InnoDB is the default if not present.
            "Charset": use the given character set. Default is latin1
          force:
            if True, requested tables are DROP if they exist.
            if False, returned with S_ERROR if table exist.

        """

        # First check consistency of request
        if not isinstance(tableDict, dict):
            return S_ERROR(DErrno.EMYSQL, f"Argument is not a dictionary: {type(tableDict)}( {tableDict} )")

        tableList = list(tableDict)
        if len(tableList) == 0:
            return S_OK(0)
        for table in tableList:
            thisTable = tableDict[table]
            # Check if Table is properly described with a dictionary
            if not isinstance(thisTable, dict):
                return S_ERROR(
                    DErrno.EMYSQL, f"Table description is not a dictionary: {type(thisTable)}( {thisTable} )"
                )
            if "Fields" not in thisTable:
                return S_ERROR(DErrno.EMYSQL, f"Missing `Fields` key in `{table}` table dictionary")

        tableCreationList = [[]]

        auxiliaryTableList = []

        i = 0
        extracted = True
        while tableList and extracted:
            # iterate extracting tables from list if they only depend on
            # already extracted tables.
            extracted = False
            auxiliaryTableList += tableCreationList[i]
            i += 1
            tableCreationList.append([])
            for table in list(tableList):
                toBeExtracted = True
                thisTable = tableDict[table]
                if "ForeignKeys" in thisTable:
                    thisKeys = thisTable["ForeignKeys"]
                    for key, auxTable in thisKeys.items():
                        forTable = auxTable.split(".")[0]
                        forKey = key
                        if forTable != auxTable:
                            forKey = auxTable.split(".")[1]
                        if forTable not in auxiliaryTableList:
                            toBeExtracted = False
                            break
                        if key not in thisTable["Fields"]:
                            return S_ERROR(
                                DErrno.EMYSQL,
                                f"ForeignKey `{key}` -> `{forKey}` not defined in Primary table `{table}`.",
                            )
                        if forKey not in tableDict[forTable]["Fields"]:
                            return S_ERROR(
                                DErrno.EMYSQL,
                                "ForeignKey `%s` -> `%s` not defined in Auxiliary table `%s`."
                                % (key, forKey, forTable),
                            )

                if toBeExtracted:
                    # self.log.debug('Table %s ready to be created' % table)
                    extracted = True
                    tableList.remove(table)
                    tableCreationList[i].append(table)

        if tableList:
            return S_ERROR(DErrno.EMYSQL, f"Recursive Foreign Keys in {', '.join(tableList)}")

        for tableList in tableCreationList:
            for table in tableList:
                # Check if Table exist
                retDict = self.__checkTable(table, force=force)
                if not retDict["OK"]:
                    return retDict

                thisTable = tableDict[table]
                cmdList = []
                for field in thisTable["Fields"].keys():
                    cmdList.append(f"`{field}` {thisTable['Fields'][field]}")

                if "PrimaryKey" in thisTable:
                    if isinstance(thisTable["PrimaryKey"], str):
                        cmdList.append(f"PRIMARY KEY ( `{thisTable['PrimaryKey']}` )")
                    else:
                        cmdList.append(
                            "PRIMARY KEY ( %s )" % ", ".join(["`%s`" % str(f) for f in thisTable["PrimaryKey"]])
                        )

                if "Indexes" in thisTable:
                    indexDict = thisTable["Indexes"]
                    for index in indexDict:
                        indexedFields = "`, `".join(indexDict[index])
                        cmdList.append(f"INDEX `{index}` ( `{indexedFields}` )")

                if "UniqueIndexes" in thisTable:
                    indexDict = thisTable["UniqueIndexes"]
                    for index in indexDict:
                        indexedFields = "`, `".join(indexDict[index])
                        cmdList.append(f"UNIQUE INDEX `{index}` ( `{indexedFields}` )")
                if "ForeignKeys" in thisTable:
                    thisKeys = thisTable["ForeignKeys"]
                    for key, auxTable in thisKeys.items():
                        forTable = auxTable.split(".")[0]
                        forKey = key
                        if forTable != auxTable:
                            forKey = auxTable.split(".")[1]

                        # cmdList.append( '`%s` %s' % ( forTable, tableDict[forTable]['Fields'][forKey] )
                        cmdList.append(
                            "FOREIGN KEY ( `%s` ) REFERENCES `%s` ( `%s` )"
                            " ON DELETE RESTRICT" % (key, forTable, forKey)
                        )

                engine = thisTable.get("Engine", "InnoDB")
                charset = thisTable.get("Charset", "latin1")

                cmd = "CREATE TABLE `{}` (\n{}\n) ENGINE={} DEFAULT CHARSET={}".format(
                    table,
                    ",\n".join(cmdList),
                    engine,
                    charset,
                )
                retDict = self._transaction([cmd])
                if not retDict["OK"]:
                    return retDict
                # self.log.debug('Table %s created' % table)

        return S_OK()

    def _to_value(self, param):
        """
        Convert to string
        """
        return str(param[0])

    def _getConnection(self, retries=MAXCONNECTRETRY):
        """Return  a new connection to the DB,

        Try the Queue, if it is empty add a newConnection to the Queue and retry
        it will retry MAXCONNECTRETRY to open a new connection and will return
        an error if it fails.

        :param int retries: Number of time it will retry to open a connection
        """
        # self.log.debug('_getConnection:')

        if not self.__initialized:
            error = "DB not properly initialized"
            gLogger.error(error)
            return S_ERROR(DErrno.EMYSQL, error)

        return self.__connectionPool.get(self.__dbName, retries)

    ########################################################################################
    #
    #  Transaction functions
    #
    ########################################################################################

    def transactionStart(self):
        return self.__connectionPool.transactionStart(self.__dbName)

    def transactionCommit(self):
        return self.__connectionPool.transactionCommit(self.__dbName)

    def transactionRollback(self):
        return self.__connectionPool.transactionRollback(self.__dbName)

    ########################################################################################
    #
    #  Utility functions
    #
    ########################################################################################

    def countEntries(
        self, table, condDict, older=None, newer=None, timeStamp=None, connection=False, greater=None, smaller=None
    ):
        """
        Count the number of entries wit the given conditions
        """
        table = _quotedList([table])
        if not table:
            error = "Invalid table argument"
            # self.log.debug('countEntries:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        try:
            cond = self.buildCondition(
                condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, greater=greater, smaller=smaller
            )
        except Exception as x:
            return S_ERROR(DErrno.EMYSQL, x)

        cmd = f"SELECT COUNT(*) FROM {table} {cond}"
        res = self._query(cmd, conn=connection)
        if not res["OK"]:
            return res

        return S_OK(res["Value"][0][0])

    ########################################################################################
    def getCounters(
        self,
        table,
        attrList,
        condDict,
        older=None,
        newer=None,
        timeStamp=None,
        connection=False,
        greater=None,
        smaller=None,
    ):
        """
        Count the number of records on each distinct combination of AttrList, selected
        with condition defined by condDict and time stamps
        """
        table = _quotedList([table])
        if not table:
            error = "Invalid table argument"
            # self.log.debug('getCounters:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        attrNames = _quotedList(attrList)
        if attrNames is None:
            error = "Invalid updateFields argument"
            # self.log.debug('getCounters:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        try:
            cond = self.buildCondition(
                condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, greater=greater, smaller=smaller
            )
        except Exception as x:
            return S_ERROR(DErrno.EMYSQL, x)

        cmd = f"SELECT {attrNames}, COUNT(*) FROM {table} {cond} GROUP BY {attrNames} ORDER BY {attrNames}"
        res = self._query(cmd, conn=connection)
        if not res["OK"]:
            return res

        resultList = []
        for raw in res["Value"]:
            attrDict = {}
            for ind, attr in enumerate(attrList):
                attrDict[attr] = raw[ind]
            item = (attrDict, raw[len(attrList)])
            resultList.append(item)
        return S_OK(resultList)

    #########################################################################################
    def getDistinctAttributeValues(
        self,
        table,
        attribute,
        condDict=None,
        older=None,
        newer=None,
        timeStamp=None,
        connection=False,
        greater=None,
        smaller=None,
    ):
        """
        Get distinct values of a table attribute under specified conditions
        """
        table = _quotedList([table])
        if not table:
            error = "Invalid table argument"
            # self.log.debug('getDistinctAttributeValues:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        attributeName = _quotedList([attribute])
        if not attributeName:
            error = "Invalid attribute argument"
            # self.log.debug('getDistinctAttributeValues:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        try:
            cond = self.buildCondition(
                condDict=condDict, older=older, newer=newer, timeStamp=timeStamp, greater=greater, smaller=smaller
            )
        except Exception as exc:
            return S_ERROR(DErrno.EMYSQL, exc)

        cmd = f"SELECT DISTINCT( {attributeName} ) FROM {table} {cond} ORDER BY {attributeName}"
        res = self._query(cmd, conn=connection)
        if not res["OK"]:
            return res
        attr_list = [x[0] for x in res["Value"]]
        return S_OK(attr_list)

    #############################################################################
    def buildCondition(
        self,
        condDict=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        limit=False,
        greater=None,
        smaller=None,
        offset=None,
        useLikeQuery=False,
    ):
        """Build SQL condition statement from provided condDict and other extra check on
        a specified time stamp.
        The conditions dictionary specifies for each attribute one or a List of possible
        values
        greater and smaller are dictionaries in which the keys are the names of the fields,
        that are requested to be >= or < than the corresponding value.
        For compatibility with current usage it uses Exceptions to exit in case of
        invalid arguments
        For performing LIKE queries use the parameter useLikeQuery=True
        """
        condition = ""
        conjunction = "WHERE"

        if condDict is not None:
            for aName, attrValue in condDict.items():
                if isinstance(aName, str):
                    attrName = _quotedList([aName])
                elif isinstance(aName, tuple):
                    attrName = "(" + _quotedList(list(aName)) + ")"
                if not attrName:
                    error = "Invalid condDict argument"
                    # self.log.debug('buildCondition:', error)
                    raise Exception(error)
                if isinstance(attrValue, list):
                    retDict = self._escapeValues(attrValue)
                    if not retDict["OK"]:
                        # self.log.debug('buildCondition:', retDict['Message'])
                        raise Exception(retDict["Message"])
                    else:
                        escapeInValues = retDict["Value"]
                        multiValue = ", ".join(escapeInValues)
                        condition = f" {condition} {conjunction} {attrName} IN ( {multiValue} )"
                        conjunction = "AND"
                else:
                    retDict = self._escapeValues([attrValue])
                    if not retDict["OK"]:
                        # self.log.debug('buildCondition:', retDict['Message'])
                        raise Exception(retDict["Message"])
                    else:
                        escapeInValue = retDict["Value"][0]
                        if useLikeQuery:
                            condition = f" {condition} {conjunction} {attrName} LIKE {escapeInValue}"
                        else:
                            if escapeInValue == "NULL":
                                condition = f" {condition} {conjunction} {attrName} IS NULL"
                            else:
                                condition = f" {condition} {conjunction} {attrName} = {escapeInValue}"
                        conjunction = "AND"

        if timeStamp:
            timeStamp = _quotedList([timeStamp])
            if not timeStamp:
                error = "Invalid timeStamp argument"
                # self.log.debug('buildCondition:', error)
                raise Exception(error)
            if newer:
                retDict = self._escapeValues([newer])
                if not retDict["OK"]:
                    # self.log.debug('buildCondition:', retDict['Message'])
                    raise Exception(retDict["Message"])
                else:
                    escapeInValue = retDict["Value"][0]
                    condition = f" {condition} {conjunction} {timeStamp} >= {escapeInValue}"
                    conjunction = "AND"
            if older:
                retDict = self._escapeValues([older])
                if not retDict["OK"]:
                    # self.log.debug('buildCondition:', retDict['Message'])
                    raise Exception(retDict["Message"])
                else:
                    escapeInValue = retDict["Value"][0]
                    condition = f" {condition} {conjunction} {timeStamp} < {escapeInValue}"

        if isinstance(greater, dict):
            for attrName, attrValue in greater.items():
                attrName = _quotedList([attrName])
                if not attrName:
                    error = "Invalid greater argument"
                    # self.log.debug('buildCondition:', error)
                    raise Exception(error)

                retDict = self._escapeValues([attrValue])
                if not retDict["OK"]:
                    # self.log.debug('buildCondition:', retDict['Message'])
                    raise Exception(retDict["Message"])
                else:
                    escapeInValue = retDict["Value"][0]
                    condition = f" {condition} {conjunction} {attrName} >= {escapeInValue}"
                    conjunction = "AND"

        if isinstance(smaller, dict):
            for attrName, attrValue in smaller.items():
                attrName = _quotedList([attrName])
                if not attrName:
                    error = "Invalid smaller argument"
                    # self.log.debug('buildCondition:', error)
                    raise Exception(error)

                retDict = self._escapeValues([attrValue])
                if not retDict["OK"]:
                    # self.log.debug('buildCondition:', retDict['Message'])
                    raise Exception(retDict["Message"])
                else:
                    escapeInValue = retDict["Value"][0]
                    condition = f" {condition} {conjunction} {attrName} < {escapeInValue}"
                    conjunction = "AND"

        orderList = []
        orderAttrList = orderAttribute
        if not isinstance(orderAttrList, list):
            orderAttrList = [orderAttribute]
        for orderAttr in orderAttrList:
            if orderAttr is None:
                continue
            if not isinstance(orderAttr, str):
                error = "Invalid orderAttribute argument"
                # self.log.debug('buildCondition:', error)
                raise Exception(error)

            orderField = _quotedList(orderAttr.split(":")[:1])
            if not orderField:
                error = "Invalid orderAttribute argument"
                # self.log.debug('buildCondition:', error)
                raise Exception(error)

            if len(orderAttr.split(":")) == 2:
                orderType = orderAttr.split(":")[1].upper()
                if orderType in ["ASC", "DESC"]:
                    orderList.append(f"{orderField} {orderType}")
                else:
                    error = "Invalid orderAttribute argument"
                    # self.log.debug('buildCondition:', error)
                    raise Exception(error)
            else:
                orderList.append(orderAttr)

        if orderList:
            condition = f"{condition} ORDER BY {', '.join(orderList)}"

        if limit:
            if offset:
                condition = "%s LIMIT %d OFFSET %d" % (condition, limit, offset)
            else:
                condition = "%s LIMIT %d" % (condition, limit)

        return condition

    #############################################################################
    def getFields(
        self,
        tableName,
        outFields=None,
        condDict=None,
        limit=False,
        conn=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        greater=None,
        smaller=None,
        useLikeQuery=False,
    ):
        """
        Select "outFields" from "tableName" with condDict
        N records can match the condition
        return S_OK(tuple(Field, Value))
        if outFields is None all fields in "tableName" are returned
        if limit is not False, the given limit is set
        inValues are properly escaped using the _escape_string method, they can be single values or lists of values.
        if useLikeQuery=True, then conDict can return matched rows if "%" is defined inside conDict.
        """
        table = _quotedList([tableName])
        if not table:
            error = "Invalid tableName argument"
            # self.log.debug('getFields:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        quotedOutFields = "*"
        if outFields:
            quotedOutFields = _quotedList(outFields)
            if quotedOutFields is None:
                error = "Invalid outFields arguments"
                # self.log.debug('getFields:', error)
                return S_ERROR(DErrno.EMYSQL, error)

        # self.log.debug('getFields:', 'selecting fields %s from table %s.' % (quotedOutFields, table))

        if condDict is None:
            condDict = {}

        try:
            try:
                mylimit = limit[0]
                myoffset = limit[1]
            except TypeError:
                mylimit = limit
                myoffset = None
            condition = self.buildCondition(
                condDict=condDict,
                older=older,
                newer=newer,
                timeStamp=timeStamp,
                orderAttribute=orderAttribute,
                limit=mylimit,
                greater=greater,
                smaller=smaller,
                offset=myoffset,
                useLikeQuery=useLikeQuery,
            )
        except Exception as x:
            return S_ERROR(DErrno.EMYSQL, x)

        return self._query(f"SELECT {quotedOutFields} FROM {table} {condition}", conn=conn)

    #############################################################################
    def deleteEntries(
        self,
        tableName,
        condDict=None,
        limit=False,
        conn=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        greater=None,
        smaller=None,
    ):
        """
        Delete rows from "tableName" with
        N records can match the condition
        if limit is not False, the given limit is set
        String type values will be appropriately escaped, they can be single values or lists of values.
        """
        table = _quotedList([tableName])
        if not table:
            error = "Invalid tableName argument"
            # self.log.debug('deleteEntries:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        # self.log.debug('deleteEntries:', 'deleting rows from table %s.' % table)

        try:
            condition = self.buildCondition(
                condDict=condDict,
                older=older,
                newer=newer,
                timeStamp=timeStamp,
                orderAttribute=orderAttribute,
                limit=limit,
                greater=greater,
                smaller=smaller,
            )
        except Exception as x:
            return S_ERROR(DErrno.EMYSQL, x)

        return self._update(f"DELETE FROM {table} {condition}", conn=conn)

    #############################################################################
    def updateFields(
        self,
        tableName,
        updateFields=None,
        updateValues=None,
        condDict=None,
        limit=False,
        conn=None,
        updateDict=None,
        older=None,
        newer=None,
        timeStamp=None,
        orderAttribute=None,
        greater=None,
        smaller=None,
    ):
        """
        Update "updateFields" from "tableName" with "updateValues".
        updateDict alternative way to provide the updateFields and updateValues
        N records can match the condition
        return S_OK( number of updated rows )
        if limit is not False, the given limit is set
        String type values will be appropriately escaped.

        """
        if not updateFields and not updateDict:
            return S_OK(0)

        table = _quotedList([tableName])
        if not table:
            error = "Invalid tableName argument"
            # self.log.debug('updateFields:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        retDict = _checkFields(updateFields, updateValues)
        if not retDict["OK"]:
            error = "Mismatch between updateFields and updateValues."
            # self.log.debug('updateFields:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        if updateFields is None:
            updateFields = []
            updateValues = []

        if updateDict:
            if not isinstance(updateDict, dict):
                error = "updateDict must be a of Type DictType"
                # self.log.debug('updateFields:', error)
                return S_ERROR(DErrno.EMYSQL, error)
            try:
                updateFields += list(updateDict)
                updateValues += [updateDict[k] for k in updateFields]
            except TypeError:
                error = "updateFields and updateValues must be a list"
                # self.log.debug('updateFields:', error)
                return S_ERROR(DErrno.EMYSQL, error)

        updateValues = self._escapeValues(updateValues)
        if not updateValues["OK"]:
            # self.log.debug('updateFields:', updateValues['Message'])
            return updateValues
        updateValues = updateValues["Value"]

        # self.log.debug('updateFields:', 'updating fields %s from table %s.' % (', '.join(updateFields), table))

        try:
            condition = self.buildCondition(
                condDict=condDict,
                older=older,
                newer=newer,
                timeStamp=timeStamp,
                orderAttribute=orderAttribute,
                limit=limit,
                greater=greater,
                smaller=smaller,
            )
        except Exception as x:
            return S_ERROR(DErrno.EMYSQL, x)

        updateString = ",".join(
            [f"{_quotedList([updateFields[k]])} = {updateValues[k]}" for k in range(len(updateFields))]
        )

        return self._update(f"UPDATE {table} SET {updateString} {condition}", conn=conn)

    #############################################################################
    def insertFields(self, tableName, inFields=None, inValues=None, conn=None, inDict=None):
        """
        Insert a new row in "tableName" assigning the values "inValues" to the
        fields "inFields".
        String type values will be appropriately escaped.
        """
        table = _quotedList([tableName])
        if not table:
            error = "Invalid tableName argument"
            # self.log.debug('insertFields:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        retDict = _checkFields(inFields, inValues)
        if not retDict["OK"]:
            # self.log.debug('insertFields:', retDict['Message'])
            return retDict

        if inFields is None:
            inFields = []
            inValues = []

        if inDict:
            if not isinstance(inDict, dict):
                error = "inDict must be a of Type DictType"
                # self.log.debug('insertFields:', error)
                return S_ERROR(DErrno.EMYSQL, error)
            try:
                inFields += list(inDict)
                inValues += [inDict[k] for k in inFields]
            except TypeError:
                error = "inFields and inValues must be a list"
                # self.log.debug('insertFields:', error)
                return S_ERROR(DErrno.EMYSQL, error)

        inFieldString = _quotedList(inFields)
        if inFieldString is None:
            error = "Invalid inFields arguments"
            # self.log.debug('insertFields:', error)
            return S_ERROR(DErrno.EMYSQL, error)

        inFieldString = f"(  {inFieldString} )"

        retDict = self._escapeValues(inValues)
        if not retDict["OK"]:
            # self.log.debug('insertFields:', retDict['Message'])
            return retDict
        inValueString = ", ".join(retDict["Value"])
        inValueString = f"(  {inValueString} )"

        # self.log.debug('insertFields:', 'inserting %s into table %s'
        #               % (inFieldString, table))

        return self._update(f"INSERT INTO {table} {inFieldString} VALUES {inValueString}", conn=conn)

    @captureOptimizerTraces
    def executeStoredProcedure(self, packageName, parameters, outputIds, *, conn=None):
        if conn:
            connection = conn
        else:
            conDict = self._getConnection()
            if not conDict["OK"]:
                return conDict

            connection = conDict["Value"]
        cursor = connection.cursor()
        try:
            cursor.callproc(packageName, parameters)
            row = []
            for oId in outputIds:
                resName = f"@_{packageName}_{oId}"
                cursor.execute(f"SELECT {resName}")
                row.append(cursor.fetchone()[0])
            retDict = S_OK(row)
        except Exception as x:
            retDict = self._except("_query", x, "Execution failed.", packageName)
            connection.rollback()

        try:
            cursor.close()
        except Exception:
            pass
        return retDict

    # For the procedures that execute a select without storing the result
    @captureOptimizerTraces
    def executeStoredProcedureWithCursor(self, packageName, parameters, *, conn=None):
        if conn:
            connection = conn
        else:
            conDict = self._getConnection()
            if not conDict["OK"]:
                return conDict

            connection = conDict["Value"]

        cursor = connection.cursor()
        try:
            #       execStr = "call %s(%s);" % ( packageName, ",".join( map( str, parameters ) ) )
            execStr = "call {}({});".format(
                packageName,
                ",".join(['"%s"' % param if isinstance(param, str) else str(param) for param in parameters]),
            )
            cursor.execute(execStr)
            rows = cursor.fetchall()
            retDict = S_OK(rows)
        except Exception as x:
            retDict = self._except("_query", x, "Execution failed.", packageName)
            connection.rollback()
        try:
            cursor.close()
        except Exception:
            pass

        return retDict
