# We disable pylint no-callable because of https://github.com/PyCQA/pylint/issues/8138

""" Frontend for ReqDB

    :mod: RequestDB

    =======================

    .. module: RequestDB

    :synopsis: db holding Requests

    db holding Request, Operation and File
"""
import datetime
import errno
import random

from urllib.parse import quote_plus

from sqlalchemy import (
    TEXT,
    BigInteger,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    distinct,
    func,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import backref, joinedload, registry, relationship, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import update

# # from DIRAC
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.RequestManagementSystem.Client.File import File
from DIRAC.RequestManagementSystem.Client.Operation import Operation
from DIRAC.RequestManagementSystem.Client.Request import Request

# FIXME: here for backward compatibility with 8.0 requests
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getDNForUsername

# Metadata instance that is used to bind the engine, Object and tables
metadata = MetaData()
mapper_registry = registry()

# Description of the file table

fileTable = Table(
    "File",
    metadata,
    Column("FileID", Integer, primary_key=True),
    Column("OperationID", Integer, ForeignKey("Operation.OperationID", ondelete="CASCADE"), nullable=False),
    Column("Status", Enum("Waiting", "Done", "Failed", "Scheduled"), server_default="Waiting"),
    Column("LFN", String(255), index=True),
    Column("PFN", String(255)),
    Column("ChecksumType", Enum("ADLER32", "MD5", "SHA1", ""), server_default=""),
    Column("Checksum", String(255)),
    Column("GUID", String(36)),
    Column("Size", BigInteger),
    Column("Attempt", Integer),
    Column("Error", String(255)),
    mysql_engine="InnoDB",
)

# Map the File object to the fileTable, with a few special attributes

mapper_registry.map_imperatively(
    File,
    fileTable,
    properties={
        "_Status": fileTable.c.Status,
        "_LFN": fileTable.c.LFN,
        "_ChecksumType": fileTable.c.ChecksumType,
        "_GUID": fileTable.c.GUID,
    },
)


# Description of the Operation table

operationTable = Table(
    "Operation",
    metadata,
    Column("TargetSE", String(255)),
    Column("CreationTime", DateTime),
    Column("SourceSE", String(255)),
    Column("Arguments", TEXT),
    Column("Error", String(255)),
    Column("Type", String(64), nullable=False),
    Column("Order", Integer, nullable=False),
    Column(
        "Status",
        Enum("Waiting", "Assigned", "Queued", "Done", "Failed", "Canceled", "Scheduled"),
        server_default="Queued",
    ),
    Column("LastUpdate", DateTime),
    Column("SubmitTime", DateTime),
    Column("Catalog", String(255)),
    Column("OperationID", Integer, primary_key=True),
    Column("RequestID", Integer, ForeignKey("Request.RequestID", ondelete="CASCADE"), nullable=False),
    mysql_engine="InnoDB",
)


# Map the Operation object to the operationTable, with a few special attributes

mapper_registry.map_imperatively(
    Operation,
    operationTable,
    properties={
        "_CreationTime": operationTable.c.CreationTime,
        "_Arguments": operationTable.c.Arguments,
        "_Order": operationTable.c.Order,
        "_Status": operationTable.c.Status,
        "_LastUpdate": operationTable.c.LastUpdate,
        "_SubmitTime": operationTable.c.SubmitTime,
        "_Catalog": operationTable.c.Catalog,
        "__files__": relationship(
            File,
            backref=backref("_parent", lazy="immediate"),
            lazy="immediate",
            passive_deletes=True,
            cascade="all, delete-orphan",
        ),
    },
)


# Description of the Request Table

requestTable = Table(
    "Request",
    metadata,
    Column("CreationTime", DateTime),
    Column("JobID", Integer, server_default="0"),
    Column("Owner", String(255)),
    Column("OwnerDN", String(255)),  # FIXME: here for backward compatibility with 8.0 clients
    Column("RequestName", String(255), nullable=False),
    Column("Error", String(255)),
    Column("Status", Enum("Waiting", "Assigned", "Done", "Failed", "Canceled", "Scheduled"), server_default="Waiting"),
    Column("LastUpdate", DateTime),
    Column("OwnerGroup", String(32)),
    Column("SubmitTime", DateTime),
    Column("RequestID", Integer, primary_key=True),
    Column("SourceComponent", String(255)),
    Column("NotBefore", DateTime),
    mysql_engine="InnoDB",
)

# Map the Request object to the requestTable, with a few special attributes
mapper_registry.map_imperatively(
    Request,
    requestTable,
    properties={
        "_CreationTime": requestTable.c.CreationTime,
        "_SourceComponent": requestTable.c.SourceComponent,
        "_Status": requestTable.c.Status,
        "_LastUpdate": requestTable.c.LastUpdate,
        "_SubmitTime": requestTable.c.SubmitTime,
        "_NotBefore": requestTable.c.NotBefore,
        "__operations__": relationship(
            Operation,
            backref=backref("_parent", lazy="immediate"),
            order_by=operationTable.c.Order,
            lazy="immediate",
            passive_deletes=True,
            cascade="all, delete-orphan",
        ),
    },
)


########################################################################
class RequestDB:
    """
    .. class:: RequestDB

    db holding requests
    """

    def __getDBConnectionInfo(self, fullname):
        """Collect from the CS all the info needed to connect to the DB.
        This should be in a base class eventually
        """

        result = getDBParameters(fullname)
        if not result["OK"]:
            raise Exception(f"Cannot get database parameters: {result['Message']}")

        dbParameters = result["Value"]
        self.dbHost = dbParameters["Host"]
        self.dbPort = dbParameters["Port"]
        self.dbUser = dbParameters["User"]
        self.dbPass = quote_plus(dbParameters["Password"])
        self.dbName = dbParameters["DBName"]

    def __init__(self, parentLogger=None):
        """c'tor

        :param self: self reference
        """

        if not parentLogger:
            parentLogger = gLogger
        self.log = parentLogger.getSubLogger(self.__class__.__name__)

        # Initialize the connection info
        self.__getDBConnectionInfo("RequestManagement/ReqDB")

        runDebug = gLogger.getLevel() == "DEBUG"
        self.engine = create_engine(
            f"mysql://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}/{self.dbName}",
            echo=runDebug,
            pool_recycle=3600,
        )

        metadata.bind = self.engine

        self.DBSession = sessionmaker(bind=self.engine)

    def createTables(self):
        """create tables"""
        try:
            metadata.create_all(self.engine)
        except SQLAlchemyError as e:
            return S_ERROR(e)
        return S_OK()

    def cancelRequest(self, requestID):
        session = self.DBSession()
        try:
            updateRet = session.execute(
                update(Request)
                .where(Request.RequestID == requestID)  # pylint: disable=no-member
                .values(
                    {
                        Request._Status: "Canceled",  # pylint: disable=no-member
                        Request._LastUpdate: datetime.datetime.utcnow(),  # pylint: disable=no-member
                    }
                )
                .execution_options(synchronize_session=False)
            )  # See FTS3DB for synchronize_session
            session.commit()

            # No row was changed
            if not updateRet.rowcount:
                return S_ERROR(f"No such request {requestID}")

            return S_OK()

        except Exception as e:
            session.rollback()
            self.log.exception("cancelRequest: unexpected exception", lException=e)
            return S_ERROR(f"cancelRequest: unexpected exception {e}")
        finally:
            session.close()

    def putRequest(self, request):
        """update or insert request into db

        :param ~Request.Request request: Request instance
        """

        session = self.DBSession(expire_on_commit=False)
        try:
            try:
                if hasattr(request, "RequestID"):
                    status = (
                        session.query(Request._Status)  # pylint: disable=no-member
                        .filter(Request.RequestID == request.RequestID)  # pylint: disable=no-member
                        .one()
                    )

                    if status[0] == "Canceled":
                        self.log.info(
                            f"Request {request.RequestID}({request.RequestName}) was canceled, don't put it back"
                        )
                        return S_OK(request.RequestID)

            except NoResultFound:
                pass

            # Since the object request is not attached to the session, we merge it to have an update
            # instead of an insert with duplicate primary key
            request = session.merge(request)
            session.add(request)
            session.commit()
            session.expunge_all()

            return S_OK(request.RequestID)

        except Exception as e:
            session.rollback()
            self.log.exception("putRequest: unexpected exception", lException=e)
            return S_ERROR(f"putRequest: unexpected exception {e}")
        finally:
            session.close()

    def getScheduledRequest(self, operationID):
        session = self.DBSession()
        try:
            requestID = (
                session.query(Request.RequestID)  # pylint: disable=no-member
                .join(Request.__operations__)  # pylint: disable=no-member
                .filter(Operation.OperationID == operationID)  # pylint: disable=no-member
                .one()
            )
            return self.getRequest(requestID[0])
        except NoResultFound:
            return S_OK()
        finally:
            session.close()

    #
    #   def getRequestName( self, requestID ):
    #     """ get Request.RequestName for a given Request.RequestID """
    #
    #     session = self.DBSession()
    #     try:
    #       requestName = session.query( Request.RequestName )\
    #                            .filter( Request.RequestID == requestID )\
    #                            .one()
    #       return S_OK( requestName[0] )
    #     except NoResultFound, e:
    #       return S_ERROR( "getRequestName: no request found for RequestID=%s" % requestID )
    #     finally:
    #       session.close()

    def getRequest(self, reqID=0, assigned=True):
        """read request for execution

        :param reqID: request's ID (default 0) If 0, take a pseudo random one

        """

        # expire_on_commit is set to False so that we can still use the object after we close the session
        session = self.DBSession(expire_on_commit=False)
        log = self.log.getSubLogger("getRequest" if assigned else "peekRequest")

        requestID = None

        try:
            if reqID:
                requestID = reqID

                log.verbose(f"selecting request '{reqID}'{' (Assigned)' if assigned else ''}")
                status = None
                try:
                    status = (
                        session.query(Request._Status)  # pylint: disable=no-member
                        .filter(Request.RequestID == reqID)  # pylint: disable=no-member
                        .one()
                    )
                except NoResultFound:
                    return S_ERROR(f"getRequest: request '{reqID}' not exists")

                if status and status == "Assigned" and assigned:
                    return S_ERROR(f"getRequest: status of request '{reqID}' is 'Assigned', request cannot be selected")

            else:
                now = datetime.datetime.utcnow().replace(microsecond=0)
                reqIDs = set()
                try:
                    reqAscIDs = (
                        session.query(Request.RequestID)  # pylint: disable=no-member
                        .filter(Request._Status == "Waiting")  # pylint: disable=no-member
                        .filter(Request._NotBefore < now)  # pylint: disable=no-member
                        .order_by(Request._LastUpdate)  # pylint: disable=no-member
                        .limit(100)
                        .all()
                    )

                    reqIDs = {reqID[0] for reqID in reqAscIDs}

                    reqDescIDs = (
                        session.query(Request.RequestID)  # pylint: disable=no-member
                        .filter(Request._Status == "Waiting")  # pylint: disable=no-member
                        .filter(Request._NotBefore < now)  # pylint: disable=no-member
                        .order_by(Request._LastUpdate.desc())  # pylint: disable=no-member
                        .limit(50)
                        .all()
                    )

                    reqIDs |= {reqID[0] for reqID in reqDescIDs}
                # No Waiting requests
                except NoResultFound:
                    return S_OK()

                if not reqIDs:
                    return S_OK()

                reqIDs = list(reqIDs)
                random.shuffle(reqIDs)
                requestID = reqIDs[0]

            # If we are here, the request MUST exist, so no try catch
            # the joinedload is to force the non-lazy loading of all the attributes, especially _parent
            request = (
                session.query(Request)
                .options(
                    joinedload(Request.__operations__).joinedload(Operation.__files__)  # pylint: disable=no-member
                )
                .filter(Request.RequestID == requestID)  # pylint: disable=no-member
                .one()
            )

            if not reqID:
                log.verbose(
                    "selected request %s('%s')%s"
                    % (request.RequestID, request.RequestName, " (Assigned)" if assigned else "")
                )

            if assigned:
                session.execute(
                    update(Request)
                    .where(Request.RequestID == requestID)  # pylint: disable=no-member
                    .values(
                        {
                            Request._Status: "Assigned",  # pylint: disable=no-member
                            Request._LastUpdate: datetime.datetime.utcnow(),  # pylint: disable=no-member
                        }
                    )
                )
                session.commit()

            session.expunge_all()

            # FIXME: code for backward compatibility
            if not request.Owner:
                # We go under the assumption that in this case OwnerDN exists
                if request.OwnerDN:
                    res = getDNForUsername(request.OwnerDN)
                    if not res["OK"]:
                        return res
                    request.Owner = res["Value"][0]
                else:
                    request.Owner = "Unknown"
            # ##

            return S_OK(request)

        except Exception as e:
            session.rollback()
            log.exception("getRequest: unexpected exception", lException=e)
            return S_ERROR(f"getRequest: unexpected exception : {e}")
        finally:
            session.close()

    def getBulkRequests(self, numberOfRequest=10, assigned=True):
        """read as many requests as requested for execution

        :param int numberOfRequest: Number of Request we want (default 10)
        :param bool assigned: if True, the status of the selected requests are set to assign

        :returns: a dictionary of Request objects indexed on the RequestID

        """

        # expire_on_commit is set to False so that we can still use the object after we close the session
        session = self.DBSession(expire_on_commit=False)
        log = self.log.getSubLogger("getBulkRequest" if assigned else "peekBulkRequest")

        requestDict = {}

        try:
            # If we are here, the request MUST exist, so no try catch
            # the joinedload is to force the non-lazy loading of all the attributes, especially _parent
            try:
                now = datetime.datetime.utcnow().replace(microsecond=0)
                requestIDs = (
                    session.query(Request.RequestID)  # pylint: disable=no-member
                    .with_for_update()
                    .filter(Request._Status == "Waiting")  # pylint: disable=no-member
                    .filter(Request._NotBefore < now)  # pylint: disable=no-member
                    .order_by(Request._LastUpdate)  # pylint: disable=no-member
                    .limit(numberOfRequest)
                    .all()
                )

                requestIDs = [ridTuple[0] for ridTuple in requestIDs]
                log.debug(f"Got request ids {requestIDs}")

                requests = (
                    session.query(Request)
                    .options(
                        joinedload(Request.__operations__).joinedload(Operation.__files__)  # pylint: disable=no-member
                    )
                    .filter(Request.RequestID.in_(requestIDs))  # pylint: disable=no-member
                    .all()
                )
                log.debug(f"Got {len(requests)} Request objects ")
                requestDict = {req.RequestID: req for req in requests}
            # No Waiting requests
            except NoResultFound:
                pass

            if assigned and requestDict:
                session.execute(
                    update(Request)
                    .where(Request.RequestID.in_(requestDict.keys()))  # pylint: disable=no-member
                    .values(
                        {
                            Request._Status: "Assigned",  # pylint: disable=no-member
                            Request._LastUpdate: datetime.datetime.utcnow(),  # pylint: disable=no-member
                        }
                    )
                )
            session.commit()

            session.expunge_all()

        except Exception as e:
            session.rollback()
            log.exception("unexpected exception", lException=e)
            return S_ERROR(f"getBulkRequest: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(requestDict)

    def peekRequest(self, requestID):
        """get request (ro), no update on states

        :param requestID: Request.RequestID
        """
        return self.getRequest(requestID, False)

    def getRequestIDsList(self, statusList=None, limit=None, since=None, until=None, getJobID=False):
        """select requests with status in :statusList:"""
        statusList = statusList if statusList else list(Request.FINAL_STATES)
        limit = limit if limit else 100
        session = self.DBSession()
        requestIDs = []
        try:
            if getJobID:
                reqQuery = session.query(
                    Request.RequestID, Request._Status, Request._LastUpdate, Request.JobID  # pylint: disable=no-member
                ).filter(
                    Request._Status.in_(statusList)  # pylint: disable=no-member
                )
            else:
                reqQuery = session.query(
                    Request.RequestID, Request._Status, Request._LastUpdate  # pylint: disable=no-member
                ).filter(
                    Request._Status.in_(statusList)  # pylint: disable=no-member
                )
            if since:
                reqQuery = reqQuery.filter(Request._LastUpdate > since)  # pylint: disable=no-member
            if until:
                reqQuery = reqQuery.filter(Request._LastUpdate < until)  # pylint: disable=no-member

            reqQuery = reqQuery.order_by(Request._LastUpdate).limit(limit)  # pylint: disable=no-member
            requestIDs = [list(reqIDTuple) for reqIDTuple in reqQuery.all()]

        except Exception as e:
            session.rollback()
            self.log.exception("getRequestIDsList: unexpected exception", lException=e)
            return S_ERROR(f"getRequestIDsList: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(requestIDs)

    def deleteRequest(self, requestID):
        """delete request given its ID

        :param str requestID: request.RequestID
        :param mixed connection: connection to use if any
        """

        session = self.DBSession()

        try:
            session.query(Request).filter(Request.RequestID == requestID).delete()  # pylint: disable=no-member
            session.commit()
        except Exception as e:
            session.rollback()
            self.log.exception("deleteRequest: unexpected exception", lException=e)
            return S_ERROR(f"deleteRequest: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK()

    def getDBSummary(self):
        """get db summary"""
        # # this will be returned
        retDict = {"Request": {}, "Operation": {}, "File": {}}

        session = self.DBSession()

        try:
            requestQuery = (
                session.query(Request._Status, func.count(Request.RequestID))  # pylint: disable=not-callable,no-member
                .group_by(Request._Status)  # pylint: disable=no-member
                .all()
            )

            for status, count in requestQuery:
                retDict["Request"][status] = count

            operationQuery = (
                session.query(
                    Operation.Type,  # pylint: disable=no-member
                    Operation._Status,  # pylint: disable=no-member
                    func.count(Operation.OperationID),  # pylint: disable=not-callable,no-member
                )
                .group_by(Operation.Type, Operation._Status)  # pylint: disable=no-member
                .all()
            )

            for oType, status, count in operationQuery:
                retDict["Operation"].setdefault(oType, {})[status] = count

            fileQuery = (
                session.query(File._Status, func.count(File.FileID))  # pylint: disable=not-callable, no-member
                .group_by(File._Status)  # pylint: disable=no-member
                .all()
            )

            for status, count in fileQuery:
                retDict["File"][status] = count

        except Exception as e:
            self.log.exception("getDBSummary: unexpected exception", lException=e)
            return S_ERROR(f"getDBSummary: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(retDict)

    def getRequestSummaryWeb(self, selectDict, sortList, startItem, maxItems):
        """Returns a list of Request for the web portal

        :param dict selectDict: parameter on which to restrain the query {key : Value}
                                key can be any of the Request columns, 'Type' (interpreted as Operation.Type)
                                and 'FromData' and 'ToData' are matched against the LastUpdate field
        :param sortList: [sorting column, ASC/DESC]
        :type sortList: python:list
        :param int startItem: start item (for pagination)
        :param int maxItems: max items (for pagination)
        """

        parameterList = [
            "RequestID",
            "RequestName",
            "JobID",
            "Owner",
            "OwnerGroup",
            "Status",
            "Error",
            "CreationTime",
            "LastUpdate",
        ]

        resultDict = {}

        session = self.DBSession()

        try:
            summaryQuery = session.query(
                Request.RequestID,  # pylint: disable=no-member
                Request.RequestName,  # pylint: disable=no-member
                Request.JobID,  # pylint: disable=no-member
                Request.Owner,  # pylint: disable=no-member
                Request.OwnerGroup,  # pylint: disable=no-member
                Request._Status,  # pylint: disable=no-member
                Request.Error,  # pylint: disable=no-member
                Request._CreationTime,  # pylint: disable=no-member
                Request._LastUpdate,  # pylint: disable=no-member
            )

            for key, value in selectDict.items():
                if key == "ToDate":
                    summaryQuery = summaryQuery.filter(Request._LastUpdate < value)  # pylint: disable=no-member
                elif key == "FromDate":
                    summaryQuery = summaryQuery.filter(Request._LastUpdate > value)  # pylint: disable=no-member
                else:
                    tableName = "Request"

                    if key == "Type":
                        summaryQuery = summaryQuery.join(Request.__operations__).group_by(  # pylint: disable=no-member
                            Request.RequestID,  # pylint: disable=no-member
                            Request.RequestName,  # pylint: disable=no-member
                            Request.JobID,  # pylint: disable=no-member
                            Request.Owner,  # pylint: disable=no-member
                            Request.OwnerGroup,  # pylint: disable=no-member
                            Request._Status,  # pylint: disable=no-member
                            Request.Error,  # pylint: disable=no-member
                            Request._CreationTime,  # pylint: disable=no-member
                            Request._LastUpdate,  # pylint: disable=no-member
                            Operation.Type,  # pylint: disable=no-member
                        )
                        tableName = "Operation"
                    elif key == "Status":
                        key = "_Status"

                    if isinstance(value, list):
                        summaryQuery = summaryQuery.filter(eval(f"{tableName}.{key}.in_({value})"))
                    else:
                        summaryQuery = summaryQuery.filter(eval(f"{tableName}.{key}") == value)

            if sortList:
                summaryQuery = summaryQuery.order_by(eval(f"Request.{sortList[0][0]}.{sortList[0][1].lower()}()"))

            try:
                requestLists = summaryQuery.all()
            except NoResultFound:
                resultDict["ParameterNames"] = parameterList
                resultDict["Records"] = []

                return S_OK(resultDict)
            except Exception as e:
                return S_ERROR(f"Error getting the webSummary {e}")

            nRequests = len(requestLists)

            if startItem <= len(requestLists):
                firstIndex = startItem
            else:
                return S_ERROR("getRequestSummaryWeb: Requested index out of range")

            if (startItem + maxItems) <= len(requestLists):
                secondIndex = startItem + maxItems
            else:
                secondIndex = len(requestLists)

            records = []
            for i in range(firstIndex, secondIndex):
                row = requestLists[i]
                records.append([str(x) for x in row])

            resultDict["ParameterNames"] = parameterList
            resultDict["Records"] = records
            resultDict["TotalRecords"] = nRequests

            return S_OK(resultDict)
        #
        except Exception as e:
            self.log.exception("getRequestSummaryWeb: unexpected exception", lException=e)
            return S_ERROR(f"getRequestSummaryWeb: unexpected exception : {e}")

        finally:
            session.close()

    def getRequestCountersWeb(self, groupingAttribute, selectDict):
        """For the web portal.
        Returns a dictionary {value : counts} for a given key.
        The key can be any field from the RequestTable. or "Type",
        which will be interpreted as 'Operation.Type'
        """

        resultDict = {}

        session = self.DBSession()

        if groupingAttribute == "Type":
            groupingAttribute = "Operation.Type"
        elif groupingAttribute == "Status":
            groupingAttribute = "Request._Status"
        else:
            groupingAttribute = f"Request.{groupingAttribute}"

        try:
            summaryQuery = session.query(
                eval(groupingAttribute), func.count(Request.RequestID)  # pylint: disable=not-callable,no-member
            )

            for key, value in selectDict.items():
                if key == "ToDate":
                    summaryQuery = summaryQuery.filter(Request._LastUpdate < value)  # pylint: disable=no-member
                elif key == "FromDate":
                    summaryQuery = summaryQuery.filter(Request._LastUpdate > value)  # pylint: disable=no-member
                else:
                    objectType = "Request"
                    if key == "Type":
                        summaryQuery = summaryQuery.join(Request.__operations__)  # pylint: disable=no-member
                        objectType = "Operation"
                    elif key == "Status":
                        key = "_Status"

                    if isinstance(value, list):
                        summaryQuery = summaryQuery.filter(eval(f"{objectType}.{key}.in_({value})"))
                    else:
                        summaryQuery = summaryQuery.filter(eval(f"{objectType}.{key}") == value)

            summaryQuery = summaryQuery.group_by(eval(groupingAttribute))

            try:
                requestLists = summaryQuery.all()
                resultDict = dict(requestLists)
            except NoResultFound:
                pass
            except Exception as e:
                return S_ERROR(f"Error getting the webCounters {e}")

            return S_OK(resultDict)

        except Exception as e:
            self.log.exception("getRequestSummaryWeb: unexpected exception", lException=e)
            return S_ERROR(f"getRequestSummaryWeb: unexpected exception : {e}")

        finally:
            session.close()

    def getDistinctValues(self, tableName, columnName):
        """For a given table and a given field, return the list of of distinct values in the DB"""

        session = self.DBSession()
        distinctValues = []
        if columnName == "Status":
            columnName = "_Status"
        try:
            result = session.query(distinct(eval(f"{tableName}.{columnName}"))).all()
            distinctValues = [dist[0] for dist in result]
        except NoResultFound:
            pass
        except Exception as e:
            self.log.exception("getDistinctValues: unexpected exception", lException=e)
            return S_ERROR(f"getDistinctValues: unexpected exception : {e}")

        finally:
            session.close()

        return S_OK(distinctValues)

    def getRequestIDsForJobs(self, jobIDs):
        """returns request ids for jobs given jobIDs

        :param list jobIDs: list of jobIDs
        :return: S_OK( "Successful" : { jobID1 : Request, jobID2: Request, ... }
                       "Failed" : { jobID3: "error message", ... } )
        """
        self.log.debug(f"getRequestIDsForJobs: got {str(jobIDs)} jobIDs to check")
        if not jobIDs:
            return S_ERROR("Must provide jobID list as argument.")
        if isinstance(jobIDs, int):
            jobIDs = [jobIDs]
        jobIDs = set(jobIDs)

        reqDict = {"Successful": {}, "Failed": {}}

        session = self.DBSession()

        try:
            ret = (
                session.query(Request.JobID, Request.RequestID)  # pylint: disable=no-member
                .filter(Request.JobID.in_(jobIDs))  # pylint: disable=no-member
                .all()
            )

            reqDict["Successful"] = {jobId: reqID for jobId, reqID in ret}
            reqDict["Failed"] = {jobid: "Request not found" for jobid in jobIDs - set(reqDict["Successful"])}
        except Exception as e:
            self.log.exception("getRequestIDsForJobs: unexpected exception", lException=e)
            return S_ERROR(f"getRequestIDsForJobs: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(reqDict)

    def readRequestsForJobs(self, jobIDs=None):
        """read request for jobs

        :param list jobIDs: list of JobIDs
        :return: S_OK( "Successful" : { jobID1 : Request, jobID2: Request, ... }
                       "Failed" : { jobID3: "error message", ... } )
        """
        self.log.debug(f"readRequestForJobs: got {str(jobIDs)} jobIDs to check")
        if not jobIDs:
            return S_ERROR("Must provide jobID list as argument.")
        if isinstance(jobIDs, int):
            jobIDs = [jobIDs]
        jobIDs = set(jobIDs)

        reqDict = {"Successful": {}, "Failed": {}}

        # expire_on_commit is set to False so that we can still use the object after we close the session
        session = self.DBSession(expire_on_commit=False)

        try:
            ret = (
                session.query(Request.JobID, Request)  # pylint: disable=no-member
                .options(
                    joinedload(Request.__operations__).joinedload(Operation.__files__)  # pylint: disable=no-member
                )
                .filter(Request.JobID.in_(jobIDs))  # pylint: disable=no-member
                .all()
            )

            reqDict["Successful"] = {jobId: reqObj for jobId, reqObj in ret}

            reqDict["Failed"] = {jobid: "Request not found" for jobid in jobIDs - set(reqDict["Successful"])}
            session.expunge_all()
        except Exception as e:
            self.log.exception("readRequestsForJobs: unexpected exception", lException=e)
            return S_ERROR(f"readRequestsForJobs: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(reqDict)

    def getRequestStatus(self, requestID):
        """get request status for a given request ID"""
        self.log.debug(f"getRequestStatus: checking status for '{requestID}' request")
        session = self.DBSession()
        try:
            status = (
                session.query(Request._Status).filter(Request.RequestID == requestID).one()  # pylint: disable=no-member
            )
        except NoResultFound:
            return S_ERROR(errno.ENOENT, f"Request {requestID} does not exist")
        finally:
            session.close()
        return S_OK(status[0])

    def getBulkRequestStatus(self, requestIDs):
        """get requests statuses for given request IDs"""
        session = self.DBSession()
        try:
            statuses = (
                session.query(Request.RequestID, Request._Status)  # pylint: disable=no-member
                .filter(Request.RequestID.in_(requestIDs))  # pylint: disable=no-member
                .all()  # pylint: disable=no-member
            )
            status_dict = {req_id: req_status for req_id, req_status in statuses}
        except Exception as e:
            # log as well?
            return S_ERROR(f"Failed to getBulkRequestStatus {e!r}")
        finally:
            session.close()
        return S_OK(status_dict)

    def getRequestFileStatus(self, requestID, lfnList):
        """get status for files in request given its id

        A single status is returned by file, which corresponds
        to the most representative one. That is:

        * Failed: if it has failed in any of the operation
        * Scheduled: if it is Scheduled in any of the operation
        * Waiting: if the process is ongoing
        * Done: if everything was executed

        :param str requestID: Request.RequestID
        :param lfnList: list of LFNs
        :type lfnList: python:list
        """

        session = self.DBSession()
        try:
            res = dict.fromkeys(lfnList, "UNKNOWN")
            requestRet = (
                session.query(File._LFN, File._Status)  # pylint: disable=no-member
                .join(Request.__operations__)  # pylint: disable=no-member
                .join(Operation.__files__)  # pylint: disable=no-member
                .filter(Request.RequestID == requestID)  # pylint: disable=no-member
                .filter(File._LFN.in_(lfnList))  # pylint: disable=no-member
                .order_by(Operation._Order)  # pylint: disable=no-member
                .all()
            )

            for lfn, status in requestRet:
                # If the file was in one of these two state in the previous
                # operations, that's the one we want to return
                if res.get(lfn) not in ("Failed", "Scheduled"):
                    res[lfn] = status
            return S_OK(res)

        except Exception as e:
            self.log.exception("getRequestFileStatus: unexpected exception", lException=e)
            return S_ERROR(f"getRequestFileStatus: unexpected exception : {e}")
        finally:
            session.close()

    def getRequestInfo(self, requestID):
        """get request info given Request.RequestID"""

        session = self.DBSession()

        try:
            requestInfoQuery = session.query(
                Request.RequestID,  # pylint: disable=no-member
                Request._Status,  # pylint: disable=no-member
                Request.RequestName,  # pylint: disable=no-member
                Request.JobID,  # pylint: disable=no-member
                Request.Owner,  # pylint: disable=no-member
                Request.OwnerGroup,  # pylint: disable=no-member
                Request._SourceComponent,  # pylint: disable=no-member
                Request._CreationTime,  # pylint: disable=no-member
                Request._SubmitTime,  # pylint: disable=no-member
                Request._LastUpdate,  # pylint: disable=no-member
            ).filter(
                Request.RequestID == requestID  # pylint: disable=no-member
            )

            try:
                requestInfo = requestInfoQuery.one()
            except NoResultFound:
                return S_ERROR("No such request")

            return S_OK(list(requestInfo))

        except Exception as e:
            self.log.exception("getRequestInfo: unexpected exception", lException=e)
            return S_ERROR(f"getRequestInfo: unexpected exception : {e}")

        finally:
            session.close()

    def getDigest(self, requestID):
        """get digest for request given its id

        :param str requestName: request id
        """
        self.log.debug(f"getDigest: will create digest for request '{requestID}'")
        request = self.getRequest(requestID, False)
        if not request["OK"]:
            self.log.error("getDiges", request["Message"])
            return request
        request = request["Value"]
        if not isinstance(request, Request):
            self.log.info("getDigest: request '%s' not found")
            return S_OK()
        return request.getDigest()

    def getRequestIDForName(self, requestName):
        """read request id for given name
            if the name is not unique, an error is returned

        :param requestName: name of the request
        """
        session = self.DBSession()

        reqID = 0
        try:
            ret = (
                session.query(Request.RequestID)  # pylint: disable=no-member
                .filter(Request.RequestName == requestName)  # pylint: disable=no-member
                .all()
            )
            if not ret:
                return S_ERROR(f"No such request {requestName}")
            if len(ret) > 1:
                return S_ERROR(f"RequestName {requestName} not unique ({len(ret)} matches)")

            reqID = ret[0][0]

        except NoResultFound:
            return S_ERROR("No such request")
        except Exception as e:
            self.log.exception("getRequestIDsForName: unexpected exception", lException=e)
            return S_ERROR(f"getRequestIDsForName: unexpected exception : {e}")
        finally:
            session.close()

        return S_OK(reqID)
