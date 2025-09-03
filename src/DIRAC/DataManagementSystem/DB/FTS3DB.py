"""Frontend to FTS3 MySQL DB. Written using sqlalchemy"""

# We disable the no-member error because
# they are constructed by SQLAlchemy for all
# the objects mapped to a table.
# pylint: disable=no-member

import datetime
import errno
from urllib.parse import quote_plus

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    SmallInteger,
    String,
    Table,
    create_engine,
    func,
    text,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import registry, relationship, sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import delete, select, update
from sqlalchemy.sql.expression import and_

# # from DIRAC
from DIRAC import S_ERROR, S_OK, gLogger
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.DataManagementSystem.Client.FTS3File import FTS3File
from DIRAC.DataManagementSystem.Client.FTS3Job import FTS3Job
from DIRAC.DataManagementSystem.Client.FTS3Operation import FTS3Operation, FTS3StagingOperation, FTS3TransferOperation

metadata = MetaData()
mapper_registry = registry()

# Define the default utc_timestampfunction.
# We overwrite it in the case of sqlite in the tests
# because sqlite does not know UTC_TIMESTAMP
utc_timestamp = func.utc_timestamp

fts3FileTable = Table(
    "Files",
    metadata,
    Column("fileID", Integer, primary_key=True),
    Column("operationID", Integer, ForeignKey("Operations.operationID", ondelete="CASCADE"), nullable=False),
    Column("attempt", Integer, server_default="0"),
    Column("lastUpdate", DateTime, onupdate=utc_timestamp()),
    Column("rmsFileID", Integer, server_default="0"),
    Column("lfn", String(1024)),
    Column("checksum", String(255)),
    Column("size", BigInteger),
    Column("targetSE", String(255), nullable=False),
    Column("error", String(2048)),
    Column("ftsGUID", String(255)),
    Column("status", Enum(*FTS3File.ALL_STATES), server_default=FTS3File.INIT_STATE, index=True),
    mysql_engine="InnoDB",
)

mapper_registry.map_imperatively(FTS3File, fts3FileTable)


fts3JobTable = Table(
    "Jobs",
    metadata,
    Column("jobID", Integer, primary_key=True),
    Column("operationID", Integer, ForeignKey("Operations.operationID", ondelete="CASCADE"), nullable=False),
    Column("submitTime", DateTime),
    Column("lastUpdate", DateTime, onupdate=utc_timestamp()),
    Column("lastMonitor", DateTime),
    Column("completeness", Float),
    # Could be fetched from Operation, but bad for perf
    Column("username", String(255)),
    # Could be fetched from Operation, but bad for perf
    Column("userGroup", String(255)),
    Column("ftsGUID", String(255)),
    Column("ftsServer", String(255)),
    Column("error", String(2048)),
    Column("status", Enum(*FTS3Job.ALL_STATES), server_default=FTS3Job.INIT_STATE, index=True),
    Column("assignment", String(255), server_default=None),
    Index("idx_jobs_lastupdate_assignment", "lastUpdate", "assignment"),
    mysql_engine="InnoDB",
)

mapper_registry.map_imperatively(FTS3Job, fts3JobTable)


fts3OperationTable = Table(
    "Operations",
    metadata,
    Column("operationID", Integer, primary_key=True),
    Column("username", String(255)),
    Column("userGroup", String(255)),
    # -1 because with 0 we would get any random request
    # when performing reqClient.getRequest
    Column("rmsReqID", Integer, server_default="-1"),
    Column("rmsOpID", Integer, server_default="0", index=True),
    Column("sourceSEs", String(255)),
    Column("activity", String(255)),
    Column("priority", SmallInteger),
    Column("creationTime", DateTime),
    Column("lastUpdate", DateTime, onupdate=utc_timestamp()),
    Column("status", Enum(*FTS3Operation.ALL_STATES), server_default=FTS3Operation.INIT_STATE, index=True),
    Column("error", String(1024)),
    Column("type", String(255)),
    Column("assignment", String(255), server_default=None),
    Index("idx_operations_lastupdate_assignment", "lastUpdate", "assignment"),
    mysql_engine="InnoDB",
)


fts3Operation_mapper = mapper_registry.map_imperatively(
    FTS3Operation,
    fts3OperationTable,
    properties={
        "ftsFiles": relationship(
            FTS3File,
            lazy="joined",  # Immediately load the entirety of the object
            innerjoin=True,  # Use inner join instead of left outer join
            cascade="all, delete-orphan",  # if a File is removed from the list,
            # remove it from the DB
            passive_deletes=True,  # used together with cascade='all, delete-orphan'
        ),
        "ftsJobs": relationship(
            FTS3Job,
            lazy="selectin",  # Immediately load the entirety of the object,
            # but use a subquery to do it
            # This is to avoid the cartesian product between the three tables.
            # https://docs.sqlalchemy.org/en/20/orm/queryguide/relationships.html#selectin-eager-loading
            # We use selectin and not subquery because of https://github.com/DIRACGrid/DIRAC/issues/6814
            cascade="all, delete-orphan",  # if a File is removed from the list,
            # remove it from the DB
            passive_deletes=True,  # used together with cascade='all, delete-orphan'
        ),
    },
    polymorphic_on="type",
    polymorphic_identity="Abs",
)

mapper_registry.map_imperatively(
    FTS3TransferOperation, fts3OperationTable, inherits=fts3Operation_mapper, polymorphic_identity="Transfer"
)

mapper_registry.map_imperatively(
    FTS3StagingOperation, fts3OperationTable, inherits=fts3Operation_mapper, polymorphic_identity="Staging"
)


# About synchronize_session:
# The FTS3DB class uses SQLAlchemy in a mixed mode:
# both the ORM and the Core style.
# Up to 1.3, the `session.execute` statements had no ORM functionality,
# meaning that the session cache were not updated when using `update` or `delete`
# Although it could be an issue, it was not really one in our case, since we always close
# the session
# As of 1.4, `session.execute` supports ORM functionality, and thus needs more info to know
# how to manage `update` or `delete`. Hense the `synchronize_session` option.
# We set it to `False` simply because we do not rely on the session cache.
# Please see https://github.com/sqlalchemy/sqlalchemy/discussions/6159 for detailed discussion


########################################################################
class FTS3DB:
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

    def __init__(self, pool_size=15, url=None, parentLogger=None):
        """c'tor

        :param self: self reference
        :param pool_size: size of the connection pool to the DB

        """

        if not parentLogger:
            parentLogger = gLogger

        self.log = parentLogger.getSubLogger(self.__class__.__name__)

        if not url:
            # Initialize the connection info
            self.__getDBConnectionInfo("DataManagement/FTS3DB")

            url = f"mysql://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}/{self.dbName}"

        runDebug = gLogger.getLevel() == "DEBUG"
        self.engine = create_engine(
            url,
            echo=runDebug,
            pool_size=pool_size,
            pool_recycle=3600,
        )

        metadata.bind = self.engine

        self.dbSession = sessionmaker(bind=self.engine)

    def createTables(self):
        """create tables"""
        try:
            metadata.create_all(self.engine)
        except SQLAlchemyError as e:
            return S_ERROR(e)
        return S_OK()

    def persistOperation(self, operation):
        """update or insert request into db
            Also release the assignment tag

        :param operation: FTS3Operation instance
        """

        session = self.dbSession(expire_on_commit=False)

        # set the assignment to NULL
        # so that another agent can work on the request
        operation.assignment = None
        # because of the merge we have to explicitely set lastUpdate
        operation.lastUpdate = utc_timestamp()
        try:
            # Merge it in case it already is in the DB
            operation = session.merge(operation)
            session.add(operation)
            session.commit()
            session.expunge_all()

            return S_OK(operation.operationID)

        except SQLAlchemyError as e:
            session.rollback()
            self.log.exception("persistOperation: unexpected exception", lException=e)
            return S_ERROR(f"persistOperation: unexpected exception {e}")
        finally:
            session.close()

    def getOperation(self, operationID):
        """read request

          This does not set the assignment flag

        :param operationID: ID of the FTS3Operation

        """

        # expire_on_commit is set to False so that we can still use the object
        # after we close the session
        session = self.dbSession(expire_on_commit=False)

        try:
            operation = session.query(FTS3Operation).filter(getattr(FTS3Operation, "operationID") == operationID).one()

            session.commit()

            ###################################
            session.expunge_all()
            return S_OK(operation)

        except NoResultFound as e:
            # We use the ENOENT error, even if not really a file error :)
            return S_ERROR(errno.ENOENT, f"No FTS3Operation with id {operationID}")
        except SQLAlchemyError as e:
            return S_ERROR(f"getOperation: unexpected exception : {e}")
        finally:
            session.close()

    def getActiveJobs(self, limit=20, lastMonitor=None, jobAssignmentTag="Assigned"):
        """Get  the FTSJobs that are not in a final state, and are not assigned for monitoring
         or has its operation being treated

         By assigning the job to the DB:
           * it cannot be monitored by another agent
           * the operation to which it belongs cannot be treated

        :param limit: max number of Jobs to retrieve
        :param lastMonitor: jobs monitored earlier than the given date
        :param jobAssignmentTag: if not None, block the Job for other queries,
                               and use it as a prefix for the value in the operation table

        :returns: list of FTS3Jobs

        """
        session = self.dbSession(expire_on_commit=False)

        try:
            # the tild sign is for "not"

            ftsJobsQuery = (
                session.query(FTS3Job)
                .join(FTS3Operation)
                .filter(FTS3Job.status.in_(FTS3Job.NON_FINAL_STATES))
                .filter(FTS3Operation.status == "Active")
                .filter(FTS3Job.assignment.is_(None))
                .filter(FTS3Operation.assignment.is_(None))
            )

            if lastMonitor:
                ftsJobsQuery = ftsJobsQuery.filter(FTS3Job.lastMonitor < lastMonitor)

            if jobAssignmentTag:
                ftsJobsQuery = ftsJobsQuery.with_for_update()

            ftsJobsQuery = ftsJobsQuery.order_by(FTS3Job.lastMonitor.asc())
            ftsJobsQuery = ftsJobsQuery.limit(limit)

            ftsJobs = ftsJobsQuery.all()

            if jobAssignmentTag:
                jobAssignmentTag += f"_{datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

                jobIds = [job.jobID for job in ftsJobs]
                if jobIds:
                    session.execute(
                        update(FTS3Job)
                        .where(FTS3Job.jobID.in_(jobIds))
                        .values({"assignment": jobAssignmentTag})
                        .execution_options(synchronize_session=False)  # see comment about synchronize_session
                    )

            session.commit()

            session.expunge_all()

            return S_OK(ftsJobs)

        except SQLAlchemyError as e:
            session.rollback()
            return S_ERROR(f"getAllActiveJobs: unexpected exception : {e}")
        finally:
            session.close()

    def updateFileStatus(self, fileStatusDict, ftsGUID=None):
        """Update the file ftsStatus and error
         The update is only done if the file is not in a final state
         (To avoid bringing back to life a file by consuming MQ a posteriori)



         TODO: maybe it should query first the status and filter the rows I want to update !

        :param fileStatusDict: { fileID : { status , error, ftsGUID } }
        :param ftsGUID: If specified, only update the rows where the ftsGUID matches this value.
                        This avoids two jobs handling the same file one after another to step on each other foot.
                        Note that for the moment it is an optional parameter, but it may turn mandatory soon.

        """

        # This here is inneficient as we update every files, even if it did not change, and we commit every time.
        # It would probably be best to update only the files that changed.
        # However, commiting every time is the recommendation of MySQL
        # (https://dev.mysql.com/doc/refman/8.0/en/innodb-deadlocks-handling.html)

        for fileID, valueDict in fileStatusDict.items():
            session = self.dbSession()
            try:
                updateDict = {FTS3File.status: valueDict["status"]}

                # We only update error if it is specified
                if "error" in valueDict:
                    newError = valueDict["error"]
                    # Replace empty string with None
                    if not newError:
                        newError = None
                    updateDict[FTS3File.error] = newError

                # We only update ftsGUID if it is specified
                if "ftsGUID" in valueDict:
                    newFtsGUID = valueDict["ftsGUID"]
                    # Replace empty string with None
                    if not newFtsGUID:
                        newFtsGUID = None
                    updateDict[FTS3File.ftsGUID] = newFtsGUID

                # We only update the lines matching:
                # * the good fileID
                # * the status is not Final

                whereConditions = [FTS3File.fileID == fileID, ~FTS3File.status.in_(FTS3File.FINAL_STATES)]

                # If an ftsGUID is specified, add it to the `where` condition
                if ftsGUID:
                    whereConditions.append(FTS3File.ftsGUID == ftsGUID)

                updateQuery = (
                    update(FTS3File)
                    .where(and_(*whereConditions))
                    .values(updateDict)
                    .execution_options(synchronize_session=False)
                )  # see comment about synchronize_session

                session.execute(updateQuery)

                session.commit()

            except SQLAlchemyError as e:
                session.rollback()
                self.log.exception("updateFileFtsStatus: unexpected exception", lException=e)
                return S_ERROR(f"updateFileFtsStatus: unexpected exception {e}")
            finally:
                session.close()

        return S_OK()

    def updateJobStatus(self, jobStatusDict):
        """Update the job Status and error
         The update is only done if the job is not in a final state
         The assignment flag is released

        :param jobStatusDict: { jobID : { status , error, completeness } }
        """
        session = self.dbSession()
        try:
            for jobID, valueDict in jobStatusDict.items():
                updateDict = {FTS3Job.status: valueDict["status"]}

                # We only update error if it is specified
                if "error" in valueDict:
                    newError = valueDict["error"]
                    # Replace empty string with None
                    if not newError:
                        newError = None
                    updateDict[FTS3Job.error] = newError

                if "completeness" in valueDict:
                    updateDict[FTS3Job.completeness] = valueDict["completeness"]

                if valueDict.get("lastMonitor"):
                    updateDict[FTS3Job.lastMonitor] = utc_timestamp()

                updateDict[FTS3Job.assignment] = None

                session.execute(
                    update(FTS3Job)
                    .where(and_(FTS3Job.jobID == jobID, ~FTS3Job.status.in_(FTS3Job.FINAL_STATES)))
                    .values(updateDict)
                    .execution_options(synchronize_session=False)  # see comment about synchronize_session
                )
            session.commit()

            return S_OK()

        except SQLAlchemyError as e:
            session.rollback()
            self.log.exception("updateJobStatus: unexpected exception", lException=e)
            return S_ERROR(f"updateJobStatus: unexpected exception {e}")
        finally:
            session.close()

    def cancelNonExistingJob(self, operationID, ftsGUID):
        """
        Cancel an FTS3Job with the associated FTS3Files.
        This is to be used when the job is not found on the server when monitoring.

        The status of the job and files will be 'Canceled'.
        The error is specifying that the job is not found.
        The ftsGUID of the file is released as to be able to pick it up again

        :param operationID: guess
        :param ftsGUID: guess

        :returns: S_OK() if successful, S_ERROR otherwise
        """
        session = self.dbSession()
        try:
            # We update both the rows of the Jobs and the Files tables
            # having matching operationID and ftsGUID
            # https://docs.sqlalchemy.org/en/13/core/tutorial.html#multiple-table-updates

            # We do not need to specify that the File status should be final
            # since they would have been updated before and the ftsGUID already removed
            updStmt = (
                update(FTS3Job)
                .values(
                    {
                        FTS3File.status: "Canceled",
                        FTS3File.ftsGUID: None,
                        FTS3File.error: f"Job {ftsGUID} not found",
                        FTS3Job.status: "Canceled",
                        FTS3Job.error: f"Job {ftsGUID} not found",
                    }
                )
                .where(
                    and_(
                        FTS3File.operationID == FTS3Job.operationID,
                        FTS3File.ftsGUID == FTS3Job.ftsGUID,
                        FTS3Job.operationID == operationID,
                        FTS3Job.ftsGUID == ftsGUID,
                    )
                )
                .execution_options(synchronize_session=False)
            )  # see comment about synchronize_session

            session.execute(updStmt)
            session.commit()

            return S_OK()

        except SQLAlchemyError as e:
            session.rollback()
            self.log.exception("cancelNonExistingJob: unexpected exception", lException=e)
            return S_ERROR(f"cancelNonExistingJob: unexpected exception {e}")
        finally:
            session.close()

    def getNonFinishedOperations(self, limit=20, operationAssignmentTag="Assigned"):
        """Get all the non assigned FTS3Operations that are not yet finished, so either Active or Processed.
        An operation won't be picked if it is already assigned, or one of its job is.

        :param limit: max number of operations to retrieve
        :param operationAssignmentTag: if not None, block the operations for other queries,
                              and use it as a prefix for the value in the operation table
        :return: list of Operations
        """

        session = self.dbSession(expire_on_commit=False)

        try:
            ftsOperations = []

            # We need to do the select in two times because the join clause that makes the limit difficult
            # We get the list of operations ID that have associated jobs assigned
            opIDsWithJobAssigned = select(FTS3Job.operationID).filter(~FTS3Job.assignment.is_(None))
            operationIDsQuery = (
                session.query(FTS3Operation.operationID, FTS3Operation.lastUpdate)
                .filter(FTS3Operation.status.in_(["Active", "Processed"]))
                .filter(FTS3Operation.assignment.is_(None))
                .filter(~FTS3Operation.operationID.in_(opIDsWithJobAssigned))
                .order_by(FTS3Operation.lastUpdate.asc())
                .limit(limit)
                .distinct()
            )

            # Block the Operations for other requests
            if operationAssignmentTag:
                operationIDsQuery = operationIDsQuery.with_for_update()

            operationIDs = operationIDsQuery.all()

            operationIDs = [oidTuple[0] for oidTuple in operationIDs]

            if operationIDs:
                # Fetch the operation object for these IDs
                ftsOperations = session.query(FTS3Operation).filter(FTS3Operation.operationID.in_(operationIDs)).all()

                if operationAssignmentTag:
                    operationAssignmentTag += f"_{datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

                    session.execute(
                        update(FTS3Operation)
                        .where(FTS3Operation.operationID.in_(operationIDs))
                        .values({"assignment": operationAssignmentTag})
                        .execution_options(synchronize_session=False)  # see comment about synchronize_session
                    )

            session.commit()
            session.expunge_all()

            return S_OK(ftsOperations)

        except SQLAlchemyError as e:
            session.rollback()
            return S_ERROR(f"getAllProcessedOperations: unexpected exception : {e}")
        finally:
            session.close()

    def kickStuckOperations(self, limit=20, kickDelay=2):
        """finds operations that have not been updated for more than a given
          time but are still assigned and resets the assignment

        :param int limit: number of operations to treat
        :param int kickDelay: age of the lastUpdate in hours
        :returns: S_OK/S_ERROR with number of kicked operations

        """

        session = self.dbSession(expire_on_commit=False)

        try:
            ftsOps = (
                session.query(FTS3Operation.operationID)
                .filter(
                    FTS3Operation.lastUpdate < (func.date_sub(utc_timestamp(), text("INTERVAL %d HOUR" % kickDelay)))
                )
                .filter(~FTS3Operation.assignment.is_(None))
                .limit(limit)
            )

            opIDs = [opTuple[0] for opTuple in ftsOps]
            rowCount = 0

            if opIDs:
                result = session.execute(
                    update(FTS3Operation)
                    .where(FTS3Operation.operationID.in_(opIDs))
                    .where(
                        FTS3Operation.lastUpdate
                        < (func.date_sub(utc_timestamp(), text("INTERVAL %d HOUR" % kickDelay)))
                    )
                    .values({"assignment": None})
                    .execution_options(synchronize_session=False)  # see comment about synchronize_session
                )
                rowCount = result.rowcount

            session.commit()
            session.expunge_all()

            return S_OK(rowCount)

        except SQLAlchemyError as e:
            session.rollback()
            return S_ERROR(f"kickStuckOperations: unexpected exception : {e}")
        finally:
            session.close()

    def kickStuckJobs(self, limit=20, kickDelay=2):
        """finds jobs that have not been updated for more than a given
          time but are still assigned and resets the assignment

        :param int limit: number of jobs to treat
        :param int kickDelay: age of the lastUpdate in hours
        :returns: S_OK/S_ERROR with number of kicked jobs

        """

        session = self.dbSession(expire_on_commit=False)

        try:
            ftsJobs = (
                session.query(FTS3Job.jobID)
                .filter(FTS3Job.lastUpdate < (func.date_sub(utc_timestamp(), text("INTERVAL %d HOUR" % kickDelay))))
                .filter(~FTS3Job.assignment.is_(None))
                .limit(limit)
            )

            jobIDs = [jobTuple[0] for jobTuple in ftsJobs]
            rowCount = 0

            if jobIDs:
                result = session.execute(
                    update(FTS3Job)
                    .where(FTS3Job.jobID.in_(jobIDs))
                    .where(FTS3Job.lastUpdate < (func.date_sub(utc_timestamp(), text("INTERVAL %d HOUR" % kickDelay))))
                    .values({"assignment": None})
                    .execution_options(synchronize_session=False)  # see comment about synchronize_session
                )
                rowCount = result.rowcount

            session.commit()
            session.expunge_all()

            return S_OK(rowCount)

        except SQLAlchemyError as e:
            session.rollback()
            return S_ERROR(f"kickStuckJobs: unexpected exception : {e}")
        finally:
            session.close()

    def deleteFinalOperations(self, limit=20, deleteDelay=180):
        """deletes operation in final state that are older than given time

        :param int limit: number of operations to treat
        :param int deleteDelay: age of the lastUpdate in days
        :returns: S_OK/S_ERROR with number of deleted operations
        """

        session = self.dbSession(expire_on_commit=False)

        fromDate = datetime.datetime.utcnow() - datetime.timedelta(days=deleteDelay)
        try:
            ftsOps = (
                session.query(FTS3Operation.operationID)
                .filter(FTS3Operation.lastUpdate < fromDate)
                .filter(FTS3Operation.status.in_(FTS3Operation.FINAL_STATES))
                .limit(limit)
            )

            opIDs = [opTuple[0] for opTuple in ftsOps]
            rowCount = 0
            if opIDs:
                result = session.execute(
                    delete(FTS3Operation)
                    .where(FTS3Operation.operationID.in_(opIDs))
                    .execution_options(synchronize_session=False)
                )
                rowCount = result.rowcount

            session.commit()
            session.expunge_all()

            return S_OK(rowCount)

        except SQLAlchemyError as e:
            session.rollback()
            return S_ERROR(f"deleteFinalOperations: unexpected exception : {e}")
        finally:
            session.close()

    def getOperationsFromRMSOpID(self, rmsOpID):
        """Returns the FTS3Operations matching a given RMS OperationID

          This does not set the assignment flag

        :param rmsOpID: ID of the RMS Operation

        """

        # expire_on_commit is set to False so that we can still use the object
        # after we close the session
        session = self.dbSession(expire_on_commit=False)

        try:
            operations = session.query(FTS3Operation).filter(FTS3Operation.rmsOpID == rmsOpID).all()

            session.commit()

            ###################################
            session.expunge_all()
            return S_OK(operations)

        except NoResultFound as e:
            # If there is no such operation, return an empty list
            return S_OK([])
        except SQLAlchemyError as e:
            return S_ERROR(f"getOperationsFromRMSOpID: unexpected exception : {e}")
        finally:
            session.close()
