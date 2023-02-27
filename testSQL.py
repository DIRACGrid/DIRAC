from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import relationship, sessionmaker, registry
from sqlalchemy.sql import update, delete, select
from sqlalchemy import (
    create_engine,
    event,
    engine,
    Table,
    Column,
    MetaData,
    ForeignKey,
    Integer,
    String,
    DateTime,
    Enum,
    BigInteger,
    SmallInteger,
    Float,
    func,
    text,
)


class FTS3Operation:
    ALL_STATES = [
        "Submitted",
        "Canceled",
        "Failed",
    ]

    INIT_STATE = "Submitted"

    def __init__(self):
        self.ftsJob = []
        self.ftsFiles = []


class FTS3Job:
    ALL_STATES = [
        "Submitted",
        "Canceled",
        "Failed",
    ]

    INIT_STATE = "Submitted"


class FTS3File:
    ALL_STATES = ["New", "Canceled", "Staging", "Failed"]

    INIT_STATE = "New"


metadata = MetaData()
mapper_registry = registry()


fts3FileTable = Table(
    "Files",
    metadata,
    Column("fileID", Integer, primary_key=True),
    Column("operationID", Integer, ForeignKey("Operations.operationID", ondelete="CASCADE"), nullable=False),
    Column("fileName", String(255)),
    mysql_engine="InnoDB",
)

mapper_registry.map_imperatively(FTS3File, fts3FileTable)


fts3JobTable = Table(
    "Jobs",
    metadata,
    Column("jobID", Integer, primary_key=True),
    Column("operationID", Integer, ForeignKey("Operations.operationID", ondelete="CASCADE"), nullable=False),
    Column("ftsGUID", String(255)),
    mysql_engine="InnoDB",
)

mapper_registry.map_imperatively(FTS3Job, fts3JobTable)


fts3OperationTable = Table(
    "Operations",
    metadata,
    Column("operationID", Integer, primary_key=True),
    Column("username", String(255)),
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
            # https://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html#subquery-eager-loading
            cascade="all, delete-orphan",  # if a File is removed from the list,
            # remove it from the DB
            passive_deletes=True,  # used together with cascade='all, delete-orphan'
        ),
    },
)


@event.listens_for(engine.Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Make sure that the foreign keys are checked
    See https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#foreign-key-support
    """
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


engine = create_engine("sqlite+pysqlite:///:memory:", echo=True)

metadata.bind = engine
dbSession = sessionmaker(bind=engine)
metadata.create_all(engine)
session = dbSession()

# insert
with dbSession() as session:
    for i in range(10):
        op = FTS3Operation()
        op.username = str(i)
        f1 = FTS3File()
        f1.fileName = f"/{i}"
        op.ftsFiles.append(f1)

        j1 = FTS3Job()
        j1.ftsGUID = chr(65 + 2 * i)
        op.ftsJobs.append(j1)
        j2 = FTS3Job()
        j2.ftsGUID = chr(65 + 2 * i + 1)
        op.ftsJobs.append(j2)
        session.add(op)
    session.commit()


try:
    ftsOperations = []

    operationIDs = list(range(5))

    ftsOperations = session.query(FTS3Operation).filter(FTS3Operation.operationID.in_(operationIDs)).all()

    print(ftsOperations)
    session.commit()
    session.expunge_all()


except SQLAlchemyError as e:
    session.rollback()
finally:
    session.close()
