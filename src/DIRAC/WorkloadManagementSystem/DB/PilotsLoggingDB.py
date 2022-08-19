""" PilotsLoggingDB class is a front-end to the Pilots Logging Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:

    addPilotsLogging()
    getPilotsLogging()
    deletePilotsLoggin()

"""
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.ResourceStatusSystem.Utilities import Utils

from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine, Column, MetaData, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.exc import SQLAlchemyError


TABLESLIST = ["PilotsLogging"]

metadata = MetaData()
Base = declarative_base()


#############################################################################
class PilotsLoggingDB:
    def __init__(self, parentLogger=None):

        if not parentLogger:
            parentLogger = gLogger
        self.log = parentLogger.getSubLogger(self.__class__.__name__)

        result = getDBParameters("WorkloadManagement/PilotsLoggingDB")
        if not result["OK"]:
            raise RuntimeError("Cannot get database parameters: %s" % result["Message"])

        dbParameters = result["Value"]
        self.dbHost = dbParameters["Host"]
        self.dbPort = dbParameters["Port"]
        self.dbUser = dbParameters["User"]
        self.dbPass = dbParameters["Password"]
        self.dbName = dbParameters["DBName"]

        # These are the list of tables that will be created.
        # They can be extended in an extension module
        self.tablesList = getattr(Utils.voimport("DIRAC.WorkloadManagementSystem.DB.PilotsLoggingDB"), "TABLESLIST")

        self.__initializeConnection()
        resp = self.__initializeDB()
        if not resp["OK"]:
            raise Exception("Couldn't create tables: " + resp["Message"])

    ##########################################################################################

    def __initializeConnection(self):
        """
        This should be in a base class eventually
        """

        self.engine = create_engine(
            f"mysql://{self.dbUser}:{self.dbPass}@{self.dbHost}:{self.dbPort}/{self.dbName}",
            pool_recycle=3600,
            echo_pool=True,
            echo=self.log.getLevel() == "DEBUG",
        )
        self.sqlalchemySession = scoped_session(sessionmaker(bind=self.engine))
        self.inspector = Inspector.from_engine(self.engine)

    ##########################################################################################
    def __initializeDB(self):
        """
        Create the tables, if they are not there yet
        """

        tablesInDB = self.inspector.get_table_names()

        for table in self.tablesList:
            if table not in tablesInDB:
                found = False
                # is it in the extension? (fully or extended)
                for ext in gConfig.getValue("DIRAC/Extensions", []):
                    try:
                        getattr(__import__(ext + __name__, globals(), locals(), [table]), table).__table__.create(
                            self.engine
                        )  # pylint: disable=no-member
                        found = True
                        break
                    except (ImportError, AttributeError):
                        continue
                    # If not found in extensions, import it from DIRAC base.
                if not found:
                    getattr(__import__(__name__, globals(), locals(), [table]), table).__table__.create(
                        self.engine
                    )  # pylint: disable=no-member
            else:
                gLogger.debug("Table %s already exists" % table)

        return S_OK()

    ##########################################################################################
    def addPilotsLogging(self, pilotUUID, timestamp, source, phase, status, messageContent):
        """Add new pilot logging entry"""

        session = self.sqlalchemySession()
        logging = PilotsLogging(pilotUUID, timestamp, source, phase, status, messageContent)

        try:
            session.add(logging)
        except SQLAlchemyError as e:
            session.rollback()
            session.close()
            return S_ERROR(DErrno.ESQLA, "Failed to add PilotsLogging: " + str(e))

        try:
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            session.close()
            return S_ERROR(DErrno.ESQLA, "Failed to commit PilotsLogging: " + str(e))

        return S_OK()

    ##########################################################################################
    def getPilotsLogging(self, pilotUUID):
        """Get list of logging entries for pilot"""

        session = self.sqlalchemySession()

        pilotLogging = []
        for pl in (
            session.query(PilotsLogging)
            .filter(PilotsLogging.pilotUUID == pilotUUID)
            .order_by(PilotsLogging.timestamp)
            .all()
        ):
            entry = {}
            entry["pilotUUID"] = pl.pilotUUID
            entry["timestamp"] = pl.timestamp
            entry["source"] = pl.source
            entry["phase"] = pl.phase
            entry["status"] = pl.status
            entry["messageContent"] = pl.messageContent
            pilotLogging.append(entry)

        return S_OK(pilotLogging)

    ##########################################################################################
    def deletePilotsLogging(self, pilotUUID):
        """Delete all logging entries for pilot"""

        if isinstance(pilotUUID, str):
            pilotUUID = [
                pilotUUID,
            ]

        session = self.sqlalchemySession()

        session.query(PilotsLogging).filter(PilotsLogging.pilotUUID.in_(pilotUUID)).delete(synchronize_session="fetch")

        try:
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            session.close()
            return S_ERROR(DErrno.ESQLA, "Failed to commit: " + str(e))

        return S_OK()

    ##########################################################################################


class PilotsLogging(Base):
    """PilotsLogging table"""

    __tablename__ = "PilotsLogging"
    __table_args__ = {"mysql_engine": "InnoDB", "mysql_charset": "utf8"}

    logID = Column("LogID", Integer, primary_key=True, autoincrement=True)
    pilotUUID = Column("pilotUUID", String(255), nullable=False)
    timestamp = Column("timestamp", String(255), nullable=False)
    source = Column("source", String(255), nullable=False)
    phase = Column("phase", String(255), nullable=False)
    status = Column("status", String(255), nullable=False)
    messageContent = Column("messageContent", String(255), nullable=False)

    def __init__(self, pilotUUID, timestamp, source, phase, status, messageContent):
        self.pilotUUID = pilotUUID
        self.timestamp = timestamp
        self.source = source
        self.phase = phase
        self.status = status
        self.messageContent = messageContent
