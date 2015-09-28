""" PilotsLoggingDB class is a front-end to the Pilots Logging Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:


"""

__RCSID__ = "$Id$"

from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE, getCESiteMapping
import DIRAC.Core.Utilities.Time as Time
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getDNForUsername
from types import IntType, LongType, ListType
import threading

from sqlalchemy.sql.schema import ForeignKey, PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import DateTime
from sqlalchemy.orm import session, sessionmaker, scoped_session, mapper
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.exc import SQLAlchemyError

import datetime
import time

DEBUG = 1

metadata = MetaData()
Base = declarative_base()

#############################################################################
class PilotsLoggingDB( DB ):

  def __init__( self ):

    DB.__init__( self, 'PilotAgentsDB', 'WorkloadManagement/PilotsLoggingDB' )
    self.lock = threading.Lock()

    self.__initializeConnection('WorkloadManagement/PilotsLoggingDB')
    resp = self.__initializeDB()
    if not resp['OK']:
      raise Exception("Couldn't create tables: " + resp['Message'])

##########################################################################################
  def __initializeConnection(self, dbPath):

      self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s'
                                   %( self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName ),
                                   pool_recycle = 3600, echo_pool = True )
      self.sqlalchemySession = scoped_session( sessionmaker( bind = self.engine ) )
      self.inspector = Inspector.from_engine( self.engine )

##########################################################################################
  def __initializeDB( self ):

    tablesInDB = self.inspector.get_table_names()

    if not 'PilotsUUIDtoID' in tablesInDB:
      try:
        PilotsUUIDtoID.__table__.create( self.engine )
      except SQLAlchemyError as e:
        return S_ERROR(e)
    else:
      gLogger.debug("Table PilotsUUIDtoID exists")
      return S_OK()

    if not 'PilotsLogging' in tablesInDB:
      try:
        PilotsLogging.__table__.create( self.engine )
      except SQLAlchemyError as e:
        return S_ERROR(e)
    else:
      gLogger.debug("Table PilotsLogging exists")

##########################################################################################
  def addPilotsLogging(self, pilotUUID, status, minorStatus, timeStamp, source):
    """Add new pilot logging entry"""

    session = self.sqlalchemySession()
    logging = PilotsLogging(pilotUUID, status, minorStatus, timeStamp, source)

    try:
      session.add(logging)
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to add PilotsLogging: " + e.message)

    try:
      session.commit()
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to commit PilotsLogging: " + e.message)

    return S_OK()

##########################################################################################
  def getPilotsLogging(self, pilotID):
    """Get list of logging entries for pilot"""

    session = self.sqlalchemySession()

    pilotLogging = []
    for pl in session.query(PilotsLogging).join(PilotsUUIDtoID).filter(PilotsUUIDtoID.pilotID == pilotID).order_by(PilotsLogging.timeStamp).all():
      entry = {}
      entry['PilotUUID'] = pl.pilotUUID
      entry['PilotID'] = pilotID
      entry['Status'] = pl.status
      entry['MinorStatus'] = pl.minorStatus
      entry['TimeStamp'] = time.mktime(pl.timeStamp.timetuple())
      entry['Source'] = pl.source
      pilotLogging.append(entry)

    return S_OK(pilotLogging)
##########################################################################################
  def deletePilotsLogging(self, pilotID):
    """Delete all logging entries for pilot"""

    session = self.sqlalchemySession()

    #session.query(PilotsLogging).join(PilotsUUIDtoID).filter(PilotsUUIDtoID.pilotID == pilotID).delete(synchronize_session = 'fetch')
    session.query(PilotsUUIDtoID).filter(PilotsUUIDtoID.pilotID == pilotID).delete()

    try:
      session.commit()
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to commit: " + e.message)

    return S_OK()

##########################################################################################
  def addPilotsUUID(self, pilotUUID):
    """Add new pilot UUID to UUID ID mapping, not knowing ID yet"""

    session = self.sqlalchemySession()

    resp = session.query(PilotsUUIDtoID).filter(PilotsUUIDtoID.pilotUUID == pilotUUID).count()
    if resp > 0:
      return S_OK()

    uuid2id = PilotsUUIDtoID(pilotUUID)
    try:
      session.add(uuid2id)
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to add PilotsUUIDtoID: " + e.message)

    try:
      session.commit()
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to commit PilotsUUIDtoID: " + e.message)

    return S_OK()

##########################################################################################
  def setPilotsUUIDtoIDMapping(self, pilotUUID, pilotID):
    """Assign pilot ID to UUID"""

    session = self.sqlalchemySession()

    mapping = session.query(PilotsUUIDtoID).get(pilotUUID)
    mapping.pilotID = pilotID
    try:
      session.commit()
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to commit PilotsUUIDtoID mapping: " + e.message)

    return S_OK()

##########################################################################################
  def addPilotsUUIDtoIDmapping(self, pilotUUID, pilotID):
    """Add new pilot UUID to ID mapping"""

    session = self.sqlalchemySession()

    uuid2id = PilotsUUIDtoID(pilotUUID, pilotID)
    try:
      session.add(uuid2id)
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to add PilotsUUIDtoID: " + e.message)

    try:
      session.commit()
    except SQLAlchemyError as e:
      session.rollback()
      session.close()
      return S_ERROR("Failed to commit PilotsUUIDtoID: " + e.message)

    return S_OK()

##########################################################################################

class PilotsLogging( Base ):

  __tablename__ = 'PilotsLogging'
  __table_args__ = {
                    'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'
                    }

  logID = Column( 'LogID', Integer, primary_key = True, autoincrement = True )
  pilotUUID = Column( 'PilotUUID', String(255), ForeignKey( 'PilotsUUIDtoID.PilotUUID', ondelete='CASCADE' ), nullable = False)
  status = Column( 'Status', String( 32 ), default = '', nullable = False )
  minorStatus = Column( 'MinorStatus', String( 128 ), default = '', nullable = False )
  timeStamp = Column( 'TimeStamp', DateTime, nullable = False )
  source = Column( 'Source', String(32), default = 'Unknown', nullable = False )

  def __init__(self, pilotUUID, status, minorStatus, timeStamp, source):
    self.pilotUUID = pilotUUID
    self.status = status
    self.minorStatus = minorStatus
    self.timeStamp = datetime.datetime.fromtimestamp(timeStamp)
    self.source = source


class PilotsUUIDtoID( Base ):

  __tablename__ = 'PilotsUUIDtoID'
  __table_args__ = {
                    'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'
                    }


  pilotUUID = Column( 'PilotUUID', String(255), primary_key = True )
  pilotID = Column ( 'PilotID', Integer, nullable = True )
  pilotLogs = relationship("PilotsLogging", backref="UUIDtoID")

  def __init__(self, pilotUUID, pilotID = None):
    self.pilotUUID = pilotUUID
    self.pilotID = pilotID
