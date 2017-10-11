""" PilotsLoggingDB class is a front-end to the Pilots Logging Database.
    This database keeps track of all the submitted grid pilot jobs.
    It also registers the mapping of the DIRAC jobs to the pilot
    agents.

    Available methods are:

    addPilotsLogging()
    getPilotsLogging()
    deletePilotsLoggin()
    
"""

__RCSID__ = "$Id$"

from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
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

metadata = MetaData( )
Base = declarative_base( )


#############################################################################
class PilotsLoggingDB( ):
  def __init__( self ):

    result = getDBParameters( 'WorkloadManagement/PilotsLoggingDB' )
    if not result['OK']:
      raise RuntimeError( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result['Value']
    self.dbHost = dbParameters['Host']
    self.dbPort = dbParameters['Port']
    self.dbUser = dbParameters['User']
    self.dbPass = dbParameters['Password']
    self.dbName = dbParameters['DBName']

    self.__initializeConnection( 'WorkloadManagement/PilotsLoggingDB' )
    resp = self.__initializeDB( )
    if not resp['OK']:
      raise Exception( "Couldn't create tables: " + resp['Message'] )

  ##########################################################################################

  def __initializeConnection( self, dbPath ):

    self.engine = create_engine( 'mysql://%s:%s@%s:%s/%s'
                                 % (self.dbUser, self.dbPass, self.dbHost, self.dbPort, self.dbName),
                                 pool_recycle = 3600, echo_pool = True )
    self.sqlalchemySession = scoped_session( sessionmaker( bind = self.engine ) )
    self.inspector = Inspector.from_engine( self.engine )

  ##########################################################################################
  def __initializeDB( self ):

    tablesInDB = self.inspector.get_table_names( )

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
      gLogger.debug( "Table PilotsLogging exists" )

    return S_OK( )

  ##########################################################################################
  def addPilotsLogging( self, pilotUUID, timestamp, source, phase, status, messageContent ):
    """Add new pilot logging entry"""

    session = self.sqlalchemySession( )
    logging = PilotsLogging( pilotUUID, timestamp, source, phase, status, messageContent )

    try:
      session.add( logging )
    except SQLAlchemyError as e:
      session.rollback( )
      session.close( )
      return S_ERROR( DErrno.ESQLA, "Failed to add PilotsLogging: " + e.message )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to commit PilotsLogging: " + e.message)
      session.rollback( )
      session.close( )

    return S_OK( )

  ##########################################################################################
  def getPilotsLogging( self, pilotUUID ):
    """Get list of logging entries for pilot"""

    session = self.sqlalchemySession( )

    pilotLogging = []
    for pl in session.query( PilotsLogging ).filter(PilotsLogging.pilotUUID == pilotUUID ).order_by(
        PilotsLogging.timeStamp ).all( ):
      entry = {}
      entry['pilotUUID'] = pl.pilotUUID
      entry['timestamp'] = pl.timestamp
      entry['source'] = pl.source
      entry['phase'] = pl.phase
      entry['status'] = pl.status
      entry['messageContent'] = pl.messageContent
      pilotLogging.append( entry )

    return S_OK( pilotLogging )

  ##########################################################################################
  def deletePilotsLogging( self, pilotUUID ):
    """Delete all logging entries for pilot"""

    if isinstance( pilotUUID, basestring ):
      pilotUUID = [pilotUUID, ]

    session = self.sqlalchemySession( )

    session.query( PilotsLogging ).filter( PilotsLogging.pilotUUID._in( pilotUUID ) ).delete(
      synchronize_session = 'fetch' )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to commit: " + e.message)
      session.rollback( )
      session.close( )

    return S_OK( )

  ##########################################################################################
  def addPilotsUUID( self, pilotUUID ):
    """Add new pilot UUID to UUID ID mapping, not knowing ID yet"""

    session = self.sqlalchemySession( )

    resp = session.query( PilotsUUIDtoID ).filter( PilotsUUIDtoID.pilotUUID == pilotUUID ).count( )
    if resp > 0:
      return S_OK( )

    uuid2id = PilotsUUIDtoID( pilotUUID )
    try:
      session.add( uuid2id )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to add PilotsUUIDtoID: " + e.message)
      session.rollback( )
      session.close( )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to commit PilotsUUIDtoID: " + e.message)
      session.rollback( )
      session.close( )

    return S_OK( )

  ##########################################################################################
  def setPilotsUUIDtoIDMapping( self, pilotUUID, pilotID ):
    """Assign pilot ID to UUID"""

    session = self.sqlalchemySession( )

    mapping = session.query( PilotsUUIDtoID ).get( pilotUUID )
    mapping.pilotID = pilotID
    try:
      session.commit( )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to commit PilotsUUIDtoID mapping: " + e.message)
      session.rollback( )
      session.close( )

    return S_OK( )

  ##########################################################################################
  def addPilotsUUIDtoIDmapping( self, pilotUUID, pilotID ):
    """Add new pilot UUID to ID mapping"""

    session = self.sqlalchemySession( )

    uuid2id = PilotsUUIDtoID( pilotUUID, pilotID )
    try:
      session.add( uuid2id )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to add PilotsUUIDtoID: " + e.message)
      session.rollback( )
      session.close( )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      return S_ERROR("Failed to commit PilotsUUIDtoID: " + e.message)
      session.rollback( )
      session.close( )

    return S_OK( )


##########################################################################################

class PilotsLogging( Base ):
  __tablename__ = 'PilotsLogging'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  logID = Column( 'LogID', Integer, primary_key = True, autoincrement = True )
  pilotUUID = Column( 'pilotUUID', String( 255 ), nullable = False )
  timestamp = Column( 'timestamp', String( 255 ), nullable = False )
  source = Column( 'source', String( 255 ), nullable = False )
  phase = Column( 'phase', String( 255 ), nullable = False )
  status = Column( 'status', String( 255 ), nullable = False )
  messageContent = Column( 'messageContent', String( 255 ), nullable = False )

  def __init__( self, pilotUUID, timestamp, source, phase, status, messageContent ):
    self.pilotUUID = pilotUUID
    self.timestamp = timestamp
    self.source = source
    self.phase = phase
    self.status = status
    self.messageContent = messageContent
