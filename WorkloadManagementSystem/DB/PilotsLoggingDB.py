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
from DIRAC.Core.Utilities import DErrno
from DIRAC.ConfigurationSystem.Client.Utilities import getDBParameters
from DIRAC.Core.Utilities.SiteCEMapping import getSiteForCE, getCESiteMapping
import DIRAC.Core.Utilities.Time as Time
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUsernameForDN, getDNForUsername
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
class PilotsLoggingDB(  ):

  def __init__( self ):

    result = getDBParameters( 'WorkloadManagement/PilotsLoggingDB' )
    if not result['OK']:
      raise RuntimeError( 'Cannot get database parameters: %s' % result['Message'] )

    dbParameters = result[ 'Value' ]
    self.dbHost = dbParameters[ 'Host' ]
    self.dbPort = dbParameters[ 'Port' ]
    self.dbUser = dbParameters[ 'User' ]
    self.dbPass = dbParameters[ 'Password' ]
    self.dbName = dbParameters[ 'DBName' ]

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

    if not 'PilotsLogging' in tablesInDB:
      try:
        PilotsLogging.__table__.create( self.engine )
      except SQLAlchemyError as e:
        return S_ERROR( DErrno.ESQLA, e )
    else:
      gLogger.debug( "Table PilotsLogging exists" )

    return S_OK( )

  ##########################################################################################
  def addPilotsLogging( self, pilotRef, status, minorStatus, timeStamp, source ):
    """Add new pilot logging entry"""

    session = self.sqlalchemySession( )
    logging = PilotsLogging( pilotRef, status, minorStatus, timeStamp, source )

    try:
      session.add( logging )
    except SQLAlchemyError as e:
      session.rollback( )
      session.close( )
      return S_ERROR( DErrno.ESQLA, "Failed to add PilotsLogging: " + e.message )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      session.rollback( )
      session.close( )
      return S_ERROR( DErrno.ESQLA, "Failed to commit PilotsLogging: " + e.message )

    return S_OK( )

  ##########################################################################################
  def getPilotsLogging( self, pilotRef ):
    """Get list of logging entries for pilot"""

    session = self.sqlalchemySession( )

    pilotLogging = []
    for pl in session.query( PilotsLogging ).filter(PilotsLogging.pilotRef == pilotRef ).order_by(
        PilotsLogging.timeStamp ).all( ):
      entry = {}
      entry['PilotRef'] = pl.pilotRef
      entry['Status'] = pl.status
      entry['MinorStatus'] = pl.minorStatus
      entry['TimeStamp'] = time.mktime( pl.timeStamp.timetuple( ) )
      entry['Source'] = pl.source
      pilotLogging.append( entry )

    return S_OK( pilotLogging )

  ##########################################################################################
  def deletePilotsLogging( self, pilotRef ):
    """Delete all logging entries for pilot"""

    if isinstance( pilotRef, basestring ):
      pilotRef = [pilotRef, ]

    session = self.sqlalchemySession( )

    session.query( PilotsLogging ).filter( PilotsLogging.pilotRef == pilotRef ).delete(
      synchronize_session = 'fetch' )

    try:
      session.commit( )
    except SQLAlchemyError as e:
      session.rollback( )
      session.close( )
      return S_ERROR( DErrno.ESQLA, "Failed to commit: " + e.message )

    return S_OK( )

##########################################################################################

class PilotsLogging( Base ):
  __tablename__ = 'PilotsLogging'
  __table_args__ = {
    'mysql_engine': 'InnoDB',
    'mysql_charset': 'utf8'
  }

  logID = Column( 'LogID', Integer, primary_key = True, autoincrement = True )
  pilotRef = Column( 'PilotRef', String( 255 ), nullable = False )
  status = Column( 'Status', String( 32 ), default = '', nullable = False )
  minorStatus = Column( 'MinorStatus', String( 128 ), default = '', nullable = False )
  timeStamp = Column( 'TimeStamp', DateTime, nullable = False )
  source = Column( 'Source', String( 32 ), default = 'Unknown', nullable = False )

  def __init__( self, pilotRef, status, minorStatus, timeStamp, source ):
    self.pilotRef = pilotRef
    self.status = status
    self.minorStatus = minorStatus
    self.timeStamp = datetime.datetime.fromtimestamp( timeStamp )
    self.source = source


