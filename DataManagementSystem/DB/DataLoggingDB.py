########################################################################
# $HeadURL$
########################################################################
""" :mod: DataLoggingDB
    ===================
 
    .. module: DataLoggingDB
    :synopsis: front-end to the Data Logging database

    The following methods are provided:

    addFileRecord()
    getFileLoggingInfo()
"""

__RCSID__ = "$Id$"

## imports
import os
import sys
from types import StringTypes
## from DIRAC
from DIRAC.Core.Utilities import Time
from DIRAC import gConfig, gLogger, S_OK
from DIRAC.Core.Base.DB import DB

## DIRAC epoc timestamp
MAGIC_EPOC_NUMBER = 1270000000

#############################################################################
class DataLoggingDB( DB ):
  """ .. class:: DataLoggingDB 

  Python interface to DataLoggingDB.

  DROP TABLE IF EXISTS DataLoggingInfo;
  CREATE TABLE DataLoggingInfo (
    FileID INTEGER NOT NULL AUTO_INCREMENT,
    LFN VARCHAR(255) NOT NULL,
    Status VARCHAR(255) NOT NULL,
    MinorStatus VARCHAR(255) NOT NULL DEFAULT 'Unknown',
    StatusTime DATETIME,
    StatusTimeOrder DOUBLE(11,3) NOT NULL,
    Source VARCHAR(127) NOT NULL DEFAULT 'Unknown',
    PRIMARY KEY (FileID),
    INDEX (LFN)
    );
  """
  ## table name
  tableName = 'DataLoggingInfo'
  ## table def
  tableDict = { tableName: { 'Fields' : { 'FileID': 'INTEGER NOT NULL AUTO_INCREMENT',
                                          'LFN': 'VARCHAR(255) NOT NULL',
                                          'Status': 'VARCHAR(255) NOT NULL',
                                          'MinorStatus': 'VARCHAR(255) NOT NULL DEFAULT "Unknown" ',
                                          'StatusTime':'DATETIME NOT NULL',
                                          'StatusTimeOrder': 'DOUBLE(11,3) NOT NULL',
                                          'Source': 'VARCHAR(127) NOT NULL DEFAULT "Unknown"',
                                         },
                             'PrimaryKey': 'FileID',
                             'Indexes': { 'LFN': ['LFN']
                                         }
                            }
               }

  def __init__( self, maxQueueSize = 10 ):
    """ c'tor

    :param self: self reference
    :param int maxQueueSize: query queue size
    """
    DB.__init__( self, "DataLoggingDB", "DataManagement/DataLoggingDB", maxQueueSize )
    self.gLogger = gLogger

  def _checkTable( self ):
    """ Make sure the table is created
    """
    return self._createTables( self.tableDict, force = False )

  def addFileRecords( self, fileTuples ):
    """ Simple wrapper around multiple insertion of file records

    :param self: self reference
    :param list fileTuples: list of tuples ( lfn, status, minor, date, source )
    """
    result = S_OK( 0 )
    for lfn, status, minor, date, source in fileTuples:
      res = self.addFileRecord( [lfn], status, minor, date, source )
      if not res["OK"]:
        return res
      result['Value'] += res['Value']
      result['lastRowId'] = res['lastRowId']
    return result

  def addFileRecord( self, lfns, status, minor = "Unknown", date = None, source = "Unknown" ):
    """ Add a new entry to the DataLoggingDB table. 

    :warning: Optionally the time stamp of the status can be provided in a form of a string 
    in a format '%Y-%m-%d %H:%M:%S' or as datetime.datetime object. If the time stamp is not 
    provided the current UTC time is used.

    :param self: self reference
    :param list lfns: list of LFNs
    :param str status: status
    :param str minor: additional information
    :param mixed date: date and time
    :param str source: source setting the new status
    """
    self.gLogger.info( "Entering records for %s lfns: %s/%s from source %s" % ( len( lfns ), status, minor, source ) )
    _date = date
    if not date:
      _date = Time.dateTime()
    if type( date ) in StringTypes:
      _date = Time.fromString( date )

    try:
      time_order = Time.toEpoch( _date )
    except AttributeError:
      gLogger.error( 'Wrong date argument given using current time stamp' )
      date = Time.dateTime()
      time_order = Time.toEpoch( _date )

    # Reduce to a smallest number and add more precision
    time_order = time_order - MAGIC_EPOC_NUMBER + _date.microsecond / 1000000.

    inDict = { 'Status': status,
               'MinorStatus': minor,
               'StatusTime': date,
               'StatusTimeOrder': time_order,
               'Source': source
              }
    result = S_OK( 0 )
    for lfn in lfns:
      inDict['LFN'] = lfn
      res = self.insertFields( self.tableName, inDict = inDict )
      if not res['OK']:
        return res
      result['Value'] += res['Value']
      result['lastRowId'] = res['lastRowId']
    return result

  def getFileLoggingInfo( self, lfn ):
    """ Returns a Status,StatusTime,StatusSource tuple
        for each record found for the file specified by its LFN in historical order
    """
    return self.getFields( self.tableName, ['Status', 'MinorStatus', 'StatusTime', 'Source'],
                           condDict = {'LFN': lfn }, orderAttribute = ['StatusTimeOrder', 'StatusTime'] )

  def getUniqueStates( self ):
    """ Returns the distinct status from the data logging DB
    """
    return self.getDistinctAttributeValues( self.tableName, 'Status' )
