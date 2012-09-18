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
#MAGIC_EPOC_NUMBER = 1270000000
NEW_MAGIC_EPOCH_2K = 323322400

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
      time_order = Time.to2K( _date ) - NEW_MAGIC_EPOCH_2K
    except AttributeError:
      gLogger.error( 'Wrong date argument given using current time stamp' )
      date = Time.dateTime()
      time_order = Time.to2K( date ) - NEW_MAGIC_EPOCH_2K

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

def test():
  """ Some test cases
  """

  # building up some fake CS values
  gConfig.setOptionValue( 'DIRAC/Setup', 'Test' )
  gConfig.setOptionValue( '/DIRAC/Setups/Test/DataManagement', 'Test' )

  host = '127.0.0.1'
  user = 'Dirac'
  pwd = 'Dirac'
  db = 'AccountingDB'

  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataLoggingDB/Host', host )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataLoggingDB/DBName', db )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataLoggingDB/User', user )
  gConfig.setOptionValue( '/Systems/DataManagement/Test/Databases/DataLoggingDB/Password', pwd )

  db = DataLoggingDB()
  assert db._connect()['OK']

  lfns = ['/Test/00001234/File1', '/Test/00001234/File2']
  status = 'TestStatus'
  minor = 'MinorStatus'
  date1 = Time.toString()
  date2 = Time.dateTime()
  source = 'Somewhere'

  fileTuples = ( ( lfns[0], status, minor, date1, source ), ( lfns[1], status, minor, date2, source ) )

  try:
    gLogger.info( '\n Creating Table\n' )
    # Make sure it is there and it has been created for this test
    result = db._checkTable()
    assert result['OK']

    result = db._checkTable()
    assert not result['OK']
    assert result['Message'] == 'The requested table already exist'

    gLogger.info( '\n Inserting some records\n' )

    result = db.addFileRecord( lfns, status, date = '2012-04-28 09:49:02.545466' )
    assert result['OK']
    assert result['Value'] == 2
    assert result['lastRowId'] == 2

    result = db.addFileRecords( fileTuples )
    assert result['OK']

    gLogger.info( '\n Retrieving some records\n' )

    result = db.getFileLoggingInfo( lfns[0] )
    assert result['OK']
    assert len( result['Value'] ) == 2

    result = db.getFileLoggingInfo( lfns[1] )
    assert result['OK']
    assert len( result['Value'] ) == 2

    result = db.getUniqueStates()
    assert result['OK']
    assert result['Value'] == [status]


    gLogger.info( '\n Removing Table\n' )
    result = db._update( 'DROP TABLE `%s`' % db.tableName )
    assert result['OK']

    gLogger.info( '\n OK\n' )


  except AssertionError:
    print 'ERROR ',
    if not result['OK']:
      print result['Message']
    else:
      print result

    sys.exit( 1 )

if __name__ == '__main__':
  from DIRAC.Core.Base import Script
  Script.parseCommandLine()
  gLogger.setLevel( 'VERBOSE' )

  if 'PYTHONOPTIMIZE' in os.environ and os.environ['PYTHONOPTIMIZE']:
    gLogger.info( 'Unset pyhthon optimization "PYTHONOPTIMIZE"' )
    sys.exit( 0 )

  test()
