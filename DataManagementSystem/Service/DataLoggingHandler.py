########################################################################
# $HeadURL$
########################################################################
""" 
:mod: DataLoggingHandler

 .. module: DataLoggingHandler
 :synopsis: DataLoggingHandler is the implementation of the Data Logging
service in the DISET framework.

The following methods are available in the Service interface::

* addFileRecord()
* addFileRecords()
* getFileLoggingInfo()

"""

__RCSID__ = "$Id$"

## imports
from types import StringType, ListType, TupleType
## from DIRAC
from DIRAC import S_OK
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB

## global instance of the DataLoggingDB
gDataLoggingDB = False

def initializeDataLoggingHandler( serviceInfo ):
  """ handler initialisation """
  global gDataLoggingDB
  gDataLoggingDB = DataLoggingDB()

  res = gDataLoggingDB._connect()
  if not res['OK']:
    return res
  res = gDataLoggingDB._checkTable()
  if not res['OK'] and not res['Message'] == 'The requested table already exist':
    return res
  return S_OK()

class DataLoggingHandler( RequestHandler ):
  """ 
  .. class:: DataLoggingClient
  
  Request handler for DataLogging service.
  """

  types_addFileRecord = [ [StringType, ListType], StringType, StringType, StringType, StringType ]
  @staticmethod
  def export_addFileRecord( lfn, status, minor, date, source ):
    """ Add a logging record for the given file
    
    :param self: self reference
    :param mixed lfn: list of strings or a string with LFN
    :param str status: file status
    :param str minor: minor status (additional information)
    :param mixed date: datetime.datetime or str(datetime.datetime) or ""
    :param str source: source setting a new status 
    """
    if type( lfn ) == StringType:
      lfns = [ lfn ]
    else:
      lfns = lfn
    return gDataLoggingDB.addFileRecord( lfns, status, minor, date, source )

  types_addFileRecords = [ [ ListType, TupleType ] ]
  @staticmethod
  def export_addFileRecords( fileTuples ):
    """ Add a group of logging records
    """
    return gDataLoggingDB.addFileRecords( fileTuples )

  types_getFileLoggingInfo = [ StringType ]
  @staticmethod
  def export_getFileLoggingInfo( lfn ):
    """ Get the file logging information
    """
    return gDataLoggingDB.getFileLoggingInfo( lfn )
  
  types_getUniqueStates = []
  @staticmethod
  def export_getUniqueStates():
    """ Get all the unique states
    """
    return gDataLoggingDB.getUniqueStates()
  
