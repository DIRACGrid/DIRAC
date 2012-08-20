########################################################################
# $HeadURL$
########################################################################
""" DataLoggingHandler is the implementation of the Data Logging
    service in the DISET framework.

    The following methods are available in the Service interface::

    * addFileRecord()
    * addFileRecords()
    * getFileLoggingInfo()

"""
__RCSID__ = "$Id$"

## imports
import os
from types import StringType, ListType, TupleType, DictType
## from DIRAC
from DIRAC import gLogger, gConfig, rootPath, S_OK, S_ERROR
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB
from DIRAC.ConfigurationSystem.Client import PathFinder

## global instance of the DataLoggingDB
logDB = False

def initializeDataLoggingHandler( serviceInfo ):
  """ handler initialisation """
  global logDB
  logDB = DataLoggingDB()

  res = logDB._connect()
  if not res['OK']:
    return res
  res = logDB._checkTable()
  if not res['OK'] and not res['Message'] == 'The requested table already exist':
    return res

  return S_OK()

class DataLoggingHandler( RequestHandler ):
  """ 
  .. class:: DataLoggingClient
  
  Request handler for DataLogging service.
  """

  types_addFileRecord = [ [StringType, ListType], StringType, StringType, StringType, StringType ]
  def export_addFileRecord( self, lfn, status, minor, date, source ):
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
    result = logDB.addFileRecord( lfns, status, minor, date, source )
    return result

  types_addFileRecords = [ [ ListType, TupleType ] ]
  def export_addFileRecords( self, fileTuples ):
    """ Add a group of logging records
    """
    result = logDB.addFileRecords( fileTuples )
    return result

  types_getFileLoggingInfo = [ StringType ]
  def export_getFileLoggingInfo( self, lfn ):
    """ Get the file logging information
    """
    result = logDB.getFileLoggingInfo( lfn )
    return result

  types_getUniqueStates = []
  def export_getUniqueStates( self ):
    """ Get all the unique states
    """
    result = logDB.getUniqueStates()
    return result

