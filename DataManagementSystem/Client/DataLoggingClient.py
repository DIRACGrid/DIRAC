########################################################################
# $HeadURL $
# File: DataLoggingClient.py
########################################################################
""" Client for DataLoggingDB
"""
__RCSID__ = "$Id$"

## imports
from DIRAC import gLogger, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder

class DataLoggingClient:
  """ Client for DataLoggingDB
  """
  def __init__(self, url=False, useCertificates=False ):
    """ Constructor of the DataLogging client

    :param self: self reference
    :param str url: service URL
    :param useCertificates: flag to use certificates
    """
    try:
      if not url:
        self.url = PathFinder.getServiceURL( "DataManagement/DataLogging" )
      else:
        self.url = url
    except Exception, error:
      gLogger.exception( "DataLoggingClient.__init__: Exception while obtaining service URL.",
                         lException = error )

  def addFileRecords( self, fileTuples ):
    """ add records for files

    :param self: self reference
    :param list fileTuples: list of tuples with file information
    """
    if not self.url:
      gLogger.warn("addFileRecords: service URL is NOT defined!")
      return S_OK()

    try:
      client = RPCClient( self.url, timeout=120 )
      return client.addFileRecords( fileTuples )
    except Exception, error:
      errStr = "DataLoggingClient.addFileRecords: Exception while adding file records."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)

  def addFileRecord( self, lfn, status, minor, date, source ):
    """ add record for LFN :lfn: 
     
    :param self: self reference
    :param str lfn: LFN
    :param str status: file status
    :param str minor: additional information
    :param mixed date: datetime.datetime or str(datetime.datetime) or ""
    :param str source: source of new state
    """
    if not self.url:
      gLogger.warn("addFileRecord: service URL is NOT defined!")
      return S_OK()
    try:
      client = RPCClient( self.url, timeout=120 )
      return client.addFileRecord( lfn, status, minor, date, source )
    except Exception, error:
      errStr = "DataLoggingClient.addFileRecord: Exception while adding file record."
      gLogger.exception( errStr, lException=error )
      return S_ERROR(errStr)
