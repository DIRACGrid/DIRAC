########################################################################
# $Id: DataLoggingHandler.py,v 1.1 2008/02/18 18:40:23 atsareg Exp $
########################################################################

""" DataLoggingHandler is the implementation of the Data Logging
    service in the DISET framework

    The following methods are available in the Service interface

    addFileRecord()
    getFileLoggingInfo()

"""

__RCSID__ = "$Id: DataLoggingHandler.py,v 1.1 2008/02/18 18:40:23 atsareg Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.DataLoggingDB import DataLoggingDB

# This is a global instance of the DataLoggingDB class
logDB = False

def initializeDataLoggingHandler( serviceInfo ):

  global logDB
  logDB = DataLoggingDB()
  return S_OK()

class DataLoggingHandler( RequestHandler ):

  ###########################################################################
  types_addFileRecord = [StringType,StringType]
  def export_addFileRecord(self,lfn,status,date='',source='Unknown'):
    """ Add a logging record for the given file
    """

    result = logDB.addFileRecord(lfn,status,date,source)
    return result

  ###########################################################################
  types_getFileLoggingInfo = [StringType]
  def export_getFileLoggingInfo(self,lfn):
    """ Get the file logging information
    """
    result = logDB.getFileLoggingInfo(lfn)
    return result

