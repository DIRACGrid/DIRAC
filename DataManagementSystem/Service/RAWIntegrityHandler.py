""" Simple DISET interface to the RAW Integrity DB to allow access to the RAWIntegrityDB
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.RAWIntegrityDB import RAWIntegrityDB

rawIntegrityDB = False

def initializeRAWIntegrityHandler(serviceInfo):
  global rawIntegrityDB
  rawIntegrityDB = RAWIntegrityDB()
  return S_OK()

class RAWIntegrityHandler(RequestHandler):

  types_addFile = [StringType,StringType,IntType,StringType,StringType,StringType]
  def export_addFile(self,lfn,pfn,size,se,guid,checksum):
    """ Add a file to the RAW integrity DB
    """
    try:
      gLogger.info("RAWIntegrityHandler.addFile: Attempting to add %s to the database." % lfn)
      res = rawIntegrityDB.addFile(lfn,pfn,size,se,guid,checksum)
      return res
    except Exception,x:
      errStr = "RAWIntegrityHandler.addFile: Exception while adding file to database."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)

