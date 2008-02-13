""" TransferDBMonitoringHandler is the implementation of the TransferDB monitoring service in the DISET framework
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.TransferDB import TransferDB

# These are global instances of the DB classes
transferDB = False

def initializeTransferDBMonitoringHandler(serviceInfo):

  global transferDB
  transferDB = TransferDB()
  return S_OK()

class TransferDBMonitoringHandler(RequestHandler):

  types_getFTSInfo = [IntType]
  def export_getFTSInfo(self,ftsReqID):
   """ Get the details of a particular FTS job
   """
   return transferDB.getFTSJobDetail(ftsReqID) 

  types_getFTSJobs = []
  def export_getFTSJobs(self):
    """ Get all the FTS jobs from the DB
    """
    return transferDB.getFTSJobs()

