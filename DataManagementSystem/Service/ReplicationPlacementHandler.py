""" ReplicationPlacementHandler is the service interface to the ReplicationPlacementDB.
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gMonitor, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.ReplicationPlacementDB import ReplicationPlacementDB
from DIRAC.Core.Transformation.TransformationHandler import TransformationHandler

# This is a global instance of the ReplicationPlacementDB class
ReplicationPlacementDB = False

def initializeReplicationPlacementHandler(serviceInfo):
  global ReplicationPlacementDB
  ReplicationPlacementDB = ReplicationPlacementDB()
  return S_OK()

class ReplicationPlacementHandler(TransformationHandler):

  def __init__(self,*args,**kargs):

    self.setDatabase(ReplicationPlacementDB)
    TransformationHandler.__init__(*args,**kargs)