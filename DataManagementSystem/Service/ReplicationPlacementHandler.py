""" ReplicationPlacementHandler is the service interface to the ReplicationPlacementDB.
"""

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gMonitor, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.DB.ReplicationPlacementDB import ReplicationPlacementDB
from DIRAC.Core.Transformation.TransformationHandler import TransformationHandler

# This is a global instance of the ReplicationPlacementDB class
placementDB = False

def initializeReplicationPlacementHandler(serviceInfo):
  global placementDB
  placementDB = ReplicationPlacementDB()
  return S_OK()

class ReplicationPlacementHandler(TransformationHandler):

  def __init__(self,*args,**kargs):

    self.setDatabase(placementDB)
    TransformationHandler.__init__(self,*args,**kargs)

  types_publishTransformation = [ StringType, StringType, StringType, StringType, IntType, BooleanType, DictType, StringType, StringType, StringType ]
  def export_publishTransformation( self,transName,description,longDescription,fileMask='',groupsize=0,update=False,bkQuery = {},plugin='',transGroup='',transType=''):
    """ Publish new transformation in the TransformationDB
    """
    authorDN = self._clientTransport.peerCredentials['DN']
    authorGroup = self._clientTransport.peerCredentials['group']
    try:
      res = placementDB.addTransformation(transName,description,longDescription,authorDN,authorGroup,transType,plugin,'ReplicationPlacementAgent',fileMask,bkQuery,transGroup)
      if res['OK']:
        message = 'Transformation created'
        res = self.database.updateTransformationLogging(transName,message,authorDN)
      return res
    except Exception,x:
      errStr = "ReplicationPlacementHandler.publishTransformation: Exception while adding transformation."
      gLogger.exception(errStr,lException=x)
      return S_ERROR(errStr)
