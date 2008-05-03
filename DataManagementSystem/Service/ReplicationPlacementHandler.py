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

  types_publishTransformation = []
  def export_publishTransformation(self, transName, desciption,longDesription, type, plugin, fileMask):
    """ Publish new transformation in the TransformationDB
    """
    authorDN = self._clientTransport.peerCredentials['DN']
    #authorName = self._clientTransport.peerCredentials['user']
    authorGroup = self._clientTransport.peerCredentials['group']
    try:
      res = placementDB.addTransformation(transName,desciption,longDesription,authorDN,authorGroup,type,plugin,'ReplicationPlacementAgent',fileMask)
      if res['OK']:
        message = 'Transformation created'
        res = self.database.updateTransformationLogging(transName,message,authorDN)
      return res
    except Exception,x:
      errStr = "ReplicationPlacementHandler.publishTransformation: Exception while adding transformation."
      gLogger.exception(errStr,str(x))
      return S_ERROR(errStr)

  types_addTransformationParameters = []
  def export_addTransformationParameters(self,transNameOrID,parameterDict):
    authorDN = self._clientTransport.peerCredentials['DN']
    for paramName,paramValue in parameterDict.items():
      result = self.database.addTransformationParameter(transNameOrID,paramName,paramValue)
      if result['OK']:
        message = 'Added parameter %s' % paramName
        result = self.database.updateTransformationLogging(transNameOrID,message,authorDN)
    return result
