""" DISET request handler base class for the TransformationDB.
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB

class TransformationHandler(RequestHandler):

  def setDatabase(self,oDatabase):
    self.database = oDatabase

  types_getName = []
  def export_getName(self):
    res = self.database.getName()
    return res

  types_removeTransformation = [[LongType, IntType, StringType]]
  def export_removeTransformation(self,transformationName):
    res = self.database.removeTransformation(transformationName)
    authorDN = self._clientTransport.peerCredentials['DN']
    if res['OK']:
      message = 'Removed'
      res = self.database.updateTransformationLogging(transformationName,message,authorDN)
    return res

  types_setTransformationStatus = [[LongType, IntType, StringType],StringType]
  def export_setTransformationStatus(self,transformationName,status):
    res = self.database.setTransformationStatus(transformationName,status)
    authorDN = self._clientTransport.peerCredentials['DN']
    if res['OK']:
      message = "Status changed to %s" % status
      res = self.database.updateTransformationLogging(transformationName,message,authorDN)
    return res
    
  types_setTransformationAgentType = [ [LongType, IntType, StringType], StringType ]
  def export_setTransformationAgentType( self, idOrName, status ):
    result = self.database.setTransformationAgentType(idOrName, status)
    if not result['OK']:
      gLogger.error(result['Message'])
    return result

  types_setTransformationMask = [[LongType, IntType, StringType],StringType]
  def export_setTransformationMask(self,transformationName,fileMask):
    res = self.database.setTransformationMask(transformationName,fileMask)
    authorDN = self._clientTransport.peerCredentials['DN']
    if res['OK']:
      message = "Mask changed to %s" % fileMask
      res = self.database.updateTransformationLogging(transformationName,message,authorDN)
    return res

  types_changeTransformationName = [[LongType, IntType, StringType],StringType]
  def export_changeTransformationName(self,transformationName,newName):
    res = self.database.changeTransformationName(transformationName,newName)
    authorDN = self._clientTransport.peerCredentials['DN']
    if res['OK']:
      message = "Transformation name changed to %s" % newName
      res = self.database.updateTransformationLogging(newName,message,authorDN)
    return res

  ############################################################################

  types_getTransformationStats = [[LongType, IntType, StringType]]
  def export_getTransformationStats(self,transformationName):
    res = self.database.getTransformationStats(transformationName)
    return res

  types_getTransformation = [[LongType, IntType, StringType]]
  def export_getTransformation(self,transformationName):
    res = self.database.getTransformation(transformationName)
    return res

  types_getAllTransformations = []
  def export_getAllTransformations(self):
    res = self.database.getAllTransformations()
    return res

  types_getFilesForTransformation = [[LongType, IntType, StringType]]
  def export_getFilesForTransformation(self,transformationName,orderByJobs=False):
    res = self.database.getFilesForTransformation(transformationName,orderByJobs)
    return res

  types_getInputData = [[LongType, IntType, StringType],StringType]
  def export_getInputData(self,transformationName,status):
    res = self.database.getInputData(transformationName,status)
    return res

  types_setFileSEForTransformation = [[LongType, IntType, StringType],StringType,ListType]
  def export_setFileSEForTransformation(self,transformationName,storageElement,lfns):
    res = self.database.setFileSEForTransformation(transformationName,storageElement,lfns)
    return res

  types_setFileStatusForTransformation = [[LongType, IntType, StringType],StringType,ListType]
  def export_setFileStatusForTransformation(self,transformationName,status,lfns):
    res = self.database.setFileStatusForTransformation(transformationName,status,lfns)
    return res

  types_setFileJobID = [[LongType, IntType, StringType],IntType,ListType]
  def export_setFileJobID(self,transformationName,jobID,lfns):
    res = self.database.setFileJobID(transformationName,jobID,lfns)
    return res

  ####################################################################
  #
  # These are the methods to file manipulation
  #

  types_addDirectory = [StringType]
  def export_addDirectory(self,path): #,force):
    res = self.database.addDirectory(path) #,force)
    return res

  types_exists = [ListType]
  def export_exists(self,lfns):
    res = self.database.exists(lfns)
    return res

  types_addFile = [ListType]
  def export_addFile(self,fileTuples,force):
    res = self.database.addFile(fileTuples,force)
    return res

  types_removeFile = [ListType]
  def export_removeFile(self,lfns):
    res = self.database.removeFile(lfns)
    return res

  types_addReplica = [ListType]
  def export_addReplica(self,replicaTuples,force):
    res = self.database.addReplica(replicaTuples,force)
    return res

  types_removeReplica = [ListType]
  def export_removeReplica(self,replicaTuples):
    res = self.database.removeReplica(replicaTuples)
    return res

  types_getReplicas = [ListType]
  def export_getReplicas(self,lfns):
    res = self.database.getReplicas(lfns)
    return res

  types_getReplicaStatus = [ListType]
  def export_getReplicaStatus(self,replicaTuples):
    res = self.database.getReplicaStatus(replicaTuples)
    return res

  types_setReplicaStatus = [ListType]
  def export_setReplicaStatus(self,replicaTuples):
    res = self.database.setReplicaStatus(replicaTuples)
    return res

  types_setReplicaHost = [ListType]
  def export_setReplicaHost(self,replicaTuples):
    res = self.database.setReplicaHost(replicaTuples)
    return res

