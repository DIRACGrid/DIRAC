""" DISET request handler base class for the TransformationDB.
"""
from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB

class TransformationDBHandler(RequestHandler):

  def setDatabase(self,oDatabase):
    self.database = oDatabase

  types_addDirectory = [StringType]
  def export_addDirectory(self,path,force):
    res = self.database.addDirectory(path,force)
    return res

  types_addTransformation = [StringType,StringType,IntType]
  def export_addTransformation(self,transformationName,fileMask,groupSize):
    res = self.database.addTransformation(transformationName,fileMask,groupSize)
    return res

  types_removeTransformation = [StringType]
  def export_removeTransformation(self,transformationName):
    res = self.database.removeTransformation(transformationName)
    return res

  types_setTransformationStatus = [StringType,StringType]
  def export_setTransformationStatus(self,transformationName,status):
    res = self.database.setTransformationStatus(transformationName,status)
    return res


  types_getTransformationStats = [StringType]
  def export_getTransformationStats(self,transformationName):
    res = self.database.setTransformationStatus(transformationName)
    return res

  types_getTransformation = [StringType]
  def export_getTransformation(self,transformationName):
    res = self.database.getTransformation(transformationName)
    return res

  types_setTransformationMask = [StringType,StringType]
  def export_setTransformationMask(self,transformationName,fileMask):
    res = self.database.setTransformationMask(transformationName,fileMask)
    return res

  types_changeTransformationName = [StringType,StringType]
  def export_changeTransformationName(self,transformationName,newName):
    res = self.database.changeTransformationName(transformationName,newName)
    return res

  types_getAllTransformations = []
  def export_getAllTransformations(self):
    res = self.database.getAllTransformations()
    return res

  types_getFilesForTransformation = [StringType]
  def export_getFilesForTransformation(self,transformationName,orderByJobs):
    res = self.database.getFilesForTransformation(transformationName,orderByJobs)
    return res

  types_getInputData = [StringType]
  def export_getInputData(self,transformationName,status):
    res = self.database.getInputData(transformationName,status)
    return res

  types_setFileSEForTransformation = [StringType,StringType,ListType]
  def export_setFileSEForTransformation(self,transformationName,storageElement,lfns):
    res = self.database.setFileSEForTransformation(transformationName,storageElement,lfns)
    return res

  types_setFileStatusForTransformation = [StringType,StringType,ListType]
  def export_setFileStatusForTransformation(self,transformationName,status,lfns):
    res = self.database.setFileStatusForTransformation(transformationName,status,lfns)
    return res

  types_setFileStatus = [StringType,StringType,StringType]
  def export_setFileStatus(self,transformationName,lfn,status):
    res = self.database.setFileStatus(transformationName,lfn,status)
    return res

  types_setFileJobID = [StringType,IntType,ListType]
  def export_setFileJobID(self,transformationName,jobID,lfns):
    res = self.database.setFileJobID(transformationName,jobID,lfns)
    return res

  ####################################################################
  #
  # These are the methods to file manipulation
  #

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

  types_exists = [ListType]
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

  types_getName = []
  def export_getName(self):
    res = self.database.getName()
    return res
