""" Base class for the TransformationDBClient for access file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
from DIRAC.Core.DISET.RPCClient import RPCClient
import types

class TransformationDBClient(FileCatalogueBase):
  """ Exposing the functionality of the replica tables for the TransformationDB
  """
  def setServer(self,url):
    self.server = url

  def isOK(self):
    return self.valid

  def getName(self,DN=''):
    """ Get the file catalog type name
    """
    return self.name

  def addReplica(self,tuple,force=False):
    """ Add a replica to the database
    """
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.addReplica: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.addReplica(tuples,force)

  def removeReplica(self,tuple):
    """ Remove replica from database
    """
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.removeReplica: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.removeReplica(tuples)

  def addFile(self,tuple,force=False):
    """ Add a file to the database
    """
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.addFile: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.addFile(tuples,force)

  def removeFile(self,lfn):
    """ Remove file from the database
    """
    if type(lfn) in types.StringTypes:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      errStr = "TransformationDBClient.removeFile: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.removeFile(lfns)

  def getReplicas(self,lfn):
    """ Remove file from the database
    """
    if type(lfn) in types.StringTypes:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      errStr = "TransformationDBClient.getReplicas: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.getReplicas(lfns)

  def getReplicaStatus(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.getReplicasStatus: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server)
    return server.getReplicaStatus(tuples)

  def setReplicaStatus(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.setReplicasStatus: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.setReplicaStatus(tuples)

  def setReplicaHost(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "TransformationDBClient.setReplicaHost: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    server = RPCClient(self.server,timeout=120)
    return server.setReplicaHost(tuples)
