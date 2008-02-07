""" Client for PlacementDB file catalog tables
"""

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.ConfigurationSystem.Client import PathFinder
import types

class PlacementDBClient:
  """ File catalog client for placement DB
  """

  def __init__(self, url=False, useCertificates=False):
    """ Constructor of the PlacementDB catalogue client
    """
    self.name = 'PlaceDB'
    self.valid = True
    try:
      if not url:
        self.server = RPCClient("DataManagement/PlacementDB",useCertificates,timeout=120)
      else:
        self.server = RPCClient(self.url,useCertificates,timeout = 120)
    except Exception,x:
      self.valid = False
 
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
      errStr = "PlacementDBClient.addReplica: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr) 
      return S_ERROR(errStr)
    return self.server.addReplica(tuples,force)

  def removeReplica(self,tuple):   
    """ Remove replica from database
    """
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "PlacementDBClient.removeReplica: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.removeReplica(tuples)

  def addFile(self,tuple,force=False):
    """ Add a file to the database
    """
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "PlacementDBClient.addFile: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)      
    return self.server.addFile(tuples,force)

  def removeFile(self,lfn):
    """ Remove file from the database
    """
    if type(lfn) in types.StringTypes:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      errStr = "PlacementDBClient.removeFile: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.removeFile(lfns)

  def getReplicas(self,lfn):
    """ Remove file from the database
    """
    if type(lfn) in types.StringTypes:
      lfns = [lfn]
    elif type(lfn) == types.ListType:
      lfns = lfn
    else:
      errStr = "PlacementDBClient.getReplicas: Supplied lfn must be string or list of strings."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.getReplicas(lfns)

  def getReplicaStatus(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "PlacementDBClient.getReplicasStatus: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.getReplicaStatus(tuples)

  def setReplicaStatus(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "PlacementDBClient.setReplicasStatus: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.setReplicaStatus(tuples)

  def setReplicaHost(self,tuple):
    if type(tuple) == types.TupleType:
      tuples = [tuple]
    elif type(tuple) == types.ListType:
      tuples = tuple
    else:
      errStr = "PlacementDBClient.setReplicaHost: Supplied file info must be tuple or list of tuples."
      gLogger.error(errStr)
      return S_ERROR(errStr)
    return self.server.setReplicaHost(tuples)              
