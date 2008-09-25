# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/DataManagementSystem/Client/Catalog/Attic/BookkeepingDBOldClient.py,v 1.4 2008/09/25 16:09:59 acsmith Exp $

""" Client for the BookkeeppingDB file catalog old XML based service
"""

__RCSID__ = "$Id: BookkeepingDBOldClient.py,v 1.4 2008/09/25 16:09:59 acsmith Exp $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
import types, os
from DIRAC.ConfigurationSystem.Client import PathFinder

class BookkeepingDBOldClient(FileCatalogueBase):
  """ File catalog client for placement DB
  """
  def __init__(self, url=False):
    """ Constructor of the BookkeepingDB catalog client
    """
    self.name = 'BookkeepingDBOld'
    self.valid = True
    try:
      if not url:
        self.url = PathFinder.getServiceURL('Bookkeeping/BookkeepingManagerOld')
      else:
        self.url = url
    except Exception,x:
      print x
      self.valid = False

  def isOK(self):
    return self.valid


  def addFile(self,fileTuple):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid,checksum)
        A list of tuples may also be supplied.
    """
    successful = {}
    failed = {}
    if type(fileTuple) == types.TupleType:
      files = [fileTuple]
    elif type(fileTuple) == types.ListType:
      files = fileTuple
    else:
      return S_ERROR('BookkeepingDBClient.addFile: Must supply a file tuple of list of tuples')
    replicaTupleList = []
    for lfn,pfn,size,se,guid,checksum in files:
      replicaTupleList.append((lfn,pfn,se,'IGNORE'))
    return self.addReplica(replicaTupleList)

  def addReplica(self,replicaTuple):
    """ This adds a replica to the catalogue
        The tuple to be supplied is of the following form:
          (lfn,pfn,se,master)
        where master = True or False
    """
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('BookkeepingDBOldClient.addReplica: Must supply a replica tuple of list of tuples')
    successful = {}
    failed = {}
    for lfn,pfn,se,master in replicas:
      res = self.__addReplica(lfn,pfn,se)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def __addReplica(self,lfn,pfn,se):
    """ Add replica info to the Bookkeeping database"
    """
    repscript = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
  <Replica File="%s"
           Name="%s"
           Location="%s"
           SE="%s" />
</Replicas>
""" % (lfn,pfn,se,se)
    fname = os.path.basename(lfn)+".A.xml"
    server = RPCClient(self.url)
    result = server.sendBookkeeping(fname,repscript)
    return result

  def removeFile(self,path):
    if type(path) == types.StringType:
      lfns = [path]
    elif type(path) == types.ListType:
       lfns = path
    else:
      return S_ERROR('BookkeepingDBOldClient.removeFile: Must supply a path or list of paths')
    successful = {}
    failed = {}
    server = RPCClient(self.url)
    for lfn in lfns:
      repscript = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
  <Replica File="%s"
           Name=""
           Location="anywhere"
           Action="Delete"  />
</Replicas>
""" % (lfn)
      fname = os.path.basename(lfn)+".R.xml"
      res = server.sendBookkeeping(fname,repscript)
      if not res['OK']:
        failed[lfn] = res['Message']
      else:
        successful[lfn] = True
    resDict = {'Successful':successful,'Failed':failed}
    return S_OK(resDict)

  def removeReplica(self,replicaTuple):
    """ Remove replica info from bookkeeping (this is a dummy method as the replica flag should only be unset when there are no replicas)
    """
    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    elif type(replicaTuple) == types.ListType:
      replicas = replicaTuple
    else:
      return S_ERROR('BookkeepingDBOldClient.removeReplica: Must supply a file tuple or list of file typles')
    successful = {}
    for lfn,pfn,se in replicas:
      successful[lfn] = True
    resDict = {'Failed':{},'Successful':successful}
    return S_OK(resDict)
