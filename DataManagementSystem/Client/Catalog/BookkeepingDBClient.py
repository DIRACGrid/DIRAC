""" Client for PlacementDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.DataManagementSystem.Client.Catalog.FileCatalogueBase import FileCatalogueBase
import types, os

class BookkeepingDBClient(FileCatalogueBase):
  """ File catalog client for placement DB
  """
  def __init__(self, url=False):
    """ Constructor of the PlacementDB catalogue client
    """
    self.name = 'BookkeepingDB'
    self.valid = True
    try:
      if not url:
        self.server = RPCClient("Bookkeeping/BookkeepingManager")
      else:
        self.server = RPCClient(url)
    except Exception,x:
      print x
      self.valid = False

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
    #print "sending",fname
    result = self.server.sendBookkeeping(fname,repscript)

    return result

  def addFile(self,fileTuple):
    """ A tuple should be supplied to this method which contains:
        (lfn,pfn,size,se,guid)
        A list of tuples may also be supplied.
    """

    if type(fileTuple) == types.TupleType:
      files = [fileTuple]
    else:
      files = fileTuple
    replicaTupleList = []
    for fileTuple in files:
      lfn,pfn,size,se,guid = fileTuple
      replicaTupleList.append((lfn,pfn,se))

    return self.addReplica(replicaTupleList)

  def addReplica(self, replicaTuple):
    """ Add replica info
    """

    result = S_OK()

    if type(replicaTuple) == types.TupleType:
      replicas = [replicaTuple]
    else:
      replicas = replicaTuple

    failed_lfns = []

    for replicaTuple in replicas:
      lfn,pfn,se = replicaTuple
      resRep = self.__addReplica(lfn,pfn,se)
      if not resRep['OK']:
        failed_lfns.append((lfn,se))

    if failed_lfns:
      result = S_ERROR('Failed to add all replicas')
      result['FailedLFNs'] = failed_lfns
    else:
      return S_OK()

  def removeFile(self,lfns):
    """Remove the LFN record from the BK Catalog
    """

    if type(lfns) == types.StringType:
      lfnList = [lfns]
    else:
      lfnList = lfns

    result = S_OK()
    failed_lfns = []

    for lfn in lfnList:

      repscript = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
  <Replica File="%s"
           Name=""
           Location="anywhere"
           Action="Delete"  />
</Replicas>
""" % (lfn)
      #print repscript
      fname = os.path.basename(lfn)+".R.xml"
      #print "sending",fname
      resRem = self.server.sendBookkeeping(fname,repscript)
      if not resRem['OK']:
        failed_lfns.append(lfn)

    if failed_lfns:
      result = S_ERROR('Failed to remove all files')
      result['FailedLFNs'] = failed_lfns
    else:
      return S_OK()


  def removeReplica(self,replicas):
    """ Remove replica info from bookkeeping
    """

    if type(replicas) == types.StringType:
      replicaList = [replicas]
    else:
      replicaList = replicas

    result = S_OK()
    failed_lfns = []

    for lfn,se in replicaList:
      repscript = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
  <Replica File="%s"
           Name="%s"
           Location="%s"
           SE="%s"
           Action="Delete"  />
</Replicas>
""" % (lfn,'ANY',se,se)
      #print repscript
      fname = os.path.basename(lfn)+".R.xml"
      #print "sending",fname
      resRem = self.server.sendBookkeeping(fname,repscript)
      if not resRem['OK']:
        failed_lfns.append((lfn,se))

    if failed_lfns:
      result = S_ERROR('Failed to remove all replicas')
      result['FailedLFNs'] = failed_lfns
    else:
      return S_OK()
