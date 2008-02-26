""" Client for PlacementDB file catalog tables
"""
from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.DISET.RPCClient import RPCClient
import types

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
        self.server = RPCClient("BookkeepingManagement/BookkeepingManager")
      else:
        self.server = RPCClient(url)
    except:
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
    for replicaTuple in replicas:
      lfn,pfn,se = replicaTuple
      resRep = self.__addReplica(lfn,pfn,se)
      if not resRep['OK']:
        result = S_ERROR('Failed to send replica data for '+lfn)

    return result