# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/DISETSubRequest.py,v 1.4 2008/08/01 17:20:55 acsmith Exp $
"""
   DISETSubRequest Class encapsulates a request definition to accomplish a DISET
   RPC call

"""

__RCSID__ = "$Id: DISETSubRequest.py,v 1.4 2008/08/01 17:20:55 acsmith Exp $"

import commands, datetime
from DIRAC.Core.Utilities import DEncode

class DISETSubRequest:

  #############################################################################

  def __init__(self,rpcStub= None):
    """Instantiates the Workflow object and some default parameters.
    """
    self.subAttributeNames = ['Status','SubRequestID','Operation','CreationTime','LastUpdate','Arguments']
    self.subAttributes = {}

    for attr in self.subAttributeNames:
      self.subAttributes[attr] = "Unknown"

    # Some initial values
    self.subAttributes['Status'] = "Waiting"
    status,self.subAttributes['SubRequestID'] = commands.getstatusoutput('uuidgen')
    self.subAttributes['CreationTime'] = str(datetime.datetime.utcnow())

    if rpcStub:
      self.subAttributes['Arguments'] = DEncode.encode(rpcStub)

  def setRPCStub(self,rpcStub):
    """ Define the  RPC call details
    """
    self.subAttributes['Operation'] = rpcStub[0]
    self.subAttributes['Arguments'] = DEncode.encode(rpcStub)

  def getDictionary(self):
    """ Get the request representation as a dictionary
    """
    resultDict = {}
    resultDict['Attributes'] = self.subAttributes
    return resultDict
