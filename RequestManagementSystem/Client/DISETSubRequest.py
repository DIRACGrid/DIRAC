# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/RequestManagementSystem/Client/DISETSubRequest.py,v 1.7 2008/08/25 08:35:47 atsareg Exp $
"""
   DISETSubRequest Class encapsulates a request definition to accomplish a DISET
   RPC call

"""

__RCSID__ = "$Id: DISETSubRequest.py,v 1.7 2008/08/25 08:35:47 atsareg Exp $"

import commands
from DIRAC.Core.Utilities import DEncode
from DIRAC import Time
from DIRAC.Core.Utilities.File import makeGuid

class DISETSubRequest:

  #############################################################################

  def __init__(self,rpcStub= None,executionOrder=0):
    """Instantiates the Workflow object and some default parameters.
    """
    self.subAttributeNames = ['Status','SubRequestID','Operation','ExecutionOrder','CreationTime','LastUpdate','Arguments']
    self.subAttributes = {}

    for attr in self.subAttributeNames:
      self.subAttributes[attr] = "Unknown"

    # Some initial values
    self.subAttributes['Status'] = "Waiting"
    self.subAttributes['SubRequestID'] = makeGuid()
    self.subAttributes['CreationTime'] = Time.toString()
    self.subAttributes['ExecutionOrder'] = executionOrder

    if rpcStub:
      self.subAttributes['Arguments'] = DEncode.encode(rpcStub)
      self.subAttributes['Operation'] = rpcStub[1]

  def setRPCStub(self,rpcStub):
    """ Define the  RPC call details
    """
    self.subAttributes['Operation'] = rpcStub[1]
    self.subAttributes['Arguments'] = DEncode.encode(rpcStub)

  def getDictionary(self):
    """ Get the request representation as a dictionary
    """
    resultDict = {}
    resultDict['Attributes'] = self.subAttributes
    return resultDict
